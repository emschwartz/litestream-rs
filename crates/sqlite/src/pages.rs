//! The reader's own `sqlite3_file`, and database pages read through it.
//!
//! `SQLITE_FCNTL_FILE_POINTER` hands out the pager's file handle (`sqlite3.c:192592`). Its methods
//! are copied out of the VFS's `sqlite3_io_methods` once, field by field through raw pointers
//! (never a reference to the whole table, so a shorter version 1 table is not read past its end):
//! `xRead` and `xFileSize` for pages, `xShmMap` for the wal-index. The handle lives as long as the
//! reader connection, which [`crate::Sqlite`] owns, so the copies stay valid by construction.
//!
//! Reading pages this way opens no descriptor: the unix VFS's `xRead` is a `pread` on the
//! descriptor SQLite already holds. A `std::fs` descriptor would have to be closed one day, and
//! closing it drops every POSIX lock the process holds on the database.

use std::ffi::{CStr, c_int, c_void};
use std::io;
use std::mem::offset_of;
use std::ptr::{self, NonNull};

use lsr_core::{PageSource, SourceError};
use lsr_ltx::{DbPages, PageNo};
use rusqlite::{Connection, ffi};

use crate::Sqlite;
use crate::error::{Refusal, rc_error};

type XRead = unsafe extern "C" fn(*mut ffi::sqlite3_file, *mut c_void, c_int, i64) -> c_int;
type XFileSize = unsafe extern "C" fn(*mut ffi::sqlite3_file, *mut i64) -> c_int;
/// `xShmMap(file, region, size, extend, &out)`.
pub(crate) type XShmMap =
    unsafe extern "C" fn(*mut ffi::sqlite3_file, c_int, c_int, c_int, *mut *mut c_void) -> c_int;

/// A connection's main database file and the three methods this crate calls on it.
#[derive(Debug)]
pub(crate) struct FileHandle {
    pub(crate) file: NonNull<ffi::sqlite3_file>,
    read: XRead,
    file_size: XFileSize,
    pub(crate) shm_map: XShmMap,
}

/// The raw handle, for SQLite calls made while `conn` is borrowed.
#[expect(
    unsafe_code,
    reason = "rusqlite gives the raw handle only through an unsafe fn"
)]
pub(crate) fn handle(conn: &Connection) -> *mut ffi::sqlite3 {
    // SAFETY: the pointer is used only for calls made while `conn` is borrowed, and it is never closed.
    unsafe { conn.handle() }
}

/// The pager's `sqlite3_file` for `schema`.
#[expect(
    unsafe_code,
    reason = "SQLITE_FCNTL_FILE_POINTER has no safe wrapper in rusqlite"
)]
pub(crate) fn file_pointer(
    conn: &Connection,
    schema: &CStr,
) -> Result<NonNull<ffi::sqlite3_file>, Refusal> {
    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    let db = handle(conn);
    // SAFETY: a live handle, a NUL-terminated name, and FILE_POINTER writes exactly one
    // `sqlite3_file*` through its argument (`sqlite3.c:192592`).
    let rc = unsafe {
        ffi::sqlite3_file_control(
            db,
            schema.as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            (&raw mut file).cast::<c_void>(),
        )
    };
    if rc != ffi::SQLITE_OK {
        return Err(Refusal::NoFilePointer { rc });
    }
    NonNull::new(file).ok_or(Refusal::FileNotOpen)
}

impl FileHandle {
    /// The methods of `file`, refused when the table predates shared memory or lacks one.
    #[expect(
        unsafe_code,
        reason = "the io_methods table is C memory read field by field through raw pointers"
    )]
    pub(crate) fn new(file: NonNull<ffi::sqlite3_file>) -> Result<Self, Refusal> {
        // SAFETY: `file` points to a live `sqlite3_file`, whose first field is `pMethods`.
        let methods = unsafe { (*file.as_ptr()).pMethods };
        if methods.is_null() {
            return Err(Refusal::FileNotOpen);
        }
        let at = |offset: usize| methods.wrapping_byte_add(offset);
        // SAFETY: every io_methods table starts with `iVersion`.
        let version = unsafe {
            at(offset_of!(ffi::sqlite3_io_methods, iVersion))
                .cast::<c_int>()
                .read()
        };
        if version < 2 {
            return Err(Refusal::OldIoMethods { version });
        }
        // SAFETY: `xRead` is a version 1 field, present in every table.
        let read = unsafe {
            at(offset_of!(ffi::sqlite3_io_methods, xRead))
                .cast::<Option<XRead>>()
                .read()
        };
        // SAFETY: `xFileSize` is a version 1 field, present in every table.
        let file_size = unsafe {
            at(offset_of!(ffi::sqlite3_io_methods, xFileSize))
                .cast::<Option<XFileSize>>()
                .read()
        };
        // SAFETY: version 2 and later tables include `xShmMap` (`sqlite3.h`, `sqlite3_io_methods`).
        let shm_map = unsafe {
            at(offset_of!(ffi::sqlite3_io_methods, xShmMap))
                .cast::<Option<XShmMap>>()
                .read()
        };
        Ok(Self {
            file,
            read: read.ok_or(Refusal::MissingMethod { name: "xRead" })?,
            file_size: file_size.ok_or(Refusal::MissingMethod { name: "xFileSize" })?,
            shm_map: shm_map.ok_or(Refusal::MissingMethod { name: "xShmMap" })?,
        })
    }

    /// Fills `buf` from `offset`; SQLite's result code.
    #[expect(unsafe_code, reason = "a call through the VFS's xRead")]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<(), c_int> {
        let amount = c_int::try_from(buf.len()).map_err(|_big| ffi::SQLITE_RANGE)?;
        let offset = i64::try_from(offset).map_err(|_big| ffi::SQLITE_RANGE)?;
        // SAFETY: the method belongs to `file`, which is live; `buf` is valid for `amount`
        // bytes of writes, and xRead writes no more than that (zero-filling a short read).
        let rc = unsafe {
            (self.read)(
                self.file.as_ptr(),
                buf.as_mut_ptr().cast::<c_void>(),
                amount,
                offset,
            )
        };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(rc)
        }
    }

    /// The file's length in bytes.
    #[expect(unsafe_code, reason = "a call through the VFS's xFileSize")]
    fn size(&self) -> Result<u64, c_int> {
        let mut size: i64 = 0;
        // SAFETY: the method belongs to `file`, which is live, and writes one `i64`.
        let rc = unsafe { (self.file_size)(self.file.as_ptr(), &raw mut size) };
        if rc != ffi::SQLITE_OK {
            return Err(rc);
        }
        u64::try_from(size).map_err(|_neg| ffi::SQLITE_IOERR_FSTAT)
    }
}

/// The byte offset of `pgno`, `(pgno - 1) * page_size`, checked.
fn page_offset(pgno: PageNo, page_size: u32) -> Option<u64> {
    u64::from(pgno.get())
        .checked_sub(1)?
        .checked_mul(u64::from(page_size))
}

impl PageSource for Sqlite {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        let mut inner = self.lock();
        inner.ensure_pinned()?;
        let len = inner.file().size().map_err(rc_error)?;
        let pages = len
            .checked_div(u64::from(inner.page_size().get()))
            .unwrap_or(0);
        u32::try_from(pages)
            .map(DbPages::new)
            .map_err(|_big| SourceError::Io(io::ErrorKind::InvalidData))
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        let mut inner = self.lock();
        inner.ensure_pinned()?;
        let size = inner.page_size().get();
        if u32::try_from(buf.len()) != Ok(size) {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        let offset = page_offset(pgno, size).ok_or(SourceError::Io(io::ErrorKind::InvalidInput))?;
        inner.file().read_at(buf, offset).map_err(|rc| {
            if rc == ffi::SQLITE_IOERR_SHORT_READ {
                SourceError::Short { pgno }
            } else {
                rc_error(rc)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_table_or_method_is_refused_before_any_call() {
        let mut no_methods = ffi::sqlite3_file {
            pMethods: ptr::null(),
        };
        assert_eq!(
            FileHandle::new(NonNull::from(&mut no_methods)).err(),
            Some(Refusal::FileNotOpen)
        );
        #[expect(
            unsafe_code,
            reason = "a synthetic io_methods table for the refusal test"
        )]
        // SAFETY: every field is an integer or an `Option` of a function pointer; zero is valid.
        let mut methods: ffi::sqlite3_io_methods = unsafe { std::mem::zeroed() };
        for version in [0, 1] {
            methods.iVersion = version;
            let mut file = ffi::sqlite3_file {
                pMethods: &raw const methods,
            };
            assert_eq!(
                FileHandle::new(NonNull::from(&mut file)).err(),
                Some(Refusal::OldIoMethods { version })
            );
        }
        methods.iVersion = 3;
        let mut file = ffi::sqlite3_file {
            pMethods: &raw const methods,
        };
        assert_eq!(
            FileHandle::new(NonNull::from(&mut file)).err(),
            Some(Refusal::MissingMethod { name: "xRead" })
        );
    }

    #[test]
    fn an_unknown_schema_has_no_file_pointer() {
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            file_pointer(&conn, c"nope").err(),
            Some(Refusal::NoFilePointer {
                rc: ffi::SQLITE_ERROR
            })
        );
    }

    #[test]
    fn page_offsets_are_checked() {
        let size = lsr_ltx::PageSize::new(4096).unwrap();
        assert_eq!(page_offset(PageNo::new(1, size).unwrap(), 4096), Some(0));
        assert_eq!(page_offset(PageNo::new(3, size).unwrap(), 4096), Some(8192));
        let big = lsr_ltx::PageSize::new(65536).unwrap();
        assert_eq!(
            page_offset(PageNo::new(u32::MAX, big).unwrap(), 65536),
            Some(u64::from(u32::MAX - 1) * 65536)
        );
    }
}
