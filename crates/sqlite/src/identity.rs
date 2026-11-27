//! Which file each connection actually opened, read from SQLite's own descriptor.
//!
//! A path can be stat'ed before and after an open, but a file swapped in between the open and
//! the second stat passes both checks. Only the descriptor SQLite holds says which file it
//! opened. SQLite has no file control that hands it out on Unix, so it is read from the
//! `unixFile` behind the reader's `sqlite3_file`: its fourth field, `int h`, after three
//! pointers (`pMethod`, `pVfs`, `pInode`; `sqlite3.c`, `struct unixFile`). That layout holds for
//! every VFS in SQLite's unix family and for no other, so the VFS is checked by name first
//! (`SQLITE_FCNTL_VFS_POINTER`), and any other VFS answers "cannot tell".
//!
//! The descriptor is `fstat`ed through a `File` that is never dropped: closing any descriptor on
//! the database drops every POSIX lock the process holds on it (see the crate doc).

use crate::conn::DatabaseFile;

#[cfg(unix)]
mod unix {
    use std::ffi::{c_int, c_void};
    use std::fs::File;
    use std::mem::ManuallyDrop;
    use std::os::fd::{FromRawFd, RawFd};
    use std::ptr::NonNull;

    use crate::capi::{Handle, MAIN};
    use crate::conn::DatabaseFile;
    use crate::ffi;
    use crate::pages::file_pointer;

    /// The head of SQLite's `struct unixFile`, up to the descriptor.
    #[repr(C)]
    struct UnixFileHead {
        methods: *const c_void,
        vfs: *mut c_void,
        inode: *mut c_void,
        h: c_int,
    }

    /// Whether `name` is one of SQLite's own unix VFSes, all of which open a `unixFile`.
    pub(super) fn is_unix_family(name: &str) -> bool {
        name == "unix" || name.starts_with("unix-")
    }

    #[expect(
        unsafe_code,
        reason = "the descriptor is a field of SQLite's unixFile, read through a raw pointer"
    )]
    fn descriptor(file: NonNull<ffi::sqlite3_file>) -> Option<RawFd> {
        // SAFETY: the caller checked the VFS is in the unix family, whose `sqlite3_file` is a
        // live `unixFile` beginning with three pointers and the descriptor.
        let h = unsafe { (*file.as_ptr().cast::<UnixFileHead>()).h };
        (h >= 0).then_some(h)
    }

    /// The file `fd` names, by `fstat`; the descriptor stays open.
    #[expect(
        unsafe_code,
        reason = "a borrowed descriptor wrapped in a File that is never dropped"
    )]
    fn stat(fd: RawFd) -> Option<DatabaseFile> {
        // SAFETY: `fd` is open for the reader's whole life, and `ManuallyDrop` keeps the `File`
        // from closing it, which would drop the process's POSIX locks on the database.
        let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
        file.metadata()
            .ok()
            .map(|meta| DatabaseFile::of_metadata(&meta))
    }

    pub(super) fn opened(conn: Handle<'_>) -> Option<DatabaseFile> {
        if !is_unix_family(&conn.vfs_name()?) {
            return None;
        }
        let file = file_pointer(conn, MAIN).ok()?;
        stat(descriptor(file)?)
    }
}

/// The file `conn` holds open as its main database; `None` when the VFS is not SQLite's unix
/// VFS or the platform is not Unix, where it cannot be told.
#[cfg(unix)]
pub(crate) fn opened(conn: crate::capi::Handle<'_>) -> Option<DatabaseFile> {
    unix::opened(conn)
}

#[cfg(not(unix))]
pub(crate) const fn opened(_conn: crate::capi::Handle<'_>) -> Option<DatabaseFile> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::unix::is_unix_family;

    #[test]
    fn only_sqlites_unix_vfses_are_read_as_unix_files() {
        for name in [
            "unix",
            "unix-none",
            "unix-excl",
            "unix-dotfile",
            "unix-flock",
        ] {
            assert!(is_unix_family(name), "{name}");
        }
        for name in ["lsr-fault", "memdb", "unixish", "win32"] {
            assert!(!is_unix_family(name), "{name}");
        }
    }
}
