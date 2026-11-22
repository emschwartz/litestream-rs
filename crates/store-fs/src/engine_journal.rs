//! The engine journal's file on a local filesystem (`docs/journal.md`): `<meta>/lsr-journal`.
//!
//! Every call is the one system call [`JournalFile`] names, with nothing added: no retry, no
//! buffering, no rename, no unlink. Reads and writes are positioned (`pread`, `pwrite`) and
//! loop until done or an error. The writer in `lsr-core` decides the order; this only performs
//! it. The engine's exclusive lock is a separate file (`lsr-lock`), so this file takes none.

use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use lsr_core::JournalFile;
use lsr_journal::{SLOT, Slot};

/// The journal's name in the meta directory.
pub const JOURNAL_NAME: &str = "lsr-journal";

/// The journal file at `<meta>/lsr-journal`.
#[derive(Debug)]
pub struct FsJournalFile {
    dir: PathBuf,
    path: PathBuf,
    file: Option<File>,
}

impl FsJournalFile {
    /// The journal in the meta directory `dir`, not yet opened.
    #[must_use]
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            path: dir.join(JOURNAL_NAME),
            file: None,
        }
    }

    /// The open handle, opening the existing file for a writer whose boot `open` failed.
    fn handle(&mut self) -> Result<&File, ErrorKind> {
        if self.file.is_none() {
            self.open()?;
        }
        self.file.as_ref().ok_or(ErrorKind::NotFound)
    }
}

impl JournalFile for FsJournalFile {
    fn open(&mut self) -> Result<(), ErrorKind> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|err| err.kind())?;
        self.file = Some(file);
        Ok(())
    }

    fn read_slot(&mut self, slot: Slot) -> Result<Option<[u8; SLOT]>, ErrorKind> {
        let mut bytes = [0; SLOT];
        match self.handle()?.read_exact_at(&mut bytes, slot.offset()) {
            Ok(()) => Ok(Some(bytes)),
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(err) => Err(err.kind()),
        }
    }

    fn create(&mut self) -> Result<(), ErrorKind> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
            .map_err(|err| err.kind())?;
        self.file = Some(file);
        Ok(())
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ErrorKind> {
        self.handle()?
            .write_all_at(bytes, offset)
            .map_err(|err| err.kind())
    }

    fn sync(&mut self) -> Result<(), ErrorKind> {
        self.handle()?.sync_data().map_err(|err| err.kind())
    }

    fn sync_dir(&mut self) -> Result<(), ErrorKind> {
        File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|err| err.kind())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use lsr_core::JournalWriter;
    use lsr_journal::{FILE, Journal, Seq, Unproven, Verdict};

    use super::*;

    fn debt(unproven: Unproven) -> Journal {
        Journal {
            unproven,
            ..Journal::ABSENT
        }
    }

    #[test]
    fn a_journal_is_created_on_the_first_write_and_read_at_the_next_boot() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = FsJournalFile::new(dir.path());
        let (mut writer, parsed) = JournalWriter::open(&mut file);
        assert_eq!(parsed.verdict, Verdict::Missing);
        writer.write(&mut file, &debt(Unproven::Truncate)).unwrap();
        let (_, parsed) = JournalWriter::open(&mut FsJournalFile::new(dir.path()));
        let Verdict::Read { seq, journal, .. } = parsed.verdict else {
            panic!("{parsed:?}");
        };
        assert_eq!((seq, journal), (Seq::FIRST, debt(Unproven::Truncate)));
    }

    #[test]
    fn records_alternate_across_boots_and_the_file_never_grows_past_8192_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let steps = [
            Unproven::Truncate,
            Unproven::Snapshot,
            Unproven::No,
            Unproven::Truncate,
        ];
        for (at, unproven) in steps.into_iter().enumerate() {
            let mut file = FsJournalFile::new(dir.path());
            let (mut writer, _) = JournalWriter::open(&mut file);
            writer.write(&mut file, &debt(unproven)).unwrap();
            let (_, parsed) = JournalWriter::open(&mut FsJournalFile::new(dir.path()));
            let Verdict::Read { seq, journal, .. } = parsed.verdict else {
                panic!("{parsed:?}");
            };
            assert_eq!(seq.get(), u64::try_from(at).unwrap() + 1);
            assert_eq!(journal, debt(unproven));
            let len = fs::metadata(dir.path().join(JOURNAL_NAME)).unwrap().len();
            assert_eq!(len, u64::try_from(FILE).unwrap());
        }
    }

    #[test]
    fn a_short_file_reads_its_short_slot_as_short_and_is_finished_before_record_2() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = FsJournalFile::new(dir.path());
        let (mut writer, _) = JournalWriter::open(&mut file);
        writer.write(&mut file, &debt(Unproven::No)).unwrap();
        let path = dir.path().join(JOURNAL_NAME);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(4096).unwrap();
        let mut file = FsJournalFile::new(dir.path());
        let (mut writer, parsed) = JournalWriter::open(&mut file);
        assert!(matches!(parsed.verdict, Verdict::Read { seq, .. } if seq == Seq::FIRST));
        writer.write(&mut file, &debt(Unproven::Truncate)).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            u64::try_from(FILE).unwrap()
        );
    }

    #[test]
    fn a_file_with_no_record_is_unreadable_and_rewritten_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(JOURNAL_NAME);
        fs::write(&path, [0xee; FILE]).unwrap();
        let inode = fs::metadata(&path).unwrap();
        let mut file = FsJournalFile::new(dir.path());
        let (mut writer, parsed) = JournalWriter::open(&mut file);
        assert!(
            matches!(parsed.verdict, Verdict::Unreadable(_)),
            "{parsed:?}"
        );
        writer.write(&mut file, &debt(Unproven::No)).unwrap();
        let after = fs::metadata(&path).unwrap();
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            inode.ino(),
            after.ino(),
            "the journal was replaced, not rewritten"
        );
        let (_, parsed) = JournalWriter::open(&mut FsJournalFile::new(dir.path()));
        assert!(matches!(parsed.verdict, Verdict::Read { seq, .. } if seq == Seq::FIRST));
    }
}
