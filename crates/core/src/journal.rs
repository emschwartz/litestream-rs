//! The engine journal's writer, and the door to its file (`docs/journal.md`).
//!
//! The record and its codec are `lsr-journal`'s; this module decides which bytes go where and
//! when to sync, through [`JournalFile`], so a test can drive every system call. The protocol is
//! the one `lsr_spec::journal` checks exhaustively, and the differential test runs this writer
//! against that model step for step:
//!
//! - **Establish** (no file, no record, or a slot that failed to read): a file that exists is
//!   first zeroed whole and synced; a file that does not is created. Then one write of the
//!   record with sequence 1 and zeros, `fsync`, and an `fsync` of the directory.
//! - **Finish** (the winner has sequence 1): zeros into the other slot, `fsync`, directory
//!   `fsync`. Only then may a record numbered 2 be written.
//! - **Boot sync**: the first record of a lifetime is preceded by an `fdatasync`.
//! - **Record**: the next number into the slot that does not hold the newest record, then
//!   `fdatasync`. A failure moves nothing; the next attempt rewrites the same slot.
//!
//! Every record is checked against the one before it ([`Journal::monotonic`]) under
//! [`crate::invariant!`]: the engine asking for a falling floor is a bug, reported, never written.

use std::io::ErrorKind;

use lsr_journal::{
    FILE, Journal, Opened, Parsed, SLOT, Seq, Slot, SlotRead, Verdict, Why, encode, parse,
};

use crate::fault::{BugSite, Fault};

/// The journal file's system calls. Each either does all it says or fails; a write that fails
/// may have changed the file's cache only by whole pages it was given.
pub trait JournalFile {
    /// Opens the file that exists; [`ErrorKind::NotFound`] when there is none.
    ///
    /// # Errors
    ///
    /// The operating system's.
    fn open(&mut self) -> Result<(), ErrorKind>;

    /// All 4096 bytes of `slot`, or `None` when the file ends first.
    ///
    /// # Errors
    ///
    /// The operating system's.
    fn read_slot(&mut self, slot: Slot) -> Result<Option<[u8; SLOT]>, ErrorKind>;

    /// `open(O_CREAT)` without truncating.
    ///
    /// # Errors
    ///
    /// The operating system's.
    fn create(&mut self) -> Result<(), ErrorKind>;

    /// Writes all of `bytes` at `offset`.
    ///
    /// # Errors
    ///
    /// The operating system's, having written a prefix at most.
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ErrorKind>;

    /// `fdatasync` (`fsync` where the length may have changed).
    ///
    /// # Errors
    ///
    /// The operating system's. A failure is never retried bare by the writer.
    fn sync(&mut self) -> Result<(), ErrorKind>;

    /// `fsync` of the directory holding the file.
    ///
    /// # Errors
    ///
    /// The operating system's.
    fn sync_dir(&mut self) -> Result<(), ErrorKind>;
}

impl<T: JournalFile + ?Sized> JournalFile for &mut T {
    fn open(&mut self) -> Result<(), ErrorKind> {
        (**self).open()
    }

    fn read_slot(&mut self, slot: Slot) -> Result<Option<[u8; SLOT]>, ErrorKind> {
        (**self).read_slot(slot)
    }

    fn create(&mut self) -> Result<(), ErrorKind> {
        (**self).create()
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ErrorKind> {
        (**self).write_at(offset, bytes)
    }

    fn sync(&mut self) -> Result<(), ErrorKind> {
        (**self).sync()
    }

    fn sync_dir(&mut self) -> Result<(), ErrorKind> {
        (**self).sync_dir()
    }
}

/// What boot's read of the journal reports (health names `JournalMissing`, `JournalUnreadable`,
/// `JournalNewer`, `JournalSlotUnreadable`). Each is an event, never a cost: the engine runs as
/// it would with no journal at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalSignal {
    /// No journal, while staging or the replica holds files: an upgrade, a meta directory
    /// inherited from Go or not kept, or a deleted journal. No observation separates them.
    Missing,
    /// A journal that holds no record this binary can read.
    Unreadable(Why),
    /// The newest record is from a newer version.
    Newer {
        /// Its version.
        version: u16,
    },
    /// A slot failed to read: the record read may be older than the newest one written.
    SlotUnreadable,
}

/// Boot's judgement of the journal it read: `fresh` when staging and the replica are both
/// empty, so a missing journal is a first run and reports nothing.
#[must_use]
pub fn judge(parsed: &Parsed, fresh: bool) -> Option<JournalSignal> {
    match parsed.verdict {
        Verdict::Missing if fresh => None,
        Verdict::Missing => Some(JournalSignal::Missing),
        Verdict::Unreadable(why) => Some(JournalSignal::Unreadable(why)),
        Verdict::Newer { version, .. } => Some(JournalSignal::Newer { version }),
        Verdict::Read { .. } if parsed.slot_failed => Some(JournalSignal::SlotUnreadable),
        Verdict::Read { .. } => None,
    }
}

/// Why a record was not made durable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum JournalWriteError {
    /// A system call failed: the journal is unwritable for this attempt (the engine proceeds
    /// on its in-memory facts, as main does).
    #[error("journal unwritable: {0:?}")]
    Unwritable(ErrorKind),
    /// The engine asked for something the journal must refuse.
    #[error("journal bug: {0:?}")]
    Bug(Fault),
}

/// What the writer found at boot: where to continue from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Base {
    Establish,
    Winner { slot: Slot, seq: Seq },
}

/// The writer, and the file it owns.
#[derive(Clone, Debug)]
pub struct JournalWriter<F> {
    file: F,
    base: Base,
    exists: bool,
    created: bool,
    synced: bool,
    last: Option<Journal>,
    durable: Option<Journal>,
}

/// One writer step's failure: a system call's error.
fn io(kind: ErrorKind) -> JournalWriteError {
    JournalWriteError::Unwritable(kind)
}

/// The invariant tier's check, apart so its early return is a [`Fault`].
fn monotonic(last: Option<&Journal>, new: &Journal) -> Result<(), Fault> {
    let ok = last.is_none_or(|last| last.monotonic(new).is_ok());
    crate::invariant!(ok, BugSite::JournalNotMonotonic);
    Ok(())
}

/// The sequence number after `seq`, or the invariant tier's fault.
fn following(seq: Seq) -> Result<Seq, Fault> {
    let next = seq.next();
    crate::invariant!(next.is_some(), BugSite::JournalSequence);
    Ok(next.unwrap_or(seq))
}

impl<F: JournalFile> JournalWriter<F> {
    /// Opens the journal at boot: reads each slot with its own call and judges.
    pub fn open(mut file: F) -> (Self, Parsed) {
        let opened = file.open();
        let s0 = opened.is_ok().then(|| file.read_slot(Slot::S0));
        let s1 = opened.is_ok().then(|| file.read_slot(Slot::S1));
        let view = |read: &Option<Result<Option<[u8; SLOT]>, ErrorKind>>| -> Option<[u8; SLOT]> {
            read.as_ref()
                .and_then(|r| r.as_ref().ok().copied().flatten())
        };
        let (b0, b1) = (view(&s0), view(&s1));
        let slot = |read: &Option<Result<Option<[u8; SLOT]>, ErrorKind>>, bytes| match (read, bytes)
        {
            (_, Some(bytes)) => SlotRead::Bytes(bytes),
            (Some(Ok(None)), None) => SlotRead::Short,
            (Some(Err(_)) | Some(Ok(Some(_))) | None, None) => SlotRead::Io,
        };
        let parsed = parse(match opened {
            Err(ErrorKind::NotFound) => Opened::Absent,
            Err(_) => Opened::Io,
            Ok(()) => Opened::Slots(slot(&s0, b0.as_ref()), slot(&s1, b1.as_ref())),
        });
        let base = match parsed.verdict {
            Verdict::Read { slot, seq, .. } | Verdict::Newer { slot, seq, .. }
                if !parsed.slot_failed =>
            {
                Base::Winner { slot, seq }
            }
            Verdict::Read { .. }
            | Verdict::Newer { .. }
            | Verdict::Missing
            | Verdict::Unreadable(_) => Base::Establish,
        };
        let created = matches!(base, Base::Winner { seq, .. } if seq.proves_created());
        let last = match parsed.verdict {
            Verdict::Read { journal, .. } => Some(journal),
            Verdict::Newer { .. } | Verdict::Missing | Verdict::Unreadable(_) => None,
        };
        let writer = Self {
            file,
            base,
            exists: !matches!(parsed.verdict, Verdict::Missing),
            created,
            synced: false,
            last,
            durable: None,
        };
        (writer, parsed)
    }

    /// Makes `journal` the durable record, or says why not. On failure the next call starts
    /// the same step again; nothing about the file is assumed.
    ///
    /// # Errors
    ///
    /// [`JournalWriteError::Bug`] when `journal` is not monotonic over the last record this
    /// writer read or was asked for; [`JournalWriteError::Unwritable`] when a call failed.
    pub fn write(&mut self, journal: &Journal) -> Result<(), JournalWriteError> {
        monotonic(self.last.as_ref(), journal).map_err(JournalWriteError::Bug)?;
        self.last = Some(*journal);
        let Base::Winner { slot, seq } = self.base else {
            return self.establish(journal);
        };
        if !self.created {
            self.file
                .write_at(slot.other().offset(), &[0; SLOT])
                .map_err(io)?;
            self.file.sync().map_err(io)?;
            self.file.sync_dir().map_err(io)?;
            self.created = true;
            self.synced = true;
        }
        if !self.synced {
            self.file.sync().map_err(io)?;
            self.synced = true;
        }
        let next = slot.other();
        let seq = following(seq).map_err(JournalWriteError::Bug)?;
        self.file
            .write_at(next.offset(), &encode(journal, seq))
            .map_err(io)?;
        self.file.sync().map_err(io)?;
        self.base = Base::Winner { slot: next, seq };
        self.durable = Some(*journal);
        Ok(())
    }

    /// Writes the file whole with `journal` as its first record.
    fn establish(&mut self, journal: &Journal) -> Result<(), JournalWriteError> {
        if self.exists {
            self.file.write_at(0, &[0; FILE]).map_err(io)?;
            self.file.sync().map_err(io)?;
        } else {
            self.file.create().map_err(io)?;
            self.exists = true;
        }
        let mut whole = [0; FILE];
        for (to, from) in whole.iter_mut().zip(encode(journal, Seq::FIRST)) {
            *to = from;
        }
        self.file.write_at(0, &whole).map_err(io)?;
        self.file.sync().map_err(io)?;
        self.file.sync_dir().map_err(io)?;
        self.base = Base::Winner {
            slot: Slot::S0,
            seq: Seq::FIRST,
        };
        self.created = true;
        self.synced = true;
        self.durable = Some(*journal);
        Ok(())
    }

    /// The newest record this lifetime made durable: none until its own write succeeds.
    #[must_use]
    pub const fn durable(&self) -> Option<&Journal> {
        self.durable.as_ref()
    }

    /// The file.
    pub const fn file(&mut self) -> &mut F {
        &mut self.file
    }

    /// The file, the writer gone (a process that dies).
    pub fn into_file(self) -> F {
        self.file
    }

    /// For a differential test: whether the next write establishes, the newest record's slot
    /// and number, and whether creation is finished and this lifetime has synced.
    #[must_use]
    pub const fn position(&self) -> (bool, Option<(Slot, u64)>, bool, bool) {
        let winner = match self.base {
            Base::Establish => None,
            Base::Winner { slot, seq } => Some((slot, seq.get())),
        };
        (
            matches!(self.base, Base::Establish),
            winner,
            self.created,
            self.synced,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use lsr_journal::{Journal, SLOT, Slot};

    use super::{JournalFile, JournalWriter};

    /// A file that records each call and answers as an empty, writable one would.
    #[derive(Debug, Default)]
    struct Recording {
        calls: Vec<&'static str>,
    }

    impl JournalFile for Recording {
        fn open(&mut self) -> Result<(), ErrorKind> {
            self.calls.push("open");
            Err(ErrorKind::NotFound)
        }

        fn read_slot(&mut self, _: Slot) -> Result<Option<[u8; SLOT]>, ErrorKind> {
            self.calls.push("read_slot");
            Ok(None)
        }

        fn create(&mut self) -> Result<(), ErrorKind> {
            self.calls.push("create");
            Ok(())
        }

        fn write_at(&mut self, _: u64, _: &[u8]) -> Result<(), ErrorKind> {
            self.calls.push("write_at");
            Ok(())
        }

        fn sync(&mut self) -> Result<(), ErrorKind> {
            self.calls.push("sync");
            Ok(())
        }

        fn sync_dir(&mut self) -> Result<(), ErrorKind> {
            self.calls.push("sync_dir");
            Ok(())
        }
    }

    /// The engine lends the file as `&mut dyn JournalFile`; every call made through the borrow
    /// must reach the file, or a sync the writer counts on would be dropped silently.
    #[test]
    fn a_borrowed_file_receives_every_call() {
        let mut file = Recording::default();
        let borrowed: &mut (dyn JournalFile + Send) = &mut file;
        let (mut writer, _) = JournalWriter::open(borrowed);
        writer.write(&Journal::ABSENT).unwrap();
        writer.write(&Journal::ABSENT).unwrap();
        let mut slot_reads = Recording::default();
        let borrowed: &mut (dyn JournalFile + Send) = &mut slot_reads;
        assert_eq!(borrowed.read_slot(Slot::S0), Ok(None));
        assert_eq!(slot_reads.calls, ["read_slot"]);
        assert_eq!(
            file.calls,
            [
                "open", "create", "write_at", "sync", "sync_dir", "write_at", "sync"
            ]
        );
    }
}
