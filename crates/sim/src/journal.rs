//! The engine journal (`docs/journal.md`) on the simulator's disk.
//!
//! One file and its directory entry, with a page cache over a disk: a process crash keeps
//! everything; a power loss keeps what the run's crash law draws. Under
//! [`CrashLaw::Blocks`] each block written since the last sync keeps its old or new bytes and
//! the length is drawn ([`crate::crash_law::crash_image`]), and an entry the directory never
//! synced stays or goes on a coin. Under [`CrashLaw::AllOrNothing`] every write since the last
//! sync is undone and an unsynced entry is gone. The handle is shared, so the file lent to the
//! engine each tick and the world that crashes it are one file.

use std::io::ErrorKind;
use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::JournalFile;
use lsr_journal::{SLOT, Slot};

use crate::crash_law::{self, BlockSize, CrashLaw, Rng};

/// The file as the cache and the disk hold it.
#[derive(Debug, Default)]
struct Disk {
    entry_cache: bool,
    entry_disk: bool,
    cache: Vec<u8>,
    disk: Vec<u8>,
    dirty: Vec<(u64, u64)>,
    law: Option<(BlockSize, Rng)>,
}

/// The journal file; clones share it.
#[derive(Clone, Debug, Default)]
pub struct SimJournal {
    disk: Arc<Mutex<Disk>>,
}

fn at(offset: u64) -> usize {
    usize::try_from(offset).unwrap_or(usize::MAX)
}

impl SimJournal {
    fn with<T>(&self, f: impl FnOnce(&mut Disk) -> T) -> T {
        f(&mut self.disk.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// From now on a power loss keeps what `law` draws.
    pub fn crash_by_law(&self, law: CrashLaw) {
        self.with(|disk| {
            disk.law = match law {
                CrashLaw::AllOrNothing => None,
                CrashLaw::Blocks { block, seed } => Some((block, Rng(seed))),
            };
        });
    }

    /// A power loss: the cache is gone, the disk keeps what the law draws.
    pub fn power_loss(&self) {
        self.with(|disk| {
            let entry = match (disk.entry_disk, disk.entry_cache, disk.law.as_mut()) {
                (true, _, _) => true,
                (false, true, Some((_, rng))) => rng.coin(),
                (false, true | false, _) => false,
            };
            if !entry {
                *disk = Disk {
                    law: disk.law.take(),
                    ..Disk::default()
                };
                return;
            }
            let kept = match disk.law.as_mut() {
                Some((block, rng)) => crash_law::crash_image(
                    &disk.disk,
                    &disk.cache,
                    &disk.dirty,
                    &disk.disk,
                    *block,
                    rng,
                ),
                None => disk.disk.clone(),
            };
            disk.cache.clone_from(&kept);
            disk.disk = kept;
            disk.dirty.clear();
            disk.entry_cache = true;
            disk.entry_disk = true;
        });
    }

    /// The bytes a process reads now, or `None` with no file.
    #[must_use]
    pub fn bytes(&self) -> Option<Vec<u8>> {
        self.with(|disk| disk.entry_cache.then(|| disk.cache.clone()))
    }
}

impl JournalFile for SimJournal {
    fn open(&mut self) -> Result<(), ErrorKind> {
        self.with(|disk| {
            if disk.entry_cache {
                Ok(())
            } else {
                Err(ErrorKind::NotFound)
            }
        })
    }

    fn read_slot(&mut self, slot: Slot) -> Result<Option<[u8; SLOT]>, ErrorKind> {
        self.with(|disk| {
            if !disk.entry_cache {
                return Err(ErrorKind::NotFound);
            }
            let from = at(slot.offset());
            Ok(disk
                .cache
                .get(from..from.saturating_add(SLOT))
                .and_then(|bytes| bytes.try_into().ok()))
        })
    }

    fn create(&mut self) -> Result<(), ErrorKind> {
        self.with(|disk| {
            if !disk.entry_cache {
                *disk = Disk {
                    entry_cache: true,
                    law: disk.law.take(),
                    ..Disk::default()
                };
            }
            Ok(())
        })
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ErrorKind> {
        self.with(|disk| {
            if !disk.entry_cache {
                return Err(ErrorKind::NotFound);
            }
            let from = at(offset);
            let to = from.saturating_add(bytes.len());
            if disk.cache.len() < to {
                disk.cache.resize(to, 0);
            }
            if let Some(range) = disk.cache.get_mut(from..to) {
                range.copy_from_slice(bytes);
            }
            let wide = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
            disk.dirty.push((wide(from), wide(to)));
            Ok(())
        })
    }

    fn sync(&mut self) -> Result<(), ErrorKind> {
        self.with(|disk| {
            disk.disk.clone_from(&disk.cache);
            disk.dirty.clear();
            Ok(())
        })
    }

    fn sync_dir(&mut self) -> Result<(), ErrorKind> {
        self.with(|disk| {
            disk.entry_disk = disk.entry_cache;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use lsr_core::JournalWriter;
    use lsr_journal::{Journal, Unproven, Verdict};

    use super::*;

    fn debt() -> Journal {
        Journal {
            unproven: Unproven::Truncate,
            ..Journal::ABSENT
        }
    }

    #[test]
    fn a_synced_record_survives_a_power_loss_under_every_law() {
        for seed in 0..64 {
            let mut journal = SimJournal::default();
            journal.crash_by_law(CrashLaw::drawn(seed));
            let (mut writer, _) = JournalWriter::open(&mut journal);
            writer.write(&mut journal, &debt()).unwrap();
            journal.power_loss();
            let (_, parsed) = JournalWriter::open(&mut journal);
            assert!(
                matches!(parsed.verdict, Verdict::Read { journal, .. } if journal == debt()),
                "seed {seed}: {parsed:?}"
            );
        }
    }

    #[test]
    fn an_unsynced_create_may_be_lost_and_nothing_else_is_read() {
        let mut lost = 0;
        for seed in 0..64 {
            let mut journal = SimJournal::default();
            journal.crash_by_law(CrashLaw::drawn(seed));
            journal.create().unwrap();
            journal.write_at(0, &[7; 100]).unwrap();
            journal.power_loss();
            let (_, parsed) = JournalWriter::open(&mut journal);
            match parsed.verdict {
                Verdict::Missing => lost += 1,
                Verdict::Unreadable(_) => {}
                other @ (Verdict::Newer { .. } | Verdict::Read { .. }) => {
                    panic!("seed {seed}: {other:?}")
                }
            }
        }
        assert!(lost > 0 && lost < 64, "{lost}");
    }
}
