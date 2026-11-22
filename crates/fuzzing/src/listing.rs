//! Replica listings built from fuzz bytes with `arbitrary`, for the plan
//! targets judged by the restore-plan spec.
//!
//! Every value is a valid listing: keys are deduplicated (a store lists a key
//! once), ranges are ordered, levels are 0 to 9. The shapes the contract's
//! boundaries turn on are made likely on purpose: small ids so files overlap,
//! touch and leave holes; files from transaction 1 at every level; level-9
//! files that do not start there (refused); ids against `u64::MAX`, where one
//! past a reach does not exist; created-at times on a coarse grid so ties with
//! each other and with the instant are common, some at or before the epoch
//! (unset); lost metadata, so the store falls back to a last-modified time
//! that dates no commit.

use std::collections::BTreeSet;

use arbitrary::{Arbitrary, Unstructured};
use lsr_core::{CreatedAt, FileInfo, FileKey, Level, ListedTime};
use lsr_ltx::{Timestamp, Txid, TxidRange};

/// Most files a listing holds: the planner is quadratic at worst, the spec cubic.
const MAX_FILES: usize = 48;

/// Where small ids start near the top of the range: 96 below `u64::MAX`.
const NEAR_TOP: u64 = u64::MAX - 96;

/// One file and everything a store may say about its time.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ArbFile {
    pub(crate) key: FileKey,
    /// What the writer recorded, in milliseconds.
    pub(crate) created: i64,
    /// The backend's own time: what a lazy listing shows, and what a store
    /// whose metadata was lost reports as the created-at time.
    pub(crate) last_modified: i64,
    /// The stored created-at time was lost.
    pub(crate) lost: bool,
    /// Listed with its exact created-at time (else only the last-modified one).
    pub(crate) listed_exact: bool,
}

impl ArbFile {
    /// The created-at time a store reports for the file.
    pub(crate) const fn truth(&self) -> CreatedAt {
        if self.lost {
            CreatedAt::fallback(Timestamp::from_unix_millis(self.last_modified))
        } else {
            CreatedAt::mtime(Timestamp::from_unix_millis(self.created))
        }
    }

    /// The file listed with its exact created-at time.
    pub(crate) const fn exact(&self) -> FileInfo {
        FileInfo {
            key: self.key,
            size: 1,
            listed: ListedTime::Created(self.truth()),
        }
    }
}

/// A listing: the files, and the first id small offsets count from.
#[derive(Clone, Debug)]
pub(crate) struct ArbListing {
    pub(crate) base: u64,
    pub(crate) files: Vec<ArbFile>,
}

/// A time on a 10 ms grid from -10 to 400, so equal times are common.
fn grid_time(u: &mut Unstructured<'_>) -> arbitrary::Result<i64> {
    Ok(i64::from(u.int_in_range(-1_i8..=40)?).saturating_mul(10))
}

fn arb_key(u: &mut Unstructured<'_>, base: u64) -> arbitrary::Result<Option<FileKey>> {
    let level = u.int_in_range(0_u8..=9)?;
    let offset = u.int_in_range(0_u64..=40)?;
    let from_one = u.ratio(1_u8, 4)?;
    let span = if u.ratio(1_u8, 16)? {
        u.arbitrary::<u64>()?
    } else {
        u.int_in_range(0_u64..=20)?
    };
    let first = base.saturating_add(1).saturating_add(offset);
    let min = if from_one { 1 } else { first };
    let max = first.saturating_add(span).max(min);
    let range = Txid::new(min)
        .zip(Txid::new(max))
        .and_then(|(min, max)| TxidRange::new(min, max));
    Ok(range
        .zip(Level::new(level))
        .map(|(range, level)| FileKey::new(level, range)))
}

impl<'a> Arbitrary<'a> for ArbListing {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let base = if u.ratio(1_u8, 4)? { NEAR_TOP } else { 0 };
        let count = u.int_in_range(0..=MAX_FILES)?;
        let mut keys = BTreeSet::new();
        let mut files = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(key) = arb_key(u, base)? else {
                continue;
            };
            let created = grid_time(u)?;
            let last_modified = if u.arbitrary()? {
                created
            } else {
                grid_time(u)?
            };
            let file = ArbFile {
                key,
                created,
                last_modified,
                lost: u.ratio(1_u8, 5)?,
                listed_exact: u.arbitrary()?,
            };
            if keys.insert(key) {
                files.push(file);
            }
        }
        Ok(Self { base, files })
    }
}

impl ArbListing {
    /// Every file listed with its exact created-at time.
    pub(crate) fn exact(&self) -> Vec<FileInfo> {
        self.files.iter().map(ArbFile::exact).collect()
    }

    /// A transaction id near the files: around the base, one off a file's
    /// end, or against `u64::MAX`.
    pub(crate) fn txid(&self, u: &mut Unstructured<'_>) -> arbitrary::Result<Option<Txid>> {
        let raw = match u.int_in_range(0_u8..=2)? {
            0 => self
                .base
                .saturating_add(1)
                .saturating_add(u.int_in_range(0_u64..=70)?),
            1 => {
                let file = u.choose(&self.files).ok();
                let end = file.map_or(1, |file| file.key.range().max().get());
                match u.int_in_range(0_u8..=2)? {
                    0 => end.saturating_sub(1),
                    1 => end,
                    _ => end.saturating_add(1),
                }
            }
            _ => u64::MAX.saturating_sub(u.int_in_range(0_u64..=3)?),
        };
        Ok(Txid::new(raw))
    }

    /// An instant that ties with a file's time, or sits one either side of
    /// it, or anywhere on the grid.
    pub(crate) fn instant(&self, u: &mut Unstructured<'_>) -> arbitrary::Result<Timestamp> {
        let at = match u.choose(&self.files).ok() {
            Some(file) if u.ratio(3_u8, 4)? => {
                let at = if u.arbitrary()? {
                    file.created
                } else {
                    file.last_modified
                };
                at.saturating_add(i64::from(u.int_in_range(-1_i8..=1)?))
            }
            Some(_) | None => grid_time(u)?,
        };
        Ok(Timestamp::from_unix_millis(at))
    }
}
