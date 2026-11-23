//! Every level-0 file the replica takes is witnessed.
//!
//! The engine may upload a staged file only when it made those bytes itself
//! or the live WAL vouched for them at boot (see the module doc of
//! `crates/core/src/witness.rs`). The judge restates that from ground truth:
//! a level-0 put by one engine incarnation is either byte-equal to a file
//! that incarnation staged (the staging recorder's digest, since its start),
//! or it holds exactly what the model's WAL held for its range at that
//! incarnation's boot: its range lies in the generation the WAL was in then,
//! starts and ends on commits of it, every page it holds is the end commit's
//! image of that page, and every page that changed over the range is among
//! them, at the end commit's size. A
//! staging backup put back, or a staged file replaced by another valid one,
//! passes neither, whether it went up at boot or under a running engine.

use lsr_core::{FileKey, ObjectStore, ReadLen, conformance::block_on};
use lsr_ltx::{Decoder, Event, Header};
use lsr_wal::{Salts, WalOffset};

use super::Violation;
use crate::model::{CommitRec, Model};
use crate::store::{RecorderEvent, content_digest};

/// One engine incarnation, from its start to the crash or stop that ends it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Incarnation {
    /// The staging recorder's length when it started: files it staged come
    /// after.
    pub staged_from: usize,
    /// The WAL's generation when it booted, once it has.
    pub boot_salts: Option<Salts>,
    /// How many commits the model held when it booted: a commit made later
    /// (including the new truth a harness step adopts) is in no file its boot
    /// could witness.
    pub boot_commits: usize,
}

/// What a judged put turned out to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Judged {
    /// Not a level-0 put, or the replica no longer holds what it took.
    Skipped,
    /// The incarnation staged those bytes.
    StagedHere,
    /// The model's WAL held that range at the incarnation's boot.
    InBootWal,
}

/// Judges the replica put `put` made by `life` against the staging
/// recorder's `staging` events and the replica as `remote` holds it now.
///
/// # Errors
/// [`Violation::UnwitnessedPut`] when the put is neither; a
/// [`Violation::BugFault`] when the replica cannot be read.
pub fn judge_put(
    model: &Model,
    staging: &[RecorderEvent],
    remote: &crate::remote::Bucket,
    put: &RecorderEvent,
    life: Incarnation,
) -> Result<Judged, Violation> {
    let RecorderEvent::Put {
        key, digest, size, ..
    } = put
    else {
        return Ok(Judged::Skipped);
    };
    if key.level().get() != 0 {
        return Ok(Judged::Skipped);
    }
    let staged_here = staging.iter().skip(life.staged_from).any(|event| {
        matches!(event, RecorderEvent::Put { key: k, digest: d, size: s, .. }
            if k == key && d == digest && s == size)
    });
    if staged_here {
        return Ok(Judged::StagedHere);
    }
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap_or(ReadLen::CHUNK);
    let bytes = match block_on(remote.read(key, 0, all)) {
        Ok(bytes) => bytes,
        Err(lsr_core::StoreError::NotFound { .. }) => return Ok(Judged::Skipped),
        Err(err) => return Err(super::bug_fault(format!("reading {key:?}: {err}"))),
    };
    if content_digest(&bytes) != *digest {
        return Ok(Judged::Skipped);
    }
    match in_boot_wal(model, &bytes, life) {
        Ok(()) => Ok(Judged::InBootWal),
        Err(why) => Err(Violation::UnwitnessedPut {
            key: format!("{} {}", key.level().get(), describe(key)),
            why: why.to_owned(),
        }),
    }
}

fn describe(key: &FileKey) -> String {
    format!("{}-{}", key.range().min().get(), key.range().max().get())
}

/// Whether `bytes` hold exactly what the model's WAL held for their range
/// when `life` booted; the first rule broken when not.
fn in_boot_wal(model: &Model, bytes: &[u8], life: Incarnation) -> Result<(), &'static str> {
    let salts = life.boot_salts;
    let (header, pages) = decode(bytes).ok_or("it does not decode")?;
    let fields = header.fields();
    let origin = fields
        .wal
        .filter(|origin| origin.size() > 0)
        .ok_or("it names no WAL range")?;
    let (salt1, salt2) = origin.salts();
    let file_salts = Salts { salt1, salt2 };
    if Some(file_salts) != salts {
        return Err("its range is not in the generation the engine booted on");
    }
    // A range from the first frame follows a replica file that ended there:
    // a snapshot of the generation's empty WAL. What changed over it is not
    // known here, so only its pages and size are judged.
    let first_frame = WalOffset::start(fields.page_size).get();
    let start = match origin.offset() {
        offset if offset == first_frame => None,
        offset => Some(
            at_boot(model, file_salts, offset, life.boot_commits)
                .ok_or("its range does not start on a commit")?,
        ),
    };
    let end_offset = origin.offset().saturating_add(origin.size());
    let end = at_boot(model, file_salts, end_offset, life.boot_commits)
        .ok_or("its range does not end on a commit")?;
    let after = &end.image;
    if fields.commit != after.db_size() {
        return Err("its commit size is not the end commit's");
    }
    let held = pages
        .iter()
        .all(|(pgno, data)| after.page(*pgno).is_some_and(|want| *want == *data));
    if !held {
        return Err("a page it holds is not the end commit's");
    }
    let changed = start.is_none_or(|start| {
        (1..=after.db_size())
            .filter(|pgno| after.page(*pgno) != start.image.page(*pgno))
            .all(|pgno| pages.iter().any(|(held, _)| *held == pgno))
    });
    if !changed {
        return Err("it lacks a page the range changed");
    }
    Ok(())
}

/// The commit that ended at byte `end` of generation `salts` among the first
/// `commits` the model made: after a power loss the WAL regrows over the
/// same offsets, so the newest one wins (as [`Model::find`] has it).
fn at_boot(model: &Model, salts: Salts, end: u64, commits: usize) -> Option<&CommitRec> {
    model
        .commits()
        .iter()
        .take(commits)
        .filter(|rec| rec.salts == salts && rec.end.get() == end)
        .max_by_key(|rec| (rec.at, rec.id))
}

/// Each page a file holds: its number and bytes.
type Pages = Vec<(u32, Vec<u8>)>;

/// The header and every page of a whole, verified LTX file.
fn decode(bytes: &[u8]) -> Option<(Header, Pages)> {
    let mut decoder = Decoder::new();
    let mut pages = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (used, event) = decoder.feed(rest).ok()?;
        if let Some(Event::Page(page)) = event {
            pages.push((page.pgno().get(), page.data().to_vec()));
        }
        if used == 0 {
            return None;
        }
        rest = rest.get(used..)?;
    }
    let verified = decoder.finish().ok()?;
    Some((*verified.header(), pages))
}
