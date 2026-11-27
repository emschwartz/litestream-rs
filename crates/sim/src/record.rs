//! Regression histories as Rust source.
//!
//! A regression pinned by a seed number replays whatever the generator draws for that seed
//! today, so a new draw anywhere in the generator silently gives it another history, one that
//! may no longer reach the bug it was written for. A recorded history cannot drift: it is the
//! operations themselves, written out as a `const` array that [`crate::run::run_ops`] runs, to
//! be shrunk into an explicit scenario (`docs/decisions/D165.md`). Each operation is written as a
//! fully qualified constructor expression. Every match is exhaustive, so a new operation, fault
//! shape or field does not compile until it says how it is written; a shape no recorded history
//! has needed yet (most [`StoreError`] variants) is refused with [`Unprintable`] instead of
//! written wrongly.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{Denied, SourceError, SourceOp, StoreError, StoreOp};
use lsr_ltx::{PageNo, PageSize};

use crate::generate::{
    AppAt, AppOp, EnvFault, Hold, HttpFault, MetaOp, Op, RemoteOp, Target, WalOp,
};
use crate::sqlite::{CrashKind, OtherProcessMode};
use crate::store::FaultShape;

/// An operation holds a value this module does not write yet; the text names it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("cannot record {0}")]
pub struct Unprintable(pub String);

/// The source of a module holding `ops` as `pub const OPS: &[Op]`, with `doc` as its module
/// documentation (one line per element).
///
/// # Errors
/// [`Unprintable`] for the first value this module cannot write.
pub fn module(doc: &[&str], ops: &[Op]) -> Result<String, Unprintable> {
    let mut body = String::from("pub const OPS: &[Op] = &[\n");
    for op in ops {
        body.push_str(&format!("    {},\n", self::op(op)?));
    }
    body.push_str("];\n");
    let mut out = String::new();
    for line in doc {
        out.push_str(&format!("//! {line}\n").replace("//! \n", "//!\n"));
    }
    out.push('\n');
    for (crate_path, names) in IMPORTS {
        let used: Vec<&str> = names
            .iter()
            .copied()
            .filter(|name| *name == "Op" || body.contains(&format!("{name}::")))
            .collect();
        match used.as_slice() {
            [] => {}
            [one] => out.push_str(&format!("use {crate_path}::{one};\n")),
            many => out.push_str(&format!("use {crate_path}::{{{}}};\n", many.join(", "))),
        }
    }
    out.push('\n');
    out.push_str(&body);
    Ok(out)
}

/// What a recorded history may name, by import path: a file imports only those its operations use.
const IMPORTS: &[(&str, &[&str])] = &[
    ("std::io", &["ErrorKind"]),
    ("std::time", &["Duration"]),
    (
        "lsr_core",
        &["Denied", "SourceError", "SourceOp", "StoreError", "StoreOp"],
    ),
    ("lsr_ltx", &["PageNo", "PageSize"]),
    (
        "lsr_sim::generate",
        &[
            "AppAt",
            "AppOp",
            "DenialCode",
            "EnvFault",
            "Hold",
            "HttpFault",
            "HttpShape",
            "MetaOp",
            "Op",
            "PartsOp",
            "RemoteOp",
            "Target",
            "WalOp",
        ],
    ),
    ("lsr_sim", &["CrashKind", "FaultShape", "OtherProcessMode"]),
];

/// One operation as a constructor expression.
///
/// # Errors
/// [`Unprintable`] for a value this module cannot write.
pub fn op(op: &Op) -> Result<String, Unprintable> {
    Ok(match op {
        Op::Commit { pages, grow } => format!("Op::Commit {{ pages: {pages}, grow: {grow} }}"),
        Op::CommitInTick { pages } => format!("Op::CommitInTick {{ pages: {pages} }}"),
        Op::Spill => "Op::Spill".to_owned(),
        Op::OtherConnectionCommit { pages } => {
            format!("Op::OtherConnectionCommit {{ pages: {pages} }}")
        }
        Op::Rollback => "Op::Rollback".to_owned(),
        Op::Vacuum { page_size } => format!("Op::Vacuum {{ page_size: {} }}", size(*page_size)),
        Op::OtherProcessReader { begin } => format!("Op::OtherProcessReader {{ begin: {begin} }}"),
        Op::OtherProcessCheckpoint(mode) => {
            format!("Op::OtherProcessCheckpoint({})", checkpoint(*mode))
        }
        Op::Crash(kind) => format!("Op::Crash({})", crash(*kind)),
        Op::StoreFault {
            target,
            op,
            ahead,
            shape,
        } => format!(
            "Op::StoreFault {{ target: {}, op: {}, ahead: {ahead}, shape: {} }}",
            self::target(*target),
            store_op(*op),
            self::shape(shape)?
        ),
        Op::EnvFault(fault) => format!("Op::EnvFault({})", env(*fault)?),
        Op::ClockJump(millis) => format!("Op::ClockJump({millis})"),
        Op::Pause { read, millis } => format!("Op::Pause {{ read: {read}, millis: {millis} }}"),
        Op::Tick => "Op::Tick".to_owned(),
        Op::Settle => "Op::Settle".to_owned(),
        Op::UntilPublished => "Op::UntilPublished".to_owned(),
        Op::Shutdown => "Op::Shutdown".to_owned(),
        Op::ShutdownWithSnapshot => "Op::ShutdownWithSnapshot".to_owned(),
        Op::ShutdownWithRetention => "Op::ShutdownWithRetention".to_owned(),
        Op::RequestSnapshot => "Op::RequestSnapshot".to_owned(),
        Op::Restore => "Op::Restore".to_owned(),
        Op::Restart => "Op::Restart".to_owned(),
        Op::TrustLocal => "Op::TrustLocal".to_owned(),
        Op::PowerLoss => "Op::PowerLoss".to_owned(),
        Op::LyingWalSync => "Op::LyingWalSync".to_owned(),
        Op::Meta(meta) => format!("Op::Meta({})", self::meta(*meta)),
        Op::Wal(wal) => format!("Op::Wal({})", self::wal(*wal)),
        Op::Structured { seq, data } => {
            format!("Op::Structured {{ seq: {seq}, data: {data} }}")
        }
        Op::SaveDatabase => "Op::SaveDatabase".to_owned(),
        Op::PutBackDatabase { adopt } => format!("Op::PutBackDatabase {{ adopt: {adopt} }}"),
        Op::OurReader { begin } => format!("Op::OurReader {{ begin: {begin} }}"),
        Op::Remote(remote) => format!("Op::Remote({})", self::remote(*remote)),
        Op::AppAutocheckpoint(frames) => format!("Op::AppAutocheckpoint({frames:?})"),
        Op::HttpFault(fault) => format!("Op::HttpFault({})", http_fault(*fault)),
        Op::Parts(parts) => format!("Op::Parts(PartsOp::{parts:?})"),
        Op::Shrink { pages } => format!("Op::Shrink {{ pages: {pages} }}"),
        Op::SecondWriter { ticks } => format!("Op::SecondWriter {{ ticks: {ticks} }}"),
        Op::StagingDamage(meta) => format!("Op::StagingDamage({})", self::meta(*meta)),
        Op::Heal => "Op::Heal".to_owned(),
        Op::WalDamage { op, ticks } => {
            format!("Op::WalDamage {{ op: {}, ticks: {ticks} }}", wal(*op))
        }
        Op::Hold { hold, ticks } => {
            format!("Op::Hold {{ hold: {}, ticks: {ticks} }}", self::hold(*hold))
        }
        Op::AppInTick { at, op } => format!(
            "Op::AppInTick {{ at: {}, op: {} }}",
            app_at(*at),
            app_op(*op)
        ),
        Op::Verify => "Op::Verify".to_owned(),
    })
}

fn app_at(at: AppAt) -> String {
    match at {
        AppAt::Call(calls) => format!("AppAt::Call({calls})"),
        AppAt::Before(op) => format!("AppAt::Before({})", source_op(op)),
    }
}

fn app_op(op: AppOp) -> String {
    match op {
        AppOp::Commit { pages, grow } => {
            format!("AppOp::Commit {{ pages: {pages}, grow: {grow} }}")
        }
        AppOp::ReaderBegin => "AppOp::ReaderBegin".to_owned(),
        AppOp::ReaderEnd => "AppOp::ReaderEnd".to_owned(),
        AppOp::Checkpoint(mode) => format!("AppOp::Checkpoint({})", checkpoint(mode)),
        AppOp::DeleteWal => "AppOp::DeleteWal".to_owned(),
        AppOp::Spill { pages } => format!("AppOp::Spill {{ pages: {pages} }}"),
    }
}

fn size(page_size: Option<PageSize>) -> String {
    page_size.map_or_else(
        || "None".to_owned(),
        |size| format!("PageSize::new({})", size.get()),
    )
}

const fn checkpoint(mode: OtherProcessMode) -> &'static str {
    match mode {
        OtherProcessMode::Passive => "OtherProcessMode::Passive",
        OtherProcessMode::Restart => "OtherProcessMode::Restart",
        OtherProcessMode::Truncate => "OtherProcessMode::Truncate",
        OtherProcessMode::Full => "OtherProcessMode::Full",
    }
}

const fn crash(kind: CrashKind) -> &'static str {
    match kind {
        CrashKind::Process => "CrashKind::Process",
        CrashKind::Power => "CrashKind::Power",
    }
}

const fn target(target: Target) -> &'static str {
    match target {
        Target::Staging => "Target::Staging",
        Target::Remote => "Target::Remote",
    }
}

const fn store_op(op: StoreOp) -> &'static str {
    match op {
        StoreOp::List => "StoreOp::List",
        StoreOp::Read => "StoreOp::Read",
        StoreOp::Upload => "StoreOp::Upload",
        StoreOp::Delete => "StoreOp::Delete",
    }
}

fn shape(shape: &FaultShape) -> Result<String, Unprintable> {
    Ok(match shape {
        FaultShape::Error(err) => format!("FaultShape::Error({})", store_error(err)?),
        FaultShape::ErrorAfterPublish(err) => {
            format!("FaultShape::ErrorAfterPublish({})", store_error(err)?)
        }
        FaultShape::Scratch(err) => format!("FaultShape::Scratch({})", store_error(err)?),
        FaultShape::SizeMismatch => "FaultShape::SizeMismatch".to_owned(),
        FaultShape::ChecksumMismatch => "FaultShape::ChecksumMismatch".to_owned(),
        FaultShape::CorruptPublish => "FaultShape::CorruptPublish".to_owned(),
        FaultShape::Hang => "FaultShape::Hang".to_owned(),
        FaultShape::HangAfterPublish => "FaultShape::HangAfterPublish".to_owned(),
        FaultShape::HangEvery => "FaultShape::HangEvery".to_owned(),
        FaultShape::FlipByteOnRead { at } => format!("FaultShape::FlipByteOnRead {{ at: {at} }}"),
        FaultShape::Rate { per_mille, err } => format!(
            "FaultShape::Rate {{ per_mille: {per_mille}, err: {} }}",
            store_error(err)?
        ),
    })
}

/// The two store errors the generator draws; any other is refused.
fn store_error(err: &StoreError) -> Result<String, Unprintable> {
    match err {
        StoreError::Transient { op, kind } => Ok(format!(
            "StoreError::Transient {{ op: {}, kind: {} }}",
            store_op(*op),
            io_kind(*kind)?
        )),
        StoreError::Throttled { op, retry_after } => Ok(format!(
            "StoreError::Throttled {{ op: {}, retry_after: {} }}",
            store_op(*op),
            retry_after.map_or_else(|| Ok("None".to_owned()), duration)?
        )),
        StoreError::Denied { op, why } => Ok(format!(
            "StoreError::Denied {{ op: {}, why: {} }}",
            store_op(*op),
            denied(*why)
        )),
        StoreError::Refused { op, status } => Ok(format!(
            "StoreError::Refused {{ op: {}, status: {status} }}",
            store_op(*op)
        )),
        StoreError::NotFound { .. }
        | StoreError::AlreadyExists { .. }
        | StoreError::ScratchExhausted { .. }
        | StoreError::CreateOnlyUnsupported { .. }
        | StoreError::ObjectTooLarge { .. }
        | StoreError::Corrupt { .. } => Err(Unprintable(format!("{err:?}"))),
    }
}

const fn denied(why: Denied) -> &'static str {
    match why {
        Denied::Credentials => "Denied::Credentials",
        Denied::Permission => "Denied::Permission",
        Denied::ClockSkew => "Denied::ClockSkew",
        Denied::BucketMissing => "Denied::BucketMissing",
        Denied::Tls => "Denied::Tls",
    }
}

fn duration(wait: Duration) -> Result<String, Unprintable> {
    if !wait.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(Unprintable(format!("{wait:?}, finer than a millisecond")));
    }
    Ok(format!("Some(Duration::from_millis({}))", wait.as_millis()))
}

/// The kinds the generator draws, written by name; any other is refused, so a new one is
/// added here on purpose rather than printed by a `Debug` text that may not be its name.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "io::ErrorKind is non_exhaustive; every kind not named is refused, never guessed"
)]
fn io_kind(kind: ErrorKind) -> Result<String, Unprintable> {
    let name = match kind {
        ErrorKind::ConnectionReset => "ConnectionReset",
        ErrorKind::Other => "Other",
        ErrorKind::TimedOut => "TimedOut",
        ErrorKind::InvalidData => "InvalidData",
        ErrorKind::PermissionDenied => "PermissionDenied",
        ErrorKind::BrokenPipe => "BrokenPipe",
        ErrorKind::StorageFull => "StorageFull",
        ErrorKind::QuotaExceeded => "QuotaExceeded",
        ErrorKind::UnexpectedEof => "UnexpectedEof",
        other => return Err(Unprintable(format!("io::ErrorKind::{other:?}"))),
    };
    Ok(format!("ErrorKind::{name}"))
}

fn http_fault(fault: HttpFault) -> String {
    let (name, shape) = match fault {
        HttpFault::SlowDown(shape) => ("SlowDown", shape),
        HttpFault::TooManyRequests(shape) => ("TooManyRequests", shape),
        HttpFault::Internal(shape) => ("Internal", shape),
        HttpFault::RequestTimeout(shape) => ("RequestTimeout", shape),
        HttpFault::AfterPublish(shape) => ("AfterPublish", shape),
        HttpFault::TruncateBody(shape) => ("TruncateBody", shape),
        HttpFault::WrongEtag(shape) => ("WrongEtag", shape),
        HttpFault::BodyMismatch(shape) => ("BodyMismatch", shape),
        HttpFault::ConnectionReset(shape) => ("ConnectionReset", shape),
        HttpFault::Hang(shape) => ("Hang", shape),
        HttpFault::Denied(shape, code) => {
            return format!("HttpFault::Denied(HttpShape::{shape:?}, DenialCode::{code:?})");
        }
        HttpFault::DenyDeletes => return "HttpFault::DenyDeletes".to_owned(),
    };
    format!("HttpFault::{name}(HttpShape::{shape:?})")
}

fn env(fault: EnvFault) -> Result<String, Unprintable> {
    Ok(match fault {
        EnvFault::DeleteWal => "EnvFault::DeleteWal".to_owned(),
        EnvFault::WipeStaging => "EnvFault::WipeStaging".to_owned(),
        EnvFault::Source(op, err) => {
            format!(
                "EnvFault::Source({}, {})",
                source_op(op),
                source_error(err)?
            )
        }
        EnvFault::ShmMidUpdate => "EnvFault::ShmMidUpdate".to_owned(),
        EnvFault::ShmEndInsideTx => "EnvFault::ShmEndInsideTx".to_owned(),
        EnvFault::OverwriteLastFrame => "EnvFault::OverwriteLastFrame".to_owned(),
        EnvFault::UnlinkWal => "EnvFault::UnlinkWal".to_owned(),
        EnvFault::EmptyDatabase => "EnvFault::EmptyDatabase".to_owned(),
        EnvFault::TruncateFails => "EnvFault::TruncateFails".to_owned(),
        EnvFault::FlipDatabasePage(pgno) => format!("EnvFault::FlipDatabasePage({pgno})"),
        EnvFault::DriverWritesNothing => "EnvFault::DriverWritesNothing".to_owned(),
    })
}

const fn source_op(op: SourceOp) -> &'static str {
    match op {
        SourceOp::Open => "SourceOp::Open",
        SourceOp::Len => "SourceOp::Len",
        SourceOp::Committed => "SourceOp::Committed",
        SourceOp::PageCount => "SourceOp::PageCount",
        SourceOp::ReadPage => "SourceOp::ReadPage",
        SourceOp::BeginBarrier => "SourceOp::BeginBarrier",
        SourceOp::EndBarrier => "SourceOp::EndBarrier",
        SourceOp::Checkpoint => "SourceOp::Checkpoint",
        SourceOp::BumpSeq => "SourceOp::BumpSeq",
        SourceOp::BeginWriteLock => "SourceOp::BeginWriteLock",
        SourceOp::EndWriteLock => "SourceOp::EndWriteLock",
        SourceOp::Sync => "SourceOp::Sync",
        SourceOp::RenewRead => "SourceOp::RenewRead",
    }
}

fn source_error(err: SourceError) -> Result<String, Unprintable> {
    Ok(match err {
        SourceError::Io(kind) => format!("SourceError::Io({})", io_kind(kind)?),
        SourceError::DiskFull => "SourceError::DiskFull".to_owned(),
        SourceError::FsyncFailed => "SourceError::FsyncFailed".to_owned(),
        SourceError::Busy => "SourceError::Busy".to_owned(),
        SourceError::ReadersHeld => "SourceError::ReadersHeld".to_owned(),
        SourceError::WritesHeld => "SourceError::WritesHeld".to_owned(),
        SourceError::Unlinked => "SourceError::Unlinked".to_owned(),
        SourceError::ReadTxLost => "SourceError::ReadTxLost".to_owned(),
        SourceError::Short { pgno } => short(pgno)?,
    })
}

/// A page number as a const expression; the lock page, which is no page there, is refused.
fn short(pgno: PageNo) -> Result<String, Unprintable> {
    let page = pgno.get();
    if PageSize::new(4096).and_then(|size| PageNo::new(page, size)) != Some(pgno) {
        return Err(Unprintable(format!("Short {{ {pgno:?} }}")));
    }
    Ok(format!(
        "SourceError::Short {{ pgno: PageNo::new({page}, PageSize::new(4096).unwrap()).unwrap() }}"
    ))
}

const fn meta(meta: MetaOp) -> &'static str {
    match meta {
        MetaOp::Save => "MetaOp::Save",
        MetaOp::Restore => "MetaOp::Restore",
        MetaOp::DeleteNewest => "MetaOp::DeleteNewest",
        MetaOp::FlipNewestByte => "MetaOp::FlipNewestByte",
        MetaOp::TruncateNewest => "MetaOp::TruncateNewest",
        MetaOp::StripNewestOrigin => "MetaOp::StripNewestOrigin",
        MetaOp::DeleteAll => "MetaOp::DeleteAll",
    }
}

const fn remote(remote: RemoteOp) -> &'static str {
    match remote {
        RemoteOp::PutUnrecognized => "RemoteOp::PutUnrecognized",
        RemoteOp::RemoveUnrecognized => "RemoteOp::RemoveUnrecognized",
        RemoteOp::RollBack => "RemoteOp::RollBack",
        RemoteOp::DropCreatedAt => "RemoteOp::DropCreatedAt",
        RemoteOp::RefuseCreateOnly => "RemoteOp::RefuseCreateOnly",
    }
}

fn wal(wal: WalOp) -> String {
    match wal {
        WalOp::TearHeader => "WalOp::TearHeader".to_owned(),
        WalOp::TruncateAtFrame(frame) => format!("WalOp::TruncateAtFrame({frame})"),
        WalOp::RewriteSameLength(seed) => format!("WalOp::RewriteSameLength({seed})"),
        WalOp::RegrowSameSalts(seed) => format!("WalOp::RegrowSameSalts({seed})"),
        WalOp::RegrowWithoutCommitMarks(seed) => format!("WalOp::RegrowWithoutCommitMarks({seed})"),
        WalOp::FlipLastFrameBit => "WalOp::FlipLastFrameBit".to_owned(),
    }
}

fn hold(hold: Hold) -> String {
    match hold {
        Hold::StoreDown(op) => format!("Hold::StoreDown({})", store_op(op)),
        Hold::CredentialsRevoked => "Hold::CredentialsRevoked".to_owned(),
        Hold::DiskFull => "Hold::DiskFull".to_owned(),
        Hold::LyingReads => "Hold::LyingReads".to_owned(),
        Hold::ReadersHeld => "Hold::ReadersHeld".to_owned(),
        Hold::Wal(op) => format!("Hold::Wal({})", wal(op)),
        Hold::LyingDisk => "Hold::LyingDisk".to_owned(),
        Hold::StoreDownAll => "Hold::StoreDownAll".to_owned(),
        Hold::SecondWriter => "Hold::SecondWriter".to_owned(),
        Hold::Hang(op) => format!("Hold::Hang({})", store_op(op)),
        Hold::SlowStaging(factor) => format!("Hold::SlowStaging({factor})"),
    }
}

#[cfg(test)]
mod tests;
