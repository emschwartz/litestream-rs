//! The fault VFS's side of the crash law ([`lsr_sim::crash_law`]): what each file's writes
//! since its last sync replaced, so a power loss can rebuild the synced image and hand it to the
//! law.

pub use lsr_sim::crash_law::{BlockSize, CrashLaw, Rng};

/// One file's writes since its last sync.
#[derive(Debug, Default)]
pub(super) struct Undo {
    /// The length at the last sync.
    pub(super) synced_len: u64,
    /// What each write or truncate replaced, oldest first.
    pub(super) replaced: Vec<(u64, Vec<u8>)>,
    /// The byte ranges each write or truncate touched, `[from, to)`.
    pub(super) dirty: Vec<(u64, u64)>,
}

fn at(offset: u64) -> usize {
    usize::try_from(offset).unwrap_or(usize::MAX)
}

/// Puts `data` into `bytes` at `offset`, growing it with zeros.
pub(super) fn write_at(bytes: &mut Vec<u8>, offset: u64, data: &[u8]) {
    let from = at(offset);
    let to = from.saturating_add(data.len());
    if bytes.len() < to {
        bytes.resize(to, 0);
    }
    if let Some(dst) = bytes.get_mut(from..to) {
        dst.copy_from_slice(data);
    }
}

/// The file as of its last sync, from what it holds now.
pub(super) fn synced_image(now: &[u8], undo: &Undo) -> Vec<u8> {
    let mut old = now.to_vec();
    for (offset, bytes) in undo.replaced.iter().rev() {
        write_at(&mut old, *offset, bytes);
    }
    old.resize(at(undo.synced_len), 0);
    old
}

/// What a crash leaves of a file that holds `now`, under the block law; `ghost` is the stale
/// bytes a block may show past the data written.
pub(super) fn crash_image(
    now: &[u8],
    undo: &Undo,
    ghost: &[u8],
    block: BlockSize,
    rng: &mut Rng,
) -> Vec<u8> {
    let old = synced_image(now, undo);
    lsr_sim::crash_law::crash_image(&old, now, &undo.dirty, ghost, block, rng)
}
