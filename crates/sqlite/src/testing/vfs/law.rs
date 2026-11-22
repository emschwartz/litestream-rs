//! What a power loss leaves of a file: the crash laws of [`super::FaultVfs`].
//!
//! [`CrashLaw::AllOrNothing`] undoes every write since the file's last sync. It is the law the
//! suite was written against, and stays the default so its cases keep their meaning.
//!
//! [`CrashLaw::Blocks`] is the law of a page cache over a disk that writes blocks in any order
//! (ALICE's model, Pillai et al., OSDI 2014). For each file, take its image as of its last sync
//! (the synced image) and its image now. Every block of `block` bytes that a write or truncate
//! touched since the sync independently keeps its synced or its current bytes; every other
//! block is the same in both. The length is drawn apart from the blocks: a file that grew keeps
//! its synced length, its current length, or a block boundary between the two; a file that
//! shrank keeps either length. Where the kept length runs past the bytes a block's drawn image
//! has, the rest of that block is zeros or stale bytes (what the file held at that offset
//! before a truncate or an overwrite, else zeros), drawn per block. SQLite's frames are 24 bytes
//! longer than a page, so a frame straddles blocks and a crash can keep its header and lose its
//! page, or lose a frame in the middle of a run of kept ones.
//!
//! Directory entries follow the same law under [`CrashLaw::Blocks`] (module doc of the VFS).

/// A disk block: the unit a crash keeps or loses whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockSize {
    /// 512 bytes: a sector of an older disk.
    B512,
    /// 4096 bytes: a page-cache page, a modern disk's sector.
    B4096,
}

impl BlockSize {
    /// The block's length in bytes.
    #[must_use]
    pub const fn bytes(self) -> usize {
        match self {
            Self::B512 => 512,
            Self::B4096 => 4096,
        }
    }
}

/// What a power loss keeps of the writes since each file's last sync (module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashLaw {
    /// Nothing: every write since the last sync is undone, and deletions stay done.
    AllOrNothing,
    /// Each dirty block, the length, and each unsynced directory entry independently.
    Blocks {
        /// The block size.
        block: BlockSize,
        /// Seeds every draw the law makes.
        seed: u64,
    },
}

impl CrashLaw {
    /// [`CrashLaw::Blocks`] with its block size drawn from `seed` (one run, one disk).
    #[must_use]
    pub fn drawn(seed: u64) -> Self {
        let mut rng = Rng(seed);
        let block = if rng.coin() {
            BlockSize::B4096
        } else {
            BlockSize::B512
        };
        Self::Blocks {
            block,
            seed: rng.next(),
        }
    }
}

/// splitmix64: the law's draws, a pure function of its seed.
#[derive(Clone, Copy, Debug)]
pub(super) struct Rng(pub(super) u64);

impl Rng {
    pub(super) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ z.wrapping_shr(31)
    }

    pub(super) fn coin(&mut self) -> bool {
        self.next() & 1 == 0
    }

    /// A draw in `0..upper` (0 when `upper` is 0).
    pub(super) fn below(&mut self, upper: usize) -> usize {
        let upper = u64::try_from(upper).unwrap_or(u64::MAX);
        usize::try_from(self.next().checked_rem(upper).unwrap_or(0)).unwrap_or(0)
    }
}

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

/// The length a crash keeps (module doc).
fn crash_len(synced: usize, now: usize, block: usize, rng: &mut Rng) -> usize {
    if now < synced {
        return if rng.coin() { now } else { synced };
    }
    if now == synced {
        return now;
    }
    // Block boundaries strictly between the two lengths.
    let first = synced.checked_div(block).unwrap_or(0).saturating_add(1);
    let last = now.saturating_sub(1).checked_div(block).unwrap_or(0);
    match rng.below(3) {
        0 => synced,
        1 if last >= first => first
            .saturating_add(rng.below(last.saturating_sub(first).saturating_add(1)))
            .saturating_mul(block),
        _ => now,
    }
}

/// What a crash leaves of a file that holds `now`, under the block law (module doc); `ghost`
/// is the stale bytes a block may show past the data written.
pub(super) fn crash_image(
    now: &[u8],
    undo: &Undo,
    ghost: &[u8],
    block: BlockSize,
    rng: &mut Rng,
) -> Vec<u8> {
    let old = synced_image(now, undo);
    let block = block.bytes();
    let len = crash_len(old.len(), now.len(), block, rng);
    let mut out = Vec::with_capacity(len);
    let mut from = 0;
    while from < len {
        let to = from.saturating_add(block).min(len);
        let dirty = undo.dirty.iter().any(|(a, b)| at(*a) < to && at(*b) > from);
        let src = if !dirty || rng.coin() { now } else { &old };
        let stale = rng.coin();
        for i in from..to {
            let byte = src
                .get(i)
                .or_else(|| (!dirty).then(|| old.get(i)).flatten())
                .or_else(|| stale.then(|| ghost.get(i)).flatten());
            out.push(byte.copied().unwrap_or(0));
        }
        from = to;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn undo(synced_len: u64, replaced: Vec<(u64, Vec<u8>)>, dirty: Vec<(u64, u64)>) -> Undo {
        Undo {
            synced_len,
            replaced,
            dirty,
        }
    }

    /// Over many draws, an overwrite of three blocks yields every mix of old and new blocks,
    /// and nothing else.
    #[test]
    fn each_dirty_block_keeps_its_old_or_new_bytes_independently() {
        let now = [vec![1_u8; 512], vec![2; 512], vec![3; 512]].concat();
        let u = undo(1536, vec![(0, vec![0; 1536])], vec![(0, 1536)]);
        let mut seen = std::collections::BTreeSet::new();
        let mut rng = Rng(7);
        for _ in 0..200 {
            let out = crash_image(&now, &u, &[], BlockSize::B512, &mut rng);
            assert_eq!(out.len(), 1536);
            let mix: Vec<u8> = out.chunks(512).map(|c| c[0]).collect();
            for (chunk, m) in out.chunks(512).zip(&mix) {
                assert!(chunk.iter().all(|b| b == m), "a block is whole");
            }
            seen.insert(mix);
        }
        assert_eq!(seen.len(), 8, "{seen:?}");
    }

    /// An append keeps none, part (to a block boundary) or all of its length; a kept length
    /// past a lost block shows zeros or the stale bytes there.
    #[test]
    fn an_append_keeps_its_length_apart_from_its_bytes() {
        let now = vec![9_u8; 4096 * 3];
        let u = undo(4096, vec![], vec![(4096, 4096 * 3)]);
        let ghost = vec![5_u8; 4096 * 3];
        let mut lens = std::collections::BTreeSet::new();
        let mut fills = std::collections::BTreeSet::new();
        let mut rng = Rng(11);
        for _ in 0..300 {
            let out = crash_image(&now, &u, &ghost, BlockSize::B4096, &mut rng);
            lens.insert(out.len());
            assert!(
                out[..4096].iter().all(|b| *b == 9),
                "the synced block stays"
            );
            for chunk in out[4096..].chunks(4096) {
                assert!(chunk.iter().all(|b| *b == chunk[0]));
                fills.insert(chunk[0]);
            }
        }
        assert_eq!(lens, [4096, 8192, 12288].into_iter().collect());
        assert_eq!(fills, [0, 5, 9].into_iter().collect());
    }

    /// A truncate that was not synced may come back with the old bytes.
    #[test]
    fn an_unsynced_truncate_may_come_back() {
        let now = vec![];
        let u = undo(1024, vec![(0, vec![4; 1024])], vec![(0, 1024)]);
        let mut rng = Rng(3);
        let outs: std::collections::BTreeSet<Vec<u8>> = (0..50)
            .map(|_| crash_image(&now, &u, &[], BlockSize::B512, &mut rng))
            .collect();
        assert!(outs.contains(&vec![]));
        assert!(outs.contains(&vec![4; 1024]));
    }

    #[test]
    fn a_drawn_law_uses_both_block_sizes() {
        let sizes: std::collections::BTreeSet<usize> = (0..32)
            .filter_map(|seed| match CrashLaw::drawn(seed) {
                CrashLaw::Blocks { block, .. } => Some(block.bytes()),
                CrashLaw::AllOrNothing => None,
            })
            .collect();
        assert_eq!(sizes, [512, 4096].into_iter().collect());
    }
}
