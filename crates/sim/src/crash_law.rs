//! What a power loss leaves of a file: the block crash law, shared by the simulator's SQLite
//! rules ([`crate::sqlite`]) and the real-SQLite fault VFS (lsr-sqlite's `testing::vfs`), so the
//! two disks are one.
//!
//! [`CrashLaw::AllOrNothing`] undoes every write since the file's last sync.
//!
//! [`CrashLaw::Blocks`] is the law of a page cache over a disk that writes blocks in any order
//! (ALICE's model, Pillai et al., OSDI 2014). Take a file's image as of its last sync (the
//! synced image) and its image now. Every block of `block` bytes that a write or truncate
//! touched since the sync independently keeps its synced or its current bytes; every other
//! block is the same in both. The length is drawn apart from the blocks: a file that grew keeps
//! its synced length, its current length, or a block boundary between the two; a file that
//! shrank keeps either length. Where the kept length runs past the bytes a block's drawn image
//! has, the rest of that block is zeros or stale bytes (what the file held at that offset
//! before a truncate or an overwrite, else zeros), drawn per block. SQLite's frames are 24 bytes
//! longer than a page, so a frame straddles blocks and a crash can keep its header and lose its
//! page, or lose a frame in the middle of a run of kept ones.

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
            seed: rng.draw(),
        }
    }
}

/// splitmix64: the law's draws, a pure function of its seed.
#[derive(Clone, Copy, Debug)]
pub struct Rng(pub u64);

impl Rng {
    /// The next draw.
    pub fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ z.wrapping_shr(31)
    }

    /// A fair coin.
    pub fn coin(&mut self) -> bool {
        self.draw() & 1 == 0
    }

    /// A draw in `0..upper` (0 when `upper` is 0).
    pub fn below(&mut self, upper: usize) -> usize {
        let upper = u64::try_from(upper).unwrap_or(u64::MAX);
        usize::try_from(self.draw().checked_rem(upper).unwrap_or(0)).unwrap_or(0)
    }
}

fn at(offset: u64) -> usize {
    usize::try_from(offset).unwrap_or(usize::MAX)
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

/// What a crash leaves of a file whose synced image is `old` and which holds `now`, under the
/// block law (module doc). `dirty` are the byte ranges `[from, to)` written or truncated since
/// the sync; `ghost` is the stale bytes a block may show past the data written.
#[must_use]
pub fn crash_image(
    old: &[u8],
    now: &[u8],
    dirty: &[(u64, u64)],
    ghost: &[u8],
    block: BlockSize,
    rng: &mut Rng,
) -> Vec<u8> {
    let block = block.bytes();
    let len = crash_len(old.len(), now.len(), block, rng);
    let mut out = Vec::with_capacity(len);
    let mut from = 0;
    while from < len {
        let to = from.saturating_add(block).min(len);
        let touched = dirty.iter().any(|(a, b)| at(*a) < to && at(*b) > from);
        let src = if !touched || rng.coin() { now } else { old };
        let stale = rng.coin();
        for i in from..to {
            let byte = src
                .get(i)
                .or_else(|| (!touched).then(|| old.get(i)).flatten())
                .or_else(|| stale.then(|| ghost.get(i)).flatten());
            out.push(byte.copied().unwrap_or(0));
        }
        from = to;
    }
    out
}

/// The ranges of the blocks of `block` bytes where `old` and `now` differ, lengths included:
/// the dirty ranges of a file whose writes are known only by their result.
#[must_use]
pub fn differing_blocks(old: &[u8], now: &[u8], block: BlockSize) -> Vec<(u64, u64)> {
    let block = block.bytes();
    let len = old.len().max(now.len());
    let mut out = Vec::new();
    let mut from = 0;
    while from < len {
        let to = from.saturating_add(block).min(len);
        if old.get(from..to) != now.get(from..to) {
            let wide = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
            out.push((wide(from), wide(to)));
        }
        from = to;
    }
    out
}

/// What a crash that left `out` of a file kept, over the blocks where `old` (the synced image)
/// and `now` differ: whether it kept any, and whether it kept one after a block it lost (a
/// hole, the shape a prefix law cannot make).
#[must_use]
pub fn kept_and_holes(old: &[u8], now: &[u8], out: &[u8], block: BlockSize) -> (bool, bool) {
    let dirty = differing_blocks(old, now, block);
    let kept = |(a, b): &(u64, u64)| {
        let range = at(*a)..at(*b);
        let got = out.get(range.clone());
        got.is_some() && got == now.get(range.clone()) && got != old.get(range)
    };
    let first_lost = dirty.iter().position(|r| !kept(r));
    (
        dirty.iter().any(kept),
        first_lost.is_some_and(|lost| dirty.iter().skip(lost).any(kept)),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Over many draws, an overwrite of three blocks yields every mix of old and new blocks,
    /// and nothing else.
    #[test]
    fn each_dirty_block_keeps_its_old_or_new_bytes_independently() {
        let now = [vec![1_u8; 512], vec![2; 512], vec![3; 512]].concat();
        let old = vec![0_u8; 1536];
        let mut seen = BTreeSet::new();
        let mut rng = Rng(7);
        for _ in 0..200 {
            let out = crash_image(&old, &now, &[(0, 1536)], &[], BlockSize::B512, &mut rng);
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
        let old = vec![9_u8; 4096];
        let ghost = vec![5_u8; 4096 * 3];
        let mut lens = BTreeSet::new();
        let mut fills = BTreeSet::new();
        let mut rng = Rng(11);
        for _ in 0..300 {
            let dirty = [(4096, 4096 * 3)];
            let out = crash_image(&old, &now, &dirty, &ghost, BlockSize::B4096, &mut rng);
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
        let old = vec![4_u8; 1024];
        let mut rng = Rng(3);
        let outs: BTreeSet<Vec<u8>> = (0..50)
            .map(|_| crash_image(&old, &[], &[(0, 1024)], &[], BlockSize::B512, &mut rng))
            .collect();
        assert!(outs.contains(&vec![]));
        assert!(outs.contains(&vec![4; 1024]));
    }

    #[test]
    fn a_drawn_law_uses_both_block_sizes() {
        let sizes: BTreeSet<usize> = (0..32)
            .filter_map(|seed| match CrashLaw::drawn(seed) {
                CrashLaw::Blocks { block, .. } => Some(block.bytes()),
                CrashLaw::AllOrNothing => None,
            })
            .collect();
        assert_eq!(sizes, [512, 4096].into_iter().collect());
    }

    #[test]
    fn differing_blocks_cover_changed_bytes_and_lengths() {
        let old = vec![0_u8; 1024];
        let mut now = old.clone();
        now[600] = 1;
        now.extend([7; 10]);
        assert_eq!(
            differing_blocks(&old, &now, BlockSize::B512),
            [(512, 1024), (1024, 1034)]
        );
    }
}
