//! One file in one directory, under the block crash law, at the grain the journal's reader can
//! tell apart.
//!
//! The file is two 4096-byte slots. A slot's record sits in its first 512 bytes and the rest of
//! the slot is zeros in every image any writer makes, so a slot's state is the content of that
//! first sector ([`Content`]), whether the sector is unreadable, and whether it is a hole. The
//! length is kept in the three classes that decide what a slot read returns ([`Len`]).
//!
//! Each slot has a disk image and, when a write made the page cache differ from the disk, a
//! cache image. Processes read the cache where there is one, else the disk. Rules:
//!
//! - A write changes the cache and marks the slot's block dirty. A write past the length extends
//!   it. With a block of 8192 bytes or more both slots share a block, so a write to one marks
//!   both dirty (the other's bytes are rewritten unchanged).
//! - A successful fsync makes the disk equal the cache, length included.
//! - A failed fsync leaves each dirty block on disk old, new or (on a [`Device::Tears`] disk)
//!   torn, the length old or new, and marks every page clean while the cache keeps the new bytes:
//!   the "fsyncgate" behaviour, so a bare fsync after it returns success and writes nothing.
//! - A clean page may be evicted at any time; the next read then sees the disk.
//! - A power loss keeps each dirty block's old or new bytes (jointly for both slots under a
//!   large block), keeps the synced length, the current length or a block boundary between,
//!   keeps or loses a directory entry that was not synced, and empties the cache. A
//!   [`Device::Tears`] disk may also leave one sector it was writing unreadable.
//! - A process crash changes nothing here.
//! - A directory fsync that fails leaves the entry on disk or not; a later one that succeeds
//!   persists it (directory metadata is journalled: a failed commit is retried or aborts the
//!   filesystem, it is not marked clean like a data page).

/// One of the journal's two slots.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub enum Slot {
    /// Bytes 0 to 4096.
    S0,
    /// Bytes 4096 to 8192.
    S1,
}

impl Slot {
    /// The other slot.
    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::S0 => Self::S1,
            Self::S1 => Self::S0,
        }
    }

    /// Both slots, in file order.
    pub const BOTH: [Self; 2] = [Self::S0, Self::S1];
}

/// One value per slot.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Pair<T> {
    s0: T,
    s1: T,
}

impl<T> Pair<T> {
    /// Both values.
    pub const fn new(s0: T, s1: T) -> Self {
        Self { s0, s1 }
    }

    /// The value for `slot`.
    pub const fn get(&self, slot: Slot) -> &T {
        match slot {
            Slot::S0 => &self.s0,
            Slot::S1 => &self.s1,
        }
    }

    /// The value for `slot`, to change.
    pub const fn get_mut(&mut self, slot: Slot) -> &mut T {
        match slot {
            Slot::S0 => &mut self.s0,
            Slot::S1 => &mut self.s1,
        }
    }
}

/// A record's place in the recorder's log of every record any writer issued.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct Rid(pub u8);

/// What the first sector of a slot holds.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Content {
    /// Zeros: never written, a hole, or the zeros a create wrote. No magic, so not a record.
    Zero,
    /// A partly written record or a torn sector: its checksum fails.
    Garbage,
    /// The whole image of an issued record: magic, length and checksum check.
    Rec(Rid),
}

/// The file's length, in the classes a slot read can tell apart.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub enum Len {
    /// Below 4096 bytes: both slots read short.
    Short0,
    /// 4096 to 8191 bytes: slot 1 reads short.
    Short1,
    /// 8192 bytes.
    Full,
}

impl Len {
    /// Whether a file this long holds all of `slot`.
    #[must_use]
    pub const fn covers(self, slot: Slot) -> bool {
        match (self, slot) {
            (Self::Full, Slot::S0 | Slot::S1) | (Self::Short1, Slot::S0) => true,
            (Self::Short0, Slot::S0 | Slot::S1) | (Self::Short1, Slot::S1) => false,
        }
    }

    /// The shortest length that holds `slot`.
    #[must_use]
    pub const fn to_hold(slot: Slot) -> Self {
        match slot {
            Slot::S0 => Self::Short1,
            Slot::S1 => Self::Full,
        }
    }
}

/// The unit a crash keeps or loses whole.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Block {
    /// 512 bytes.
    B512,
    /// 4096 bytes: one block per slot.
    B4096,
    /// 8192 bytes or more (16 KiB and 64 KiB pages): both slots in one block.
    Large,
}

/// What the device does to a sector it is writing when the power goes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Device {
    /// Nothing: a sector holds its old or its new bytes (the block law alone).
    Atomic,
    /// It may leave one sector it was writing unreadable. With `unchanged_safe`, never a sector
    /// whose new bytes equal its old ones (the assumption a large block needs, review 5 J3).
    Tears {
        /// Whether a sector rewritten with identical bytes is safe.
        unchanged_safe: bool,
    },
}

/// One slot's state.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Sector {
    cache: Option<Content>,
    disk: Content,
    dirty: bool,
    bad: bool,
    hole: bool,
}

impl Sector {
    /// Whether the disk sector is unreadable.
    #[must_use]
    pub const fn bad(&self) -> bool {
        self.bad
    }

    const EMPTY: Self = Self {
        cache: None,
        disk: Content::Zero,
        dirty: false,
        bad: false,
        hole: false,
    };

    /// What a process reads, unless the sector is unreadable.
    const fn current(&self) -> Content {
        match self.cache {
            Some(content) => content,
            None => self.disk,
        }
    }

    /// The sector after its new bytes reached the disk.
    const fn landed(self) -> Self {
        let written = self.cache.is_some();
        Self {
            cache: None,
            disk: self.current(),
            dirty: false,
            bad: self.bad && !written,
            hole: self.hole && !written,
        }
    }

    /// The sector after a crash left `disk` on the platter.
    const fn crashed(self, disk: Content, kept_new: bool) -> Self {
        let written = kept_new && self.cache.is_some();
        Self {
            cache: None,
            disk,
            dirty: false,
            bad: self.bad && !written,
            hole: self.hole && !written,
        }
    }
}

/// What a slot read returns.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SlotRead {
    /// All 4096 bytes; their first sector holds this.
    Got(Content),
    /// End of file before the slot's end.
    Short,
    /// An I/O error.
    Eio,
}

/// How a failed write left the cache.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Partial {
    /// Nothing was written.
    Untouched,
    /// Part of the record's sector was written: it is garbage.
    Torn,
    /// The record's sector was written, the zeros after it were not.
    Head,
    /// (The create's 8192-byte write only.) Slot 0 was written in full, slot 1 not at all.
    FirstSlot,
}

/// What a failed fsync left on disk for one dirty block.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Reach {
    /// The old bytes.
    Old,
    /// The new bytes.
    New,
    /// A torn, unreadable sector ([`Device::Tears`] only).
    Torn,
}

/// The file and its directory entry.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Fs {
    entry_cache: bool,
    entry_disk: bool,
    len_cache: Len,
    len_disk: Len,
    slots: Pair<Sector>,
}

mod crash;

pub use crash::Reaches;

impl Fs {
    /// No file.
    pub const ABSENT: Self = Self {
        entry_cache: false,
        entry_disk: false,
        len_cache: Len::Short0,
        len_disk: Len::Short0,
        slots: Pair::new(Sector::EMPTY, Sector::EMPTY),
    };

    /// Whether the file exists for a process (`open` does not return ENOENT).
    #[must_use]
    pub const fn exists(&self) -> bool {
        self.entry_cache
    }

    /// Whether the directory entry is on disk.
    #[must_use]
    pub const fn entry_durable(&self) -> bool {
        self.entry_disk
    }

    /// The length on disk.
    #[must_use]
    pub const fn len_durable(&self) -> Len {
        self.len_disk
    }

    /// The length a process sees.
    #[must_use]
    pub const fn len(&self) -> Len {
        self.len_cache
    }

    /// The state of `slot`.
    #[must_use]
    pub const fn sector(&self, slot: Slot) -> Sector {
        *self.slots.get(slot)
    }

    /// The disk image of `slot`'s first sector.
    #[must_use]
    pub const fn disk(&self, slot: Slot) -> Content {
        self.slots.get(slot).disk
    }

    /// Reads all of `slot`.
    #[must_use]
    pub const fn read(&self, slot: Slot) -> SlotRead {
        if !self.len_cache.covers(slot) {
            return SlotRead::Short;
        }
        let sector = self.slots.get(slot);
        match sector.cache {
            Some(content) => SlotRead::Got(content),
            None if sector.bad => SlotRead::Eio,
            None => SlotRead::Got(sector.disk),
        }
    }

    /// Whether writing all of `slot` needs a new block: it extends the file, or fills a hole
    /// that no unsynced write has reserved.
    #[must_use]
    pub const fn allocates(&self, slot: Slot) -> bool {
        let sector = self.slots.get(slot);
        !self.len_cache.covers(slot) || (sector.hole && !sector.dirty)
    }

    /// `open(O_CREAT)` of a file that does not exist: an empty file, its entry not yet synced.
    #[must_use]
    pub const fn create(self) -> Self {
        if self.entry_cache {
            return self;
        }
        Self {
            entry_cache: true,
            ..Self::ABSENT
        }
    }

    /// Writes all of `slot` with `content` in its first sector (and zeros after), or fails
    /// having done `partial` of it. Under a large block the other slot's block is dirtied too.
    #[must_use]
    pub fn write(
        mut self,
        slot: Slot,
        content: Content,
        block: Block,
        partial: Option<Partial>,
    ) -> Self {
        let written = match partial {
            None | Some(Partial::Head | Partial::FirstSlot) => Some(content),
            Some(Partial::Torn) => Some(Content::Garbage),
            Some(Partial::Untouched) => None,
        };
        let Some(written) = written else {
            return self;
        };
        let sector = self.slots.get_mut(slot);
        sector.cache = Some(written);
        sector.dirty = true;
        if matches!(block, Block::Large) {
            self.slots.get_mut(slot.other()).dirty = true;
        }
        // A write that stopped inside the slot leaves the file ending inside it.
        let len = match (partial, slot) {
            (None | Some(Partial::FirstSlot), _) => Len::to_hold(slot),
            (Some(_), Slot::S0) => Len::Short0,
            (Some(_), Slot::S1) => Len::Short1,
        };
        self.len_cache = self.len_cache.max(len);
        self.normalize()
    }

    /// `ftruncate` to 8192 bytes: the length grows, and slot 1 becomes a hole if it was past the
    /// end. Only the planted create that skips writing zeros does this.
    #[must_use]
    pub fn set_full_len(mut self) -> Self {
        if !self.len_cache.covers(Slot::S1) {
            let sector = self.slots.get_mut(Slot::S1);
            *sector = Sector {
                hole: true,
                dirty: true,
                ..Sector::EMPTY
            };
            self.len_cache = Len::Full;
        }
        self
    }

    /// `fsync` or `fdatasync` returned success.
    #[must_use]
    pub fn synced(mut self) -> Self {
        for slot in Slot::BOTH {
            let sector = self.slots.get_mut(slot);
            if sector.dirty {
                *sector = sector.landed();
            }
        }
        self.len_disk = self.len_cache;
        self
    }

    /// The directory `fsync` returned success.
    #[must_use]
    pub const fn dir_synced(mut self) -> Self {
        self.entry_disk = self.entry_cache;
        self
    }

    /// A slot's sector went bad on disk (a latent sector error).
    #[must_use]
    pub fn latent(mut self, slot: Slot) -> Self {
        self.slots.get_mut(slot).bad = true;
        self
    }

    /// Drops every clean page whose bytes differ from the disk, or `None` when there is none.
    #[must_use]
    pub fn evicted(mut self) -> Option<Self> {
        let mut any = false;
        for slot in Slot::BOTH {
            let sector = self.slots.get_mut(slot);
            if !sector.dirty && sector.cache.is_some() {
                sector.cache = None;
                any = true;
            }
        }
        any.then_some(self)
    }

    /// The same state with redundant cache images dropped, so equal states compare equal.
    fn normalize(mut self) -> Self {
        for slot in Slot::BOTH {
            let sector = self.slots.get_mut(slot);
            if !sector.dirty && sector.cache == Some(sector.disk) && !sector.bad {
                sector.cache = None;
            }
        }
        self
    }

    /// Every content a slot holds, in cache or on disk, for the recorder's bookkeeping.
    pub fn contents(&self) -> impl Iterator<Item = Content> + '_ {
        Slot::BOTH.into_iter().flat_map(|slot| {
            let sector = self.slots.get(slot);
            [Some(sector.disk), sector.cache].into_iter().flatten()
        })
    }

    /// The same state with every record renamed by `rename`.
    #[must_use]
    pub fn renamed(mut self, rename: impl Fn(Rid) -> Rid) -> Self {
        let map = |content: Content| match content {
            Content::Rec(rid) => Content::Rec(rename(rid)),
            Content::Zero | Content::Garbage => content,
        };
        for slot in Slot::BOTH {
            let sector = self.slots.get_mut(slot);
            sector.disk = map(sector.disk);
            sector.cache = sector.cache.map(map);
        }
        self
    }
}
