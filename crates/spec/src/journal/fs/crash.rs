//! What a power loss or a failed fsync leaves on disk: every image the law allows.

use super::{Block, Content, Device, Fs, Len, Pair, Reach, Sector, Slot};

/// What one slot's disk image became in a crash or a failed fsync.
pub type Reaches = Pair<Option<Reach>>;

impl Fs {
    /// Every state a power loss can leave, each with what became of each dirty slot.
    #[must_use]
    pub fn power_losses(&self, block: Block, device: Device) -> Vec<(Self, Reaches)> {
        let mut out = Vec::new();
        if !self.entry_cache {
            return vec![(Self::ABSENT, Pair::new(None, None))];
        }
        if !self.entry_disk {
            out.push((Self::ABSENT, Pair::new(None, None)));
        }
        for len in self.kept_lengths(block) {
            for reaches in self.reach_choices(block, device) {
                let mut fs = self.landing(reaches);
                fs.entry_disk = true;
                fs.len_cache = len;
                fs.len_disk = len;
                for slot in Slot::BOTH {
                    if !len.covers(slot) {
                        *fs.slots.get_mut(slot) = Sector::EMPTY;
                    }
                }
                if !out.iter().any(|(seen, _)| *seen == fs) {
                    out.push((fs, reaches));
                }
            }
        }
        out
    }

    /// Every state a failed fsync can leave: the disk as for a power loss, every page clean, the
    /// cache still holding what the process wrote.
    #[must_use]
    pub fn sync_failures(&self, block: Block, device: Device) -> Vec<(Self, Reaches)> {
        let lens = if self.len_cache == self.len_disk {
            vec![self.len_disk]
        } else {
            vec![self.len_disk, self.len_cache]
        };
        let mut out: Vec<(Self, Reaches)> = Vec::new();
        for len_disk in lens {
            for reaches in self.reach_choices(block, device) {
                let mut fs = self.landing(reaches);
                fs.len_disk = len_disk;
                fs.len_cache = self.len_cache;
                for slot in Slot::BOTH {
                    let current = self.slots.get(slot).current();
                    let sector = fs.slots.get_mut(slot);
                    if current != sector.disk || sector.bad {
                        sector.cache = Some(current);
                    }
                }
                let fs = fs.normalize();
                if !out.iter().any(|(seen, _)| *seen == fs) {
                    out.push((fs, reaches));
                }
            }
        }
        out
    }

    /// The lengths a crash may keep: the synced one, the current one, or a block boundary
    /// between, in the classes a reader tells apart.
    fn kept_lengths(&self, block: Block) -> Vec<Len> {
        let (from, to) = (self.len_disk, self.len_cache);
        if from >= to {
            return vec![to];
        }
        match block {
            Block::Large => vec![from, to],
            Block::B512 | Block::B4096 => [Len::Short0, Len::Short1, Len::Full]
                .into_iter()
                .filter(|len| *len >= from && *len <= to)
                .collect(),
        }
    }

    /// Every way the dirty blocks can land: old or new, jointly under a large block, and on a
    /// tearing device at most one sector it was writing left unreadable.
    fn reach_choices(&self, block: Block, device: Device) -> Vec<Reaches> {
        let dirty = |slot: Slot| self.slots.get(slot).dirty;
        let both = [Reach::Old, Reach::New];
        let mut choices: Vec<Reaches> = Vec::new();
        let per_slot = |slot: Slot| -> Vec<Option<Reach>> {
            if dirty(slot) {
                both.map(Some).to_vec()
            } else {
                vec![None]
            }
        };
        match block {
            Block::Large if dirty(Slot::S0) || dirty(Slot::S1) => {
                for reach in both {
                    let joint = |slot: Slot| dirty(slot).then_some(reach);
                    choices.push(Pair::new(joint(Slot::S0), joint(Slot::S1)));
                }
            }
            Block::Large | Block::B512 | Block::B4096 => {
                for r0 in per_slot(Slot::S0) {
                    for r1 in per_slot(Slot::S1) {
                        choices.push(Pair::new(r0, r1));
                    }
                }
            }
        }
        let Device::Tears { unchanged_safe } = device else {
            return choices;
        };
        let tearable = |slot: Slot| {
            let sector = self.slots.get(slot);
            sector.dirty && (sector.current() != sector.disk || !unchanged_safe)
        };
        let mut torn = Vec::new();
        for choice in &choices {
            for slot in Slot::BOTH.into_iter().filter(|slot| tearable(*slot)) {
                let mut with = *choice;
                *with.get_mut(slot) = Some(Reach::Torn);
                torn.push(with);
            }
        }
        choices.extend(torn);
        choices
    }

    /// The disk after each dirty slot landed as `reaches` says; the cache emptied, every page
    /// clean, the entry and the lengths as they were.
    fn landing(&self, reaches: Reaches) -> Self {
        let mut fs = *self;
        for slot in Slot::BOTH {
            let sector = *self.slots.get(slot);
            let landed = match reaches.get(slot) {
                None => Sector {
                    cache: None,
                    dirty: false,
                    ..sector
                },
                Some(Reach::Old) => sector.crashed(sector.disk, false),
                Some(Reach::New) => sector.crashed(sector.current(), true),
                Some(Reach::Torn) => Sector {
                    bad: true,
                    ..sector.crashed(Content::Garbage, true)
                },
            };
            *fs.slots.get_mut(slot) = landed;
        }
        fs
    }
}
