//! The seeded SQLite workload the application side of a fixture run applies.

use std::path::Path;

use rusqlite::Connection;

use crate::error::CompatError;
use crate::scenario::Step;

/// SplitMix64: a tiny deterministic generator, so a seed replays the same SQL anywhere.
#[derive(Debug, Clone)]
pub struct SplitMix(u64);

impl SplitMix {
    /// A generator from a seed.
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next 64 bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ z.wrapping_shr(31)
    }

    /// A value in `0..bound` (modulo bias is irrelevant for fixtures); 0 when `bound` is 0.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64().checked_rem(bound).unwrap_or(0)
    }

    /// Fills `len` bytes with printable words, so pages compress the way real text does.
    pub fn text(&mut self, len: usize) -> String {
        const WORDS: [&str; 8] = [
            "alpha", "bravo", "delta", "echo", "lima", "oscar", "tango", "zulu",
        ];
        let mut out = String::with_capacity(len.saturating_add(8));
        while out.len() < len {
            let pick = usize::try_from(self.below(8)).unwrap_or(0);
            out.push_str(WORDS.get(pick).copied().unwrap_or("x"));
            out.push(' ');
        }
        out.truncate(len);
        out
    }
}

/// The application's connection and the state the steps need.
#[derive(Debug)]
pub struct Workload {
    conn: Connection,
    rng: SplitMix,
    page_size: u32,
    next_id: i64,
    touches: u32,
}

impl Workload {
    /// Creates the database in WAL mode at the page size, with autocheckpoint off (Litestream
    /// owns checkpoints), and its schema. Replication starts after this returns.
    pub fn create(path: &Path, page_size: u32, seed: u64) -> Result<Self, CompatError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "page_size", page_size)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "wal_autocheckpoint", 0)?;
        conn.execute_batch(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER NOT NULL, note TEXT);
             CREATE INDEX items_name ON items(name);
             CREATE TABLE blobs(id INTEGER PRIMARY KEY, data BLOB NOT NULL);
             CREATE TABLE scratch(id INTEGER PRIMARY KEY, filler TEXT NOT NULL);
             CREATE TABLE marker(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
             INSERT INTO marker VALUES (1, '00000000');",
        )?;
        Ok(Self {
            conn,
            rng: SplitMix::new(seed),
            page_size,
            next_id: 1,
            touches: 0,
        })
    }

    /// Applies one step as committed transactions.
    pub fn apply(&mut self, step: Step) -> Result<(), CompatError> {
        match step {
            Step::Insert(n) => self.insert_items(n),
            Step::Update(n) => self.update_items(n),
            Step::Delete(n) => self.delete_items(n),
            Step::Blob(pages) => self.insert_blob(pages),
            Step::FillScratch(n) => self.fill_scratch(n),
            Step::DropScratch => Ok(self.conn.execute_batch("DROP TABLE scratch")?),
            Step::Vacuum => Ok(self.conn.execute_batch("VACUUM")?),
        }
    }

    /// A commit that always changes a page by a value of constant width, so a tail of any length
    /// ends in the same pages once [`Self::settle`] runs.
    pub fn touch(&mut self) -> Result<(), CompatError> {
        self.touches = self.touches.saturating_add(1);
        let v = format!("{:08}", self.touches.max(1));
        self.conn
            .execute("UPDATE marker SET v = ?1 WHERE id = 1", [v])?;
        Ok(())
    }

    /// Resets the marker to its initial value: the last commit of a run.
    pub fn settle(&mut self) -> Result<(), CompatError> {
        self.conn
            .execute("UPDATE marker SET v = '00000000' WHERE id = 1", [])?;
        Ok(())
    }

    /// The database size in pages.
    pub fn page_count(&self) -> Result<u32, CompatError> {
        Ok(self.conn.query_row("PRAGMA page_count", [], |r| r.get(0))?)
    }

    /// Frames in the WAL (`wal_checkpoint(NOOP)` reports them without checkpointing).
    pub fn wal_frames(&self) -> Result<u64, CompatError> {
        let (_busy, frames, _done): (i64, i64, i64) =
            self.conn
                .query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
        Ok(u64::try_from(frames).unwrap_or(0))
    }

    /// The page size.
    pub const fn page_size(&self) -> u32 {
        self.page_size
    }

    fn insert_items(&mut self, n: u32) -> Result<(), CompatError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt =
                tx.prepare("INSERT INTO items(id, name, qty, note) VALUES (?1, ?2, ?3, ?4)")?;
            for _ in 0..n {
                let name_len = usize::try_from(self.rng.below(24))
                    .unwrap_or(0)
                    .saturating_add(8);
                let note_len = usize::try_from(self.rng.below(120))
                    .unwrap_or(0)
                    .saturating_add(40);
                let name = self.rng.text(name_len);
                let note = self.rng.text(note_len);
                let qty = i64::try_from(self.rng.below(1000)).unwrap_or(0);
                stmt.execute((self.next_id, name, qty, note))?;
                self.next_id = self.next_id.saturating_add(1);
            }
        }
        Ok(tx.commit()?)
    }

    fn update_items(&mut self, n: u32) -> Result<(), CompatError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("UPDATE items SET qty = ?2, note = ?3 WHERE id = ?1")?;
            for _ in 0..n {
                let id = i64::try_from(self.rng.below(u64::try_from(self.next_id).unwrap_or(1)))
                    .unwrap_or(0);
                let qty = i64::try_from(self.rng.below(1000)).unwrap_or(0);
                let note_len = usize::try_from(self.rng.below(120))
                    .unwrap_or(0)
                    .saturating_add(40);
                let note = self.rng.text(note_len);
                stmt.execute((id.saturating_add(1), qty, note))?;
            }
        }
        Ok(tx.commit()?)
    }

    fn delete_items(&mut self, n: u32) -> Result<(), CompatError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("DELETE FROM items WHERE id = ?1")?;
            for _ in 0..n {
                let id = i64::try_from(self.rng.below(u64::try_from(self.next_id).unwrap_or(1)))
                    .unwrap_or(0);
                stmt.execute([id.saturating_add(1)])?;
            }
        }
        Ok(tx.commit()?)
    }

    fn insert_blob(&mut self, pages: u32) -> Result<(), CompatError> {
        let unit = self.rng.text(64).into_bytes();
        let len = usize::try_from(self.page_size.saturating_mul(pages))
            .unwrap_or(0)
            .saturating_add(123);
        let data: Vec<u8> = unit.iter().copied().cycle().take(len).collect();
        self.conn
            .execute("INSERT INTO blobs(data) VALUES (?1)", [data])?;
        Ok(())
    }

    fn fill_scratch(&mut self, n: u32) -> Result<(), CompatError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("INSERT INTO scratch(filler) VALUES (?1)")?;
            for _ in 0..n {
                stmt.execute([self.rng.text(1000)])?;
            }
        }
        Ok(tx.commit()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix_matches_its_reference_vector() {
        // First outputs of SplitMix64 seeded with 1234567 (Vigna's reference implementation).
        let mut r = SplitMix::new(1_234_567);
        assert_eq!(r.next_u64(), 6_457_827_717_110_365_317);
        assert_eq!(r.next_u64(), 3_203_168_211_198_807_973);
    }

    #[test]
    fn the_same_seed_writes_the_same_pages() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut digests = Vec::new();
        // Tails of different lengths, as timing makes them: the settled pages must agree.
        for (name, touches) in [("a.db", 2), ("b.db", 5)] {
            let path = dir.path().join(name);
            let mut w = Workload::create(&path, 4096, 7).expect("create");
            for step in crate::Scenario::Small4096.steps() {
                w.apply(step).expect("step");
            }
            for _ in 0..touches {
                w.touch().expect("touch");
            }
            w.settle().expect("settle");
            drop(w);
            let db = rusqlite::Connection::open(&path).expect("reopen");
            db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .expect("checkpoint");
            drop(db);
            digests.push(crate::pages::hash_pages(&path).expect("hash"));
        }
        assert_eq!(digests[0], digests[1]);
    }
}
