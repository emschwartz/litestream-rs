//! The engine journal's storage contract, as an exhaustive model: two fixed 4096-byte slots
//! written alternately in one file, under the block crash law, checked at every system call.
//!
//! Written from `docs/journal.md`, never from the implementation: nothing here is shared with
//! the journal's code or the engine, so a test that runs both compares two readings of one
//! document. Where the model found that document's source (the design's fifth revision) wrong,
//! the document was amended and the finding is a named scenario.
//!
//! - [`fs`] is the file and its directory under the crash law of `crates/sim/src/crash_law.rs`
//!   (each dirty block old or new, the length rule, unsynced entries may revert), restated at
//!   the grain a slot read can tell apart, with blocks larger than 4096 bytes, a device that may
//!   leave a sector it was writing unreadable, latent sector errors, failed and partial writes,
//!   failed fsyncs that mark pages clean, eviction, and a full disk.
//! - [`world`] is the reference writer and reader, the recorder of every record issued, and
//!   the properties, each checked on the transition that can break it.
//! - [`explore`] walks every reachable state of a [`world::Config`].
//!
//! Where a property holds only under some faults, the scope is stated on the property and
//! enforced by the recorder: the strict form of [`world::Property::Recovered`] is not owed after
//! a latent sector error (the newest record may sit on the bad sector), at a boot with an
//! injected read error, or on a tearing device after a failed fsync crossed into another
//! lifetime (no local observation then says which slot is safe to overwrite).

pub mod explore;
pub mod fs;
pub mod script;
pub mod world;
