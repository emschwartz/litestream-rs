# The engine journal: storage contract

Status: the storage protocol is specified, machine-checked, and implemented. The engine reads the
journal at boot and reports what it found (`Actor::journal`, `docs/BEHAVIOR.md` BOOT-25 to BOOT-29),
and records two facts in it: the unproven-WAL debt after a failed WAL fsync, written at the end of the
tick that changes it and restored at boot (`crates/core/src/actor/unproven.rs`, RUN-3, decision D100),
and a boot's verdict that the database file was ahead of its WAL, kept for seven days
(`crates/core/src/actor/ahead.rs`, BOOT-31, decision D103).
The replica thread, the simulator and the real-SQLite rig each lend it.

The model is `lsr_spec::journal` (`crates/spec/src/journal/`). It shares no code with the
implementation. Its tests are `crates/spec/tests/spec/journal.rs` (exhaustive walks and plants)
and `journal_scenarios.rs` (named histories). `just journal-model` runs the deep walk.

## The file

One file per database, `<meta>/lsr-journal`, never renamed, never unlinked, never sent to the
replica. 8192 bytes: slot 0 is bytes 0 to 4096, slot 1 is 4096 to 8192. A slot holds one record
in its first 512 bytes (magic, version, length, sequence number, body, CRC-32C over all of it)
and zeros after. The body is the five facts: the floor (destination digest, bound, optional
anchor), the unproven-WAL debt, the snapshot budget's two numbers, at most one pending proof, and
at most one verdict that the database file was found ahead of its WAL (its restore point, page
count, first page and time; `docs/decisions.md` D103). A record holding the verdict is version 2;
every other record is version 1, so a binary that predates the verdict reads it as `Newer` only
while a verdict stands. While two verdicts stand, the later keeps the earlier's restore point; the
writer refuses a record that moves it forward.

## The floor in the engine

The engine raises the floor with its uploader's confirmed transaction, for the destination its
configuration names (`ActorConfig::destination`: the sidecar's digest of the replica's endpoint,
bucket and prefix, or its directory), anchored on the confirmed file's length, raw-byte digest and
file checksum when this process holds that file's witness, and writes it at the end of the tick
(`crates/core/src/actor/floor.rs`). A boot reads it back for the same destination only; a listing
that ends lower does not lower it. A rollback seen while running withdraws the anchor and keeps
the bound: the ids stay taken. Another destination's floor stays in the record until this one
has its own, since a floor is never dropped; with no destination named the engine keeps none and
a recorded one passes through (`docs/decisions.md` D107). What a boot does with the floor it
reads is issue #171, item 4's boot rule.

## Reading

- `open`. ENOENT is `Missing`. Any other error is `Unreadable(Io)`, never `Missing`.
- Each slot is read with its own call. A slot is valid when the read returns all 4096 bytes and
  the magic, length and checksum check. A newer version's slot is valid on magic, length and
  checksum alone. A short read, an I/O error or a bad checksum makes that slot invalid and
  leaves the other alone.
- No valid slot: `Unreadable` (`Io` if a read failed, else `Invalid`). Two valid slots with one
  sequence number: `Unreadable(Tie)`; no writer produces it.
- Otherwise the valid slot with the higher sequence number wins: `Read`, or `Newer` when its
  version is above this binary's.
- A boot that saw an I/O error on either slot reports it as a logged event: the slot that failed
  may hold the newest record, so the facts read may be older than the last ones written.

`Missing`, `Unreadable` and `Newer` are typed events after which the engine runs main's rules
with absent facts (no floor, no debt known, a full budget, no proof).

## Writing

A process writes a record when the facts it holds differ from the newest record it read at boot
or made durable, and also when it has neither, so a journal exists after the first tick of every
run and a missing one at boot means it was lost (D101 in `docs/decisions.md`).

The writer keeps, per process: the slot holding the newest record it knows, its sequence
number, whether creation is known finished, whether this lifetime has synced, and the newest
record it has itself made durable (none at boot).

1. **Establish** (`Missing`, `Unreadable`, or a boot that saw a slot fail to read). If the file
   exists, first write 8192 zeros and `fdatasync`. Then `open(O_CREAT)`, one write of 8192
   bytes (the record with sequence 1 in slot 0, zeros in slot 1), `fsync`, `fsync` of the
   directory. The record is durable when the directory `fsync` returns.
2. **Finish** (the winner has sequence 1). Creation may not have returned in any lifetime, so
   before any record with sequence 2: write zeros into the other slot (extending a short file),
   `fsync`, `fsync` of the directory. The winner's slot is not touched.
3. **Boot sync** (the first write of a lifetime, after a winner was read). `fdatasync` before
   writing, so a slot an earlier process wrote and did not sync is on disk before the other
   slot is overwritten.
4. **Write**. The record with the next sequence number into the slot that does not hold the
   newest record, then `fdatasync`. On success the record is durable and the next write goes to
   the other slot. On any failure nothing moves: the next attempt rewrites the same slot with
   the same sequence number. A failed `fdatasync` is never retried bare.
5. **Monotonic.** Before a write the writer checks the new record against the last one it read
   or wrote: the same destination's bound does not fall, a floor is not dropped, the debt only
   is never forgiven: it is paid only through its cure (TRUNCATE owed, then snapshot owed, then none), and a new WAL fsync failure may raise it again at any step. A record that fails
   is refused as a bug (`invariant!`), never written.

Sequence 1 means "creation may not have finished"; sequence 2 or more proves a creation returned
in some lifetime, because no slot write is issued before it does. Every future version must keep
that rule, or a journal it leaves behind would be taken as finished when it is not.

After creation no write extends the file or fills a hole, so on a filesystem that overwrites in
place (ext4, xfs without reflinks, overlay on either) a full disk refuses no record. Creation
writes real zeros for that reason, never `ftruncate`.

## What the disk may do

The model's filesystem is `crates/sim/src/crash_law.rs`'s block law, restated at the grain a
slot read tells apart: each block a write dirtied since the last sync keeps its old or new bytes
at a power loss; the length keeps the synced value, the current one or a block boundary between;
an unsynced directory entry may revert; the cache empties. Added to it:

- **Blocks** of 512, 4096, and 8192 bytes or more. At 8192 and above both slots share a block, so
  writing one rewrites the other's sectors with the same bytes.
- **Devices.** The atomic device keeps each sector old or new. The tearing device may also leave
  one sector it was writing unreadable at a power loss or a failed `fsync`.
- **A failed `fsync`** leaves each dirty block old, new or (tearing) torn, and marks every page
  clean while the cache keeps the new bytes; a later bare `fsync` returns success and writes
  nothing.
- **Eviction** of clean pages at any time, **latent sector errors**, transient read errors on
  `open` or one slot, failed and short writes, a failed directory `fsync`, and a disk that fills.
- **A newer binary** that writes one record by the same rules, and a **destination change**.

Three assumptions, each with a test that shows the writer needs it:

1. **A slot write is page-atomic in the cache**: a failed write of one aligned 4096-byte slot from
   a resident buffer changes nothing. Linux copies into the page cache page by page and reserves
   space before the copy; a copy stops short only on a fault in the source buffer. Without it,
   a boot after a failed `fdatasync` can tear the acknowledged record
   (`the_writer_needs_page_atomic_slot_writes`).
2. **On a block of 8192 bytes or more, the device does not destroy a sector it rewrites with the
   same bytes** (review 5, J3). Without it a power loss can lose the durable record
   (`a_large_block_needs_a_device_that_spares_unchanged_sectors`). At 512 and 4096 bytes nothing
   depends on it: no write touches the block of the newest durable record.
3. **A failed directory `fsync` is not marked clean**: a later one that succeeds persists the
   entry. Directory metadata is journalled; a failed commit is retried or aborts the filesystem.

## Properties

Checked on every transition of every state the budget reaches, at every system call, with every
crash image, followed by a recovery that is itself explored to the end of the budget.

| Property | Statement |
|---|---|
| `Recovered` | No boot reads a record older than the newest one acknowledged or claimed durable, and none reads nothing once one was. Order is the order records were issued, never their sequence number. Not owed after a latent sector error, at a boot with an injected read error, after a logged reset, or on a tearing device after a failed `fsync` crossed into another lifetime |
| `MissingAfterAck` | A boot finds no file only when no record was ever acknowledged |
| `EioNotMissing` | A read error never turns into `Missing` |
| `SlotSurvives` | A slot that reads valid on its own is never lost to an error in the other |
| `FloorMonotone` | The floor read at boot or written never falls below the acknowledged floor, unless a reset was logged since (a changed destination, a verdict other than `Read`, a slot that failed to read) |
| `CreateFinished` | A record is acknowledged only once the file's directory entry and its full length are durable |
| `NoAllocation` | No write of a record with sequence 2 or more needs a new block |
| `NoTie` | Two valid slots never share a sequence number |

The walks: the block law, the tearing device and unreadable sectors, at each block size, with
four engine writes, two crashes, two failures, an eviction, a destination change, a newer
binary's record and a full disk (100 000 to 400 000 states each, under a second). The deep walk
(`just journal-model`) adds a write, a crash and media faults to every configuration: 7.5 to 12.2
million states each, about five minutes in all.

## The implementation against the model

The differential test walks every state of the model within a budget (the block law at each
block size, the tearing device, unreadable sectors) and keeps the real writer beside it, over a
byte-level file that applies the model's outcome for every system call in bytes. Each write is
replayed with the model's outcomes scripted; the writer must make the same calls in the same
order, and the file (read as records), the writer's position and every boot's verdict must equal
the model's. Thirteen planted bugs in `docs/planted-bugs.toml` (`journal-*`) put back each rule of
the writer, the codec and the model, and the named tests must fail.

## Plants

Each is a defect in the reference writer; each test first walks the same configuration clean,
then planted, and requires the named property to break.

| Plant | Caught by |
|---|---|
| No directory `fsync` at creation | `CreateFinished` |
| A sequence-1 winner taken as a finished creation (J1) | `CreateFinished` |
| Recreate without zeroing first | `Recovered` |
| Overwrite the slot holding the newest record | `Recovered`, tearing device |
| A read error reported as `Missing` | `EioNotMissing` |
| Both slots read with one call (J2) | `SlotSurvives` |
| Boot claims the winner durable | `Recovered` |
| A failed `fdatasync` taken as success | `Recovered` |
| A bare `fdatasync` retry | `Recovered` |
| No `fdatasync` | `Recovered` |
| The lower sequence number wins | `Recovered` |
| Creation by `ftruncate` (a hole) | `NoAllocation` |
| No boot sync | `Recovered`, tearing device |
| No monotonic check, with an engine that lowers the floor | `FloorMonotone` |

Overwriting the newest slot survives the atomic block law at six writes: a record in one sector
is never torn there. The second slot exists for devices that tear.

## Findings and where each is checked

Every finding of the five reviews, and every finding of the model. "Engine" means the finding is
about what the engine does with a fact, not how it is stored; it is checked in the step of the
design's migration named, not here. "Scheduling" findings belong to the scheduling note, which
is out of scope for the journal.

| Finding | Where |
|---|---|
| R1.1 budget reset on settle | Scheduling. The storage half (a spend once acknowledged survives) is `Recovered` |
| R1.2 `Adopted` charges a TRUNCATE | Engine, step 2 ("a missing journal costs nothing"). The model's absent verdicts carry no debt |
| R1.3 disk full blocks prune | `NoAllocation`; `rev4_every_record_after_creation_is_written_on_a_full_disk`. Prune under the in-memory mark when unwritable: engine, step 3 |
| R1.4 proof rationale, `Dropped` hole | No log any more. "A withdrawal is never less durable than the proof" is `Recovered` |
| R1.5 floor table not total | Engine, step 3 |
| R1.6 laundering; lock on a replaced inode; failed directory fsync; two marks; clock behind; fold order; parse split | Laundering and fold: moot. Inode: the writer never unlinks or renames (no such call exists in the model; `model_a_recreate_zeroes_the_file_first`). Directory fsync: `DirFailed` in every walk, `CreateFinished`. Two marks: `model_a_lifetime_claims_nothing_durable_before_it_writes`. Clock: scheduling. Parse split: `Verdict` is the parse; the judge is engine, step 2 |
| R1.7 bindings (d), (e) | Moot (engine) |
| R1.8 `Verified`; "the write must succeed"; powersafe overwrite | `Verified`: moot. Write: `rev1_a_record_whose_fdatasync_failed_is_read_by_the_next_process`. Powersafe: assumption 2, J3 |
| R1 testing gaps | ENOSPC: `Fill`. Failed directory fsync then a write then power: every walk. Lock race: engine, step 2. `R == J`, clock: engine, scheduling |
| R2.1 budget bound | Scheduling |
| R2.2 "the write, not the sync" for a rename | In place now: `rev1_a_record_whose_fdatasync_failed_is_read_by_the_next_process` |
| R2.3 pending rule | Engine, step 4 |
| R2.4 scratch name and Go's `*.tmp` | Moot: one name, no scratch |
| R2.5 loose rules | Engine and scheduling |
| R2.6 simplicity | Taken by the design |
| R2.7 lock limits | Engine, step 2 |
| R2.8 power loss in the lifetime law; plants | Every walk crashes at every system call. Bare retry: `planted_bare_fdatasync_retry_is_caught`. Debt-order plants: engine, step 5 |
| R3.1 regressed states without exit; no replica identity | `rev3_a_destination_change_is_a_logged_reset`; `FloorMonotone` exempts a changed destination. Exits: engine, step 3 |
| R3.2 decay constant | Scheduling |
| R3.3 boot id against the WAL's chain | Engine, step 4 (binding c) |
| R3.4 pending slot as a function | Engine, step 4 |
| R3.5 maintenance first | Scheduling |
| R3.6 unwritable journal; debt at ENOSPC | `NoAllocation`, `Fill`; "unwritable is main": engine, step 2 |
| R3.7 rename-regardless keeps the debt only while cached | `rev3_an_evicted_unsynced_record_falls_back_to_the_acknowledged_one`; eviction in every walk |
| R3.8 byte form of "no worse than main"; `PutBackOlder`; conservative debt | Engine. `PutBackOlder` is an operator act, listed under "Does not fix" |
| R4.1 first turn | Scheduling |
| R4.2 the scratch protects one write | Two slots: `NoAllocation` in every walk; `planted_create_by_set_len_is_caught`; the full-disk scenario |
| R4.3 binding (c) clauses | Engine, step 4 |
| R4.4 budget plants without killers | Scheduling |
| R4.5 healing by identical bytes; probe gap; naming; `FloorForeign` and back; rollback while running | `FloorForeign`: `rev3_a_destination_change_is_a_logged_reset`. The rest: engine |
| J1 an interrupted create read as finished | `CreateFinished`, `NoAllocation`; `j1_a_kill_between_the_two_create_fsyncs_is_finished_by_the_next_boot`, `j1_a_short_file_is_extended_before_the_second_record`; plants sequence-1 and no directory fsync |
| J2 the read path cannot express the read rule | `SlotSurvives`, `EioNotMissing`; `j2_*`; plants one read and EIO-as-missing |
| J3 large blocks | Assumption 2; `j3_*`; `a_large_block_needs_a_device_that_spares_unchanged_sectors` |
| J4 no crash-at-every-step test | This model. The oracle is stated in issue order (`Rid`), never by parsing a sequence number |
| J5 monotonic on every replace | `FloorMonotone`; `j5_the_writer_refuses_a_record_whose_floor_falls`; plant no monotonic check. The boot half ("the two valid slots must be adjacent") is not taken: `j5_a_failed_fdatasync_and_a_kill_leave_the_slots_more_than_one_apart` reaches 5 and 2 legitimately |
| J6 the lock before the file exists | Engine, step 2 |
| J7 tie rule; newer validity; count form; short writes; pending gate | Tie: `NoTie`, `Unreadable(Tie)`. Newer: `a_newer_binary_s_record_is_reported_and_the_other_slot_overwritten`. Short writes: assumption 1. Count form and gate: engine |
| B1 to B5, T1 to T3 | Scheduling note: out of scope |
| M1 a recreate restarts at sequence 1 over records the disk may still hold | Recreate zeroes and syncs first; `model_a_recreate_zeroes_the_file_first`; plant recreate without zeroing |
| M2 slots drift apart after a failed `fdatasync` and a kill, and a read error then restarts the numbering below a record on disk | A boot that saw a slot fail to read re-establishes; `model_a_boot_that_could_not_read_a_slot_re_establishes`; found by the deep walk |
| M3 on a tearing device a kill between a write and its `fdatasync` is enough to lose the acknowledged record (the design's table needed a failed sync and an eviction too) | Boot sync; `model_a_lifetime_syncs_before_its_first_write_on_a_tearing_device`; plant no boot sync |
| M4 "boot takes the winner as durable" lets the engine act on a record no disk holds | No durable claim before the lifetime's own write; `model_a_lifetime_claims_nothing_durable_before_it_writes`; plant claim winner |
| M5 a torn write in the page cache | Assumption 1 |
| M6 the sequence-1 rule binds every version | "Writing", above |
| M7 a slot that fails to read can hide the newest record | Logged as an event; `FloorMonotone` and `Recovered` scoped to it |
| M8 the second slot is for tearing devices only | `planted_wrong_slot_survives_the_block_law_alone` |
| M9 rewriting the winner during the finish would make 512 and 4096-byte blocks depend on assumption 2 | The finish touches only the other slot; the destructive device walks clean at both sizes |

## Does not fix

Everything in the design's list, plus: on a tearing device, a failed `fdatasync` followed by a
kill before any successful one leaves no local evidence of which slot is safe to overwrite, so
the next write can lose the acknowledged record to a torn sector (the reader then gets an older
record, sound field by field, or `Unreadable`). A latent sector error under the newest record
costs that record (logged).

## Working names

User-visible strings need the owner's approval. Approved: `JournalMissing`, `JournalUnreadable`,
`JournalNewer`, `JournalSlotUnreadable`, `JournalUnwritable`; the `/healthz` fields `journal` and
`journal_detail`, the gauge `lsr_journal_signal{db,signal}`, and its values `missing`,
`unreadable`, `newer_version`, `slot_unreadable`, `unwritable`.
