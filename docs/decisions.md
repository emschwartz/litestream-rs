# Decisions

The trade-offs this project has made, one entry each. D1 to D7 moved here from `docs/PLAN.md` section 8 and keep their ids. Entries marked **delegated** were decided by Fable (claude-fable-5-2) at the owner's request; the owner can overturn any of them.

Note: `docs/BEHAVIOR.md` section "open items" also uses ids D1 to D11 for defects. They are a different series. This file calls them "BEHAVIOR D11" and so on.

Each decision is a file under `docs/decisions/` named by its number (`D001.md`, `D160.md`). The directory listing is the index; `ls docs/decisions` shows the highest number, and the next free one is the highest plus one. A new decision is a new file, never an edit to a shared file.

## D78. A replace acknowledged short is never deleted (delegated)

- **Question:** D74 deletes every object a store acknowledges with another size than was written, so the engine never leaves a listable object it knows is short. Simulator seed 0x1dc shows the cost: a boot re-uploads a staged file the replica already holds, a read that flips a byte makes the good copy look torn, the uploader replaces it, the store acknowledges the replace one byte short, and the delete leaves the replica ending one transaction early. Restores of the latest state then silently lose an acknowledged transaction. Which writes may delete after a size mismatch?
- **Choice:** only a create-only write (`WriteMode::New`): the bucket refused a taken key before it took any bytes, so the object is the write's own. A replace deletes nothing and reports `removed: false` (`publish::settle`, and `Retrying`, which the command line wraps S3 in, through the same function). The short object makes a restore that meets it stop with a typed error until the caller writes the key again (the uploader retries, `UploadAmbiguous`) or removes it under a reason of its own (maintenance's executor deletes a short compaction output, whose inputs still cover the range). The simulator's store counts a short object a replace left as damage at once, as it counts one whose delete failed.
- **Options considered:** keep D74's delete for every write (silent loss of an acknowledged transaction, against the contract's "an error, never a shorter history"); read the key before every replace to learn whether it held something (a read per write, and a race with any other writer); re-put inside `publish` (it holds no bytes; the caller does, and retries already).
- **Cost accepted:** after a short replace, until the retry, the key holds a short object: a restore that needs it fails loudly, and a listing shows a key that does not decode, which the caller must not trust (`removed: false` says so). The replace had already overwritten the good bytes, so nothing restorable is lost by keeping it.
- **Revisit if:** a store is found that keeps the old object when a replace is acknowledged short (then the delete would have destroyed a good object and the keep is right twice over), or the engine gains a replace that knows the key was free.
- **Links:** D74, D75; `crates/core/src/publish.rs`, `crates/core/src/retry.rs`, `crates/sim/src/store.rs`; simulator seed 0x1dc (the journal session's `KnownWrong::AckForgottenAtRestart`).

## Unverified, not recorded

- Who decided D1 to D5 and when: `docs/PLAN.md` says "Decided" with no name or date. Listed as owner, by 2026-11-20.
- D8's exact wording: the owner approved adding the journal; the wording of the D6 amendment is Fable's.
- `snapshot-necessity` owner decisions D1 to D5 there (the price of an adopted journal, the catch-up file's failure class, download-and-diff, whether a handover heals, new cause names) and savings S2, S3, S5, S6, S7: no decision found.
