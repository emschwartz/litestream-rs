# Planted bugs

"A bug fix starts with a test that fails on the old code" is checked once, by the author. This ledger
checks it again every night: each defect fixed on `main` is put back into a scratch tree and the tests
that caught it must fail.

- `docs/planted-bugs/entries/<name>.toml` holds one entry each: the fix commit, the package, and the
  tests that must fail (fields below). One file per entry, so adding an entry never edits a file
  another open branch also appends to.
- `docs/planted-bugs/<name>.patch` re-introduces the defect. It is the fix's production change reversed
  and re-expressed against the current tree, because later renames stop the fix commit's own diff
  applying. Only production code is touched; the tests stay as they are.
- `just planted-bugs [--only NAME]` runs it: the named tests must first pass on a clean copy of HEAD,
  then fail one by one with the patch applied. A test that loops is killed after 90 s and counts as a
  kill (the table says "timeout"). Results: `killed`, `SURVIVED` (a defect in the tests), `STALE` (the
  patch no longer applies), `BUILD` (the planted tree does not compile), `BROKEN` (a named test is red
  or missing on the clean tree), `ERROR`. Anything but `killed` fails the run.
- Two canary entries check the runner itself and always run, `--only` or not: `canary-no-op` (a
  comment added) must report `SURVIVED` and `canary-fatal` (the wrong CRC table) must report
  `killed`. Before every verdict the runner also checks that the scratch tree is at this checkout's
  HEAD. A canary that misreports, or a tree at another commit, fails the run.
- One run at a time. The scratch tree and its target dir are shared, so a run first takes a lock on
  `target/planted-bugs/lock` and waits for any holder (`--no-wait` refuses instead, with exit 2).
  Every build and test process inherits the lock, so a run killed mid-way holds it until the last
  process it left exits; a new run never applies its patches under an old run's cargo or tests.
  Each build and test command runs in a session of its own, emptied when it ends or times out, so
  a test's child that a planted bug never stopped does not outlive it (Linux, through `/proc`).
- `just planted-bugs-names` (part of `just check`) fails when an entry names a test no `fn` defines, the renamed-test case that left an entry unable to run until the nightly. `just planted-bugs-diff BASE [K/N]` runs only the entries a pull request can have affected (its diff touches the entry, the patch, a file the patch edits or a file that defines a named test, or the patch no longer applies); CI runs it on pull requests in up to four shards of 12 entries, about 5 minutes for 12 entries and about 30 minutes a shard for the whole ledger.
- `just planted-bugs --shard K/N` runs every N-th entry from the K-th, plus the canaries; the nightly runs eight shards.
- `just planted-bugs-unaided [seeds]` asks a different question: with no named test run, how many
  swarm seeds does the simulator need to fail on each plant? It prints a table (seeds to first
  failure, or `none`); a plant no seed finds is a listed generator gap, not a failure.
- A new fix adds an entry and a patch in the same commit as the fix. A `STALE` patch is re-expressed
  against the current tree, or the entry is retired with a reason under "Absent and retired entries" below.
- An entry's `patch` field names another entry's patch when the same production change is planted
  against the tests of a second package (most often a `-sim` entry beside its core one), so each
  distinct patch exists once.
- `just planted-bugs-apply` runs `git apply --check` on every patch in seconds. It runs nightly
  (`.github/workflows/nightly.yml`), not in `just check`: a change that moves a line near a planted
  site does not block its land, and the stale patch is re-expressed afterwards.

Mutation testing covers the lines a pull request changes: `just mutants-diff` (CI job `mutants-diff`)
runs `cargo mutants --in-diff` over `lsr-core` and `lsr-ltx`. A missed mutant fails the job unless
`docs/mutants-escapes.txt` lists it with its reason in `docs/mutants.md`.

## Entry fields

An entry file's name is its `name` with `.toml` appended; the runner refuses one that differs.

```
  name     short unique name; the patch is docs/planted-bugs/<name>.patch
  fix      the commit that fixed it (provenance; the patch is its production change re-expressed
           against the current tree, because later renames stop the commit's own diff applying)
  package  the cargo package whose tests are run
  tests    nextest name substrings (`test(~x)`); each must fail when planted, one run apiece
  test_bin optional: restrict the run to one integration-test binary (cargo `--test`)
  features optional: cargo features to build the package with, for a test behind one that is
           not a default (a list of names)
  patch    optional: the patch's name when it is not `name`, so one defect can be planted
           against the tests of a second package
  (an entry whose `fix` is "plant" is no fixed defect: it proves a checker catches a bug class
  it was built for, with the checker's own test named)
  canary   optional, on the two canary entries only: the verdict the runner must report,
           `survived` (the no-op patch) or `killed` (the patch that breaks a test for certain).
           Any other verdict on a canary fails the run: the runner itself is broken.
  bin      optional: run the unit tests of one binary target (cargo `--bin`), for a package
           with no library, such as lsr-cli
```

## Absent and retired entries

Not here, with the reason: a6e3e38 (level-0 retention oracle: it fixed the test, retention code is unchanged, so there is nothing to plant), the verify hang fix (test-only: no production code to plant) ab0021d (a feature: reverting it does not compile). Retired with the code they planted into: staged-file-cannot-follow (bf78f6d) and staged-snapshot-of-earlier-wal (be104a7), the listing's header link between a staged file and the replica's newest, which the boot witness replaced (2fb603f); the rules they guarded are planted as the witness-* entries. page-size-after-vacuum (b352aba): its arm (an unusable newest staged file still names its page size) decides only with an empty replica, where every staged file is pending and the boot's check of staged files deletes an unusable one first. other-writer-stop-forgotten-sim (a9da6c6): since a boot refuses replica files that neither the WAL nor a takeover record vouches for, a restart after a stop for another writer's file above staging stops again with or without the stop record, so the scenario cannot tell; the record still decides the case of another writer at the engine's next id, which the core entry plants. replica-page-size-unread-sim (7bf9707): seed 0x13's run no longer reaches the boot it pinned once the engine asks for a takeover record before deleting staged files (one more tick changes the interleaving); the test stays as a regression run, and the core entry plants the defect. other-writer-stop-forgotten (a9da6c6) and stop-record-not-kept (a939338): the stop record they planted into is gone (68d2d90). A restart after a stop for another writer now stops by the boot's refusal: of files above staging (boot-gate-refuses-nothing) or of a divergence under the engine's next id (boot-divergence-snapshots-above-another-writer). regrown-wal-one-commit-sim (a8092ab): it planted the simulator's model, which records a commit at every mark of a WAL regrown with its salts. The seed-0x2b2 history reached it only through a running engine whose position sat at the start of the regrown generation, after a bucket rollback and a power loss left it there (D173 removed the handling of both). Today the engine's own frame follows every WAL restart, so a regrowth always rewrites a frame the engine has staged and the engine snapshots at the committed end, which the model already knows; no scenario written against today's engine stages a file at an earlier regrown mark, and 1,107 unaided swarm seeds found no failure with the plant applied. The recorded history stays as a plain regression run. journal-created-only-on-debt-cli (6f700f9a), retired with the confirmed floor (D107): a sidecar run that replicates confirms an upload, which raises the floor, so every such run writes a record whatever the "neither read nor wrote" rule does, and no end-to-end test can see the rule; journal-created-only-on-debt plants it against core runs. The floor itself left the engine with D173, so that retirement now stands only as history. failstop-no-commit-hook (c8b9c90), gate-hides-a-taken-hook (70f61c6) and failstop-stop-app-without-exec-allowed (c5881f5), retired with the commit hook and the refusal of `stop-app` without `-exec` (`docs/decisions/D192.md`): writes are now held by a write lock the replicator keeps, which every writer meets whatever set it up, so there is no hook to leave out or replace and no refusal to drop; gate-never-taken and gate-never-taken-replica plant the lock not taken, and gate-leaks-its-flag is re-expressed against the busy handler. failstop-judged-before-the-boot-snapshot (c5881f5), retired when the rule it broke was reversed (`docs/decisions/D201.md`): the boot is now judged once it has decided how to start, before its snapshot is staged, which is what the plant did; failstop-held-until-the-boot-snapshot plants the old rule as the defect. level-jumps-to-the-newest-input-sim (claude/level-holes), retired when a level compacts once per window (`docs/decisions/D242.md`): the scenario needed several level-1 files in one window followed at once by a level-2 compaction from a carried listing, which now takes a backlog of more than 32 level-0 files, and the engine writes the level-2 file after the last of them from a fresh listing, so the planted carry no longer decides the outcome; the core entry level-jumps-to-the-newest-input plants the same defect. The scenario stays as a regression run. sweep-lists-staging-whole (3853a92), retired with `sweep_staged`: the boot's sweep and its witness pass became one pass (`docs/decisions/D219.md`), and witness-lists-staging-whole plants that pass listing staging whole. sim-course-file-not-read-in-slot (claude/boot-local), retired with the slot read it planted into (`docs/decisions/D263.md`): a local course no longer stages a snapshot, so no file in a slot is a full image and reading the slot changed no answer. boundary-snapshot-keeps-page-table (claude/boundary-snapshot-pages), retired with the snapshot after a TRUNCATE it planted into (`docs/decisions/D263.md`): a TRUNCATE now takes no snapshot, so no page check runs at that boundary; truncate-proof-after-an-unseen-reset and the other truncate-proof plants guard what replaced it. compaction-writes-over-its-output (40f58942), retired with the read of a compaction's output key before its write (`docs/decisions/D246.md`): the read guarded a key a listing did not show, which only a lagging listing could leave (D173), and compaction-reads-its-output-key-first plants the read back as the defect. part-mismatch-deletes-the-key (claude/staged-parts) and rejected-replace-deletes (40f58942), retired with the delete after a rejected level-0 write they planted into (`docs/decisions/D280.md`): no rejected write deletes its key now, and rejected-write-deletes-the-key plants the delete back for every rejection, killed by the same tests for a rejected part and a rejected replace.

## Plants cited by behaviour rows

A behaviour row (`docs/behavior.toml`, rendered into `docs/BEHAVIOR.md`) that cites `plant::<name>`
claims its tests kill that entry's bug class, so it must also cite a test the entry runs: the
entry's package and a test filter that matches (`crates/compat/tests/compat/behavior.rs`, in `just
check`). Through `just evidence-fresh`, against `cargo nextest list`, every cited test must exist
and each cited plant must be backed by one that is not ignored, since this ledger's run starts no
ignored test. The nightly run of the ledger then makes each such citation true.
