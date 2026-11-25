# How litestream-rs was built

The code and these documents were written by Claude models, working as Claude Code agents under the direction of the repository's owner. This page says what the method was, what it caught, and what it has not proven. It was written by Claude (claude-fable-5-2). Numbers are from main at bc16a7121 on 2026-11-24; the command that produced each is in a comment beside it.

## Who wrote what

The owner set the goal, the contract and the scope, and decided the trade-offs that were the owner's to decide. Agents wrote the plan, the code, the tests and the documents, opened the pull requests and reviewed one another's work.

- Main holds 1,156 commits, the first dated 2026-11-20. <!-- git rev-list --count bc16a7121; git log --reverse --format=%ad --date=short | head -1 -->
- Commit trailers name three models: Claude Opus 5.5 (claude-opus-5-5) on 747 commits, Claude Sonnet 5.5 (claude-sonnet-5-5) on 122 and Claude Fable 5.2 (claude-fable-5-2) on 99. <!-- git log --format=%b | grep Co-Authored-By | sed 's/<.*//' | sort | uniq -c -->
- The workspace is 15 crates and about 218,000 lines of Rust, tests included. <!-- ls crates | wc -l; find crates -name '*.rs' | xargs cat | wc -l -->
- Each trade-off is one file under `docs/decisions/`: 131 of them, each with the choice, the reason for it and who decided. Many are marked delegated: an agent decided for the owner, who can overturn it. <!-- ls docs/decisions | wc -l -->

The pace is the first thing to weigh. This is days of work, not years of use, and nothing below replaces time in production.

## The contract

The agents were not asked to copy Go Litestream's behaviour. They were asked to meet a contract that a test can check:

- **The replica is the contract:** the bucket layout and the LTX files. The reader is tolerant: it reads everything Go v0.5.0 and later wrote. The writer is strict: Go v0.5.16 and later must read what it writes.
- **Equality is on restored database pages**, never on file bytes, because the two LZ4 compressors differ.
- **Never silently wrong.** A request that cannot be met exactly is an error, never a shorter history.

Inside that boundary the port may differ from Go where Go is wrong, and each difference is written down ([differences-from-go.md](differences-from-go.md)). The plan began from a classification of the fixes Go Litestream shipped after v0.5.0, sorted by what kind of check would have caught each one ([PLAN.md](PLAN.md), section 4).

## The layers of evidence

**Types and lints.** States, faults and causes are closed enums, and every match on them is exhaustive, so a new case breaks the build where it matters. Non-test code may not unwrap, panic, index, cast or do unchecked arithmetic; `unsafe` is allowed only in the SQLite crate. The core makes its decisions as pure functions and reaches time, randomness, the store and SQLite only through traits, which is what lets a simulator drive every path.

**The test suite.** 3,135 tests, 180 of them outside the land gate because they need Docker, Go, a real provider or minutes of time; named recipes run those. <!-- just evidence-fresh; wc -l target/evidence/all.txt target/evidence/ignored.txt --> A test that needs something outside the process fails when it is missing. It never passes by skipping.

**Differential tests against Go.** Pinned Go binaries are built from source. Go writes and Rust restores, Rust writes and Go restores, Go's VFS reads a Rust replica, and the two take turns on one bucket path with a kill at a random point in each handover. Every case ends by comparing restored pages. A nightly job runs this against each of the 19 releases from v0.5.0 to v0.5.18 and against upstream's main branch. <!-- git -C .upstream/litestream tag | grep -cE '^v0\.5\.[0-9]+$' --> Each of Go's 1,474 own tests is mapped to a Rust test that checks the same behaviour or to a stated reason it does not apply, and the gate fails on an unmapped one. <!-- grep -c . docs/upstream-test-map/upstream-tests.txt -->

**A deterministic simulator.** `crates/sim` runs the real engine against an in-memory store, a model of SQLite's WAL and a seeded schedule of faults: crashes, power loss, store errors, clock jumps, damage to local files, a second writer. Two judges check every run. One restores each transaction and compares it with a model's page images. The other (`crates/history`) reads only restored content and a log of commits, and shares no code with the engine. A fixed set of seeds runs on every commit and a rotating set every night.

**Real SQLite and the real binary.** The simulator's SQLite is a model, so three rigs use the real thing: crash cycles on real SQLite under a block-level power-loss law, the shipped binary on a filesystem that drops unsynced writes (LazyFS, nightly), and the shipped binary beside a writer with its replica on an S3 server behind a fault proxy, under kills, partitions, slow and cut transfers, and a Go Litestream started on the same path ([decisions/D195.md](decisions/D195.md)).

**Planted bugs.** A bug fix starts with a test that fails on the old code. To keep that true, the defect is kept as a patch, and every night each patch is applied to a scratch tree, where the tests named for it must fail. The ledger holds 223 entries, two of them canaries that check the runner itself. <!-- ls docs/planted-bugs/entries/*.toml | wc -l --> A regression test is a short written scenario, never a seed number ([decisions/D165.md](decisions/D165.md)).

**A behaviour table.** [BEHAVIOR.md](BEHAVIOR.md) is rendered from `docs/behavior.toml`: 235 rows, one for each circumstance, with what the engine does, the signal it raises and the tests behind the claim. <!-- grep -c '^\[\[row\]\]' docs/behavior.toml --> The gate checks that every cited test exists and that the rendered page matches its rows. A row with no test says so: untested, unknown, or known wrong.

**The nightly run.** Fuzzing of the decoders, the WAL reader and the restore planner (eight targets, four of which have had a 24-hour run with no finding), Miri, the address and thread sanitizers, schedule exploration of the replica thread and the S3 store's batches, memory gates at one to four GiB, and the land gate on macOS. <!-- ls fuzz/fuzz_targets | wc -l; .github/workflows/nightly.yml -->

**Real providers.** A conformance suite, create-only write races, a consistency probe and error classification ran once each against AWS S3, Cloudflare R2 and Tigris on 2026-11-24 ([spikes.md](spikes.md)).

**Reviews.** Agents with a fresh context and no stake in the code reviewed it, twice across the whole project: a skeptical review by seven read-only reviewers ([#197](https://github.com/emschwartz/litestream-rs/issues/197)) and a review of the correctness claims in the manner of a Jepsen analysis ([#328](https://github.com/emschwartz/litestream-rs/issues/328)). A model of another family (Codex) gave a second opinion on one design decision ([decisions/D190.md](decisions/D190.md)).

## What the method caught

- **Two writers at once.** On its first day the fault rig found that Go Litestream and `lsr` writing one bucket path together leave restores that are wrong with no error ([#346](https://github.com/emschwartz/litestream-rs/issues/346)). The docs had claimed the bucket would refuse the second writer.
- **Provider differences.** The provider checks found that all three providers report a wrong clock in a way the engine read as a permission error (fixed), and that R2 lists some names out of byte order ([#342](https://github.com/emschwartz/litestream-rs/issues/342)).
- **Tests that were not running.** The Jepsen-style review found the test behind the second-writer claim switched off though it passed, and a failing test for a known defect switched off so that nothing showed it. The first now runs; the second is still open ([#12](https://github.com/emschwartz/litestream-rs/issues/12)).
- **Stops in ordinary deployments.** The same review ran the real binary and found two ordinary-looking sequences that stopped replication where Go carries on. One was changed ([decisions/D194.md](decisions/D194.md)); the other is by design and is listed in [limitations.md](limitations.md).
- **Its own excess.** About a third of the engine's fix commits had been repairing defences against accidents that Go does not handle either. The owner cut those defences and wrote down what version 1 handles and what it leaves out ([decisions/D173.md](decisions/D173.md)).

## What it has not proven

- **That unknown bugs get caught.** The planted bugs are defects someone already found and fixed. The one blind measurement is old and small: the simulator alone found 7 of 22 bugs planted with no test written for them. The nightly seeds ran for days without reaching the defect in [#12](https://github.com/emschwartz/litestream-rs/issues/12), which a reviewer wrote down by hand; a simulator run reached it unaided only on 2026-11-24.
- **That the simulator's beliefs are true.** The engine and the simulator's model of SQLite were written by the same kind of author from the same reading of SQLite. The real-SQLite rigs and the Go comparison reduce that risk and do not remove it. The power-loss law is a model too, never compared with what real disks leave.
- **That it holds up over time.** No real application has run on it. The longest run on a real provider was 12 hours, at older code. The providers were checked once, from one region.
- **That an outsider agrees.** Every whole-project review was by a Claude model. No person outside the project, and no model of another family, has reviewed the whole of it.
- **That it is ready.** [release-bar.md](release-bar.md) sets 16 gates for first use, from ten million simulator seeds to a 14-day soak on each provider. None is met.

A green run shows that the checks that exist did not fail. It does not show there is nothing to find.
