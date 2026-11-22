# Prior art for the correctness skills

Research done 2026-11-22 for the skills `jepsen-review`, `crash-consistency`, `simulation-testing` and `behaviour-accounting`; the other three point here.

Tags: **measured** (a study or reproducible measurement), **practitioner** (an experienced engineer's report or a project's own docs), **vendor** (from a company selling the tool), **recalled** (from memory, not re-read today). Everything not tagged recalled was opened today and the quoted words are on the page.

## The short version

1. The best practitioners judge systems from outside, on what a client can observe, against the project's own written claims. They record what they do not know (indeterminate operations, what was not tested) as carefully as what they found.
2. Simulation works when the engine cooperates with it (injection points and assertions inside production code) and when something independent checks it. Every team that has published a miss traces it to the generator or the fault model, not to the seed count.
3. For files, the unit of loss is the block and the directory entry, a failed fsync is final, and what "durable" means differs per filesystem. This has been measured since 2014.
4. Nobody credible offers a number for "bugs remaining". Mutation scores and capture-recapture both fail their own assumptions here.

## Jepsen (jepsen-review, simulation-testing section 2)

- **Claims come from the vendor's own documentation, and contradictions between them are findings.** NATS 2.12.1: the docs promised both "self-heal and always be available" and "Linearizable"; "At most one of these claims can be true". Practitioner. https://jepsen.io/analyses/nats-2.12.1
- **Guarantees are sets of legal histories, stated by the anomalies they forbid.** "a consistency model defines a set of histories that a system can legally execute." Practitioner. https://jepsen.io/consistency
- **Judged from client observations only.** Elle is "a transactional consistency checker for black-box databases. Based purely on client observations", and it returns "a minimal set of transactions which witness a consistency violation". https://github.com/jepsen-io/elle (paper: https://arxiv.org/abs/2003.10554, recalled)
- **Three outcomes per operation, not two.** Knossos: "either ok if the operation took place, fail if the operation did not take place, or info if you're not sure what happened, e.g. the operation timed out." A process that times out is "crashed" and never reused. https://github.com/jepsen-io/knossos
- **Checkers are not trusted either.** Knossos: "treat its results as plausible but verify by hand." Elle: "Jepsen recommends checking reported anomalies by hand". The 2024 Bufstream report carries a dated correction at the top. https://jepsen.io/analyses/bufstream-0.1.0
- **Test shape**: unique self-identifying values, faults during the run, then heal everything, wait, and do a final read of all acknowledged writes (NATS report, section 2). The nemesis list there: "process pauses, crashes, network partitions, and packet loss, as well as single-bit errors and truncation of data files", plus simulated power failure ("a crash with partial amnesia") through LazyFS.
- **Limits are stated.** "they can only prove the existence of errors, not their absence"; funding and conflicts are disclosed in every report. https://jepsen.io/ethics
- **A flush interval is a durability claim.** NATS acknowledged writes and flushed "every two minutes, rather than before acknowledging them", losing committed writes on coordinated power failure. This is the durability inversion in its plainest form.

## FoundationDB: simulation and BUGGIFY (simulation-testing sections 3 and 4)

- The whole cluster runs in one deterministic single-threaded process; "tens of thousands of simulations every night". Vendor/practitioner. https://apple.github.io/foundationdb/testing.html
- **BUGGIFY**: fault points written into production code. Rules: only ever true in simulation; each point is "either enabled or disabled for the entire simulation run"; enabled points fire 25% of the time. Uses: do the minimal work, force error handling, emphasise concurrency, randomise tuning knobs. "FoundationDB was written to explicitly cooperate with the simulator". Practitioner (Alex Miller, ex-FDB). https://transactional.blog/simulation/buggify
- **Swarm testing is the default**: "Each run uses a random cluster size and configuration, random workloads, random fault injection parameters ... and enables and disables a random subset of buggification points." SIGMOD 2021 paper, section on simulation. https://sigmodrecord.org/publications/sigmodRecord/2203/pdfs/08_fdb-zhou.pdf
- **Stated limits, same paper**: simulation cannot test third-party code, and "several bugs have resulted from the true operating system contract being weaker than it was believed to be." That is the closed world model, in the words of the team with the best simulator.
- Swarm testing origin: omitting features per run found "42% more distinct ways to crash a collection of C compilers" in a week, because "some features actively prevent the system from executing interesting behaviors". Measured. Groce et al., ISSTA 2012. https://agroce.github.io/issta12.pdf
- Will Wilson's Strange Loop 2014 talk is the usual entry point (recalled, not re-watched).

## TigerBeetle: VOPR and assertions (simulation-testing sections 2 and 3)

- **Assertions as the safety net.** "Assertions downgrade catastrophic correctness bugs into liveness bugs. Assertions are a force multiplier for discovering bugs by fuzzing." Density "must average a minimum of two assertions per function." **Pair assertions**: "assert validity of data right before writing it to disk, and also immediately after reading from disk." Practitioner. https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md
- VOPR: real code, simulated network and storage, time compressed ("3.3 seconds of VOPR simulation gives you 39 minutes"). Vendor. https://tigerbeetle.com/blog/2023-07-11-we-put-a-distributed-database-in-the-browser
- **Where it missed, and why (the most useful single source for us).** Jepsen found a query bug that four fuzzers missed. Cause: the workload generated tidy objects so "the 'zig-zag' part of the merge join was never executed". TigerBeetle's own write-up: "the risk of the fuzzer's workload unintentionally projecting away dimensions of the system's reachable states", and "When a fuzzer stops finding bugs, that doesn't mean its job is done". Fix: unstructured inputs plus a detailed model. https://tigerbeetle.com/blog/2025-06-06-fuzzer-blind-spots-meet-jepsen/ and https://jepsen.io/analyses/tigerbeetle-0.16.11
- **Checksum scope and fault granularity, same report.** Checksums "exclude the padding"; a flipped padding bit passed the checksum and tripped an assertion. The VOPR missed it "because it corrupted entire sectors, rather than single bits". Same shape as our LZ4-bytes finding and our prefix-only disk.

## Antithesis (simulation-testing section 4)

- **Sometimes-assertions**: "either you're wrong, and the program state designated by the sometimes assertion is actually unreachable, or your tests are too weak to ever reach it." Coverage is "equivalent to adding assertSometimes(true) to every line"; sometimes-assertions "cover situations". They are "listed as failing by default until proven by a passing encounter." Vendor docs, but the idea stands alone. https://antithesis.com/docs/best_practices/sometimes_assertions/ and https://antithesis.com/docs/properties_assertions/assertions/
- The motivating statement: "if you had the foresight to write a test for a particular case, then you probably had the foresight to make the code handle that case too." Vendor. https://antithesis.com/blog/is_something_bugging_you/
- Their determinism comes from a hypervisor, so unmodified binaries and third-party code are inside the simulation. That closes FDB's stated gap; it is a paid service. Vendor.

## Crash consistency (crash-consistency)

- **ALICE**: 11 applications, 6 Linux filesystems, "60 vulnerabilities". Table 1 gives per-filesystem atomicity and ordering: appends are not atomic on writeback configurations; overwrites have no ordering guarantee on most; "Linux ext2 and btrfs freely reorder directory operations"; delayed allocation can persist an append after later operations except for the append-then-rename idiom. btrfs exposed 31 of the vulnerabilities. Measured. Pillai et al., OSDI 2014. https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai
- **Dan Luu, "Files are hard"**: walks one two-step update up to the correct sequence (log with checksum, fsync log, fsync directory, write, fsync, unlink, fsync directory) and notes "OS X and some versions of ext3 have an fsync that doesn't really flush to disk." Practitioner summary of the measured papers. https://danluu.com/file-consistency/
- **fsyncgate**: "When fsync() returns success it means 'all writes since the last fsync have hit disk' but we assume it means 'all writes since the last SUCCESSFUL fsync have hit disk'." Archive: https://danluu.com/fsyncgate/. Outcome: "PostgreSQL will now PANIC on fsync() failure". Per-OS table (Linux 4.16 and later: "you still only get the error once (so retrying fsync() is not OK)"): https://wiki.postgresql.org/wiki/Fsync_Errors
- **fsync failures, measured**: on ext4, XFS and btrfs "pages are always marked clean"; of PostgreSQL, LMDB, LevelDB, SQLite and Redis, "none are sufficient". Rebello et al., USENIX ATC 2020. https://www.usenix.org/conference/atc20/presentation/rebello
- **SQLite's own statement of the inversion**: with `synchronous=NORMAL` in WAL mode "recent changes to the database might be rolled back by a power loss". https://sqlite.org/compile.html and https://sqlite.org/wal.html
- **LazyFS**: a FUSE filesystem whose page cache "only flushes data if explicitly requested"; faults are clear-cache, torn-op and torn-seq. Paper: Ramos et al., "When Amnesia Strikes", VLDB 2024, doi 10.14778/3681954.3681980. https://github.com/dsrhaslab/lazyfs
- **CharybdeFS**: FUSE errno injection over a Thrift interface. Archived; it works but expect to maintain it. https://github.com/scylladb/charybdefs
- dm-log-writes, dm-flakey and CrashMonkey (Mohan et al., OSDI 2018) replay real block writes: recalled, not re-read.

## AWS lightweight formal methods (simulation-testing section 2)

- **ShardStore** (Bornholt et al., SOSP 2021): "decomposes correctness into independent properties, each checked by the most appropriate tool, and develops executable reference models as specifications"; "prevented 16 issues from reaching production". https://www.amazon.science/publications/using-lightweight-formal-methods-to-validate-a-key-value-storage-node-in-amazon-s3 (PDF: https://jamesbornholt.com/papers/shardstore-sosp21.pdf)
  - On bias: "biasing introduces the risk of baking our assumptions into our tests"; they bias "only ... where we have quantitative evidence that it is beneficial".
  - On relaxed checks under fault injection: "we separately run our property-based tests with and without failure injection enabled", so the relaxation cannot hide ordinary bugs.
  - Crash consistency as two properties over the system's own promise: **persistence** (anything reported persistent is readable after a crash) and **forward progress** (after a clean shutdown everything is persistent).
  - Their one admitted miss: a cache-miss path never reached "because the cache size was configured to be very large in all tests". A reach failure.
- **TLA+ at AWS** (Newcombe et al., 2014): prose designs "are far from precise"; model checking found design bugs testing could not. https://lamport.azurewebsites.net/tla/formal-methods-amazon.pdf
- **2025 update** (Brooker and Desai, CACM): P for protocol design; **PObserve** validates production logs "post hoc that they match behaviors allowed by the formal P specification"; deterministic simulation via the open-source Rust crates shuttle and turmoil; "92% of catastrophic failures in tested distributed systems were triggered by incorrect handling of nonfatal errors" (Yuan et al., OSDI 2014). https://cacm.acm.org/practice/systems-correctness-practices-at-amazon-web-services/

## Mutation testing and planted bugs (simulation-testing section 7)

- Mutant detection correlates with real-fault detection across 357 real faults (Just et al., FSE 2014). https://dl.acm.org/doi/10.1145/2635868.2635929. The paper's breakdown (73% of real faults coupled to some mutant, 17% not representable by any operator) is recalled, not re-read.
- The correlation is "weak when controlling for test suite size" (Papadakis et al., ICSE 2018). https://dl.acm.org/doi/10.1145/3180155.3180183
- Google runs it at review time on changed lines only, skipping "arid" lines. https://research.google/pubs/state-of-mutation-testing-at-google/
- Reading: token-level mutants measure assertion strength on code the tests already reach. They say little about discovery of faults that need a state the generator cannot draw. That is why the skill separates regression kill from unaided recall and asks for blind seeding.

## Capture-recapture (simulation-testing section 8)

- Briand, El Emam, Freimut and Laitenberger, IEEE TSE 2000: on real inspection data, "When the number of inspectors is too small, no model is sufficiently accurate and underestimation may be substantial"; below four inspectors nothing was within 20%. The equal-detection-probability assumption "requires most attention". Measured. https://doi.org/10.1109/32.852741
- Our case is worse than theirs on every assumption: findings become plants (samples not independent), a model escape has catch probability zero in the sim, and each fix changes the population. With zero overlap the estimator is undefined. Use pre-registered trial counts and the rule of three (0 failures in N gives a 95% upper bound of about 3/N; recalled, standard) instead.

## Contested points

- **Is block-level crash enumeration worth it?** ShardStore built it and reports it "has not found additional bugs and is dramatically slower", so they use coarse crash states with interleaved flushes. For us the block-subset shape was the actual bug. The difference: ShardStore's data is append-structured with soft-update dependencies, ours reads a log that is overwritten in place. Rule: the finer the in-place overwrite, the more the block law earns its cost.
- **Biased or dumb generators?** FDB tunes fault distributions carefully and biases hard with BUGGIFY; ShardStore and TigerBeetle's post-mortem warn that bias bakes in assumptions. Both are right: bias toward rare states, and keep one unstructured generator against a full model as the control.
- **Whole-system simulation or many small fuzzers?** TigerBeetle notes whole-system simulation "might not be as efficient at exercising deeper layers" (https://tigerbeetle.com/blog/2025-11-28-tale-of-four-fuzzers/). Not resolved; they run about twenty fuzzers and the VOPR.

## What prior art does that we do not yet

1. **Injection points and reach assertions inside the engine.** BUGGIFY, sometimes-assertions, and paired assertions at two per function. We inject only from outside and have three invariant sites in core. This is the cheapest way to make rare boot and recovery paths common, and to make "never reached" a red result.
2. **Indeterminate operations as a first-class outcome.** Jepsen's histories carry ok, fail and info. Our history judge, as the notes describe it, logs acknowledged commits only (not checked in code today); a commit that timed out or was cut by a crash is the case where "replica ahead of database" and "lost ack" get confused.
3. **Fault-on and fault-off twin runs, with relaxations scoped per check.** ShardStore's answer to "the oracle relaxes under faults" is to run both and keep the relaxation minimal. Ours switches checks off for the rest of the run.

Also worth taking: checking production logs against the model (PObserve), and stating each guarantee by the anomalies it forbids so that "replica holds commits the database lost" has a name before it has an incident.

## Unknown

- Whether LazyFS can be made to drop unsynced directory operations (our rig saw neither missing directory sync in 200 cycles; not checked against LazyFS's current feature list).
- Current per-filesystem length-versus-data behaviour on 6.x kernels. ALICE's table is from 2014.
