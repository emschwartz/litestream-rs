---
name: simulation-testing
description: "How to build and run deterministic simulation, fault injection and property tests for stateful systems without fooling yourself: a world model that is its own artifact, oracles that share nothing with the engine, generators frozen and budgets pre-registered before a run, unaided recall on planted bugs, and canaries for the checkers. Use when writing or changing a simulator, fuzzer, fault injector, reference model or oracle; \"add a sim test\", \"DST\", \"why didn't the sim find this\", \"the sim is green, are we done\", \"add a fault\", \"how many seeds\", \"does the fuzzer catch X\", planted-bug or mutation runs, adding an exemption or allowance to a checker; and before writing \"N seeds pass\" or \"the sim catches it\" in a commit, PR, README or report. Also /simulation-testing. Not for microbenchmarks (perf-experiment) or the fsync rules themselves (crash-consistency)."
---

# simulation-testing: a green sim proves the model is closed, not that it is right

A simulator, its fault generator and its oracle are usually written from the same list of what the world can do as the engine was. A defect outside that list is invisible to all of them at once, so 3000 of 3000 seeds passing is the expected result and carries almost no information. What finds bugs is a second, independent idea of the world. This skill is about building that in and measuring whether it is there.

Sources are in `research.md` in the `jepsen-review` skill under "Simulation".

## Two tiers: say which one you're in

- **Exploring**: trying faults, tuning weights, chasing a seed. Change anything. Claim nothing.
- **Confirming**: any number that gets written down, any "catches X", any gate. Generator frozen, budget pre-registered, the whole of section 6.

## 1. The world model is an artifact

- Keep a **fault table** separate from the engine and from the sim's op list. Each row comes from outside (the dependency's documentation or source, POSIX, the store's API reference, a paper, a measurement) and cites it.
- State faults as **laws, not shapes**. "Each unsynced block holds any version it ever held" generates shapes nobody listed. Six hand-drawn corruptions generate six.
- Derive generators from the table and assert coverage against it. A fault row with no generator says "rig: none".
- When a review, an incident or a real-binary run finds a bug the sim could not express, the row goes into the table first. One scenario per finding, each with its own oracle exemption, is the treadmill.
- States where "what is the truth" is ambiguous are the trust boundary. A generator that avoids them because the oracle needs a definite answer has excluded the bugs.

## 2. Oracles, most independent first

1. **An outside judge.** It reads only what a user can observe (a restored database, a read result) and a log of acknowledged operations kept on unfaulted storage. It shares no code with the engine and knows nothing of its ids, offsets or formats. The workload is self-certifying (row k holds a hash of k and row k-1), so content identifies its own place in history. After any rewind or failover, history is a tree; the judge asks "is this a node of the tree" and "did latest go backwards on a branch without a recorded reason".
2. **The real dependency.** Where the engine reimplements something (a file-format parser, a recovery rule), the truth is the real library run on the same bytes. A second hand-written copy is the same belief typed twice.
3. **A reference model** in its own crate with no import of the engine, checked by conformance. Keep it for logic with a small input space (a planner, a boot decision).
4. **Assertions in the engine.** Dense, paired (before the write and after the read), and compiled into what ships. They turn silent corruption into a crash the fuzzer can see.

Rules for all four:

- Record **indeterminate** results. A timed-out operation may have happened.
- An exemption is scoped to the ids or events it concerns, never to the rest of the run. Count judged versus skipped checks and assert a floor.
- An excused anomaly requires the engine's own signal. An excuse from the driver's record proves the driver knew.
- If fault injection forces a relaxed check, run the same suite **with and without** injection so the relaxation cannot hide ordinary bugs.
- **Cost is a property.** An expensive action (snapshot, full resync) in a run with no fault that justifies it fails the run. Bytes stored and requests made are bounded independently of the number of restarts.
- **Non-interference** needs no fault model: changing bytes outside what the engine claims to trust must not change the observable outcome. Compare outcomes, not file sets.

## 3. Generators

- **Laws to have**: block-subset crash (see `crash-consistency`); **lifetime** (kill after k ticks, k from 1, so some processes never finish one unit of work); **restart right after any fault**; **sustained faults** (hold until step N, judge during); lagging and truncated views; a clock that moves between two reads in one tick.
- **Swarm**: each run enables a random subset of fault kinds and operations. A feature that is always on suppresses the bugs that need its absence.
- **Bias with evidence only.** Structure in a workload projects away dimensions: a generator that made tidy, disjoint data never exercised the join path that lost results. Keep one dumb generator that draws unstructured inputs against a full model.
- **Cooperative injection.** Mark rare branches in the engine so that, in simulation only, they fire often (a short read, a minimal quorum, a forced retry). Enable a random subset per run.

## 4. Reach is asserted

A state nothing reaches prints as a missing line, which nobody reads. Keep a required list of situations ("recovered from a hole in the middle", "restored during an outage", "boot with a lagging listing") and fail when one is never hit across the run set. A reached-situation assertion is coverage aimed at what matters.

## 5. Check the checkers

- **Canaries on every checker and script**: a no-op plant must be reported as survived, a known-fatal plant as killed, and the commit of the tree under test is asserted before each verdict.
- Tests that show the judge can say no are filed as "the checker can fail". They are not evidence about the engine.
- **A checker changed after it fired gains an assertion for what it saw.** A reset, an allowance or a narrower workload with no new assertion makes the detector quieter exactly where it worked.

## 6. Confirming: running against a known bug, or for a number

Write this down before the first run:

1. The claim ("the frozen swarm finds finding 4 within 20k seeds").
2. The generator's commit: law and weights. **Frozen from here.**
3. The budget: seeds, steps, wall time.
4. The trees: the buggy head, and main if main shares the flaw.

Then run and report **seeds to first failure**. If the budget ends with no failure, that is the result: the model is still closed. Changing the generator afterwards is a new experiment with a new registration, and the report says how many registrations there were. Tuning weights until the known bug falls out measures the tuning.

One known bug caught is an existence proof. It is not a recall number.

## 7. Two numbers that must not be mixed

- **Regression kill**: the test written with the fix still fails on the old code. Useful; it says nothing about discovery.
- **Unaided recall**: the plant is applied, its named tests are disabled, and the generators alone must find it inside the budget. Report it per layer (swarm, shim rig, black-box rig) with the misses listed. A plant with unaided recall of zero is a named generator gap.

The honest version is **blind seeding**: bugs planted by an agent that has seen neither the generators nor the test names. Mutation scores and ledger kill rates over bugs already found are survivorship.

Classify every escaped defect as a **search escape** (expressible, missed) or a **model escape** (not expressible). Model escapes are the ones that matter, and no seed count fixes them.

## 8. Stopping rule

Not "found nothing this week", and not capture-recapture (the samples are not independent, catchability is not equal, and a model escape has catch probability zero). Use:

1. **Pre-registered trial counts on the most real rig**: zero anomalies in N cycles bounds the per-cycle rate at 3/N with 95% confidence, for that fault distribution only.
2. **Fault-table coverage**: every in-scope row has a generator with asserted reach and one plant only that row reveals.
3. **Blind-seeding recall per layer**, with a stated floor.
4. **Model escapes per quarter** as a trailing indicator, never as the gate.

## 9. The record line

Every written result carries one line, and names what is missing:

> `swarm @a1b2c3 (generator frozen @9f8e7d), 20k seeds x 400 steps, mix: default + block law + lifetime; 0 anomalies (<=0.015% per seed at 95%, this mix); unaided recall 14/21 (misses: boot-listing-lag, dir-fsync, ...); reach 212/212; canaries pass`

## Anti-patterns we hit

- The sim's "what the database recovers" calling the engine's own parser.
- An oracle that mapped ids to commits through a header the engine wrote.
- Txid checks switched off for the whole run after any rewind: the runs with power loss were the runs with the defects.
- After damage, the harness adopting whatever the engine's rules now read as truth.
- Reach printed by an ignored test.
- A planted-bug script that had been testing a stale tree, with nothing to notice.
- A gate of the form "the buggy head must fail from a swarm seed", which invites tuning until it does.
- More seeds, a longer nightly, more hand-shaped ops, proofs of the pure functions: effort spent inside the closed model.

## Done

The fault table exists with provenance. At least one oracle shares no code with the engine and judges observable outcomes. Reach is a failing check. Canaries pass. Any reported number has its registration and its record line, and unaided recall is reported separately from regression kill.
