---
name: jepsen-review
description: "Adversarial review of a system's correctness claims, in the manner of a Jepsen analysis: what it promises, under which faults, and what evidence backs each promise. Use on any replication, storage, consensus, queue or backup system when asked to \"review the guarantees\", \"is this safe\", \"would this survive Jepsen\", \"audit the fault model\", \"what can go wrong\", \"what are we missing\", to review a design doc, BEHAVIOR.md or README that says durable, consistent, exactly-once, never loses or point-in-time, before a release that makes such a claim, and when the tests are green and someone asks \"are we done?\". Also /jepsen-review <doc, repo or PR>. Not for building the test rig (simulation-testing), the fsync-level rules (crash-consistency), or maintaining the behaviour table (behaviour-accounting)."
---

# jepsen-review: what is promised, under which faults, shown by what

A guarantee is a predicate over what a user can observe, under named faults, backed by a check that could have failed. The review looks for the gap between those three. Most systems fail it at the third: the claim is fine, the test behind it shows something smaller.

Sources for each rule are in `research.md`, next to this file. The hunt list of bug classes is `classes.md`, also here.

## 0. Ground rules

- **Fresh context.** The reviewer has not read the author's reasoning, only the artifact and the code. If you wrote the thing, dispatch a fresh agent with the brief in `fix` step 6.
- **Pin what you read.** Name the commit. Say in the first paragraph whether anything was built or run.
- **Tag every claim** as know (read in this tree, or settled public behaviour), run (observed), or guess (reasoned or recalled). A guess may appear, but it may not carry a verdict.
- **Cite symbols and test names**, not line numbers that rot or whole files that cannot be falsified.
- **A stated reason never lowers a finding.** "Accepted", "out of scope" and "by design" are claims to check.
- "Nothing found" is an allowed answer. So is "not close, and we could not have known", when nothing was measuring.

## 1. Collect the claims

Read what the system says about itself: README, docs, config comments, health and metric names, exit codes, the words in error messages. The project's own words are the benchmark. Then look for pairs that cannot both hold ("always available" beside "linearizable"; "the number to alert on" beside "the only signal").

No written claims is a finding. Write down the claims a user would assume, and mark them as assumed.

## 2. Restate each guarantee as an observable predicate

```
G<n>. <name>
Statement:  for every <observable outcome> (a restore, a read, an ack), IF A2, A5 hold, THEN <predicate over that outcome>.
Does not say: <the reading a user will assume that the statement does not cover>
Assumptions: by number, each one enforced | checked | assumed | known false, and what happens when it is false
Violations:  open ones, by row or issue
Evidence:    test, seeds, steps, fault mix, where it runs (gate | nightly | by hand)
```

- State it over outcomes a user can see, never over internal ids, positions or flags.
- A claim with only an upper or only a lower bound is satisfied by a useless answer. Say so in the statement.
- A time claim is about intervals. A commit has (began, acknowledged). A file has (earliest content, when written). One timestamp standing for a span is a defect waiting for load.
- An exception ("except after a reported loss") needs the system's own signal for every excused case. An excuse taken from the test driver's record shows nothing about what was reported.
- Count the violations in the headline. "Two have open violations" when it is three is how v1 of a document loses its reader.

## 3. Audit the fault model: closed over the code is not closed over the world

A fault model built from the code's enums covers what the author imagined. Walk the world instead. For each family, every fault is **in** (names a rig that produces it), **out** (a reason, and the owner signed it off by name) or a **gap**. "Out, never run against" on the product's main target is unknown, not out.

| Family | Walk through |
|---|---|
| Process | crash, kill mid-write, pause (SIGSTOP, VM migration, laptop sleep) while holding a lock, crash loop with lifetimes under one unit of work, two instances at once, failover with the old host still alive |
| Clock | step forward, step back, back across a restart, skew against the remote store, two clocks compared |
| Disk | power loss (block subsets, not prefixes), fsync error once then success, ENOSPC, read-only remount, bit rot, a deleted file coming back, a new file vanishing |
| Volume | snapshot put back (all local state goes back together, the remote does not), ephemeral disk lost every start |
| Network | partition, reset mid-body, timeout after the request was sent, a well-formed wrong body from a middlebox, DNS pointing at another endpoint |
| Remote store | lagging or truncated listing, 200 with an error body, retried completion, lifecycle rules, versioning, object lock, the many causes of a 403, quota |
| Operator | config changed between restarts, an old backup restored over part of the state, files deleted by hand, repointed at an empty destination |
| Compound | fault B during recovery from fault A; any fault during boot |

Every in-scope row should cite where the fault comes from (a spec, a paper, a measurement) and one observed instance. Vendor consistency claims are claims until a probe has measured them.

## 4. Audit the evidence

Open each cited test. Do not trust the name.

- **What does it produce?** The circumstance in the row, or one injected error of a neighbouring type at call 0? Grade with the D/S/R marks in `behaviour-accounting`: detected, survived (judged while the fault holds), recovered (judged after). A lasting condition with no S is untested where it matters.
- **Does the judge share anything with the engine?** Its parser, its ids, its bookkeeping, its idea of where the log ends. A shared view cannot see a shared mistake.
- **Is it a test of the checker cited as a test of the system?** Those go on a "the checker can fail" line.
- **Does it run?** Ignored tests, hand-run campaigns and "50-seed version" count only with the job that runs them.
- **How big?** Seeds, steps, fault mix. One seed, 60 steps, no faults is a smoke test.
- **Where does the oracle switch off?** Exemptions scoped to a whole run, allowances, resets "so it stays bounded". The runs where it is off are usually the runs where the bug lives.
- **Indeterminate results.** A timed-out operation may have happened. A history that records only ok and failed overstates what it knows.
- **Final state.** Is there a judged read after faults heal and the system is quiet, and also reads during faults?

## 5. Hunt by class

Run `classes.md` as a question list against the design: amnesia, absence as proof, unowned cost, durability order, checksum scope, point for interval, replay, verdict composition, quiet detectors, prose predictions. Each class found gets its sibling grep (`fix` step 4), not one finding.

## 6. Write the report

1. **Verdict**: satisfied, close, or not close, with at most three reasons.
2. **Ranked problems**, each with the evidence and a fix.
3. **Missing rows**: circumstances with no entry. The five that matter first.
4. **Defects**: confirmed and new, each with its basis (read, run, inferred).
5. **What would make the evidence machine-checked.**
6. **Not verified**: everything recalled, inferred or not run.

State the limit once: tests show the presence of errors, not their absence.

## 7. After the review

A finding with no check is a prediction in prose, and those get rediscovered as incidents. Each finding becomes a fault-model row and a class-ledger entry first, a fix second. Then hand off: rig work to `simulation-testing`, file-level rules to `crash-consistency`, rows to `behaviour-accounting`, fixes to `fix`.

## Anti-patterns we hit

- A guarantee stated without its assumptions, two of which were marked known wrong elsewhere in the same document.
- "Tested" meaning one injected error followed by a restore after recovery.
- Guarantee evidence that was the judge's own unit tests.
- A review table that predicted the crash-loop cost in the morning and was an outage by evening, because no layer turns a review row into a check.
- Reviewing the detector for a fork instead of asking why the system creates the fork.
- Another review pass used as the primary detector. A reviewer is an independent world model; use the findings to widen the fault model.

## Done

Every guarantee is in the template with its assumptions and open violations. Every fault family has in, out-with-owner or gap entries. Every evidence citation was opened and graded. Each finding has a row, a ledger entry and an owner. The report says what was not verified.
