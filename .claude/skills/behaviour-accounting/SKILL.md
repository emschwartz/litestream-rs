---
name: behaviour-accounting
description: "How to write and keep a full behaviour table for a system: every circumstance it distinguishes, what it does, what signal it raises, and what a test has actually shown, in a form a skeptic can check by machine. Use when asked to \"document what happens when X\", \"what does it do in every case\", to write or update a BEHAVIOR.md, failure-modes doc, fault matrix or operator runbook table, to account for every error variant, when adding an enum variant, fault, health signal, stop condition or exit code that such a table should cover, before marking any behaviour \"tested\", and when a review says the document's evidence is thin. Also /behaviour-accounting. Not for auditing guarantees from outside (jepsen-review) or for building the tests the rows cite (simulation-testing)."
---

# behaviour-accounting: rows are data, marks are derived, every citation can fail

A behaviour table is a list of claims: in this circumstance the system does this, raises this signal, and puts this at risk. A skeptic accepts it when each row's evidence is a named test that produces the circumstance and would fail if the row were false. So the rows live in a data file, the document is generated from it, and a row's evidence mark is computed from what the tests show. Nobody types "tested".

## 1. Say the scope in the first paragraph

The table can be **closed over the code**: every variant of every closed enum has a row. It cannot be closed over the world. Say both, plainly, and give circumstances the code does not distinguish (a paused process, two instances on one directory, a volume snapshot put back) their own rows marked untested or unknown. A title that promises "every circumstance" over a table closed only on enums is the first thing a reviewer quotes.

## 2. The document's parts

1. **Guarantees**: each as a predicate over observable outcomes, with assumptions by number, open violations by row, and evidence with seeds, sizes and where it runs. Template in `jepsen-review` step 2.
2. **Fault model**: per family, each fault in (with its injector), out (with a reason), or gap.
3. **The table**, grouped by lifecycle phase (boot, running, checkpoint, upload, maintenance, restore, config), plus a group for world rows.
4. **Known gaps**: open defects, checker blind spots, unknowns, accepted limits. Each has an issue.
5. **How this stays true**: the checks below, and what they still do not do.

## 3. A row

```toml
[[row]]
id = "RUN-14"
circumstance = "The staging disk is full"
variants = ["Fault::LocalDiskFull"]        # closed-enum variants this row covers; empty for world rows
detect = "core::fault::Fault::from_staging" # a symbol path, never a line number
response = "retry"                          # from a closed list: continue | snapshot | retry | degraded | stop | refuse
signal = ["retrying"]                       # names from the signal table, also a closed list
at_risk = "Lag, without bound while the disk is full"
recovery = "operator"                       # automatic | operator | none | unknown
injector = "sim::FaultShape::Error(ScratchExhausted)"   # or "rig: none"
sustained = false                           # does the injector hold the condition past the judged outcome?
issue = 0                                   # required when the mark below is not a test mark

[row.evidence]
detected  = ["pkg::path::test_name"]        # one test per entry, exact, no globs, no file names
survived  = []
recovered = []
none = ""                                   # required, and only allowed, when all three are empty

[[row.kill]]
test  = "pkg::path::test_name"
plant = "retry-disk-full-as-success"        # a planted bug this test must fail under
```

## 4. Marks

Three marks are earned by tests, and they are separate:

| Mark | Earned when |
|---|---|
| **D** detected | A named test produces the circumstance, or injects its error, and asserts the response and signal the row states. Says nothing about outcomes |
| **S** survived | The user-visible outcome is judged correct **while the circumstance still holds** |
| **R** recovered | The outcome is judged correct after the circumstance cleared, or after the system's one-time decision took effect |

For a single decision (most boot rows) S has no meaning. For a lasting condition (credentials revoked, disk full, a second writer) a missing S is the gap: one injected error followed by a check after recovery is "D R".

A row with no test mark carries exactly one of: **code only** (read, not asserted), **assumed** (taken from outside, not observed), **untested**, **unknown** (nobody knows what happens), **known wrong** (a defect is open), **accepted, undetected** (a stated limit).

## 5. What the gate checks on every commit

1. **Closed worlds.** For each closed enum, every variant goes through a wildcard-free match that returns its name. The set of names equals the set of `variants` across all rows, both ways. Include error types, stop causes, exit codes and the status-to-error table for remote calls.
2. **The fault model is closed against the injectors.** Every injector variant appears as some row's `injector`; every row names one or says `rig: none`.
3. **Derived columns equal the code.** `response` and `signal` are rendered from the same functions the system uses.
4. **Every cited test exists and runs.** An ignored test counts only if the row names the job that runs it.
5. **S needs a holding injector.** A non-empty `survived` requires `sustained = true`.
6. **Marks have owners.** Any non-test mark requires an issue.
7. **Symbols resolve.** No line numbers in the data.
8. **Tests name rows.** A test that is evidence carries a tag (`/// behavior: RUN-14 detected`), and the tags equal the evidence lists. Evidence cannot be added without touching the test, and a test cannot be repurposed without the row noticing.
9. **The generated document equals the committed one.**

## 6. What the nightly run checks

10. **Every citation can fail.** Apply each row's plant; the cited test must fail. A row with evidence and no kill entry is printed "no kill demonstrated", and that count is reported.
11. **The cited test reaches the cited code.** Per-test coverage includes the `detect` symbol. This is the check that catches an adjacent test.
12. **Known wrong is an expected failure.** The row names a test that fails today. When it passes, the run fails until the mark changes.

## 7. Before you write or accept an evidence entry

Open the test and answer:

- [ ] Does it produce **this** circumstance, or an error of a neighbouring type? (An injected size mismatch is not an fsync failure. One NotFound on a read is not a deleted object.)
- [ ] Does it reach the decision the row describes, or inject that decision's result from above?
- [ ] Is the condition held, or is it one error at one call?
- [ ] Is the outcome judged, and when: during or after?
- [ ] Is it a test of the system, or a test of the checker?
- [ ] Would it fail if the row's response were different?

A "no" is not a reason to drop the row. It is the row's mark.

## 8. Keeping it alive

- A review finding with no check becomes a world row, marked open, with an issue. That is the intake.
- A new enum variant fails check 1 until it has a row.
- Each version ends with "changes from the previous version", including every downgraded mark.
- Describe main as it is. A design that is not built is not behaviour; an open branch that changes a row is a tag on that row, and the gate fails when the branch has merged and the tag remains.

## Anti-patterns we hit

- "Credentials revoked: lag without bound" cited to a test that injected one auth error and expected recovery.
- "fsync of a staged file fails" cited to a test that injected a size mismatch.
- A guarantee's evidence that was four unit tests of the judge.
- Citations to whole files and to glob shorthand, which nothing can falsify.
- A closed-world test that proved each variant had a row and each test name existed, and could not tell a true row from a false one.
- "Accepted" used as the status of a defect that undid the system's one safety stop.
- A headline count that the table contradicted: "two of four guarantees have open violations" when a third was marked in its own row.

## What this still does not do

It does not find the circumstance nobody wrote down. Only an outside rig driving the real binary adds rows no author imagined. Say so in the document.

## Done

Rows are in the data file and the document is generated. Checks 1 to 9 gate. Every mark is derived or owned by an issue. The count of "no kill demonstrated" rows and of world rows without a rig is printed where the reader sees it.
