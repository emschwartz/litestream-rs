# Planted bugs

"A bug fix starts with a test that fails on the old code" is checked once, by the author. This ledger
checks it again every night: each defect fixed on `main` is put back into a scratch tree and the tests
that caught it must fail.

- `docs/planted-bugs.toml` lists the entries: the fix commit, the package, and the tests that must fail.
- `docs/planted-bugs/<name>.patch` re-introduces the defect. It is the fix's production change reversed
  and re-expressed against the current tree, because later renames stop the fix commit's own diff
  applying. Only production code is touched; the tests stay as they are.
- `just planted-bugs [--only NAME]` runs it: the named tests must first pass on a clean copy of HEAD,
  then fail one by one with the patch applied. A test that loops is killed after 90 s and counts as a
  kill (the table says "timeout"). Results: `killed`, `SURVIVED` (a defect in the tests), `STALE` (the
  patch no longer applies), `BUILD` (the planted tree does not compile), `BROKEN` (a named test is red
  or missing on the clean tree), `ERROR`. Anything but `killed` fails the run.
- A new fix adds an entry and a patch in the same commit as the fix. A `STALE` patch is re-expressed
  against the current tree, or the entry is retired with a reason in the ledger's header.

Mutation testing covers the lines a pull request changes: `just mutants-diff` (CI job `mutants-diff`)
runs `cargo mutants --in-diff` over `lsr-core` and `lsr-ltx`. A missed mutant fails the job unless
`docs/mutants-escapes.txt` lists it with its reason in `docs/mutants.md`.

## Evidence rows

`docs/evidence.toml` holds behaviour rows as data: what each row claims a named test shows
(detected, survived or recovered), and, for a tested row, the ledger entry whose plant that test
kills. `crates/compat/tests/compat/evidence.rs` checks the file in `just check` (every kill names
an entry here whose package and test filters match the cited test) and, through `just
evidence-fresh`, against `cargo nextest list` (every cited test exists, ignored ones name the
recipe that runs them, no killing test is ignored). The nightly run of this ledger then makes
each kill true. A tested row with no plant says why in `unkilled`, and the check prints it.
