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
- Two canary entries check the runner itself and always run, `--only` or not: `canary-no-op` (a
  comment added) must report `SURVIVED` and `canary-fatal` (the wrong CRC table) must report
  `killed`. Before every verdict the runner also checks that the scratch tree is at this checkout's
  HEAD. A canary that misreports, or a tree at another commit, fails the run.
- `just planted-bugs-unaided [seeds]` asks a different question: with no named test run, how many
  swarm seeds does the simulator need to fail on each plant? It prints a table (seeds to first
  failure, or `none`); a plant no seed finds is a listed generator gap, not a failure.
- A new fix adds an entry and a patch in the same commit as the fix. A `STALE` patch is re-expressed
  against the current tree, or the entry is retired with a reason in the ledger's header.
- An entry's `patch` field names another entry's patch when the same production change is planted
  against the tests of a second package (most often a `-sim` entry beside its core one), so each
  distinct patch exists once.
- `just planted-bugs-apply` runs `git apply --check` on every patch in seconds. It runs nightly
  (`.github/workflows/nightly.yml`), not in `just check`: a change that moves a line near a planted
  site does not block its land, and the stale patch is re-expressed afterwards.

Mutation testing covers the lines a pull request changes: `just mutants-diff` (CI job `mutants-diff`)
runs `cargo mutants --in-diff` over `lsr-core` and `lsr-ltx`. A missed mutant fails the job unless
`docs/mutants-escapes.txt` lists it with its reason in `docs/mutants.md`.

## Plants cited by behaviour rows

A behaviour row (`docs/behavior.toml`, rendered into `docs/BEHAVIOR.md`) that cites `plant::<name>`
claims its tests kill that entry's bug class, so it must also cite a test the entry runs: the
entry's package and a test filter that matches (`crates/compat/tests/compat/behavior.rs`, in `just
check`). Through `just evidence-fresh`, against `cargo nextest list`, every cited test must exist
and each cited plant must be backed by one that is not ignored, since this ledger's run starts no
ignored test. The nightly run of the ledger then makes each such citation true.
