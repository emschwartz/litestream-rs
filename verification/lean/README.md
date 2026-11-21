# Lean proofs of the restore planner's extension step

Machine-checked proofs about `extend` in `crates/core/src/plan.rs`, the greedy
sweep that grows a restore chain from its snapshot. The proofs are about the
Rust code itself: Charon and Aeneas translate it into Lean
(`LsrCore/Types.lean`, `LsrCore/Funs.lean`, both generated and committed), and
the theorems are stated over that translation. They hold for every input, not
a sampled one.

## What is proved

In `LsrCore/Extend.lean`, both as total-correctness triples: `extend` returns
(no panic, no overflow, no divergence) whenever the chain's files and the pool
together fit in a `usize`, and its result satisfies:

- `extend_sound`: the snapshot is unchanged, files are only appended, and the
  appended files form a contiguous chain of pool files from the old reach to
  the new one (`IsChain`): each is of levels 0 to 8, starts at most one past
  the reach before it, and ends beyond it.
- `extend_optimal`: every contiguous chain of pool files from the old reach
  ends at or before the reach `extend` ends at. The greedy sweep goes as far as
  any choice of files could, so a gap the planner reports after `extend` is
  real.

Not proved: the rest of `plan_exact` (base choice, the target check, the
explanation of a shortfall) and the `AsOf` planner. Lifting both theorems to
`plan_exact` for `Latest` and `Txid` targets needs that function reshaped into
the subset Aeneas translates, as `extend` was.

## Trusted base

- Lean's kernel and its three standard axioms (`propext`, `Classical.choice`,
  `Quot.sound`), Mathlib, and the Aeneas Lean library's model of Rust (its
  definitions of slices, vectors, integers and loops).
- Charon and Aeneas themselves: that the translation means what the Rust means.
- One axiom, `lsr_core.plan.sort_by_min_spec` in `LsrCore/Sort.lean`: the
  `sort_by_key` call in `extend` returns a permutation of its input sorted by
  first transaction. Aeneas leaves `sort_by_key` opaque (an `opaque` constant
  in `FunsExternal.lean`, so no further axiom).
- A hand-written model of the parts of `std` Aeneas leaves opaque
  (`LsrCore/TypesExternal.lean`, `LsrCore/FunsExternal.lean`): `NonZeroU64`
  as a plain `u64` (the proofs hold for zero too, a superset of real values),
  and its `eq`, `cmp` and `checked_add` as the `u64` operations std's source
  delegates to.

`scripts/verify-lean.sh` prints the axioms each theorem rests on and fails if
there is any other.

## Running

```
just verify-lean
```

It needs, on PATH or in `AENEAS_BIN`, Charon and Aeneas
`nightly-2026.11.21-2e3d448` (Aeneas commit `2e3d448`); rustup with the
toolchain Charon drives, `nightly-2026-09-17`; and elan, which installs the
Lean in `lean-toolchain` (v4.36.0). The first `lake build` fetches the Aeneas
Lean library at the pinned commit and Mathlib's build cache (several GB, in
`.lake/`, gitignored).

After a change to `extend` or anything it calls, `./scripts/verify-lean.sh
--regen` rewrites the committed translation; then fix the proofs until
`just verify-lean` passes. Aeneas currently names two fields of std's
`ZeroablePrimitive` trait alike, which Lean rejects; the script renames the
second and fails loudly once Aeneas stops doing it.
