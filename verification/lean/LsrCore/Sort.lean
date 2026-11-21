-- The one trusted assumption of the proofs, stated about the translated code.
import LsrCore.Funs
open Aeneas Aeneas.Std Result

namespace lsr_core

/-- THE TRUSTED ASSUMPTION. `extend` sorts its sweep with Rust's
`<[T]>::sort_by_key(|info| info.key.range().min())`, which Aeneas leaves
opaque. This axiom says what the standard library documents for it: the call
returns, and its result is a permutation of the input whose keys (each file's
first transaction, compared through `Txid`'s derived `Ord`, which is `u64`
order) never decrease. It is stated for this one call site, key function and
order, not for `sort_by_key` in general, so it cannot be instantiated with an
order that is not total. -/
axiom plan.sort_by_min_spec (s : Slice layout.FileInfo) :
  ∃ r, alloc.slice.Slice.sort_by_key
      plan.extend.closure.Insts.CoreOpsFunctionFnMutTupleShared0FileInfoTxid
      lsr_ltx.ids.Txid.Insts.CoreCmpOrd s () = ok r ∧
    r.val.Perm s.val ∧
    r.val.Pairwise (fun a b =>
      (a.key.range.min : Std.U64).val ≤ (b.key.range.min : Std.U64).val)

end lsr_core
