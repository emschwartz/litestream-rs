import LsrCore.Sort

/-!
# The restore planner's extension step

Proofs about `lsr_core::plan::extend` (crates/core/src/plan.rs), as Charon and
Aeneas translate it into `LsrCore/Funs.lean`. `extend` grows a chain that
starts at a snapshot by sweeping the level 0-8 files of a pool in order of
first transaction, taking at each step the file that touches the chain's reach
and goes furthest.

The two headline theorems, at the end of the file:
- `extend_sound`: the files `extend` appends form a contiguous chain of pool
  files from the old reach to the new one (`IsChain`).
- `extend_optimal`: no contiguous chain of pool files from the old reach gets
  past the reach `extend` ends at. The planner never reports a gap that a
  different choice of files would have bridged.

Both are total-correctness triples: `extend` returns (no panic, no overflow,
no divergence) whenever the chain's files and the pool fit in a `usize`
together. They rest on Lean's own axioms, the Aeneas library, the hand-written
standard-library model in `FunsExternal.lean` and `TypesExternal.lean`, and the
one axiom `plan.sort_by_min_spec` in `Sort.lean`.
-/
open Aeneas Aeneas.Std Result ControlFlow Error
namespace lsr_core.proofs
open lsr_core

/-- The number a transaction id stands for. -/
abbrev tx (t : lsr_ltx.ids.Txid) : Nat := (t : Std.U64).val

theorem txid_cmp (a b : lsr_ltx.ids.Txid) :
    lsr_ltx.ids.Txid.Insts.CoreCmpOrd.cmp a b = ok (compare (tx a) (tx b)) := by
  simp [lsr_ltx.ids.Txid.Insts.CoreCmpOrd.cmp, core.num.nonzero.NonZero.Insts.CoreCmpOrd.cmp, liftFun2]

theorem txid_le (a b : lsr_ltx.ids.Txid) :
    core.cmp.PartialOrd.le.trait_default lsr_ltx.ids.Txid.Insts.CoreCmpPartialOrdTxid a b
      = ok (decide (tx a ≤ tx b)) := by
  simp only [core.cmp.PartialOrd.le.trait_default, core.cmp.PartialOrd.le.default,
    core.cmp.PartialOrd.le_body, lsr_ltx.ids.Txid.Insts.CoreCmpPartialOrdTxid.partial_cmp, txid_cmp]
  simp
  rcases Nat.lt_trichotomy (tx a) (tx b) with h | h | h
  · simp [Nat.compare_eq_lt.mpr h, Nat.le_of_lt h]
  · simp [h]
  · simp [Nat.compare_eq_gt.mpr h, Nat.not_le.mpr h]; omega

theorem txid_lt (a b : lsr_ltx.ids.Txid) :
    core.cmp.PartialOrd.lt.trait_default lsr_ltx.ids.Txid.Insts.CoreCmpPartialOrdTxid a b
      = ok (decide (tx a < tx b)) := by
  simp only [core.cmp.PartialOrd.lt.trait_default, core.cmp.PartialOrd.lt.default,
    core.cmp.PartialOrd.lt_body, lsr_ltx.ids.Txid.Insts.CoreCmpPartialOrdTxid.partial_cmp, txid_cmp]
  simp [Nat.compare_eq_lt]

theorem txid_gt (a b : lsr_ltx.ids.Txid) :
    core.cmp.PartialOrd.gt.trait_default lsr_ltx.ids.Txid.Insts.CoreCmpPartialOrdTxid a b
      = ok (decide (tx b < tx a)) := by
  simp only [core.cmp.PartialOrd.gt.trait_default, core.cmp.PartialOrd.gt.default,
    core.cmp.PartialOrd.gt_body, lsr_ltx.ids.Txid.Insts.CoreCmpPartialOrdTxid.partial_cmp, txid_cmp]
  simp [Nat.compare_eq_gt]

theorem txid_ne (a b : lsr_ltx.ids.Txid) :
    core.cmp.PartialEq.ne.trait_default lsr_ltx.ids.Txid.Insts.CoreCmpPartialEqTxid a b
      = ok (decide (tx a ≠ tx b)) := by
  simp [core.cmp.PartialEq.ne.trait_default, core.cmp.PartialEq.ne.default,
    lsr_ltx.ids.Txid.Insts.CoreCmpPartialEqTxid.eq,
    core.num.nonzero.NonZero.Insts.CoreCmpPartialEqNonZero.eq, liftFun2]
  constructor <;> intro h
  · exact h ▸ rfl
  · exact UScalar.eq_of_val_eq h

theorem touches_eq (m r : lsr_ltx.ids.Txid) :
    plan.touches m r = ok (decide (tx m ≤ tx r + 1)) := by
  unfold plan.touches lsr_ltx.ids.Txid.next core.num.nonzero.NonZeroU64NonZeroU64Inner.checked_add
  have h := U64.checked_add_bv_spec r 1#u64
  have hm : tx m ≤ U64.max := by scalar_tac
  cases hc : U64.checked_add r 1#u64 with
  | none =>
    simp only [hc] at h
    simp [tx] at h hm ⊢
    omega
  | some z =>
    simp only [hc] at h
    simp only [txid_le]
    simp [tx] at h hm ⊢
    omega

/-- A file's level. -/
abbrev lvl (f : layout.FileInfo) : Nat := (f.key.level : Std.U8).val
/-- A file's first transaction. -/
abbrev lo (f : layout.FileInfo) : Nat := tx f.key.range.min
/-- A file's last transaction. -/
abbrev hi (f : layout.FileInfo) : Nat := tx f.key.range.max

theorem is_snapshot_eq (l : layout.Level) :
    layout.Level.is_snapshot l = ok (decide ((l : Std.U8).val = 9)) := by
  simp [layout.Level.is_snapshot, layout.Level.SNAPSHOT]
  constructor <;> intro h
  · subst h; rfl
  · exact UScalar.eq_of_val_eq h

theorem level_ne (a b : layout.Level) :
    core.cmp.PartialEq.ne.trait_default layout.Level.Insts.CoreCmpPartialEqLevel a b
      = ok (decide ((a : Std.U8).val ≠ (b : Std.U8).val)) := by
  simp [core.cmp.PartialEq.ne.trait_default, core.cmp.PartialEq.ne.default,
    layout.Level.Insts.CoreCmpPartialEqLevel.eq]
  constructor <;> intro h
  · exact h ▸ rfl
  · exact UScalar.eq_of_val_eq h

theorem level_gt (a b : layout.Level) :
    core.cmp.PartialOrd.gt.trait_default layout.Level.Insts.CoreCmpPartialOrdLevel a b
      = ok (decide ((b : Std.U8).val < (a : Std.U8).val)) := by
  simp [core.cmp.PartialOrd.gt.trait_default, core.cmp.PartialOrd.gt.default,
    core.cmp.PartialOrd.gt_body, layout.Level.Insts.CoreCmpPartialOrdLevel.partial_cmp,
    layout.Level.Insts.CoreCmpOrd.cmp, Nat.compare_eq_gt]

/-- `prefers` as a proposition: furthest reach, then higher level, then earlier start. -/
def Prefers (f g : layout.FileInfo) : Prop :=
  hi g < hi f ∨ (hi f = hi g ∧ (lvl g < lvl f ∨ (lvl f = lvl g ∧ lo f < lo g)))

instance (f g : layout.FileInfo) : Decidable (Prefers f g) := by
  unfold Prefers; infer_instance

theorem prefers_eq (f g : layout.FileInfo) :
    plan.prefers f g = ok (decide (Prefers f g)) := by
  unfold plan.prefers Prefers
  simp only [layout.FileKey.impl.range, layout.FileKey.impl.level, lsr_ltx.ids.TxidRange.impl.min,
    lsr_ltx.ids.TxidRange.impl.max, txid_ne, txid_gt, txid_lt, level_ne, level_gt]
  by_cases h1 : hi f = hi g <;> by_cases h2 : lvl f = lvl g <;> simp_all

theorem prefers_hi {f g : layout.FileInfo} (h : Prefers f g) : hi g ≤ hi f := by
  unfold Prefers at h; omega

theorem not_prefers_hi {f g : layout.FileInfo} (h : ¬ Prefers f g) : hi f ≤ hi g := by
  unfold Prefers at h; omega


/-- The condition `ContiguousChain::push` checks: a level 0-8 file that
touches the chain's reach and passes it. -/
def Links (reach : Nat) (f : layout.FileInfo) : Prop :=
  lvl f ≠ 9 ∧ lo f ≤ reach + 1 ∧ reach < hi f

instance (r : Nat) (f : layout.FileInfo) : Decidable (Links r f) := by
  unfold Links; infer_instance

theorem vec_push_eq {α : Type} (v : alloc.vec.Vec α) (x : α) (h : v.val.length < Usize.max) :
    ∃ v', alloc.vec.Vec.push v x = ok v' ∧ v'.val = v.val ++ [x] := by
  unfold alloc.vec.Vec.push
  have : List.length v.val + 1 ≤ U32.max || List.length v.val + 1 ≤ Usize.max := by
    simp; omega
  simp only [this, _root_.dite_true]
  exact ⟨_, rfl, by simp⟩

theorem push_spec (c : plan.ContiguousChain) (f : layout.FileInfo)
    (hlen : c.files.val.length < Usize.max) :
    ∃ b c', plan.ContiguousChain.push c f = ok (b, c') ∧ (b = true ↔ Links (tx c.reaches) f) ∧
      (Links (tx c.reaches) f → c'.files.val = c.files.val ++ [f] ∧
        c'.reaches = f.key.range.max ∧ c'.snapshot = c.snapshot) ∧
      (¬ Links (tx c.reaches) f → c' = c) := by
  unfold plan.ContiguousChain.push
  simp only [layout.FileKey.impl.range, layout.FileKey.impl.level, lsr_ltx.ids.TxidRange.impl.min,
    lsr_ltx.ids.TxidRange.impl.max, is_snapshot_eq, touches_eq, txid_gt]
  obtain ⟨v, hv, hvv⟩ := vec_push_eq c.files f hlen
  by_cases hl : Links (tx c.reaches) f
  · obtain ⟨h9, ht, hp'⟩ := hl
    simp [h9, ht, hp', hv, hvv, Links]
  · have hl' := hl
    simp only [Links, not_and, Nat.not_lt] at hl'
    by_cases h9 : lvl f = 9
    · simp [h9, hl]
    · by_cases ht : lo f ≤ tx c.reaches + 1
      · have := hl' h9 ht
        simp [h9, ht, hl, Nat.not_lt.mpr this]
      · simp [h9, ht, hl]


/-- The furthest reach an extension step has seen: the reach itself, or the
end of the best file so far. -/
def cap (r : Nat) : Option layout.FileInfo → Nat
  | none => r
  | some x => hi x

/-- The invariant of `best_touching`'s loop, over the sweep `L` with reach `r`,
started at `s` and now at `n` holding `b`. -/
def Inner (L : List layout.FileInfo) (r s n : Nat) (b : Option layout.FileInfo) : Prop :=
  s ≤ n ∧ n ≤ L.length ∧
  (∀ i (h : i < L.length), s ≤ i → i < n → lo L[i] ≤ r + 1 ∧ hi L[i] ≤ cap r b) ∧
  (∀ x, b = some x → x ∈ L ∧ lo x ≤ r + 1 ∧ r < hi x ∧ s < n)

theorem cap_ge (r : Nat) (b : Option layout.FileInfo)
    (hb : ∀ x, b = some x → r < hi x) : r ≤ cap r b := by
  cases b with
  | none => simp [cap]
  | some x => have := hb x rfl; simp [cap]; omega

/-- One element of the sweep that touches the reach keeps the invariant. -/
theorem inner_step (L : List layout.FileInfo) (r s n : Nat) (b : Option layout.FileInfo)
    (hn : n < L.length) (hinv : Inner L r s n b) (ht : lo L[n] ≤ r + 1) (beats : Bool)
    (hnone : b = none → beats = true)
    (hsome : ∀ x, b = some x → (beats = true ↔ Prefers L[n] x)) :
    Inner L r s (n + 1) (if r < hi L[n] ∧ beats = true then some L[n] else b) := by
  obtain ⟨hs, -, hrange, hb⟩ := hinv
  have hcap := cap_ge r b (fun x hx => (hb x hx).2.2.1)
  by_cases hu : r < hi L[n] ∧ beats = true
  · simp only [hu, and_self, ite_true]
    have hmono : cap r b ≤ hi L[n] := by
      cases b with
      | none => simp [cap]; omega
      | some x =>
        have := (hsome x rfl).mp hu.2
        simpa [cap] using prefers_hi this
    refine ⟨by omega, by omega, ?_, ?_⟩
    · intro i hi' hsi hin
      by_cases hin' : i < n
      · have := hrange i hi' hsi hin'
        simp only [cap]; omega
      · have : i = n := by omega
        subst this
        simp only [cap]; omega
    · intro x hx
      simp at hx
      subst hx
      exact ⟨List.getElem_mem hn, ht, hu.1, by omega⟩
  · simp only [hu, ite_false]
    have hle : hi L[n] ≤ cap r b := by
      by_cases hr : r < hi L[n]
      · have hnb : beats = false := by
          cases beats <;> simp_all
        cases b with
        | none => simp_all
        | some x =>
          have : ¬ Prefers L[n] x := by
            intro hp; have := (hsome x rfl).mpr hp; simp_all
          simpa [cap] using not_prefers_hi this
      · omega
    refine ⟨by omega, by omega, ?_, fun x hx => ?_⟩
    rotate_left
    · obtain ⟨h1, h2, h3, h4⟩ := hb x hx; exact ⟨h1, h2, h3, by omega⟩
    intro i hi' hsi hin
    by_cases hin' : i < n
    · exact hrange i hi' hsi hin'
    · have : i = n := by omega
      subst this
      exact ⟨ht, hle⟩

theorem best_touching_body_spec (sweep : Slice layout.FileInfo) (reach : lsr_ltx.ids.Txid)
    (s : Nat) (n : Std.Usize) (b : Option layout.FileInfo)
    (hinv : Inner sweep.val (tx reach) s n.val b) :
    plan.best_touching_loop.body sweep reach n b ⦃ r =>
      match r with
      | .done (b', n') => Inner sweep.val (tx reach) s n'.val b' ∧
          ∀ (h : n'.val < sweep.val.length), tx reach + 1 < lo sweep.val[n'.val]
      | .cont (n', b') => Inner sweep.val (tx reach) s n'.val b' ∧
          sweep.val.length - n'.val < sweep.val.length - n.val ⦄ := by
  obtain ⟨hs, hn, hrange, hb⟩ := hinv
  unfold plan.best_touching_loop.body
  simp only [core.slice.Slice.get, core.slice.index.Usize.get, Slice.getElem?_Usize_eq]
  by_cases hlt : n.val < sweep.val.length
  · rw [List.getElem?_eq_getElem hlt]
    simp only [Std.bind_ok, layout.FileKey.impl.range,
      lsr_ltx.ids.TxidRange.impl.min, lsr_ltx.ids.TxidRange.impl.max, touches_eq, txid_gt]
    have hadd := Usize.checked_add_bv_spec n 1#usize
    have hmax : n.val + 1 ≤ Usize.max := by have := sweep.property; omega
    cases hc : Usize.checked_add n 1#usize with
    | none => simp only [hc] at hadd; simp at hadd; omega
    | some m =>
      simp only [hc] at hadd
      simp at hadd
      obtain ⟨-, hm, -⟩ := hadd
      by_cases ht : lo sweep.val[n.val] ≤ tx reach + 1
      · simp only [lo] at ht
        simp only [ht, decide_true, ite_true]
        have hstep := inner_step sweep.val (tx reach) s n.val b hlt ⟨hs, hn, hrange, hb⟩ ht
        have hend : ∀ (bt : Bool) (b0 : Option layout.FileInfo),
            Inner sweep.val (tx reach) s (n.val + 1)
              (if tx reach < hi sweep.val[n.val] ∧ bt = true then some sweep.val[n.val] else b0) →
            (do
              let best1 ← if decide (tx reach < tx sweep.val[n.val].key.range.max) = true then
                  if bt = true then ok (some sweep.val[n.val]) else ok b0
                else ok b0
              let o1 ← lift (some m)
              match o1 with
                | none => ok (done (best1, n))
                | some after => ok (cont (after, best1))) ⦃ r =>
              match r with
              | .done (b', n') => Inner sweep.val (tx reach) s n'.val b' ∧
                  ∀ (h : n'.val < sweep.val.length), tx reach + 1 < lo sweep.val[n'.val]
              | .cont (n', b') => Inner sweep.val (tx reach) s n'.val b' ∧
                  sweep.val.length - n'.val < sweep.val.length - n.val ⦄ := by
          intro bt b0 hI
          simp only [hi] at hI
          have hfin : sweep.val.length - (n.val + 1) < sweep.val.length - n.val := by omega
          by_cases hr : tx reach < tx sweep.val[n.val].key.range.max
          · cases bt
            · simp only [hr, decide_true, ite_true, Bool.false_eq_true, ite_false, and_false]
                at hI ⊢
              simp only [Std.bind_ok, lift, WP.spec_ok]
              rw [hm]; exact ⟨hI, hfin⟩
            · simp only [hr, decide_true, ite_true, and_self] at hI ⊢
              simp only [Std.bind_ok, lift, WP.spec_ok]
              rw [hm]; exact ⟨hI, hfin⟩
          · simp only [hr, decide_false, false_and, ite_false, Bool.false_eq_true] at hI ⊢
            simp only [Std.bind_ok, lift, WP.spec_ok]
            rw [hm]; exact ⟨hI, hfin⟩
        cases b with
        | none =>
          simp only [Std.bind_ok]
          exact hend true none (hstep true (fun _ => rfl) (by simp))
        | some x =>
          simp only [Std.bind_ok, prefers_eq]
          exact hend _ (some x) (hstep _ (by simp) (by simp))
      · simp only [lo] at ht
        simp only [ht, decide_false, Bool.false_eq_true, ite_false, WP.spec_ok]
        exact ⟨⟨hs, hn, hrange, hb⟩, fun _ => by simp only [lo]; omega⟩
  · rw [List.getElem?_eq_none (by omega)]
    simp only [Std.bind_ok, WP.spec_ok]
    exact ⟨⟨hs, hn, hrange, hb⟩, fun h => absurd h hlt⟩


/-- `best_touching` from `start`: the files from `start` to the returned index
all touch the reach, the next one (if any) does not, and the returned file is
one of them that passes the reach and reaches at least as far as any of them. -/
theorem best_touching_spec (sweep : Slice layout.FileInfo) (start : Std.Usize)
    (reach : lsr_ltx.ids.Txid) (hs : start.val ≤ sweep.val.length) :
    plan.best_touching sweep start reach ⦃ b n =>
      Inner sweep.val (tx reach) start.val n.val b ∧
      ∀ (h : n.val < sweep.val.length), tx reach + 1 < lo sweep.val[n.val] ⦄ := by
  unfold plan.best_touching plan.best_touching_loop
  apply loop.spec_decr_nat (fun (x : Std.Usize × Option layout.FileInfo) => sweep.val.length - x.1.val)
    (fun x => Inner sweep.val (tx reach) start.val x.1.val x.2)
  · rintro ⟨n, b⟩ hinv
    apply WP.spec_mono (best_touching_body_spec sweep reach start.val n b hinv)
    rintro (⟨b', n'⟩ | ⟨n', b'⟩) h <;> exact h
  · exact ⟨le_refl _, hs, fun i _ h1 h2 => absurd h2 (by simp only at h2 ⊢; omega),
      fun x hx => by simp at hx⟩


/-- `fs` is a contiguous chain of files from `pool` that extends a chain
reaching `r` to one reaching `r'`: each file is of levels 0 to 8, starts at
most one past the reach before it, and ends beyond it. -/
inductive IsChain (pool : List layout.FileInfo) : Nat → List layout.FileInfo → Nat → Prop
  | nil (r : Nat) : IsChain pool r [] r
  | cons (r : Nat) (f : layout.FileInfo) (fs : List layout.FileInfo) (r' : Nat) :
      f ∈ pool → Links r f → IsChain pool (hi f) fs r' → IsChain pool r (f :: fs) r'

theorem IsChain.snoc {pool : List layout.FileInfo} {r r' : Nat} {fs : List layout.FileInfo}
    {f : layout.FileInfo} (h : IsChain pool r fs r') (hf : f ∈ pool) (hl : Links r' f) :
    IsChain pool r (fs ++ [f]) (hi f) := by
  induction h with
  | nil r => exact .cons r f [] (hi f) hf hl (.nil _)
  | cons r g gs r'' hg hgl _ ih => exact .cons r g (gs ++ [f]) (hi f) hg hgl (ih hl)

theorem IsChain.le {pool : List layout.FileInfo} {r r' : Nat} {fs : List layout.FileInfo}
    (h : IsChain pool r fs r') : r ≤ r' := by
  induction h with
  | nil r => exact le_refl r
  | cons r f fs r' _ hl _ ih => exact Nat.le_of_lt (Nat.lt_of_lt_of_le hl.2.2 ih)

theorem IsChain.mono {pool pool' : List layout.FileInfo} {r r' : Nat} {fs : List layout.FileInfo}
    (h : IsChain pool r fs r') (hsub : ∀ f ∈ pool, f ∈ pool') : IsChain pool' r fs r' := by
  induction h with
  | nil r => exact .nil r
  | cons r f fs r' hf hl _ ih => exact .cons r f fs r' (hsub f hf) hl ih

/-- No level 0-8 file of `pool` that touches `R` passes it: nothing can extend
a chain that reaches `R`. -/
def Closed (pool : List layout.FileInfo) (R : Nat) : Prop :=
  ∀ f ∈ pool, lvl f ≠ 9 → lo f ≤ R + 1 → hi f ≤ R

/-- The greedy argument: from a reach at or below a closed reach `R`, no chain
gets past `R`. -/
theorem IsChain.le_closed {pool : List layout.FileInfo} {r r' R : Nat}
    {fs : List layout.FileInfo} (h : IsChain pool r fs r') (hr : r ≤ R)
    (hc : Closed pool R) : r' ≤ R := by
  induction h with
  | nil r => exact hr
  | cons r f fs r' hf hl _ ih => exact ih (hc f hf hl.1 (by have := hl.2.1; omega))

/-- The invariant of `extend`'s loop over the sorted sweep `L`, from the chain
`c0`, now at chain `c` and sweep index `n`. -/
def Outer (L : List layout.FileInfo) (c0 c : plan.ContiguousChain) (n : Nat) : Prop :=
  c.snapshot = c0.snapshot ∧ n ≤ L.length ∧
  (∃ fs, c.files.val = c0.files.val ++ fs ∧ fs.length ≤ n ∧
    IsChain L (tx c0.reaches) fs (tx c.reaches)) ∧
  ∀ i (h : i < L.length), i < n → hi L[i] ≤ tx c.reaches

/-- What `extend`'s loop ends with: the invariant, and every file of the sweep
that touches the reach ends at or before it. -/
def OuterDone (L : List layout.FileInfo) (c0 c : plan.ContiguousChain) : Prop :=
  c.snapshot = c0.snapshot ∧
  (∃ fs, c.files.val = c0.files.val ++ fs ∧ IsChain L (tx c0.reaches) fs (tx c.reaches)) ∧
  Closed L (tx c.reaches)

theorem extend_body_spec (sweep : alloc.vec.Vec layout.FileInfo) (c0 c : plan.ContiguousChain)
    (n : Std.Usize)
    (hlvl : ∀ f ∈ sweep.val, lvl f ≠ 9)
    (hsorted : sweep.val.Pairwise (fun a b => lo a ≤ lo b))
    (hcap : c0.files.val.length + sweep.val.length ≤ Usize.max)
    (hinv : Outer sweep.val c0 c n.val) :
    plan.extend_loop.body sweep c n ⦃ r =>
      match r with
      | .done c' => OuterDone sweep.val c0 c'
      | .cont (c', n') => Outer sweep.val c0 c' n'.val ∧
          sweep.val.length - n'.val < sweep.val.length - n.val ⦄ := by
  obtain ⟨hsnap, hn, ⟨fs, hfiles, hfs, hchain⟩, hbelow⟩ := hinv
  unfold plan.extend_loop.body
  simp only [plan.ContiguousChain.impl.reaches, Std.bind_ok]
  have hderef : (alloc.vec.Vec.deref sweep).val = sweep.val := by simp [alloc.vec.Vec.deref]
  apply WP.spec_bind (best_touching_spec _ n c.reaches (by rw [hderef]; exact hn))
  rintro ⟨b, n'⟩ ⟨⟨hs, hn', hrange, hb⟩, hnext⟩
  simp only [hderef] at hn' hrange hb hnext
  cases b with
  | none =>
    simp only [uncurry_apply_pair]
    simp only [WP.spec_ok]
    refine ⟨hsnap, ⟨fs, hfiles, hchain⟩, ?_⟩
    intro f hf h9 hlo
    obtain ⟨i, hi', rfl⟩ := List.getElem_of_mem hf
    by_cases hin : i < n'.val
    · by_cases hin0 : i < n.val
      · exact hbelow i hi' hin0
      · exact (hrange i hi' (by omega) hin).2
    · have hlt : n'.val < sweep.val.length := by omega
      have h1 := hnext hlt
      have h2 : lo sweep.val[n'.val] ≤ lo sweep.val[i] := by
        by_cases heq : n'.val = i
        · subst heq; exact le_refl _
        · exact List.pairwise_iff_getElem.mp hsorted _ _ hlt hi' (by omega)
      omega
  | some x =>
    obtain ⟨hxL, hxlo, hxhi, hsn⟩ := hb x rfl
    have hpush := push_spec c x (by rw [hfiles]; simp; omega)
    obtain ⟨b, c', hpeq, hbiff, hyes, -⟩ := hpush
    have hlinks : Links (tx c.reaches) x := ⟨hlvl x hxL, hxlo, hxhi⟩
    have hbt : b = true := hbiff.mpr hlinks
    subst hbt
    obtain ⟨hf', hr', hs'⟩ := hyes hlinks
    simp only [uncurry_apply_pair, hpeq, Std.bind_ok, ite_true]
    apply (WP.spec_ok _).mpr
    refine ⟨⟨hs'.trans hsnap, by omega, ⟨fs ++ [x], ?_, ?_, ?_⟩, ?_⟩, by omega⟩
    · rw [hf', hfiles]; simp
    · simp; omega
    · rw [hr']; exact hchain.snoc hxL hlinks
    · intro i hi' hin
      rw [hr']
      by_cases hin0 : i < n.val
      · have := hbelow i hi' hin0; simp only [hi] at hxhi this ⊢; omega
      · have := (hrange i hi' (by omega) hin).2
        simpa [cap] using this


theorem extend_loop_spec (sweep : alloc.vec.Vec layout.FileInfo) (c0 : plan.ContiguousChain)
    (hlvl : ∀ f ∈ sweep.val, lvl f ≠ 9)
    (hsorted : sweep.val.Pairwise (fun a b => lo a ≤ lo b))
    (hcap : c0.files.val.length + sweep.val.length ≤ Usize.max) :
    plan.extend_loop c0 sweep 0#usize ⦃ c => OuterDone sweep.val c0 c ⦄ := by
  unfold plan.extend_loop
  apply loop.spec_decr_nat
    (fun (x : plan.ContiguousChain × Std.Usize) => sweep.val.length - x.2.val)
    (fun x => Outer sweep.val c0 x.1 x.2.val)
  · rintro ⟨c, n⟩ hinv
    apply WP.spec_mono (extend_body_spec sweep c0 c n hlvl hsorted hcap hinv)
    rintro (c' | ⟨c', n'⟩) h <;> exact h
  · exact ⟨rfl, by simp, ⟨[], by simp, by simp, .nil _⟩, fun i _ h => by simp at h⟩

/-- The level 0-8 files of a pool, in pool order. -/
abbrev candidates (pool : List layout.FileInfo) : List layout.FileInfo :=
  pool.filter (fun f => !decide (lvl f = 9))

theorem extension_candidates_spec (pool : Slice layout.FileInfo) :
    plan.extension_candidates pool ⦃ v => v.val = candidates pool.val ⦄ := by
  unfold plan.extension_candidates plan.extension_candidates_loop
  simp only [SharedSlice.Insts.CoreIterTraitsCollectIntoIteratorSharedIter.into_iter, Std.bind_ok]
  apply loop.spec_decr_nat
    (fun (x : core.slice.iter.Iter layout.FileInfo × alloc.vec.Vec layout.FileInfo) =>
      pool.val.length - x.1.i)
    (fun x => x.1.slice = pool ∧ x.1.i ≤ pool.val.length ∧
      x.2.val = candidates (pool.val.take x.1.i))
  · rintro ⟨it, v⟩ ⟨hsl, hi, hv⟩
    simp only at hsl hi hv
    unfold plan.extension_candidates_loop.body core.slice.iter.IteratorSliceIter.next
    subst hsl
    dsimp only
    split
    · rename_i hlt'
      have hlt : it.i < it.slice.val.length := by simpa using hlt'
      generalize hx : it.slice[it.i]'hlt' = x
      have hxv : x = it.slice.val[it.i] := by rw [← hx]; rfl
      simp only [uncurry_apply_pair, Std.bind_ok, layout.FileKey.impl.level, is_snapshot_eq]
      have htake : it.slice.val.take (it.i + 1) = it.slice.val.take it.i ++ [x] := by
        rw [hxv]; exact List.take_succ_eq_append_getElem hlt
      by_cases h9 : lvl x = 9
      · simp only [lvl] at h9
        simp only [h9, decide_true, ite_true, WP.spec_ok]
        refine ⟨⟨trivial, by omega, ?_⟩, by omega⟩
        simp only [htake, hv, List.filter_append]
        simp [h9]
      · simp only [lvl] at h9
        have hlen : v.val.length < Usize.max := by
          have h1 := List.length_filter_le (fun f => !decide (lvl f = 9)) (it.slice.val.take it.i)
          have h2 := it.slice.property
          have h3 : (it.slice.val.take it.i).length = it.i := by simp; omega
          rw [hv]; simp only [candidates]; omega
        obtain ⟨v', hv', hvv'⟩ := vec_push_eq v x hlen
        simp only [h9, decide_false, Bool.false_eq_true, ite_false, hv', Std.bind_ok, WP.spec_ok]
        refine ⟨⟨trivial, by omega, ?_⟩, by omega⟩
        simp only [htake, hvv', hv, List.filter_append]
        simp [h9]
    · rename_i hlt'
      have hlt : ¬ it.i < it.slice.val.length := by simpa using hlt'
      simp only [uncurry_apply_pair, Std.bind_ok, WP.spec_ok]
      rw [hv, List.take_of_length_le (by omega)]
  · exact ⟨rfl, by simp, by simp⟩


theorem extend_spec (chain : plan.ContiguousChain) (pool : Slice layout.FileInfo)
    (hcap : chain.files.val.length + pool.val.length ≤ Usize.max) :
    plan.extend chain pool ⦃ c =>
      c.snapshot = chain.snapshot ∧
      (∃ fs, c.files.val = chain.files.val ++ fs ∧
        IsChain pool.val (tx chain.reaches) fs (tx c.reaches)) ∧
      Closed pool.val (tx c.reaches) ⦄ := by
  unfold plan.extend
  apply WP.spec_bind (extension_candidates_spec pool)
  intro sweep hsweep
  obtain ⟨r, hr, hperm, hsorted⟩ := plan.sort_by_min_spec (alloc.vec.Vec.deref_mut sweep).1
  simp only [alloc.vec.Vec.deref_mut] at hr hperm
  simp only [lift, Std.bind_ok, uncurry_apply_pair, alloc.vec.Vec.deref_mut]
  rw [hr, Std.bind_ok]
  have hval : (({ slice := r } : alloc.vec.Vec layout.FileInfo)).val = r.val := rfl
  have hsv : sweep.slice.val = candidates pool.val := hsweep
  have hmem : ∀ f, f ∈ r.val ↔ f ∈ candidates pool.val := fun f => by
    rw [← hsv]; exact hperm.mem_iff
  have hlvl : ∀ f ∈ (({ slice := r } : alloc.vec.Vec layout.FileInfo)).val, lvl f ≠ 9 := by
    intro f hf; rw [hval, hmem] at hf; simp at hf; exact hf.2
  have hlen : chain.files.val.length + (({ slice := r } : alloc.vec.Vec layout.FileInfo)).val.length
      ≤ Usize.max := by
    rw [hval, hperm.length_eq, hsv, candidates]
    have := List.length_filter_le (fun f => !decide (lvl f = 9)) pool.val
    omega
  apply WP.spec_mono (extend_loop_spec _ chain hlvl (by rw [hval]; exact hsorted) hlen)
  rintro c ⟨hsnap, ⟨fs, hfiles, hchain⟩, hclosed⟩
  refine ⟨hsnap, ⟨fs, hfiles, hchain.mono ?_⟩, ?_⟩
  · intro f hf; rw [hval, hmem] at hf; simp at hf; exact hf.1
  · intro f hf h9 hlo
    exact hclosed f (by rw [hval, hmem]; simp; exact ⟨hf, h9⟩) h9 hlo


/-- Goal 1, soundness. `extend` keeps the chain's snapshot, only appends, and
what it appends is a contiguous chain of pool files: each is of levels 0 to 8,
starts at most one past the reach before it, and ends beyond it. -/
theorem extend_sound (chain : plan.ContiguousChain) (pool : Slice layout.FileInfo)
    (hcap : chain.files.val.length + pool.val.length ≤ Usize.max) :
    plan.extend chain pool ⦃ c =>
      c.snapshot = chain.snapshot ∧
      ∃ fs, c.files.val = chain.files.val ++ fs ∧
        IsChain pool.val (tx chain.reaches) fs (tx c.reaches) ⦄ := by
  apply WP.spec_mono (extend_spec chain pool hcap)
  rintro c ⟨hsnap, hchain, -⟩
  exact ⟨hsnap, hchain⟩

/-- Goal 2, greedy optimality. Every contiguous chain of pool files from the
reach `extend` starts at ends at or before the reach `extend` ends at: the
greedy sweep reaches as far as any choice of files could. -/
theorem extend_optimal (chain : plan.ContiguousChain) (pool : Slice layout.FileInfo)
    (hcap : chain.files.val.length + pool.val.length ≤ Usize.max) :
    plan.extend chain pool ⦃ c =>
      ∀ gs R, IsChain pool.val (tx chain.reaches) gs R → R ≤ tx c.reaches ⦄ := by
  apply WP.spec_mono (extend_spec chain pool hcap)
  rintro c ⟨-, ⟨fs, -, hchain⟩, hclosed⟩ gs R hgs
  exact hgs.le_closed hchain.le hclosed

end lsr_core.proofs