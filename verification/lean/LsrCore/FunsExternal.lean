-- Hand-written model of the standard-library functions Aeneas leaves opaque.
-- Aeneas generates a template for this file (FunsExternal_Template.lean); this
-- is the filled-in version and part of the trusted base (see README.md).
-- Each definition follows the Rust source of the function it models.
module
public import Aeneas
public import LsrCore.Types
@[expose] public section
open Aeneas Aeneas.Std Result ControlFlow Error
open lsr_core

/-- Copying the niche marker returns it unchanged. -/
def core.num.niche_types.NonZeroU64Inner.Insts.CoreCloneClone.clone
  (x : core.num.niche_types.NonZeroU64Inner) :
  Result core.num.niche_types.NonZeroU64Inner :=
  ok x

/-- `impl PartialEq for NonZero<T>`: `self.get() == other.get()`. -/
def core.num.nonzero.NonZero.Insts.CoreCmpPartialEqNonZero.eq
  {T : Type} {Inner : Type}
  (_ZeroablePrimitiveInst : core.num.nonzero.ZeroablePrimitive T Inner)
  (cmpPartialEqInst : core.cmp.PartialEq T T)
  (a b : core.num.nonzero.NonZero T Inner) : Result Bool :=
  cmpPartialEqInst.eq a b

/-- `impl Ord for NonZero<T>`: `self.get().cmp(&other.get())`. -/
def core.num.nonzero.NonZero.Insts.CoreCmpOrd.cmp
  {T : Type} {Inner : Type}
  (_ZeroablePrimitiveInst : core.num.nonzero.ZeroablePrimitive T Inner)
  (cmpOrdInst : core.cmp.Ord T)
  (a b : core.num.nonzero.NonZero T Inner) : Result Ordering :=
  cmpOrdInst.cmp a b

/-- `NonZeroU64::checked_add`: `self.get().checked_add(other)`, rewrapped. -/
def core.num.nonzero.NonZeroU64NonZeroU64Inner.checked_add
  (a : core.num.nonzero.NonZero Std.U64 core.num.niche_types.NonZeroU64Inner)
  (other : Std.U64) :
  Result (Option (core.num.nonzero.NonZero Std.U64
    core.num.niche_types.NonZeroU64Inner)) :=
  ok (U64.checked_add a other)

/-- `<[T]>::sort_by_key`, left opaque: a constant with no definition. What the
proofs assume about it is the single axiom in `LsrCore/Sort.lean`. -/
opaque alloc.slice.Slice.sort_by_key
  {T : Type} {K : Type} {F : Type}
  (coreopsfunctionFnMutFTupleShared0TKInst : core.ops.function.FnMut F T K)
  (corecmpOrdInst : core.cmp.Ord K) :
  Slice T → F → Result (Slice T)
