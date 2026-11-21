-- Hand-written model of the standard-library types Aeneas leaves opaque.
-- Aeneas generates a template for this file (TypesExternal_Template.lean); this
-- is the filled-in version and part of the trusted base (see README.md).
module
public import Aeneas
@[expose] public section
open Aeneas Aeneas.Std Result ControlFlow Error

/-- `NonZeroU64`'s niche marker. Never inspected by the translated code. -/
@[reducible]
def core.num.niche_types.NonZeroU64Inner : Type := Std.U64

/-- `NonZero<T>` is modelled as `T` itself, without the nonzero invariant. The
proofs therefore hold for every value of `T`, zero included: a superset of
what Rust can build, so nothing proved here depends on the invariant. -/
@[reducible]
def core.num.nonzero.NonZero (T : Type) (_Inner : Type) : Type := T
