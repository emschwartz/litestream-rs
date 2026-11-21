-- Prints the axioms each headline theorem rests on; scripts/verify-lean.sh
-- fails if any is outside Lean's three and `lsr_core.plan.sort_by_min_spec`.
import LsrCore.Extend

#print axioms lsr_core.proofs.extend_sound
#print axioms lsr_core.proofs.extend_optimal
