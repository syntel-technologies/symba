-- V008__gate_failed_children.sql — track DEAD children on a gate separately.
--
-- Why: all_success must distinguish a DEAD child (blocks the gate) from a
-- ctx.skip() child (a deliberate no-op that neither succeeds nor blocks). The
-- gate previously tracked only completed_children and succeeded_children, so a
-- skip was indistinguishable from a failure and all_success could never fire once
-- any child skipped. failed_children counts DEAD children only; a skip increments
-- completed_children alone. The invariant is:
--
--     completed_children = succeeded_children + failed_children + skipped
--
-- where skipped is derived (completed - succeeded - failed), never stored.
ALTER TABLE gates
    ADD COLUMN failed_children INT NOT NULL DEFAULT 0;
