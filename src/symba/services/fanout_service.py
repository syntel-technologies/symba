# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""Fan-out + gate service.

FanOut opens a completion barrier (a `gates` row) over N child jobs and stores the
continuation JobSpec (`on_complete`) on the gate. Each child carries the gate id in
`parent_gate_id`; as children reach a terminal state they bump the gate
(`JobService._settle_gate`), and the FIRST completion that satisfies the policy
fires the continuation exactly once (fired_at claim-once).

    fan-out topology
    -----------------------
                         ┌── child_1 ─┐
        FanOut ──gate──► ├── child_2 ─┤ ──(policy satisfied)──► on_complete job
                         └── child_N ─┘
        policy ∈ {all_success, all_terminal, quorum(n)}   (core/gates.py)

Everything (gate row + all N children) is inserted in ONE transaction: children
never exist without their gate, and a crash mid-fan-out rolls back cleanly. The
≤100k width cap bounds the single-tx insert.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from symba.config import SymbaConfig
from symba.core.errors import FanOutTooLarge
from symba.core.gates import is_satisfied
from symba.core.states import JobState
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.db.records import SubmitSpec
from symba.observability.logging import logger
from symba.services.registry import WorkerRegistry

logger = logger.bind(service="fanout_service", context="engine/services")


@dataclass(slots=True)
class FanOutOutcome:
    child_job_ids: list[str]
    gate_id: str


def _quorum_n(policy: str) -> int | None:
    """Extract n from 'quorum(n)'; None for the count-less policies. Validates early."""
    if policy in ("all_success", "all_terminal"):
        return None
    if policy.startswith("quorum(") and policy.endswith(")"):
        return int(policy[len("quorum(") : -1])
    raise ValueError(f"unknown gate policy {policy!r}")


class FanOutService:
    def __init__(self, pools: Pools, registry: WorkerRegistry, config: SymbaConfig) -> None:
        self._pools = pools
        self._registry = registry
        self._max_children = config.limits.max_fanout_children

    async def fan_out(
        self,
        *,
        tenant: str,
        ctx_id: str | None,
        children: list[SubmitSpec],
        on_complete: SubmitSpec,
        policy: str,
    ) -> FanOutOutcome:
        # Guard clauses first (workspace rule): reject an empty or oversized fan-out
        # before opening a transaction.
        if not children:
            raise ValueError("fan_out requires at least one child")
        if len(children) > self._max_children:
            raise FanOutTooLarge(count=len(children), cap=self._max_children)
        quorum_n = _quorum_n(policy)  # raises ValueError on an unknown policy

        # Store the continuation as a SubmitSpec-shaped dict; tenant/ctx_id are set
        # from the gate at fire time (JobService._settle_gate), so drop any stale
        # copies here to keep one source of truth.
        on_complete_dict = asdict(on_complete)
        on_complete_dict.pop("tenant", None)
        on_complete_dict.pop("ctx_id", None)
        on_complete_dict.pop("state", None)  # gate fire always enqueues 'queued'

        child_ids: list[str] = []
        any_queued = False
        async with self._pools.general.acquire() as conn, conn.transaction():
            gate_id = await repo.create_gate(
                conn,
                tenant=tenant,
                ctx_id=ctx_id,
                policy=policy,
                expected_children=len(children),
                quorum_n=quorum_n,
                on_complete=on_complete_dict,
            )
            for child in children:
                child.tenant = tenant
                child.ctx_id = ctx_id
                child.parent_gate_id = gate_id
                child.state = JobState.QUEUED  # children are immediately claimable
                result = await repo.submit(conn, child)
                child_ids.append(result.job_id or "")
                if not result.deduplicated:
                    any_queued = True

        if any_queued:
            self._registry.wake.set()
        logger.info(
            "[fan_out] Gate opened",
            gate_id=gate_id,
            children=len(children),
            policy=policy,
            tenant=tenant,
        )
        return FanOutOutcome(child_job_ids=child_ids, gate_id=gate_id)

    async def gate_status(self, *, tenant: str, gate_id: str) -> repo.GateStatusRow | None:
        """Authoritative gate aggregate from the gates row (spec 7.2).

        Backs GetGate. The gate row is the ONLY source that can report `succeeded`
        correctly: a ctx.skip() child settles the gate but must be excluded from
        succeeded, and a client-side recount over child job states cannot tell a
        skip apart from a real success. Read from the general pool, not hot.
        """
        async with self._pools.general.acquire() as conn:
            return await repo.get_gate(conn, tenant=tenant, gate_id=gate_id)


# Re-exported so callers can assert the policy predicate against live gate counts
# without importing core directly (thin convenience; core stays the source of truth).
__all__ = ["FanOutService", "FanOutOutcome", "is_satisfied"]
