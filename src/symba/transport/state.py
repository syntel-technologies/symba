# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Shared engine state passed to transports and subsystem loops.

A single small container holds the wired dependencies (config, pools, the worker
registry, and the constructed services) plus the `serving` flag that gates
readiness. It holds no business logic itself — it just wires singletons so the
transport layer stays thin and every subsystem shares ONE registry and ONE
service instance.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from symba.config import SymbaConfig
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.observability.logging import logger
from symba.services.cancel_service import CancelService
from symba.services.checkpoint_service import CheckpointService
from symba.services.event_stream import EventStream
from symba.services.fanout_service import FanOutService
from symba.services.job_service import JobService
from symba.services.matcher import Matcher
from symba.services.query_service import QueryService
from symba.services.rate_limiter import RateLimiter
from symba.services.registry import WorkerRegistry
from symba.services.resubmit_service import ResubmitService
from symba.services.signal_service import SignalService
from symba.services.submit_service import SubmitService
from symba.transport.auth import Authenticator, build_authenticator

logger = logger.bind(service="engine_state", context="engine/transport")


def _build_registry(pools: Pools) -> WorkerRegistry:
    """Wire the registry with read-model persistence.

    The upsert/delete hooks mirror worker lifecycle into the `workers` table on the
    GENERAL pool (never the hot claim pool). Both are contained: a DB blip logs and
    returns rather than breaking the Claim stream, because the in-memory registry —
    not this table — is authoritative for matching.
    """

    async def _upsert(worker_id: str, tags: list[str], labels: dict[str, Any], slots: int, slots_busy: int) -> None:
        try:
            async with pools.general.acquire() as conn:
                await repo.worker_upsert(
                    conn, worker_id=worker_id, tags=tags, labels=labels, slots=slots, slots_busy=slots_busy
                )
        except Exception:
            logger.error("[registry] Worker upsert failed", worker_id=worker_id, exc_info=True)

    async def _delete(worker_id: str) -> None:
        try:
            async with pools.general.acquire() as conn:
                await repo.worker_delete(conn, worker_id=worker_id)
        except Exception:
            logger.error("[registry] Worker delete failed", worker_id=worker_id, exc_info=True)

    return WorkerRegistry(on_upsert=_upsert, on_delete=_delete)


@dataclass(slots=True)
class EngineState:
    config: SymbaConfig
    pools: Pools
    registry: WorkerRegistry
    jobs: JobService
    submit: SubmitService
    fanout: FanOutService
    cancel: CancelService
    signals: SignalService
    checkpoints: CheckpointService
    resubmit: ResubmitService
    query: QueryService
    events: EventStream
    matcher: Matcher
    rate_limiter: RateLimiter
    authenticator: Authenticator
    # Flipped True once subsystems are wired and the process should accept
    # traffic; readiness probes return 503 until then and again on drain.
    serving: bool = field(default=False)

    @classmethod
    def build(cls, config: SymbaConfig, pools: Pools) -> EngineState:
        registry = _build_registry(pools)
        rate_limiter = RateLimiter(config.redis, pools)
        return cls(
            config=config,
            pools=pools,
            registry=registry,
            jobs=JobService(pools, config, registry),
            submit=SubmitService(pools, registry, config),
            fanout=FanOutService(pools, registry, config),
            cancel=CancelService(pools, registry),
            signals=SignalService(pools, registry),
            checkpoints=CheckpointService(pools, rate_limiter),
            resubmit=ResubmitService(pools, registry),
            query=QueryService(pools),
            events=EventStream(pools),
            matcher=Matcher(pools, registry, config, rate_limiter),
            rate_limiter=rate_limiter,
            authenticator=build_authenticator(config.auth),
        )
