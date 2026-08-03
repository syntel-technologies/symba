"""L2 test helpers: build minimal specs and drive jobs into a target state.

Kept tiny and explicit (no factory magic) so each test reads as a story: seed
these rows, run this repo call, assert this row moved.
"""

from __future__ import annotations

from typing import Any

import asyncpg

from symba.db import repository as repo
from symba.db.records import ClaimedJob, SubmitSpec
from symba.services.registry import WorkerRegistry


def registry() -> WorkerRegistry:
    """A throwaway registry for services that need one only to signal a wake."""
    return WorkerRegistry()


def spec(
    task_name: str = "t.echo",
    *,
    tenant: str = "default",
    state: str = "queued",
    group_key: str | None = None,
    cap: int | None = None,
    dedup_key: str | None = None,
    runs_on: list[str] | None = None,
    rate_class: str | None = None,
    priority: int = 0,
    lease_ttl_s: int = 60,
    payload: dict[str, Any] | None = None,
) -> SubmitSpec:
    return SubmitSpec(
        task_name=task_name,
        payload=payload or {"n": 1},
        tenant=tenant,
        state=state,
        group_key=group_key,
        max_concurrent_per_group=cap,
        dedup_key=dedup_key,
        runs_on=runs_on or [],
        rate_class=rate_class,
        priority=priority,
        lease_ttl_s=lease_ttl_s,
    )


async def seed(conn: asyncpg.Connection, s: SubmitSpec) -> str:
    res = await repo.submit(conn, s)
    assert res.job_id is not None, "seed should not dedup"
    return res.job_id


async def claim_one(conn: asyncpg.Connection, *, worker: str = "w1", tags: list[str] | None = None) -> ClaimedJob:
    jobs = await repo.claim(
        conn,
        worker_tags=tags or [],
        exhausted_rate_classes=[],
        limit=1,
        claimed_by=worker,
        per_group_cap=1000,
    )
    assert len(jobs) == 1, f"expected exactly one claim, got {len(jobs)}"
    return jobs[0]


async def state_of(conn: asyncpg.Connection, job_id: str) -> str | None:
    return await conn.fetchval("SELECT final_state FROM jobs_all WHERE id = $1", job_id)


async def group_running(conn: asyncpg.Connection, tenant: str, group_key: str, task: str) -> int:
    val = await conn.fetchval(
        "SELECT running FROM group_running WHERE tenant=$1 AND group_key=$2 AND task_name=$3",
        tenant,
        group_key,
        task,
    )
    return val or 0
