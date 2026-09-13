# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Terminal audit lookup stays indexed as the archive grows."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import asyncpg
import pytest

from symba.db.queries import Q

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


def _nodes(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans", []):
        yield from _nodes(child)


async def test_terminal_audit_does_not_scan_large_archive(db: asyncpg.Connection) -> None:
    await db.execute(
        "INSERT INTO jobs_archive (task_name, tenant, ctx_id, payload, final_state) "
        "SELECT 'archive.plan', 'archive-tenant', 'ctx-' || n, '{}'::jsonb, 'succeeded' "
        "FROM generate_series(1, 50000) n"
    )
    await db.execute("ANALYZE jobs_archive")
    job_id = await db.fetchval("SELECT id FROM jobs_archive WHERE ctx_id = 'ctx-25000'")
    raw = await db.fetchval("EXPLAIN (ANALYZE, FORMAT JSON) " + Q.RECORD_EVENT, job_id, "succeeded", None)
    document = json.loads(raw) if isinstance(raw, str) else raw
    scans = [node for node in _nodes(document[0]["Plan"]) if node.get("Relation Name", "").startswith("jobs_archive")]
    assert scans, "The actual audit INSERT must resolve its archived job"
    assert all(node["Node Type"] in {"Index Scan", "Index Only Scan", "Bitmap Heap Scan"} for node in scans)
    assert all(node["Actual Rows"] <= 1 for node in scans)
    event = await db.fetchrow("SELECT tenant, ctx_id FROM job_events WHERE job_id = $1", job_id)
    assert event is not None
    assert dict(event) == {"tenant": "archive-tenant", "ctx_id": "ctx-25000"}
