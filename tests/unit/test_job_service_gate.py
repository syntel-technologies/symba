from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from symba.db import repository as repo
from symba.db.records import SubmitResult
from symba.services.job_service import JobService


@pytest.mark.asyncio
async def test_unsatisfied_gate_resolution_enqueues_failure_hook_once(monkeypatch: pytest.MonkeyPatch) -> None:
    resolution = repo.GateResolution(
        resolved=True,
        satisfied=False,
        on_complete={
            "task_name": "reduce",
            "payload": {},
            "on_failure": {
                "task_name": "reduce_failed",
                "payload": {"source": "gate"},
            },
        },
        tenant="tenant-a",
        ctx_id="ctx-a",
        expected=2,
        succeeded=1,
    )
    losing_bump = repo.GateResolution(
        resolved=False,
        satisfied=False,
        on_complete=resolution.on_complete,
        tenant=resolution.tenant,
        ctx_id=resolution.ctx_id,
        expected=resolution.expected,
        succeeded=resolution.succeeded,
    )
    bump_gate = AsyncMock(side_effect=[resolution, losing_bump])
    submit = AsyncMock(return_value=SubmitResult(job_id="failure-job", deduplicated=False))
    monkeypatch.setattr(repo, "bump_gate", bump_gate)
    monkeypatch.setattr(repo, "submit", submit)

    service = object.__new__(JobService)
    conn = object()
    first = await service._settle_gate(
        conn,
        parent_gate_id="gate-a",
        child_succeeded=False,
        child_failed=True,
    )
    second = await service._settle_gate(
        conn,
        parent_gate_id="gate-a",
        child_succeeded=True,
        child_failed=False,
    )

    assert first is True
    assert second is False
    submit.assert_awaited_once()
    submitted = submit.await_args.args[1]
    assert submitted.task_name == "reduce_failed"
    assert submitted.payload == {"source": "gate"}
    assert submitted.tenant == "tenant-a"
    assert submitted.ctx_id == "ctx-a"
    assert submitted.group_key == "gate-a"
    assert submitted.state == "queued"
