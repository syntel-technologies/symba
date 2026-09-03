from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from symba.db import repository as repo
from symba.db.queries import Q
from symba.db.records import TerminalRow


@pytest.mark.asyncio
async def test_insert_continuation_clears_job_local_rate_class() -> None:
    conn = AsyncMock()
    conn.fetchval.return_value = "tail-id"
    predecessor = TerminalRow(
        id="head-id",
        tenant="tenant-a",
        ctx_id="ctx-a",
        task_name="provider.call",
        group_key="document-a",
        max_concurrent_per_group=1,
        pipeline="ingestion",
        stage="summarize",
        priority=7,
        runs_on=["gpu"],
        rate_class="llm",
        lease_ttl_s=120,
        on_failure={"task_name": "stage.failed"},
    )

    result = await repo.insert_continuation(
        conn,
        next_task="db.apply",
        next_on_success="stage.complete",
        next_chain_tail=["stage.release"],
        predecessor=predecessor,
    )

    assert result == "tail-id"
    args = conn.fetchval.await_args.args
    assert args[0] == Q.COMPLETE_CONTINUATION
    assert args[11] == ["gpu"]
    assert args[12] is None
    assert args[13] == 120
    assert args[14] == {"task_name": "stage.failed"}
