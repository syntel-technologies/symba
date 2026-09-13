from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from symba.transport.client_service import ClientServicer
from symba.v1 import control_plane_pb2 as cp


@pytest.mark.asyncio
async def test_grpc_query_forwards_every_native_filter() -> None:
    query = MagicMock()
    query.jobs = AsyncMock(return_value=[])
    servicer = ClientServicer(
        submit=MagicMock(),
        fanout=MagicMock(),
        cancel=MagicMock(),
        signals=MagicMock(),
        resubmit=MagicMock(),
        query=query,
        events=MagicMock(),
    )
    request = cp.QueryRequest(
        tenant="default",
        ctx_id="doc-1",
        pipeline="iknowledge:index:index-1",
        stage="embed",
        group_key="embed:doc-1:index-1",
        page_size=25,
    )
    cutoff = datetime(2026, 8, 31, tzinfo=UTC)
    request.created_after.FromDatetime(cutoff)

    response = await servicer.Query(request, MagicMock())

    assert list(response.jobs) == []
    query.jobs.assert_awaited_once_with(
        tenant="default",
        state=None,
        task_name=None,
        ctx_id="doc-1",
        parent_gate_id=None,
        pipeline="iknowledge:index:index-1",
        stage="embed",
        group_key="embed:doc-1:index-1",
        created_after=cutoff,
        limit=25,
        offset=0,
    )
