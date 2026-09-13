"""HTTP correlation survives concurrent requests and streaming responses."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.routing import Route

from symba.observability.tracing import TraceIDMiddleware, get_trace_id, trace_id_context


@pytest.mark.asyncio
async def test_concurrent_streams_keep_trace_ids_and_restore_caller_context() -> None:
    async def stream(request: Request) -> StreamingResponse:
        async def chunks() -> AsyncIterator[str]:
            yield f"{request.state.trace_id}:"
            await asyncio.sleep(0)
            yield get_trace_id()

        return StreamingResponse(chunks())

    app = Starlette(routes=[Route("/stream", stream)])
    app.add_middleware(TraceIDMiddleware)
    token = trace_id_context.set("caller-context")
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            responses = await asyncio.gather(
                client.get("/stream", headers={"X-Symba-Request-Id": "request-a"}),
                client.get("/stream", headers={"X-Symba-Request-Id": "request-b"}),
            )
            assert [response.text for response in responses] == ["request-a:request-a", "request-b:request-b"]
            assert get_trace_id() == "caller-context"
            sequential = await client.get("/stream?trace_id=query-id")
            assert sequential.text == "query-id:query-id"
            assert get_trace_id() == "caller-context"
    finally:
        trace_id_context.reset(token)
