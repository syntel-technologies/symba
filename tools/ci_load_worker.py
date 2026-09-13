"""Run an echo worker against the isolated nightly engine using its own generated protocol."""

from __future__ import annotations

import asyncio
import contextlib
import sys
from collections.abc import AsyncIterator

import grpc

from symba.v1 import data_plane_pb2 as dp
from symba.v1 import data_plane_pb2_grpc as rpc


async def serve(target: str) -> None:
    capacity = 256
    active = 0
    updates: asyncio.Queue[int] = asyncio.Queue()
    await updates.put(capacity)

    async def frames() -> AsyncIterator[dp.ClaimRequest]:
        while True:
            try:
                free = await asyncio.wait_for(updates.get(), timeout=5)
            except TimeoutError:
                free = capacity - active
            yield dp.ClaimRequest(
                worker_id="ci-load-echo",
                free_slots=free,
                sdk_version="0.1.0",
                tags=["general"],
                registered_tasks=["loadtest.echo"],
            )

    async with grpc.aio.insecure_channel(target) as channel:
        await asyncio.wait_for(channel.channel_ready(), timeout=30)
        stub = rpc.WorkerServiceStub(channel)

        async def complete(assignment: dp.JobAssignment) -> None:
            nonlocal active
            if assignment.job.task_name != "loadtest.echo":
                raise RuntimeError("Unexpected task in isolated CI engine")
            reply = await stub.Complete(
                dp.CompleteRequest(job_id=assignment.job.id, lease_token=assignment.lease_token, result_json=b"{}"),
                timeout=20,
            )
            if not reply.accepted:
                raise RuntimeError("Engine rejected CI worker completion")
            active -= 1
            await updates.put(capacity - active)

        async with asyncio.TaskGroup() as group:
            async for assignment in stub.Claim(frames()):
                active += 1
                group.create_task(complete(assignment))


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:7233"))
