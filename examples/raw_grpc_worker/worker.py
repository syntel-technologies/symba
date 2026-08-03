"""A minimal Symba worker using the RAW gRPC stubs — no SDK.

This is the smallest thing that can claim and complete a job over the data plane.
It exists to document the wire contract; the real SDK adds supervision, retry
classification, checkpoints, schemas, graceful drain, etc. (all in the separate SDK
package).

    the Claim handshake + loop
    --------------------------
    Claim is ONE bidirectional stream per worker process:

      worker ── ClaimRequest{worker_id, tags, free_slots, sdk_version} ─► engine
             ◄─ JobAssignment{job, lease_token, lease_expires_at} ──────
      (for each assignment: run the handler, then Complete or Fail)
      worker ── ClaimRequest{free_slots=...} ─► engine   (keep slots current)

    The FIRST frame registers the worker AND carries sdk_version for the protocol
    version handshake — an out-of-window version is rejected with FAILED_PRECONDITION
    before any assignment (see core/versioning.py). Flow control is worker-driven: the
    engine never assigns beyond the last announced free_slots.

RUN (against a compose-up engine on :7233)
    uv run python examples/raw_grpc_worker/worker.py
    # then, in another shell:  cd examples/01_submit_and_poll && ./run.sh
"""

from __future__ import annotations

import asyncio
import json
import os

import grpc

from symba import PROTOCOL_VERSION
from symba.v1 import common_pb2 as c
from symba.v1 import data_plane_pb2 as dp
from symba.v1 import data_plane_pb2_grpc as dp_grpc

ENGINE = os.environ.get("SYMBA_GRPC", "localhost:7233")
WORKER_ID = os.environ.get("SYMBA_WORKER_ID", "raw-example-worker")
# Tags this worker serves. Empty = serve any untagged task (the demo.* / etl.* tasks
# the REST samples submit have no runs_on, so an untagged worker claims them).
TAGS: list[str] = []


async def handle(job: c.Job) -> dict[str, object]:
    """The 'handler': decode the payload, do trivial work, return a JSON-able result."""
    payload = json.loads(job.spec.payload_json.decode() or "{}")
    print(f"  [handle] {job.spec.task_name} id={job.id} attempt={job.attempt} payload={payload}")
    # A real handler runs the task here. We just echo.
    return {"echoed": payload, "by": WORKER_ID}


async def run() -> None:
    async with grpc.aio.insecure_channel(ENGINE) as channel:
        stub = dp_grpc.WorkerServiceStub(channel)

        # A queue of ClaimRequest frames we send TO the engine. The first is the
        # handshake; later frames update free_slots as we finish jobs.
        outbound: asyncio.Queue[dp.ClaimRequest] = asyncio.Queue()
        await outbound.put(
            dp.ClaimRequest(
                worker_id=WORKER_ID,
                tags=TAGS,
                free_slots=1,
                sdk_version=PROTOCOL_VERSION,  # protocol version handshake
                labels={"example": "raw_grpc_worker"},
            )
        )

        async def request_iterator():
            while True:
                yield await outbound.get()

        print(f"[worker] connecting to {ENGINE} as {WORKER_ID} (protocol {PROTOCOL_VERSION})")
        stream = stub.Claim(request_iterator())
        try:
            async for assignment in stream:
                job = assignment.job
                print(f"[worker] claimed {job.id} ({job.spec.task_name})")
                try:
                    result = await handle(job)
                    await stub.Complete(
                        dp.CompleteRequest(
                            job_id=job.id,
                            lease_token=assignment.lease_token,
                            result_json=json.dumps(result).encode(),
                        )
                    )
                    print(f"[worker] completed {job.id}")
                except Exception as exc:  # a handler failure -> Fail (engine decides retry/die)
                    await stub.Fail(
                        dp.FailRequest(
                            job_id=job.id,
                            lease_token=assignment.lease_token,
                            error_type=type(exc).__name__,
                            error_message=str(exc)[:2048],
                            retryable=True,
                        )
                    )
                    print(f"[worker] failed {job.id}: {exc}")
                finally:
                    # Free the slot back up so the engine assigns the next job.
                    await outbound.put(
                        dp.ClaimRequest(worker_id=WORKER_ID, tags=TAGS, free_slots=1, sdk_version=PROTOCOL_VERSION)
                    )
        except grpc.aio.AioRpcError as err:
            # A rejected handshake (bad protocol version) or a lost engine lands here.
            print(f"[worker] stream ended: {err.code().name} — {err.details()}")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[worker] bye")
