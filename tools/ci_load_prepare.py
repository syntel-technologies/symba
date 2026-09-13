"""Warm or drain an isolated benchmark stack; never use against a shared engine."""

from __future__ import annotations

import argparse
import asyncio
import time

import httpx


async def prepare(url: str, *, drain_only: bool) -> None:
    async with httpx.AsyncClient(base_url=url, timeout=30) as client:
        if not drain_only:
            deadline = time.monotonic() + 30
            while True:
                response = await client.get("/v1/workers")
                response.raise_for_status()
                workers = {row["worker_id"] for row in response.json()["workers"] if not row["stale"]}
                if {f"ci-load-echo-{n}" for n in range(4)} <= workers:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("The four benchmark workers did not register")
                await asyncio.sleep(0.2)

            async def submit() -> None:
                response = await client.post(
                    "/v1/jobs", json={"specs": [{"task_name": "loadtest.echo", "payload": {"warmup": True}}]}
                )
                response.raise_for_status()
                if len(response.json()["job_ids"]) != 1:
                    raise RuntimeError("Warm-up submission did not return one job ID")

            for _ in range(10):
                await asyncio.gather(*(submit() for _ in range(50)))
                await asyncio.sleep(0.1)

        deadline = time.monotonic() + 60
        while True:
            response = await client.get("/v1/stats/board")
            response.raise_for_status()
            counts = response.json()["counts"]
            if counts.get("dead", 0) or counts.get("cancelled", 0):
                raise RuntimeError(f"Benchmark work failed: {counts}")
            pending = sum(counts.get(state, 0) for state in ("submitted", "queued", "running", "waiting"))
            if pending == 0 and counts.get("succeeded", 0) >= 500:
                print(f"Benchmark queue drained: {counts['succeeded']} jobs succeeded")
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Benchmark workers did not drain the queue: {counts}")
            await asyncio.sleep(0.2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Explicit URL of the isolated benchmark engine")
    parser.add_argument("--drain-only", action="store_true")
    args = parser.parse_args()
    asyncio.run(prepare(args.url, drain_only=args.drain_only))
