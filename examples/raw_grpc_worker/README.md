# raw_grpc_worker — a worker without the SDK

The smallest thing that claims and completes jobs over the gRPC data plane, using the
committed protobuf stubs directly. It documents the wire contract; the real
ergonomic worker lives in the SDK repo (supervision, retry classification, checkpoints,
schema validation, graceful drain).

## Run

```bash
# from the repo root, with the stack up (docker compose up -d --wait)
uv run python examples/raw_grpc_worker/worker.py
```

Then submit work from another shell and watch it get claimed:

```bash
cd examples/01_submit_and_poll && ./run.sh
```

Configure via env: `SYMBA_GRPC` (default `localhost:7233`), `SYMBA_WORKER_ID`.

## What it shows

- **The Claim handshake** — `Claim` is one bidirectional stream. The first
  `ClaimRequest` registers the worker (`worker_id`, `tags`, `free_slots`) **and** carries
  `sdk_version` for the protocol version check. An out-of-window version is rejected
  with `FAILED_PRECONDITION` before any assignment (`core/versioning.py`).
- **Worker-driven flow control** — the engine never assigns beyond your last announced
  `free_slots`; the worker sends a fresh frame to top slots back up as jobs finish.
- **The mutation RPCs** — `Complete(result_json)` on success, `Fail(error_type,
  retryable)` on failure (the engine decides retry-vs-die). `lease_token` from the
  assignment must accompany every mutation.

## What it deliberately omits (the SDK's job)

- Heartbeats (`Heartbeat`) for long jobs and cooperative-cancel handling.
- `Wait` / signals, `PutCheckpoint`/`GetCheckpoint`, `GetResult`.
- Concurrency > 1, task registry + schemas, retry classification, graceful drain.
