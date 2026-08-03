# 06 — Signals / human-in-the-loop (WAITING)

A handler can park a job in the `WAITING` state until an external actor signals it — an
approval gate, a webhook callback, a manual review. The rendezvous is race-free and
**single-delivery**, and works in both orders (wait-then-signal or signal-then-wait).

```bash
./run.sh
```

**What it shows**

- `POST /v1/signals {wait_key, payload, signaled_by}` delivers `payload` to the job
  waiting on `wait_key`.
- `delivered: true` — a job was `WAITING` and resumed inline with the payload.
- `delivered: false` — no waiter yet; the signal is stored and consumed by the next
  `Wait(wait_key)` (so a fast signal can't be lost).

The **wait** side is a worker concern: the handler calls the `Wait` gRPC RPC, which
releases the slot and re-queues on resume (the handler re-runs from the top — checkpoint
expensive pre-wait work first; see `../raw_grpc_worker` for the RPC shape).
