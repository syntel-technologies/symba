# TODOS — deferred work

Items considered during design/stress-test reviews and deliberately deferred.
Each entry must carry What / Why / Context / Depends-on. A TODO without context
is worse than no TODO. Prune entries when they land or are rejected.

---

## 1. Write buffering (10ms flush) for the event ledger and heartbeats

**What:** Batch `job_events` inserts and heartbeat lease-extensions in an in-memory
buffer flushed every ~10ms (or N rows, whichever first), instead of one statement
per event/heartbeat.

**Why:** At target volume (millions of jobs/day, ~10 ledger events per job, worker
fleets heartbeating every 15s), per-event statements make `job_events` and
`Heartbeat` the highest-frequency writers in the system — more write amplification
than the claim path itself. Hatchet's v1 rewrite found buffered flushing to be one
of its biggest single throughput wins. A 10ms flush window adds no perceptible
latency to anything (heartbeats have seconds of slack against `lease_ttl_s`;
the ledger is read forensically, not transactionally).

**Context (state as of the 2026-07-13 stress-test, docs rev 7/v3):**
- `job_events` inserts currently ride inside the same transaction as their state
  change — this is a *correctness* property (ledger can
  never disagree with the job row) that plain buffering would forfeit. The design
  needs to split events into two classes: transition events (stay transactional)
  vs. telemetry-ish events (`heartbeat_missed`, `checkpointed` — buffer-safe).
- To keep this future open, the ledger writer is already isolated behind one
  code path (`observability/` ledger writer, per perf-review decision 12A) —
  do NOT scatter `INSERT INTO job_events` around services.
- Heartbeats are unary RPCs updating `lease_expires_at` on the hot pool; batching
  them (single `UPDATE ... WHERE id = ANY(...)`) is semantics-free as long as the
  flush interval stays « `lease_ttl_s / 3`.
- Crash window: a 10ms buffer can lose telemetry events on kill -9. Acceptable for
  telemetry class, never for transition class — state that explicitly in the impl.

**Depends on / blocked by:** M1 (single-job lifecycle) landed and the L6 load
suite producing a stored baseline — this is an optimization; it needs a benchmark
to prove itself against and a chaos (kill -9) test to prove it loses nothing
that matters.

---

## 2. Redis licensing check / Valkey option

**What:** Confirm the tri-license (RSALv2/SSPLv1/AGPLv3) of Redis 8.x is acceptable
for an open-sourced Symba; otherwise document Valkey (BSD-3, wire-compatible) as
the supported alternative and add it to the CI matrix.

**Why:** Symba is intended to be open-sourceable; shipping docs
that hard-require a source-available-licensed component may matter to adopters.
Functionally zero risk — `redis-py` works against Valkey.

**Context:** Redis is optional in Symba (buckets + checkpoint cache only).
The compose file pins `redis:8.4`. A one-line compose swap + one CI matrix leg
covers it.

**Depends on / blocked by:** Nothing. Decide before the first public release (M7).

---

## 3. Move the SPA to Vite 8 (Rolldown) + TypeScript 7

**What:** Upgrade `frontend/` from the current stable pins (Vite 6, TS 5.7) to the
newer targets (Vite 8 / Rolldown, TS 7 native compiler) once both are released
stable and the OpenAPI→TS codegen path is verified against them.

**Why:** The newer lines are targeted for build speed (Rolldown Rust bundler, Go
compiler ~10x faster). We deliberately shipped M5 on the stable lines to keep the
build reproducible and to sidestep the TS 7.0 codegen risk those newer lines
themselves carry. This is a version bump, not a rewrite — the code uses no
Rolldown-specific or TS7-specific features.

**Context (state as of M5 UI landing):**
- SPA is built and passing: `tsc -b` strict + `vite build` green, `eslint` clean.
- `openapi-typescript` 7.13 verified working under TS 5.7 (`npm run gen:api`), so the
  original "TS 7.0 has no Compiler API" break does NOT affect us today.
- The drift gate (`tools/check_openapi.py`) + committed `frontend/openapi.json` are the
  contract; the codegen output `src/api/schema.ts` is git-ignored and regenerated in
  the Docker build. When bumping, re-run `npm run gen:api` under TS 7 and confirm no
  `schema.ts` diff and a clean `tsc -b`.
- Watch: `@tailwindcss/vite`, `@vitejs/plugin-react` must have Vite 8-compatible
  releases before the bump.

**Depends on / blocked by:** Stable Vite 8 + TS 7 releases with plugin ecosystem
support. Non-urgent (build works today).

---

## 4. Cross-process trace linkage: `traceparent` in job metadata → worker `execute` span

**What:** Propagate a W3C `traceparent` through job metadata at submit so the
worker's per-attempt `execute` span links back to the engine's `submit` span, and
the engine's `claim`/`complete` spans join the same distributed trace — a single
end-to-end trace spanning app → engine → worker → engine.

**Why:** The intended span model is `submit (client) → link → claim/execute (worker) →
complete (engine)`. We shipped the three ENGINE spans (`symba.submit`,
`symba.claim`, `symba.complete`) all carrying `symba.ctx_id`, so app-side traces
already join on `ctx_id`. What's missing is the OTel *parent-child* link across the
process boundary, which gives a single collapsed waterfall in Jaeger/Tempo instead
of three separate spans correlated only by attribute. That is strictly nicer UX for
debugging latency, not a correctness gap.

**Context (state as of M6 observability landing):**
- Engine spans live in `observability/tracing.py::engine_span()` and are opened in
  `SubmitService.submit`, `matcher._observe_ready_to_claim` (claim), and
  `JobService.complete`. All no-op unless `otlp_endpoint` is set.
- The blocker is storage + the worker: there is no `traceparent`/metadata column on
  `jobs` today, and the worker that would open the `execute` span lives in the
  **separate `symba-sdk-python` repo** (out of scope here). Adding a nullable
  `trace_metadata jsonb` column to `jobs`/`jobs_archive` (next free Flyway version —
  V008 is taken by `gate_failed_children`, so this is V009+) is the engine half;
  injecting/extracting the context is the SDK half.
- Keep `ctx_id` as the attribute on every span regardless — it is the documented
  app-side join key (incl. the app's own LLM traces) and does not depend on this.

**Depends on / blocked by:** The worker SDK repo existing (it owns the `execute`
span and the context extract). Engine-side prerequisite: one migration for a
`trace_metadata` column. Non-urgent — attribute-based join on `ctx_id` works today.

---

## 5. Wire the conformance `engine` backend into pytest (live E2/E3 gate contract)

**What:** Replace the unconditional skip in
`symba-sdk-python/tests/conformance/conftest.py` (the `engine` fixture param does
`pytest.skip("dockerized-engine conformance backend not wired in this environment")`)
with a real `Backend` that drives the SDK `Engine` + a running `Worker` against a
live engine at `SYMBA_E2E_TARGET`, so `test_gate_math_counts_success_and_skips` and
`test_gate_all_skipped_still_fires` (and the rest of the corpus) actually execute
against the engine — not only SymbaTest.

**Why:** The E2 (`__gate__` manifest) and E3 (skip counting) contracts are cross-repo:
a green SymbaTest run does NOT by itself prove the real engine matches. Today the
only live proof is the standalone script `symba-sdk-python/tools/e2e_gate_manifest.py`,
which is not part of the test suite and won't run in CI. Wiring the fixture closes
the gap so a contract regression on either side fails a test instead of shipping.

**Context (state as of 2026-07-15, after E1–E4 landed):**
- The corpus is already written once against a `Backend` protocol
  (`client.submit/fan_out/signal/jobs`, `run_until_idle`) and parameterized over
  `["symbatest", "engine"]`; only the `engine` construction is missing.
- `tools/e2e_gate_manifest.py` is the working reference: it boots `Worker.arun()` as
  a background task, submits via `Engine.fan_out(..., ctx_id=...)`, awaits
  `gate.result()`, and tears the worker down. Lift that into a fixture with proper
  setup/teardown and a per-test `ctx_id`.
- Needs the engine reachable (`docker compose up -d` in the engine repo) and the
  version handshake / proto package fixes already in place (X1, X2 — done).
- `run_until_idle()` has no engine equivalent; for the engine backend it should be a
  bounded poll on job states (or a no-op, since the worker drains autonomously).

**Depends on / blocked by:** Nothing further — E1–E4 + X1–X3 are landed and the
contract is proven manually. This is test-infra hardening to keep it proven.
