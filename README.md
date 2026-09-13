<p align="center">
  <img src="docs/assets/symba_readme_banner.svg" alt="Symba — a durable job execution engine" width="680">
</p>

<p align="center"><strong>A durable job execution engine for workloads that don't fit a static workflow graph.</strong></p>

<p align="center">
  <a href="https://github.com/syntel-technologies/symba/actions/workflows/ci.yml"><img src="https://github.com/syntel-technologies/symba/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/syntel-technologies/symba/actions/workflows/nightly.yml"><img src="https://github.com/syntel-technologies/symba/actions/workflows/nightly.yml/badge.svg" alt="Nightly chaos + load"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>
  <a href="pyproject.toml"><img src="https://img.shields.io/badge/python-3.13%2B-blue.svg" alt="Python 3.13+"></a>
  <a href="docker-compose.yml"><img src="https://img.shields.io/badge/postgres-18-336791.svg" alt="Postgres 18"></a>
</p>

Submit jobs over HTTP, claim and run them from workers over gRPC, and get durable,
at-least-once execution with leases, retries, chains, fan-out/gates, cron, human-in-the-loop
signals, checkpoints, and a live operator console — with **Postgres as the only required
dependency**.

> **Status:** pre-1.0 (`0.1.0`). The engine is feature-complete: auth/tenancy, full
> metrics/alerts/tracing, load + latency gates all shipped. The **worker SDK ships in a
> separate repository** — this repo is the engine server + operator UI.

<p align="center">
  <a href="#-quickstart">Quickstart</a> •
  <a href="#-why-symba">Why Symba</a> •
  <a href="#-features">Features</a> •
  <a href="#-architecture">Architecture</a> •
  <a href="#-stack">Stack</a> •
  <a href="#-configuring-for-production">Production config</a> •
  <a href="#-documentation">Docs</a>
</p>

---

## 🤔 Why Symba

Every existing job/workflow system was built for **predictable, mostly-static pipelines** —
Airflow/Conductor want a DAG defined ahead of time, Temporal wants deterministic, replay-safe
workflow code inside its own multi-service cluster (frontend/history/matching + Cassandra or
Postgres + Elastic), and plain task-queue libraries have no fan-out/fan-in, capability
routing, or durable per-item history. All of them are now bolting "AI agent" support onto
architectures that were never designed for it.

**Our workloads don't look like that anymore.** An LLM call decides at runtime how many
follow-up calls it needs and whether to fan out into 200 parallel sub-tasks or none — the
shape of the graph isn't known until the model is already running. That **non-deterministic
execution graph** forces static workflow DSLs to bend into pretzels or get replaced. Symba
never assumes a graph: `submit`, `chain`, `fan_out`/`gate`, and `depends_on` are plain
function calls your code makes at runtime, not a build-time definition.

The other half is raw speed. Point a job engine at tens of thousands of per-chunk LLM calls,
GPU parses, and embedding jobs and most systems fall over — they were built for *slow, coarse,
occasional* steps, not for dispatching a job every few milliseconds, millions of times a day.
Symba's dispatcher is a single adaptive-tick claim loop over `FOR UPDATE SKIP LOCKED` — no
broker, no `LISTEN/NOTIFY` (a documented outage mode at scale), one clock, one hot table kept
tiny by archiving terminal rows: **job-ready to worker-claimed in under 50ms, sustained at
millions of jobs/day.**

Nothing else is both **fast enough** for high-frequency, fine-grained work and **flexible
enough** for graphs decided at runtime instead of authored ahead of time. That gap is what
Symba fills.

| | Airflow / Conductor | Temporal | Plain task-queue libraries | **Symba** |
|---|---|---|---|---|
| Workflow shape | Static DAG, defined ahead of time | Deterministic, replay-safe code | None (plain task queue) | Runtime-decided: chain / fan-out / gate / depends_on, called as code |
| Per-element durability & visibility | Coarse, per-workflow | Per-activity | Mostly opaque | Every job is a queryable, retryable row with a full audit ledger |
| Operational footprint | JVM server + its own DB/UI | Multi-service cluster (frontend/history/matching + Cassandra/Postgres + Elastic) | Broker + workers | One engine container + Postgres (Redis optional) |
| Claim latency at scale | Seconds (poll-tier stacking) | Depends on task queue backend | Varies, broker-bound | < 50ms p99, `docker compose up` is the whole test env |
| Capability-aware routing (GPU vs. API workers) | Bolted on via deployment tricks | Task queues, but heavyweight to adopt for this alone | None | First-class `runs_on` tags matched against worker tags |

## ✨ Features

- 🧱 **Durable by construction** — every state transition is one Postgres transaction; no
  job state lives only in Redis. Crash the engine or a worker at any point and the sweeper
  reclaims the lease and re-runs (crash-only, effect-once via idempotency keys).
- ⚡ **One latency tier** — dispatch is adaptive polling over a `FOR UPDATE SKIP LOCKED` claim
  on a hot table kept tiny by archive-on-terminal. No `LISTEN/NOTIFY`, no broker. Target: p95
  queue→claim < 150ms idle, ≥500 claims/s per engine.
- 🔀 **Runtime-decided graphs** — `chain`, `fan_out` + `gate` (quorum/all-success/all-terminal),
  and `depends_on` are function calls your handler makes, not a DAG you author up front.
- 🎯 **Capability routing** — jobs declare `runs_on=["gpu"]`; workers declare `tags=["gpu"]`;
  the engine only hands a job to a worker that can actually run it.
- 🚦 **Rate-limit classes** — named token buckets (e.g. `azure-gpt5: 500/min`) enforced at claim
  time, shared fairly across every worker pulling from that class.
- 🔁 **Retries, backoff, and a real DLQ** — per-job-type policy, full error history, dead jobs
  are never silently pruned.
- ✅ **Idempotent by design** — dedup keys collapse duplicate submits (webhook storms, re-sync
  scans) into one execution.
- 👤 **Human-in-the-loop** — jobs can suspend into `WAITING` and resume on an external signal or
  timeout, durable and audited like everything else.
- 💾 **Checkpoints** — persist expensive intermediate output (an LLM response) mid-job so a
  retry resumes instead of re-paying for it.
- 🔬 **Small footprint** — one engine container + Postgres 18. `docker compose up` is the whole
  system (and the full test environment, chaos suite included).
- 📊 **Operable** — immutable `job_events` ledger, `ctx_id` on everything, a Prometheus
  `/metrics` surface with shipped alert rules, OTLP tracing, and an eight-view operator SPA.

## 🏗️ Architecture

```
   HTTP/JSON control plane  :7300         gRPC data plane  :7233
   POST /v1/jobs (submit)                 Claim (bidi stream) ── workers
   GET  /v1/jobs/{id}                     Heartbeat / Complete / Fail
   GET  /v1/stats,/workers,/cron ...      Wait / Put|GetCheckpoint / GetResult
        │                                        ▲
        ▼                                        │ assign (adaptive-tick matcher)
   ┌─────────────────────────── engine ─────────────────────────┐
   │  submit → SubmitService        dispatcher → matcher         │
   │  sweeper (lease reclaim, gauges)   cron    rate-limiter      │
   └───────────────────────────────┬────────────────────────────┘
                                    ▼
                        Postgres 18  (source of truth)
                        jobs (hot) · jobs_archive · job_events
                        job_dependencies · gates · checkpoints · signals
                                    ▲
                        Redis 8 / Valkey 9  (optional: rate-limit tokens)
```

The operator SPA (`frontend/`) is a **separate container** (nginx serving a Vite build,
proxying `/v1` + `/metrics` to the engine). Nothing UI is embedded in the engine image —
workers connect over gRPC using the Symba SDK (separate repo), and everything an operator can
click in the console is also a plain REST call (`docs/api.md`).

## 🛠️ Stack

| Layer | Technology |
|---|---|
| Language / runtime | Python 3.13+ (3.14 target), fully asyncio |
| Control plane | FastAPI + Uvicorn (HTTP/JSON, SSE event stream) |
| Data plane | gRPC (`grpc.aio`), protobuf wire contract |
| Database | PostgreSQL 18 — the only required dependency (`uuidv7()` PKs, `FOR UPDATE SKIP LOCKED` claim, hand-written SQL, no ORM) |
| Cache (optional) | Redis 8 / Valkey 9 — rate-limit token buckets + checkpoint fast path only, never state |
| Migrations | Flyway (plain ordered SQL, append-only) |
| Config | pydantic-settings — `symba.toml` + `.env` + `SYMBA_*` env, fail-fast on a bad key |
| Logging | structlog, JSON in prod |
| Metrics / tracing | prometheus-client (`/metrics`) + OpenTelemetry OTLP (off by default) |
| Auth | none (loopback/dev) · shared-token · mTLS |
| Operator UI | React 19 + TanStack Query/Router + React Flow + Tailwind, served by nginx |
| Containers | Multi-stage Docker (engine + frontend), multi-arch (amd64/arm64) images on GHCR |
| CI | GitHub Actions — lint/typecheck, proto lint+breaking, unit + integration tests, migration verification, nightly chaos + load regression gate |

Deliberately **not** used: SQLAlchemy/alembic (the engine has ~20 hand-written, `EXPLAIN`-able
queries — an ORM and a JVM migration tool buy nothing here), generic task-queue libraries (the
engine *is* the queue), and no LLM-specific code anywhere in the engine — it routes jobs, it
doesn't know what they do.

## 🚀 Quickstart

For prebuilt images, see the [Docker quickstart](docs/docker.md). Once a release
ships `compose.quickstart.yml`, starting the complete stack requires only:

```bash
curl -fL https://github.com/syntel-technologies/symba/releases/latest/download/compose.quickstart.yml -o compose.quickstart.yml
docker compose -f compose.quickstart.yml up -d --wait
```

Open http://localhost:8080 and sign in with `symba-local-dev-token`. This local
evaluation stack binds ports to localhost and persists Postgres data in a Docker
volume. It uses prebuilt engine, migration, and console images. For an existing
Postgres database, the Docker guide also shows the direct `docker run` commands.

To build from a source checkout instead:

```bash
# 1. Bring up the whole stack: engine + PG18 + Flyway (migrations) + optional Redis + UI.
#    gRPC stubs are generated inside the engine image at build time — no host `make proto`.
docker compose up -d --wait

# 2. Submit a job (control plane on :7300; or :8080 via the UI's proxy).
curl -s http://localhost:7300/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"default","specs":[{"task_name":"demo.echo","payload":{"hello":"world"}}]}'

# 3. Watch it in the operator console.
open http://localhost:8080
```

Workers connect over gRPC on `:7233` using the Symba SDK (separate repo). See
[`examples/`](examples/) for ten runnable REST + raw-gRPC samples — submit & poll, dedup,
chains, fan-out/gate, signals, cancel cascades, DLQ resubmit, cron, and observability.

### Local development

```bash
uv sync --extra dev              # dev extra includes grpcio-tools for codegen
make proto                       # once per clone if you run outside Docker (auto via `make run`)
uv run pytest -m l1            # pure-logic unit tests (fast, no infra)
uv run pytest -m l2            # integration tests (spins up PG18 via testcontainers)
uv run ruff check src tests    # lint;  uv run pyright  for types
```

Requires Python 3.13+ (3.14 is the target runtime) and Postgres 18. See the
[`Makefile`](Makefile) for the full set of local targets (proto codegen, migrations,
OpenAPI drift check, frontend build).

## 🔧 Configuring for production

The dev defaults (`SYMBA_AUTH__MODE=none`, no TLS, permissive CORS-free same-origin proxy) are
intentionally wide open for a zero-config `docker compose up`. Before exposing Symba to
anything but `localhost`, change these:

| Setting | Dev default | Production |
|---|---|---|
| `SYMBA_AUTH__MODE` | `none` (loopback-only) | `token` (shared-secret/JWKS) or `mtls` — **required** for any routable deployment |
| `SYMBA_AUTH__TOKEN_JWKS_URL` / `SYMBA_AUTH__TOKENS` | unset | point at your IdP's JWKS, or a rotated shared-secret → tenant map |
| `SYMBA_POSTGRES__DSN` | local compose Postgres | your managed/HA Postgres 18, real credentials |
| `SYMBA_REDIS__URL` | unset (degraded mode) | a real Redis/Valkey endpoint for lower-latency rate limiting (optional — engine falls back to Postgres counters without it) |
| `SYMBA_LOG__FORMAT` | `console` | `json` (structured logs for your aggregator) |
| `SYMBA_OBSERVABILITY__OTLP_ENDPOINT` | unset (tracing off) | your OTel collector, to get submit→claim→complete traces |
| `SYMBA_LIMITS__TENANT_QUEUED_CAP` | `0` (unlimited) | a real per-tenant cap so one noisy tenant can't starve others |

Minimal production `.env`:

```bash
KNOR_RELEASE_ID=2026-09-02-release-r2
KNOR_RELEASE_MANIFEST_SHA256=<lowercase-sha256-of-coordinated-release_manifest.json>
SYMBA_APP__ENVIRONMENT=production
SYMBA_POSTGRES__HOST=db
SYMBA_POSTGRES__PORT=5432
SYMBA_POSTGRES__USER=symba
SYMBA_POSTGRES__PASSWORD=${DB_PASSWORD}
SYMBA_POSTGRES__DATABASE=symba
SYMBA_REDIS__URL=redis://cache:6379/0        # optional; omit for degraded mode
SYMBA_AUTH__MODE=token                        # REQUIRED for any routable deploy
SYMBA_AUTH__TOKEN_JWKS_URL=https://idp/.well-known/jwks.json
SYMBA_LOG__FORMAT=json
SYMBA_OBSERVABILITY__OTLP_ENDPOINT=http://otel-collector:4317
```

Generate and hash the coordinated top-level source manifest before setting
these two values. The production Compose overlay requires them for both the
engine and operator frontend images; their OCI version/revision, custom
release labels, and baked `KNOR_IMAGE_RELEASE_*` values must match the other
images in that release. Do not set the baked names at runtime.

The bundled `docker-compose.production.yml` also creates three fixed database
identities instead of running the engine as the PostgreSQL bootstrap
superuser. Generate independent 32-character-or-longer values for
`SYMBA_POSTGRES_ADMIN_PASSWORD`, `SYMBA_POSTGRES_MIGRATION_PASSWORD`, and
`SYMBA_POSTGRES_RUNTIME_PASSWORD`; only the last reaches the engine. Generate
an independent URL-safe `SYMBA_REDIS_PASSWORD` of at least 32 characters. The
bootstrap `postgres` role is local-peer-only after initialization; TCP
authentication is rejected even with its correct password. The overlay pins
SCRAM-SHA-256 for host database connections, authenticates Redis,
and rejects reused database, Redis, or engine-token credentials before the
long-lived engine starts.

Ship the multi-arch engine image straight from GHCR (`ghcr.io/syntel-technologies/symba:vX.Y.Z`,
built for `linux/amd64` + `linux/arm64` on every release tag) and the `frontend/` image
alongside it, or build both locally with `docker compose build`. Every setting — with its
default and operator-facing notes — is documented in full in
[`docs/configuration.md`](docs/configuration.md); the alerting rules to wire into your
Prometheus in [`deploy/prometheus/symba_alerts.yml`](deploy/prometheus/symba_alerts.yml); and
the on-call playbook in [`docs/operations_runbook.md`](docs/operations_runbook.md).

## 📚 Documentation

| Doc | What |
|---|---|
| [`docs/api.md`](docs/api.md) | HTTP control-plane reference (the live OpenAPI at `/docs` is canonical). |
| [`docs/configuration.md`](docs/configuration.md) | Every `SYMBA_*` / `symba.toml` setting, with defaults. |
| [`docs/operations_runbook.md`](docs/operations_runbook.md) | Triage drills, alert responses, degraded-mode playbook. |
| [`examples/README.md`](examples/README.md) | Ten runnable samples against the real API. |
| [`tests/load/README.md`](tests/load/README.md) | The load/latency suite and its CI gates. |

## License

Apache-2.0. See [`LICENSE`](LICENSE).

## Development and releases

See [Contributing](CONTRIBUTING.md), [the release workflow](docs/releasing.md), and [Security](SECURITY.md). Development targets `dev`; `main` requires a reviewed PR.
