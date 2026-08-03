# Symba frontend (SPA)

Standalone single-page app for the Symba operator console. Built and served as a
separate container (see `docker-compose.yml` `frontend` service); the engine is
API-only and does NOT embed the UI. Implemented in milestone M5.

## Views

| Route | View | Backing endpoint(s) |
|---|---|---|
| `/` | Live board | `GET /v1/stats/board` |
| `/pipeline` | Pipeline per ctx_id (React Flow DAG) | `GET /v1/jobs?ctx_id=` + `GET /v1/jobs/{id}/tree` |
| `/dlq` | Failed / DLQ (grouped by stack_hash, bulk resubmit) | `GET /v1/jobs?state=dead` + `POST /v1/jobs/{id}/resubmit` |
| `/waiting` | Waiting (signal form) | `GET /v1/jobs?state=waiting` + `POST /v1/signals` |
| `/jobs/:id` | Job detail (timeline, JSON, tree, checkpoints) | `GET /v1/jobs/{id}` + `/events` + `/tree` + `/checkpoints` |
| `/fleet` | Fleet | `GET /v1/workers` |
| `/queues` | Queues (depth + oldest-age gauges) | `GET /v1/stats/queues` |
| `/cron` | Cron (enable/disable) | `GET/PUT /v1/cron` |

Live updates come from one SSE channel (`GET /v1/events/stream`), which invalidates
TanStack Query caches on each `job_event` (`src/api/useEventStream.ts`).

Rule: the UI has **zero private endpoints** — every call is the public
REST API. Anything an operator can click, they can script.

## Typed API client

`frontend/openapi.json` is generated from the engine and committed. `npm run gen:api`
runs `openapi-typescript` to produce `src/api/schema.ts` (git-ignored). A CI drift
gate (`tools/check_openapi.py`) fails if the committed schema is stale, so the UI can
never silently diverge from the API. To refresh after an API change:

```bash
make openapi   # regenerate frontend/openapi.json from the engine, then commit
```

## Stack versions — deliberate deviation from target versions

The long-term target is Vite 8 (Rolldown) and TypeScript 7 (native Go compiler), but both
are pre-release lines and carry codegen risk ("the Compiler API is
absent in TS 7.0 … `openapi-typescript` codegen must be verified against TS 7, else
pin TS 6.x for the codegen step only"). To keep the build reproducible today and avoid
exactly that codegen breakage, this SPA pins the current stable lines:

- **Vite 6** (stable) instead of 8 — no Rolldown-specific config is used, so the bump
  to 8 later is a version change, not a rewrite.
- **TypeScript 5.7** (stable) instead of 7 — verified compatible with
  `openapi-typescript` 7.x.

See `../TODOS.md` for the follow-up to move to Vite 8 / TS 7 once released and the
codegen path is verified.

## Develop

```bash
npm install
npm run gen:api          # produces src/api/schema.ts from openapi.json
npm run dev              # Vite dev server on :5173, proxies /v1 -> :7300
```

Point the dev proxy at a non-local engine with `VITE_API_TARGET=http://host:7300`.

## Build & serve (container)

`Dockerfile` is a two-stage build: Vite bundle -> nginx. nginx proxies `/v1` and
`/metrics` to the engine (`ENGINE_HOST`/`ENGINE_PORT`, defaults `engine:7300`) so the
browser is same-origin and SSE works without CORS.

```bash
docker compose up --build frontend    # serves on http://localhost:8080
```
