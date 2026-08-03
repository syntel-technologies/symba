# Multi-stage build: python deps -> lean non-root runtime.
# The engine is API-only; the operator SPA is a SEPARATE image built from
# frontend/ (see docker-compose.yml `frontend` service). No UI is embedded here.

# Stage 1: Python deps (uv binary copied in, cache mount)
FROM python:3.14-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src/ src/
# Migrations are applied by the Flyway sidecar (see docker-compose.yml + flyway.toml),
# not baked into the engine image. database/symba/ is mounted into the flyway container.

# Stage 2: runtime (no uv, no compilers, non-root uid 10001)
FROM python:3.14-slim
RUN useradd -r -u 10001 symba
WORKDIR /app
COPY --from=build --chown=symba:symba /app /app
ENV PATH=/app/.venv/bin:$PATH PYTHONPATH=/app/src PYTHONUNBUFFERED=1
USER symba
EXPOSE 7233 7300
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD ["python", "-m", "symba.healthcheck"]
ENTRYPOINT ["python", "-m", "symba.main"]
