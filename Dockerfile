# syntax=docker/dockerfile:1.4@sha256:9ba7531bd80fb0a858632727cf7a112fbfd19b17e94c4e84ced81e24ef1a0dbc
# Multi-stage build: python deps -> lean non-root runtime.
# The engine is API-only; the operator SPA is a SEPARATE image built from
# frontend/ (see docker-compose.yml `frontend` service). No UI is embedded here.

# Stage 1: Python deps (uv binary copied in, cache mount)
FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6 AS build
COPY --from=ghcr.io/astral-sh/uv:0.11@sha256:77280f2f771df71f90786c314fe1bbc1e023feac652969bbf139c280babf2eb7 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Wire contract is proto/symba/v1/*.proto; Python stubs are gitignored and generated
# at build time so `docker compose up --build` works on a fresh clone with no host
# `make proto`. Same command as the Makefile `proto` target.
COPY proto/ proto/
COPY src/ src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --group codegen --no-install-project && \
    .venv/bin/python -m grpc_tools.protoc -Iproto \
        --python_out=src --grpc_python_out=src --pyi_out=src \
        proto/symba/v1/*.proto && \
    uv sync --frozen --no-dev --no-install-project

# Migrations are applied by a separately built immutable Flyway image. The
# engine image never contains migration sources and never self-migrates.

# Stage 2: runtime (no uv, no compilers, non-root uid 10001)
FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6
RUN useradd -r -u 10001 symba
WORKDIR /app
COPY --from=build --chown=symba:symba /app /app

# This engine is released as part of the same coordinated bundle as its KNOR
# callers. Defaults keep local builds simple; the production overlay requires
# both values explicitly.
ARG KNOR_RELEASE_ID=development
ARG KNOR_RELEASE_MANIFEST_SHA256=unreleased
COPY --chmod=0555 scripts/release/verify_provenance.sh /usr/local/bin/symba-verify-release
COPY --chmod=0555 scripts/engine/entrypoint.sh /usr/local/bin/symba-engine-entrypoint
RUN mkdir -p /usr/local/share/symba && \
    printf '%s\n' "$KNOR_RELEASE_ID" > /usr/local/share/symba/release-id && \
    printf '%s\n' "$KNOR_RELEASE_MANIFEST_SHA256" > /usr/local/share/symba/release-manifest-sha256 && \
    chmod 0444 /usr/local/share/symba/release-id /usr/local/share/symba/release-manifest-sha256
LABEL org.opencontainers.image.version="${KNOR_RELEASE_ID}" \
    org.opencontainers.image.revision="${KNOR_RELEASE_MANIFEST_SHA256}" \
    com.amplior.release.id="${KNOR_RELEASE_ID}" \
    com.amplior.release.manifest-sha256="${KNOR_RELEASE_MANIFEST_SHA256}"
ENV KNOR_IMAGE_RELEASE_ID="${KNOR_RELEASE_ID}" \
    KNOR_IMAGE_RELEASE_MANIFEST_SHA256="${KNOR_RELEASE_MANIFEST_SHA256}" \
    PATH=/app/.venv/bin:$PATH \
    PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1
USER symba
EXPOSE 7233 7300
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD ["python", "-m", "symba.healthcheck"]
ENTRYPOINT ["/usr/local/bin/symba-engine-entrypoint"]
