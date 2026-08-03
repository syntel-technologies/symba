"""Idempotency key derivation. Pure logic — no I/O.

Shared spec with the (future) SDK: the engine derives the same keys the SDK
exposes on Ctx, so a handler's `ctx.idempotency_key` is stable across retries
and reproducible from the job row alone.

    idempotency_key         = sha256(f"{tenant}:{dedup_key or job_id}")[:32]
    idempotency_key_attempt = f"{idempotency_key}-a{attempt}"

The base key is stable across attempts (safe for exactly-once external effects
keyed on it); the per-attempt variant is available when a caller wants a fresh
token per try.
"""

from __future__ import annotations

import hashlib


def idempotency_key(tenant: str, dedup_key: str | None, job_id: str) -> str:
    seed = f"{tenant}:{dedup_key or job_id}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]


def idempotency_key_attempt(base_key: str, attempt: int) -> str:
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    return f"{base_key}-a{attempt}"
