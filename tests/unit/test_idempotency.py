"""L1: idempotency key derivation."""

from __future__ import annotations

import pytest

from symba.core.idempotency import idempotency_key, idempotency_key_attempt

pytestmark = pytest.mark.l1


def test_key_is_stable_and_32_hex_chars():
    k = idempotency_key("t1", "dedup-a", "job-1")
    assert len(k) == 32
    assert all(c in "0123456789abcdef" for c in k)
    assert k == idempotency_key("t1", "dedup-a", "job-1")


def test_dedup_key_dominates_job_id():
    # Same dedup_key -> same key regardless of job_id (dedup semantics).
    assert idempotency_key("t1", "d", "job-1") == idempotency_key("t1", "d", "job-2")


def test_falls_back_to_job_id_without_dedup():
    assert idempotency_key("t1", None, "job-1") != idempotency_key("t1", None, "job-2")


def test_tenant_isolation():
    assert idempotency_key("t1", "d", "j") != idempotency_key("t2", "d", "j")


def test_attempt_variant_is_distinct_and_derived():
    base = idempotency_key("t1", "d", "j")
    a0 = idempotency_key_attempt(base, 0)
    a1 = idempotency_key_attempt(base, 1)
    assert a0.startswith(base) and a0.endswith("-a0")
    assert a1 != a0


def test_negative_attempt_rejected():
    with pytest.raises(ValueError):
        idempotency_key_attempt("base", -1)
