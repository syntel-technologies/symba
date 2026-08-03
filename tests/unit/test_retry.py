"""L1: backoff math."""

from __future__ import annotations

import random

import pytest

from symba.core.retry import BackoffPolicy, should_retry

pytestmark = pytest.mark.l1


def test_ceiling_grows_exponentially_then_caps():
    p = BackoffPolicy(base_s=1.0, factor=2.0, cap_s=10.0, jitter=False)
    assert p.ceiling(0) == 1.0
    assert p.ceiling(1) == 2.0
    assert p.ceiling(2) == 4.0
    assert p.ceiling(3) == 8.0
    assert p.ceiling(4) == 10.0  # capped
    assert p.ceiling(50) == 10.0  # stays capped


def test_negative_attempt_rejected():
    with pytest.raises(ValueError):
        BackoffPolicy().ceiling(-1)


def test_no_jitter_returns_ceiling():
    p = BackoffPolicy(base_s=2.0, factor=2.0, cap_s=100.0, jitter=False)
    assert p.delay(2) == p.ceiling(2)


def test_full_jitter_bounded_by_ceiling():
    p = BackoffPolicy(base_s=1.0, factor=2.0, cap_s=30.0, jitter=True)
    rng = random.Random(1234)
    for attempt in range(6):
        for _ in range(50):
            d = p.delay(attempt, rng)
            assert 0.0 <= d <= p.ceiling(attempt)


@pytest.mark.parametrize(
    ("attempt", "max_attempts", "retryable", "expected"),
    [
        (1, 5, True, True),
        (4, 5, True, True),
        (5, 5, True, False),  # budget exhausted
        (6, 5, True, False),
        (1, 5, False, False),  # non-retryable never retries
        (0, 5, False, False),
    ],
)
def test_should_retry(attempt: int, max_attempts: int, retryable: bool, expected: bool):
    assert should_retry(attempt, max_attempts, retryable) is expected
