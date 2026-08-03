"""Backoff math. Pure logic — no I/O.

Exponential backoff with full jitter:

    ceiling(attempt) = min(cap, base * factor**attempt)
    delay            = random(0, ceiling)   when jitter else ceiling

Full jitter (not equal jitter) is the AWS-recommended default: it maximizes
spread and minimizes retry storms. `attempt` is 0-based for the first retry.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    base_s: float = 1.0
    factor: float = 2.0
    cap_s: float = 300.0
    jitter: bool = True

    def ceiling(self, attempt: int) -> float:
        if attempt < 0:
            raise ValueError("attempt must be >= 0")
        raw = self.base_s * (self.factor**attempt)
        return min(self.cap_s, raw)

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        ceil = self.ceiling(attempt)
        if not self.jitter:
            return ceil
        r = rng or random
        return r.uniform(0.0, ceil)


def should_retry(attempt: int, max_attempts: int, retryable: bool) -> bool:
    """A job retries iff the error is retryable AND attempts remain.

    `attempt` is the number already made (1-based count of executions so far).
    """
    if not retryable:
        return False
    return attempt < max_attempts
