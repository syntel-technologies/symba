"""Dependency readiness rules. Pure logic — no I/O.

A submitted job with declared depends_on flips to `queued` only when BOTH:
  - remaining_deps == 0 (every upstream succeeded), AND
  - run_at <= now() (its scheduled time has arrived).

The remaining_deps counter is decremented transactionally as each upstream
completes; this module is the pure predicate the SQL encodes.
"""

from __future__ import annotations

from datetime import datetime


def is_ready(remaining_deps: int, run_at: datetime, now: datetime) -> bool:
    if remaining_deps < 0:
        raise ValueError("remaining_deps cannot be negative")
    return remaining_deps == 0 and run_at <= now


def decrement(remaining_deps: int) -> int:
    if remaining_deps <= 0:
        raise ValueError("cannot decrement remaining_deps at or below zero")
    return remaining_deps - 1
