"""Fan-out gate policies. Pure logic — no I/O.

A gate fires its continuation when its policy is satisfied over the children:
  - all_success : every child terminal AND none DEAD (a ctx.skip() is a non-failure,
                  so an all-skipped gate still fires with succeeded=0)
  - all_terminal: every child reached a terminal state (success, skip, or dead)
  - quorum(n)   : at least n children SUCCEEDED (n is an absolute count)

Child outcomes split three ways (spec 7.2): SUCCEEDED, SKIPPED (non-failure no-op),
and FAILED (DEAD). Only a DEAD child blocks all_success; a skip counts toward
completed but toward neither succeeded nor failed. The invariant is
``completed = succeeded + failed + skipped`` (skipped is derived, never stored).

The engine enforces "fire exactly once" via a fired_at claim-once UPDATE; this
module answers only "is the policy satisfied now?".
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GateProgress:
    expected: int
    completed: int  # reached any terminal state
    succeeded: int
    failed: int = 0  # DEAD children only; skips are neither succeeded nor failed

    def __post_init__(self) -> None:
        if self.expected <= 0:
            raise ValueError("expected must be positive")
        if not (0 <= self.succeeded <= self.completed <= self.expected):
            raise ValueError("gate progress counts are inconsistent")
        # failed is a subset of completed, disjoint from succeeded; the remainder
        # (completed - succeeded - failed) is the skipped count.
        if not (0 <= self.failed <= self.completed - self.succeeded):
            raise ValueError("gate progress counts are inconsistent")


def all_success(p: GateProgress) -> bool:
    # Every child terminal and none DEAD; a skip is a non-failure that does not block.
    return p.completed == p.expected and p.failed == 0


def all_terminal(p: GateProgress) -> bool:
    return p.completed == p.expected


def quorum(p: GateProgress, n: int) -> bool:
    if n <= 0:
        raise ValueError("quorum n must be positive")
    return p.succeeded >= min(n, p.expected)


def is_satisfied(policy: str, p: GateProgress) -> bool:
    if policy == "all_success":
        return all_success(p)
    if policy == "all_terminal":
        return all_terminal(p)
    if policy.startswith("quorum(") and policy.endswith(")"):
        n = int(policy[len("quorum(") : -1])
        return quorum(p, n)
    raise ValueError(f"unknown gate policy {policy!r}")
