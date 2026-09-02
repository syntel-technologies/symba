"""L1 — matcher fairness shaping.

Pure-logic proof of `_fairness_shape`: within one priority band the matcher must
round-robin across group_keys so no single group monopolizes the head of a batch.
No DB, no event loop — this is the deterministic core the L2 routing suite relies on.

    input (equal priority)      shaped (round-robin by group)
    ----------------------      -----------------------------
      A A A B              →      A B A A   (B lifted ahead of the 3rd A)
"""

from __future__ import annotations

import pytest

from symba.db.records import ClaimedJob
from symba.services.matcher import _fairness_shape

pytestmark = pytest.mark.l1


def _job(jid: str, group: str, priority: int = 5) -> ClaimedJob:
    return ClaimedJob(
        id=jid,
        task_name="t",
        tenant="default",
        group_key=group,
        max_concurrent_per_group=None,
        priority=priority,
        lease_token="lt",
        lease_ttl_s=60,
        payload={},
        attempt=1,
        raw={},
    )


def test_interleaves_groups_within_priority_band() -> None:
    jobs = [_job("a1", "A"), _job("a2", "A"), _job("a3", "A"), _job("b1", "B")]
    shaped = [j.id for j in _fairness_shape(jobs)]
    # B must appear before the last A: no full A monopoly at the head.
    assert shaped.index("b1") < shaped.index("a3"), f"group B starved: {shaped}"


def test_single_group_preserves_order() -> None:
    jobs = [_job("a1", "A"), _job("a2", "A"), _job("a3", "A")]
    assert [j.id for j in _fairness_shape(jobs)] == ["a1", "a2", "a3"]


def test_empty_is_noop() -> None:
    assert _fairness_shape([]) == []
