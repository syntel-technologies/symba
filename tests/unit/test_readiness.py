"""L1: dependency readiness."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from symba.core.readiness import decrement, is_ready

pytestmark = pytest.mark.l1

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_ready_when_no_deps_and_time_arrived():
    assert is_ready(0, _NOW - timedelta(seconds=1), _NOW)


def test_not_ready_when_deps_remain():
    assert not is_ready(2, _NOW, _NOW)


def test_not_ready_when_scheduled_in_future():
    assert not is_ready(0, _NOW + timedelta(seconds=1), _NOW)


def test_ready_at_exact_run_at_boundary():
    assert is_ready(0, _NOW, _NOW)


def test_negative_deps_is_a_bug():
    with pytest.raises(ValueError):
        is_ready(-1, _NOW, _NOW)


def test_decrement():
    assert decrement(3) == 2
    assert decrement(1) == 0


def test_decrement_below_zero_rejected():
    with pytest.raises(ValueError):
        decrement(0)
