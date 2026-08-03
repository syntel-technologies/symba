"""L1: fan-out gate policies."""

from __future__ import annotations

import pytest

from symba.core.gates import GateProgress, is_satisfied, quorum

pytestmark = pytest.mark.l1


def test_quorum_non_positive_n_rejected():
    with pytest.raises(ValueError):
        quorum(GateProgress(3, 3, 3), 0)


def test_all_success_satisfied_when_all_terminal_and_none_dead():
    # Every child succeeded: fires.
    assert is_satisfied("all_success", GateProgress(3, 3, 3, failed=0))
    # A DEAD child (failed>0) blocks all_success even when all are terminal.
    assert not is_satisfied("all_success", GateProgress(3, 3, 2, failed=1))
    # Not yet fully terminal: cannot fire.
    assert not is_satisfied("all_success", GateProgress(3, 2, 2, failed=0))


def test_all_success_all_skipped_still_fires():
    # 3 children, all skipped: completed=3, succeeded=0, failed=0. A skip is a
    # non-failure, so all_success fires with succeeded=0 (spec 7.2).
    assert is_satisfied("all_success", GateProgress(3, 3, 0, failed=0))


def test_all_success_mixed_skip_and_success_fires():
    # 2 succeeded + 1 skip, none dead -> fires; succeeded count is 2.
    assert is_satisfied("all_success", GateProgress(3, 3, 2, failed=0))


def test_all_terminal_ignores_success_count():
    assert is_satisfied("all_terminal", GateProgress(3, 3, 0))
    assert not is_satisfied("all_terminal", GateProgress(3, 2, 2))


def test_quorum_absolute_count():
    assert is_satisfied("quorum(2)", GateProgress(5, 5, 2))
    assert not is_satisfied("quorum(3)", GateProgress(5, 5, 2))


def test_quorum_clamped_to_expected():
    # quorum larger than the child count is satisfied when all succeed.
    assert is_satisfied("quorum(10)", GateProgress(3, 3, 3))


def test_unknown_policy_raises():
    with pytest.raises(ValueError):
        is_satisfied("nonsense", GateProgress(1, 1, 1))


@pytest.mark.parametrize(
    "args",
    [(0, 0, 0), (3, 4, 0), (3, 2, 3), (3, 3, 4)],
)
def test_inconsistent_progress_rejected(args: tuple[int, int, int]):
    with pytest.raises(ValueError):
        GateProgress(*args)
