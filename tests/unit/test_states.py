"""L1: state machine. Covers every legal + illegal transition."""

from __future__ import annotations

import pytest

from symba.core.states import (
    LIVE_STATES,
    TERMINAL_STATES,
    JobState,
    can_transition,
    is_live,
    is_terminal,
)

pytestmark = pytest.mark.l1


def test_live_and_terminal_partition_the_space():
    assert LIVE_STATES & TERMINAL_STATES == frozenset()
    assert LIVE_STATES | TERMINAL_STATES == frozenset(JobState)


@pytest.mark.parametrize("state", sorted(LIVE_STATES))
def test_is_live_true_for_live(state: JobState):
    assert is_live(state) and not is_terminal(state)


@pytest.mark.parametrize("state", sorted(TERMINAL_STATES))
def test_is_terminal_true_for_terminal(state: JobState):
    assert is_terminal(state) and not is_live(state)


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (JobState.SUBMITTED, JobState.QUEUED),
        (JobState.QUEUED, JobState.RUNNING),
        (JobState.RUNNING, JobState.SUCCEEDED),
        (JobState.RUNNING, JobState.DEAD),
        (JobState.RUNNING, JobState.QUEUED),  # retry
        (JobState.RUNNING, JobState.WAITING),
        (JobState.WAITING, JobState.QUEUED),  # signal
        (JobState.QUEUED, JobState.CANCELLED),
    ],
)
def test_legal_transitions(src: JobState, dst: JobState):
    assert can_transition(src, dst)


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (JobState.SUCCEEDED, JobState.QUEUED),  # terminal is a sink
        (JobState.DEAD, JobState.RUNNING),
        (JobState.CANCELLED, JobState.QUEUED),
        (JobState.SUBMITTED, JobState.RUNNING),  # must pass through queued
        (JobState.WAITING, JobState.RUNNING),
    ],
)
def test_illegal_transitions(src: JobState, dst: JobState):
    assert not can_transition(src, dst)
