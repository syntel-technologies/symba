"""Job state machine. Pure logic — no I/O.

    submitted --deps/run_at ready--> queued --claim--> running
        |                              ^                 |
        |                              |  retry(backoff) |
        |                              +-----------------+
        |                                                |
        +----------------> cancelled <---- cancel -------+
                                                         |
    running --wait_for_event--> waiting --signal--> queued
    running --success--> succeeded
    running --exhausted/fatal--> dead

LIVE states live in the `jobs` table; TERMINAL states live in `jobs_archive`.
"""

from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    SUBMITTED = "submitted"
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    DEAD = "dead"
    CANCELLED = "cancelled"


LIVE_STATES: frozenset[JobState] = frozenset({JobState.SUBMITTED, JobState.QUEUED, JobState.RUNNING, JobState.WAITING})
TERMINAL_STATES: frozenset[JobState] = frozenset({JobState.SUCCEEDED, JobState.DEAD, JobState.CANCELLED})

# Legal transitions. Any transition not listed is a bug.
TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    JobState.SUBMITTED: frozenset({JobState.QUEUED, JobState.CANCELLED}),
    JobState.QUEUED: frozenset({JobState.RUNNING, JobState.CANCELLED}),
    JobState.RUNNING: frozenset(
        {
            JobState.QUEUED,  # retry with backoff
            JobState.WAITING,  # wait_for_event
            JobState.SUCCEEDED,
            JobState.DEAD,
            JobState.CANCELLED,
        }
    ),
    JobState.WAITING: frozenset({JobState.QUEUED, JobState.CANCELLED}),
    JobState.SUCCEEDED: frozenset(),
    JobState.DEAD: frozenset(),
    JobState.CANCELLED: frozenset(),
}


def is_live(state: JobState) -> bool:
    return state in LIVE_STATES


def is_terminal(state: JobState) -> bool:
    return state in TERMINAL_STATES


def can_transition(src: JobState, dst: JobState) -> bool:
    return dst in TRANSITIONS.get(src, frozenset())
