"""Chain advance as a linked list. Pure logic — no I/O.

A chain is stored on the job row as (on_success, chain_tail):
  - on_success : the next task_name to run, or None at the end of the chain
  - chain_tail : the remaining task_names after on_success

Advancing consumes the head: the next job's on_success is chain_tail[0] and its
chain_tail is chain_tail[1:]. stop_chain drops the tail entirely.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ChainStep:
    """What to enqueue as the continuation, if any."""

    next_task: str
    on_success: str | None
    chain_tail: tuple[str, ...]


def advance(on_success: str | None, chain_tail: tuple[str, ...]) -> ChainStep | None:
    """Return the continuation for a just-succeeded job, or None if the chain ends."""
    if not on_success:
        return None
    next_on_success = chain_tail[0] if chain_tail else None
    remaining = tuple(chain_tail[1:]) if chain_tail else ()
    return ChainStep(next_task=on_success, on_success=next_on_success, chain_tail=remaining)
