"""L1: chain advance."""

from __future__ import annotations

import pytest

from symba.core.chain import advance

pytestmark = pytest.mark.l1


def test_advance_consumes_head_and_shifts_tail():
    step = advance("b", ("c", "d"))
    assert step is not None
    assert step.next_task == "b"
    assert step.on_success == "c"
    assert step.chain_tail == ("d",)


def test_advance_last_link_has_no_continuation_after():
    step = advance("b", ())
    assert step is not None
    assert step.next_task == "b"
    assert step.on_success is None
    assert step.chain_tail == ()


def test_advance_end_of_chain_returns_none():
    assert advance(None, ()) is None
    assert advance("", ("x",)) is None  # empty on_success terminates


def test_full_walk_three_step_chain():
    # a -> b -> c -> d
    step = advance("b", ("c", "d"))
    assert step is not None and step.next_task == "b"
    step = advance(step.on_success, step.chain_tail)
    assert step is not None and step.next_task == "c"
    step = advance(step.on_success, step.chain_tail)
    assert step is not None and step.next_task == "d"
    assert advance(step.on_success, step.chain_tail) is None
