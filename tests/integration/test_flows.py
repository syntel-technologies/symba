# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
"""L2 flow primitives (M2): chain continuation.

Chains are linked lists carried on the job row as (on_success, chain_tail). On each
successful Complete the engine, IN THE SAME TRANSACTION as the archive move, inserts
the next link as a fresh queued job that inherits the predecessor's lineage
(ctx_id/pipeline) and routing (group/priority/runs_on/rate_class). ctx.stop_chain
(drop_chain_tail=true) ends the chain early.

    a  ──success──►  b  ──success──►  c        (a: on_success=b, chain_tail=[c])
    ▲ archived        ▲ queued(new)    ▲ queued(new after b)
    │
    └─ drop_chain_tail=true on a  ==>  no b, chain ends
"""

from __future__ import annotations

import json
import uuid

import asyncpg
import pytest

from symba.config import load_config
from symba.db import repository as repo
from symba.db.pool import Pools
from symba.services.cancel_service import CancelService
from symba.services.job_service import JobService
from tests.integration import _helpers as h

pytestmark = [pytest.mark.l2, pytest.mark.asyncio(loop_scope="session")]


async def _chain_children(db: asyncpg.Connection, ctx_id: str) -> list[asyncpg.Record]:
    return await db.fetch(
        "SELECT task_name, state, on_success, chain_tail, ctx_id, priority, group_key "
        "FROM jobs WHERE ctx_id = $1 ORDER BY created_at",
        ctx_id,
    )


async def test_chain_continuation_advances_on_success(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    job_id = await h.seed(db, h.spec(task_name="a", group_key="g1", priority=7))
    ctx_id = str(uuid.uuid4())  # ctx_id is TEXT; pass a plain string
    await db.execute(
        "UPDATE jobs SET on_success='b', chain_tail=ARRAY['c'], ctx_id=$2, runs_on=ARRAY['cpu'] WHERE id=$1",
        job_id,
        ctx_id,
    )

    claimed = await h.claim_one(db, tags=["cpu"])
    assert claimed.id == job_id
    await svc.complete(job_id=job_id, lease_token=claimed.lease_token, result={"ok": True})

    # a archived; exactly one live continuation `b`, head consumed + tail advanced,
    # inheriting lineage (ctx_id) + routing (priority/group).
    children = await _chain_children(db, ctx_id)
    assert len(children) == 1
    b = children[0]
    assert b["task_name"] == "b"
    assert b["state"] == "queued"
    assert b["on_success"] == "c"
    assert list(b["chain_tail"]) == []
    assert b["priority"] == 7 and b["group_key"] == "g1"


async def test_chain_runs_to_completion_across_links(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    job_id = await h.seed(db, h.spec(task_name="a"))
    ctx_id = str(uuid.uuid4())  # ctx_id is TEXT; pass a plain string
    await db.execute("UPDATE jobs SET on_success='b', chain_tail=ARRAY['c'], ctx_id=$2 WHERE id=$1", job_id, ctx_id)

    # a -> completes -> b queued
    c1 = await h.claim_one(db)
    await svc.complete(job_id=c1.id, lease_token=c1.lease_token, result=None)
    # b -> completes -> c queued
    c2 = await h.claim_one(db)
    await svc.complete(job_id=c2.id, lease_token=c2.lease_token, result=None)
    # c -> completes -> end of chain, nothing new
    c3 = await h.claim_one(db)
    await svc.complete(job_id=c3.id, lease_token=c3.lease_token, result=None)

    # only c remains referenced in the live table? all three archived succeeded.
    assert await _chain_children(db, ctx_id) == []
    archived = await db.fetch("SELECT task_name FROM jobs_archive WHERE ctx_id=$1 ORDER BY finished_at", ctx_id)
    assert [r["task_name"] for r in archived] == ["a", "b", "c"]


async def test_stop_chain_drops_the_tail(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    job_id = await h.seed(db, h.spec(task_name="a"))
    ctx_id = str(uuid.uuid4())  # ctx_id is TEXT; pass a plain string
    await db.execute("UPDATE jobs SET on_success='b', chain_tail=ARRAY['c'], ctx_id=$2 WHERE id=$1", job_id, ctx_id)

    claimed = await h.claim_one(db)
    await svc.complete(job_id=job_id, lease_token=claimed.lease_token, result=None, drop_chain_tail=True)

    assert await _chain_children(db, ctx_id) == []  # stop_chain ended it


async def test_end_of_chain_inserts_nothing(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    job_id = await h.seed(db, h.spec(task_name="last"))
    ctx_id = str(uuid.uuid4())  # ctx_id is TEXT; pass a plain string
    await db.execute("UPDATE jobs SET ctx_id=$2 WHERE id=$1", job_id, ctx_id)  # on_success NULL

    claimed = await h.claim_one(db)
    await svc.complete(job_id=job_id, lease_token=claimed.lease_token, result=None)

    assert await _chain_children(db, ctx_id) == []


async def test_chain_continuation_inherits_on_failure(db: asyncpg.Connection, pools: Pools) -> None:
    """A DEAD chain tail must still fire the submitter's on_failure hook.

    Without inheritance, complete_stage dying leaves iknowledge spines in-progress
    forever because mark_stage_failed never enqueues.
    """
    svc = JobService(pools, load_config())
    job_id = await h.seed(db, h.spec(task_name="parse.parse_document"))
    ctx_id = str(uuid.uuid4())
    await db.execute(
        "UPDATE jobs SET on_success='parse.complete_stage', ctx_id=$2, on_failure=$3::jsonb WHERE id=$1",
        job_id,
        ctx_id,
        {"task_name": "mark_stage_failed", "payload": {"stage": "parse"}},
    )

    c1 = await h.claim_one(db)
    await svc.complete(job_id=c1.id, lease_token=c1.lease_token, result={"output_ref": "x"})

    cont = await db.fetchrow(
        "SELECT task_name, on_failure FROM jobs WHERE ctx_id=$1 AND task_name='parse.complete_stage'",
        ctx_id,
    )
    assert cont is not None
    assert cont["on_failure"]["task_name"] == "mark_stage_failed"

    c2 = await h.claim_one(db)
    assert c2.task_name == "parse.complete_stage"
    await _fail_die(svc, c2)

    hook = await db.fetchrow("SELECT state, payload FROM jobs WHERE task_name='mark_stage_failed'")
    assert hook is not None and hook["state"] == "queued"
    assert hook["payload"]["stage"] == "parse"


async def test_inline_upstream_delivers_chain_predecessor(db: asyncpg.Connection, pools: Pools) -> None:
    """Matcher attaches the chain predecessor into Job.upstream so ctx.output[] works."""
    from symba.services.matcher import Matcher
    from symba.services.registry import WorkerConn, WorkerRegistry

    svc = JobService(pools, load_config())
    job_id = await h.seed(db, h.spec(task_name="parse.parse_document"))
    ctx_id = str(uuid.uuid4())
    await db.execute(
        "UPDATE jobs SET on_success='parse.complete_stage', ctx_id=$2, pipeline='ingestion', stage='parse' WHERE id=$1",
        job_id,
        ctx_id,
    )

    c1 = await h.claim_one(db)
    await svc.complete(job_id=c1.id, lease_token=c1.lease_token, result={"output_ref": "parsed/doc.json"})

    registry = WorkerRegistry()
    conn = WorkerConn(worker_id="w-inline", tags=frozenset(), free_slots=2, labels={})
    await registry.register(conn)
    assigned = await Matcher(pools, registry, load_config()).pass_()
    assert assigned == 1

    assignment = conn.queue.get_nowait()
    assert assignment.job.spec.task_name == "parse.complete_stage"
    # Continuation must ship the SHARED ctx_id (not job.id) so SDK logs/Ctx
    # correlate the whole chain. Same for pipeline/stage inheritance.
    assert assignment.job.spec.ctx_id == ctx_id
    assert assignment.job.spec.ctx_id != assignment.job.id
    assert assignment.job.spec.pipeline == "ingestion"
    assert assignment.job.spec.stage == "parse"
    assert len(assignment.job.upstream) == 1
    u = assignment.job.upstream[0]
    assert u.key == "parse.parse_document"
    assert json.loads(u.result_json.decode()) == {"output_ref": "parsed/doc.json"}


async def test_inline_upstream_delivers_depends_on_alias(db: asyncpg.Connection, pools: Pools) -> None:
    """Declared depends_on producers land under their alias in Job.upstream."""
    from symba.services.matcher import Matcher
    from symba.services.registry import WorkerConn, WorkerRegistry

    svc = JobService(pools, load_config())
    up_id = await h.seed(db, h.spec(task_name="dense"))
    down_id = await h.seed(db, h.spec(task_name="store", state="submitted"))
    await db.execute("UPDATE jobs SET remaining_deps=1 WHERE id=$1", down_id)
    await repo.insert_dependency(db, job_id=down_id, depends_on_job_id=up_id, alias="vectors")

    c1 = await h.claim_one(db)
    assert c1.id == up_id
    await svc.complete(job_id=up_id, lease_token=c1.lease_token, result={"n": 42})

    registry = WorkerRegistry()
    conn = WorkerConn(worker_id="w-dep", tags=frozenset(), free_slots=2, labels={})
    await registry.register(conn)
    assigned = await Matcher(pools, registry, load_config()).pass_()
    assert assigned == 1

    assignment = conn.queue.get_nowait()
    assert assignment.job.id == down_id
    assert len(assignment.job.upstream) == 1
    u = assignment.job.upstream[0]
    assert u.key == "vectors"
    assert u.job_id == up_id
    assert json.loads(u.result_json.decode()) == {"n": 42}


# ── depends_on / static joins ──────────────────────────────────────────────────
#
#   up ──success──►  (remaining_deps: 1 -> 0)  down: submitted -> queued
#
# A downstream job with declared depends_on starts 'submitted' (not claimable) and
# only flips to 'queued' when its LAST upstream completes, in the completing job's
# own transaction. Until then it is invisible to claim.sql.


async def _state(db: asyncpg.Connection, job_id: str) -> str | None:
    return await db.fetchval("SELECT state FROM jobs WHERE id=$1", job_id)


async def test_depends_on_flips_dependent_when_last_upstream_completes(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    up_id = await h.seed(db, h.spec(task_name="up"))
    # downstream declares one dep; submit_service would set remaining_deps=1 and
    # state='submitted' — replicate that end state directly for the L2 seed.
    down_id = await h.seed(db, h.spec(task_name="down", state="submitted"))
    await db.execute("UPDATE jobs SET remaining_deps=1 WHERE id=$1", down_id)
    await repo.insert_dependency(db, job_id=down_id, depends_on_job_id=up_id, alias=None)

    # down is NOT claimable while submitted.
    assert await _state(db, down_id) == "submitted"

    claimed = await h.claim_one(db)  # only `up` is queued
    assert claimed.id == up_id
    await svc.complete(job_id=up_id, lease_token=claimed.lease_token, result={"v": 1})

    # last upstream done -> down flipped to queued in the SAME tx, now claimable.
    assert await _state(db, down_id) == "queued"
    nxt = await h.claim_one(db)
    assert nxt.id == down_id


async def test_depends_on_holds_until_all_upstreams_done(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    up1 = await h.seed(db, h.spec(task_name="up1"))
    up2 = await h.seed(db, h.spec(task_name="up2"))
    down_id = await h.seed(db, h.spec(task_name="down", state="submitted"))
    await db.execute("UPDATE jobs SET remaining_deps=2 WHERE id=$1", down_id)
    await repo.insert_dependency(db, job_id=down_id, depends_on_job_id=up1, alias="a")
    await repo.insert_dependency(db, job_id=down_id, depends_on_job_id=up2, alias="b")

    c1 = await h.claim_one(db, worker="w1")
    await svc.complete(job_id=c1.id, lease_token=c1.lease_token, result=None)
    # one upstream left -> still submitted.
    assert await _state(db, down_id) == "submitted"
    assert await db.fetchval("SELECT remaining_deps FROM jobs WHERE id=$1", down_id) == 1

    c2 = await h.claim_one(db, worker="w2")
    await svc.complete(job_id=c2.id, lease_token=c2.lease_token, result=None)
    # both done -> queued.
    assert await _state(db, down_id) == "queued"


async def test_submit_wires_dependency_edges_and_holds_dependent(db: asyncpg.Connection, pools: Pools) -> None:
    from symba.services.submit_service import SubmitService

    submit = SubmitService(pools, h.registry(), load_config())
    up_id = await h.seed(db, h.spec(task_name="up"))
    down_spec = h.spec(task_name="down")
    down_spec.depends_on = [(up_id, "src")]

    outcome = await submit.submit([down_spec])
    down_id = outcome.job_ids[0]

    # submit derived remaining_deps=1 -> state submitted; edge recorded with alias.
    assert await _state(db, down_id) == "submitted"
    edge = await db.fetchrow(
        "SELECT alias FROM job_dependencies WHERE job_id=$1 AND depends_on_job_id=$2", down_id, up_id
    )
    assert edge is not None and edge["alias"] == "src"


# ── fan-out + gate ──────────────────────────────────────────────────────────────
#
#            ┌── c1 ─┐
#   FanOut ──┤       ├── (policy satisfied) ──► on_complete job (queued once)
#            └── c2 ─┘
#
# Children carry parent_gate_id; each terminal child bumps the gate. The tx that
# crosses the policy threshold fires the continuation EXACTLY ONCE (fired_at
# claim-once), even if more children complete afterward.


async def _fanout(
    db: asyncpg.Connection,
    pools: Pools,
    *,
    policy: str,
    n: int,
    on_complete_payload: dict[str, object] | None = None,
) -> tuple[str, list[str]]:
    from symba.services.fanout_service import FanOutService

    fan = FanOutService(pools, h.registry(), load_config())
    children = [h.spec(task_name=f"child{i}") for i in range(n)]
    on_complete = h.spec(task_name="reduce", payload=on_complete_payload)
    out = await fan.fan_out(tenant="default", ctx_id=None, children=children, on_complete=on_complete, policy=policy)
    return out.gate_id, out.child_job_ids


async def _reduce_jobs(db: asyncpg.Connection) -> list[asyncpg.Record]:
    return await db.fetch("SELECT id, state FROM jobs WHERE task_name='reduce'")


async def test_gate_all_success_fires_once_after_all_children(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    _gate_id, child_ids = await _fanout(db, pools, policy="all_success", n=3)

    # First two completions do NOT fire the gate.
    for _ in range(2):
        c = await h.claim_one(db)
        await svc.complete(job_id=c.id, lease_token=c.lease_token, result=None)
    assert await _reduce_jobs(db) == []  # continuation not yet enqueued

    # Third (last) completion fires the continuation exactly once.
    c = await h.claim_one(db)
    await svc.complete(job_id=c.id, lease_token=c.lease_token, result=None)
    reduce = await _reduce_jobs(db)
    assert len(reduce) == 1 and reduce[0]["state"] == "queued"
    assert set(child_ids)  # sanity: children existed


async def test_gate_all_terminal_fires_even_with_a_dead_child(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    await _fanout(db, pools, policy="all_terminal", n=2)

    # One child succeeds, one dies (non-retryable) — all_terminal still fires.
    c1 = await h.claim_one(db, worker="w1")
    await svc.complete(job_id=c1.id, lease_token=c1.lease_token, result=None)
    assert await _reduce_jobs(db) == []

    c2 = await h.claim_one(db, worker="w2")
    await svc.fail(
        job_id=c2.id,
        lease_token=c2.lease_token,
        error_type="ValueError",
        error_message="boom",
        stack_hash="h",
        retryable=False,
    )
    reduce = await _reduce_jobs(db)
    assert len(reduce) == 1  # fired despite the dead child (all_terminal)


async def test_gate_quorum_fires_at_threshold(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    await _fanout(db, pools, policy="quorum(2)", n=3)

    c1 = await h.claim_one(db, worker="w1")
    await svc.complete(job_id=c1.id, lease_token=c1.lease_token, result=None)
    assert await _reduce_jobs(db) == []  # 1 success < quorum 2

    c2 = await h.claim_one(db, worker="w2")
    await svc.complete(job_id=c2.id, lease_token=c2.lease_token, result=None)
    assert len(await _reduce_jobs(db)) == 1  # 2nd success hits quorum -> fires

    # A third completion must NOT fire a second continuation (claim-once).
    c3 = await h.claim_one(db, worker="w3")
    await svc.complete(job_id=c3.id, lease_token=c3.lease_token, result=None)
    assert len(await _reduce_jobs(db)) == 1


# ── gate skip counting (E3) + __gate__ manifest (E2) ──────────────────────────
#
# A ctx.skip() child is a NON-failure: it settles the gate (all_terminal/completed
# accounting) but does NOT advance succeeded. all_success fires as long as no child
# is DEAD, so an all-skipped gate still fires with succeeded=0. On fire, the
# continuation payload preserves the caller's on_complete payload AND carries the
# aggregate manifest under __gate__.


async def test_gate_all_success_fires_with_a_skipped_child_not_counted(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    await _fanout(db, pools, policy="all_success", n=3, on_complete_payload={"document_id": "d1"})

    # Two genuine successes, one skip. The skip is NOT a failure, so all_success
    # still fires; the skip must NOT count toward succeeded (spec 7.2).
    c1 = await h.claim_one(db, worker="w1")
    await svc.complete(job_id=c1.id, lease_token=c1.lease_token, result={"page": 1})
    c2 = await h.claim_one(db, worker="w2")
    await svc.complete(job_id=c2.id, lease_token=c2.lease_token, result={"page": 2})
    assert await _reduce_jobs(db) == []  # not all terminal yet

    c3 = await h.claim_one(db, worker="w3")
    await svc.complete(job_id=c3.id, lease_token=c3.lease_token, result=None, skipped=True)

    reduce = await db.fetch("SELECT payload FROM jobs WHERE task_name='reduce'")
    assert len(reduce) == 1  # fired despite the skip
    payload = json.loads(reduce[0]["payload"]) if isinstance(reduce[0]["payload"], str) else reduce[0]["payload"]
    # E2: caller payload preserved AND manifest under __gate__.
    assert payload["document_id"] == "d1"
    assert payload["__gate__"]["expected"] == 3
    assert payload["__gate__"]["succeeded"] == 2  # the skip is excluded
    # Per-child results match the SDK fake shape (skips excluded).
    results = payload["__gate__"]["results"]
    assert len(results) == 2
    assert {r["job_id"] for r in results} == {c1.id, c2.id}
    assert all(r["task"] for r in results)
    assert {r["result"]["page"] for r in results} == {1, 2}


async def test_gate_all_skipped_still_fires_with_zero_succeeded(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    await _fanout(db, pools, policy="all_success", n=2, on_complete_payload={})

    for worker in ("w1", "w2"):
        c = await h.claim_one(db, worker=worker)
        await svc.complete(job_id=c.id, lease_token=c.lease_token, result=None, skipped=True)

    reduce = await db.fetch("SELECT payload FROM jobs WHERE task_name='reduce'")
    assert len(reduce) == 1  # all-skipped gate still fires
    payload = json.loads(reduce[0]["payload"]) if isinstance(reduce[0]["payload"], str) else reduce[0]["payload"]
    assert payload["__gate__"]["succeeded"] == 0
    assert payload["__gate__"]["expected"] == 2
    assert payload["__gate__"]["results"] == []  # no successes → empty results


# ── dependency death: cascade-cancel + on_failure hook ────────────────────────
#
#   up (dies) ──►  down (depends_on up)  ──►  archived 'cancelled' (root_cause=up)
#            └──►  on_failure hook       ──►  enqueued 'queued'


async def _fail_die(svc: JobService, c: object) -> None:
    await svc.fail(
        job_id=c.id,  # type: ignore[attr-defined]
        lease_token=c.lease_token,  # type: ignore[attr-defined]
        error_type="RuntimeError",
        error_message="fatal",
        stack_hash="h",
        retryable=False,
    )


async def test_dead_upstream_cascade_cancels_dependent_cone(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    up_id = await h.seed(db, h.spec(task_name="up"))
    mid_id = await h.seed(db, h.spec(task_name="mid", state="submitted"))
    leaf_id = await h.seed(db, h.spec(task_name="leaf", state="submitted"))
    await db.execute("UPDATE jobs SET remaining_deps=1 WHERE id = ANY($1::uuid[])", [mid_id, leaf_id])
    await repo.insert_dependency(db, job_id=mid_id, depends_on_job_id=up_id, alias=None)
    await repo.insert_dependency(db, job_id=leaf_id, depends_on_job_id=mid_id, alias=None)  # transitive

    c = await h.claim_one(db)  # only `up` is queued
    assert c.id == up_id
    await _fail_die(svc, c)

    # up dead; mid + leaf archived cancelled (transitive), each with a root-cause event.
    assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", up_id) == "dead"
    for dep in (mid_id, leaf_id):
        assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", dep) == "cancelled"
        ev = await db.fetchval(
            "SELECT detail->>'root_cause' FROM job_events WHERE job_id=$1 AND event='dependency_cancelled'", dep
        )
        assert ev == up_id
    assert await db.fetchval("SELECT count(*) FROM jobs") == 0  # nothing left live


async def test_on_failure_hook_enqueued_on_death(db: asyncpg.Connection, pools: Pools) -> None:
    svc = JobService(pools, load_config())
    job_id = await h.seed(db, h.spec(task_name="risky"))
    # pass a dict: the pool's jsonb codec json.dumps it (passing a JSON string here
    # would double-encode it, coming back as a str instead of a mapping).
    await db.execute(
        "UPDATE jobs SET on_failure=$2::jsonb WHERE id=$1",
        job_id,
        {"task_name": "notify_oncall", "payload": {"sev": 1}},
    )

    c = await h.claim_one(db)
    await _fail_die(svc, c)

    hook = await db.fetchrow("SELECT state, payload FROM jobs WHERE task_name='notify_oncall'")
    assert hook is not None and hook["state"] == "queued"


# ── cancel service: per-state matrix + cascade ────────────────────────────────


async def test_cancel_queued_job_archives_cancelled(db: asyncpg.Connection, pools: Pools) -> None:
    svc = CancelService(pools, h.registry())
    job_id = await h.seed(db, h.spec(state="queued"))

    outcome = await svc.cancel(job_id=job_id)
    assert outcome.cancelled and not outcome.was_running and outcome.note == "archived"
    assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", job_id) == "cancelled"


async def test_cancel_running_flags_cooperatively(db: asyncpg.Connection, pools: Pools) -> None:
    svc = CancelService(pools, h.registry())
    job_id = await h.seed(db, h.spec())
    await h.claim_one(db, worker="w9")

    outcome = await svc.cancel(job_id=job_id)
    assert outcome.cancelled and outcome.was_running and outcome.note == "cancel_requested"
    # job is still live (running) with the flag set; worker aborts on next heartbeat.
    assert await db.fetchval("SELECT cancel_requested FROM jobs WHERE id=$1", job_id) is True


async def test_cancel_terminal_is_noop(db: asyncpg.Connection, pools: Pools) -> None:
    svc = CancelService(pools, h.registry())
    job_id = await h.seed(db, h.spec(state="queued"))
    await svc.cancel(job_id=job_id)  # first cancel archives it

    outcome = await svc.cancel(job_id=job_id)  # second is idempotent
    assert not outcome.cancelled and outcome.note == "noop"


# ── regression pin: full 7-state matrix + future run_at + double-cancel ─────
# Cancel paths that only handle the running state are a common footgun: aborting a
# deferred or queued job can silently do nothing, or set a flag nothing reads. This
# matrix pins the explicit outcome for EVERY state so no state can ever silently
# fall through again.
#
#   state         cancel() outcome
#   ------------  ------------------------------------------------------------
#   submitted     archived 'cancelled'      (never claimed; decrement nothing)
#   queued        archived 'cancelled'
#   waiting       archived 'cancelled'
#   running       cancel_requested flag     (cooperative; lease is backstop)
#   succeeded     no-op (already terminal)
#   dead          no-op (already terminal)
#   cancelled     no-op (idempotent double-cancel)


@pytest.mark.parametrize("state", ["submitted", "queued", "waiting"])
async def test_cancel_nonrunning_states_archive_cancelled(db: asyncpg.Connection, pools: Pools, state: str) -> None:
    """submitted/queued/waiting all take the immediate-archive path."""
    svc = CancelService(pools, h.registry())
    job_id = await h.seed(db, h.spec(state=state))

    outcome = await svc.cancel(job_id=job_id)
    assert outcome.cancelled and not outcome.was_running and outcome.note == "archived"
    assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", job_id) == "cancelled"
    assert await db.fetchval("SELECT count(*) FROM jobs WHERE id=$1", job_id) == 0


async def test_cancel_future_run_at_needs_no_special_path(db: asyncpg.Connection, pools: Pools) -> None:
    """A scheduled (future run_at) job is just 'queued'/'submitted' and cancels
    directly — no separate score-rewrite hack needed."""
    svc = CancelService(pools, h.registry())
    job_id = await h.seed(db, h.spec(state="submitted"))
    await db.execute("UPDATE jobs SET run_at = now() + interval '1 hour' WHERE id=$1", job_id)

    outcome = await svc.cancel(job_id=job_id)
    assert outcome.cancelled and outcome.note == "archived"
    assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", job_id) == "cancelled"


@pytest.mark.parametrize("final_state", ["succeeded", "dead"])
async def test_cancel_already_terminal_is_noop(db: asyncpg.Connection, pools: Pools, final_state: str) -> None:
    """A job already archived as succeeded/dead is an idempotent no-op — cancel must
    NOT resurrect it or error."""
    svc = CancelService(pools, h.registry())
    job_id = await h.seed(db, h.spec(state="queued"))
    # Drive it straight to the archive in the requested terminal state.
    await db.execute("DELETE FROM jobs WHERE id=$1", job_id)
    await db.execute(
        "INSERT INTO jobs_archive (id, task_name, tenant, state, payload, final_state, finished_at) "
        "VALUES ($1, 't', 'default', $2, '{}'::jsonb, $2, now())",
        job_id,
        final_state,
    )

    outcome = await svc.cancel(job_id=job_id)
    assert not outcome.cancelled and not outcome.was_running and outcome.note == "noop"
    # untouched: still the original terminal state.
    assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", job_id) == final_state


async def test_cancel_double_cancel_is_idempotent(db: asyncpg.Connection, pools: Pools) -> None:
    """Cancelling the same job twice: first archives, second is a clean no-op (the
    'cancelled' terminal state also exercises the 7th state in the matrix)."""
    svc = CancelService(pools, h.registry())
    job_id = await h.seed(db, h.spec(state="queued"))

    first = await svc.cancel(job_id=job_id)
    second = await svc.cancel(job_id=job_id)
    third = await svc.cancel(job_id=job_id)  # cancelling a 'cancelled' job
    assert first.cancelled and first.note == "archived"
    assert not second.cancelled and second.note == "noop"
    assert not third.cancelled and third.note == "noop"
    assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", job_id) == "cancelled"


async def test_cancel_unknown_job_is_noop(db: asyncpg.Connection, pools: Pools) -> None:
    """Cancelling an id that was never submitted returns a no-op, not an error."""
    svc = CancelService(pools, h.registry())
    outcome = await svc.cancel(job_id=str(uuid.uuid4()))
    assert not outcome.cancelled and outcome.note == "noop"


async def test_cancel_cascade_archives_dependent_cone(db: asyncpg.Connection, pools: Pools) -> None:
    svc = CancelService(pools, h.registry())
    root = await h.seed(db, h.spec(task_name="root", state="queued"))
    dep = await h.seed(db, h.spec(task_name="dep", state="submitted"))
    await db.execute("UPDATE jobs SET remaining_deps=1 WHERE id=$1", dep)
    await repo.insert_dependency(db, job_id=dep, depends_on_job_id=root, alias=None)

    outcome = await svc.cancel(job_id=root, cascade=True)
    assert outcome.cancelled and outcome.cascaded == [dep]
    assert await db.fetchval("SELECT final_state FROM jobs_archive WHERE id=$1", dep) == "cancelled"
    root_cause = await db.fetchval(
        "SELECT detail->>'root_cause' FROM job_events WHERE job_id=$1 AND event='dependency_cancelled'", dep
    )
    assert root_cause == root
