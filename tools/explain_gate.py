"""EXPLAIN gate.

Seeds a representative UNGROUPED queued backlog (the hot, common case), then
EXPLAINs the claim query and fails if the ungrouped candidate selection does not
use ix_jobs_claimable, or if it Seq Scans that backlog. This is the single most
important performance regression guard: a plan flip here turns O(log n) claims
into O(n) table scans under load.

Scope note: claim.sql has two candidate paths. Only the UNGROUPED
path is index-driven and load-bearing under the firehose; the GROUPED path scans
`jobs WHERE group_key IS NOT NULL` (small, bounded backlog) and the terminal
UPDATE ... FROM candidate join by id are both expected, benign scans over tiny
sets. The gate therefore flags a Seq Scan ONLY when it is over the ungrouped
candidate scan (no `group_key IS NOT NULL` filter, not inside a ModifyTable).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import asyncpg

from symba.db.migrate import apply_schema
from symba.db.queries import Q

_DSN = os.environ.get("SYMBA_POSTGRES__DSN", "postgresql://symba:symba@localhost:5432/symba")
_SEED = 5000


async def _seed(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        INSERT INTO jobs (task_name, payload, tenant, state, priority, runs_on, run_at)
        SELECT 'seed', '{}'::jsonb, 'default', 'queued',
               (random() * 3)::int, ARRAY['cpu']::text[], now()
        FROM generate_series(1, $1)
        """,
        _SEED,
    )
    await conn.execute("ANALYZE jobs")


async def main() -> int:
    pool = await asyncpg.create_pool(dsn=_DSN, min_size=1, max_size=2, server_settings={"search_path": "symba"})
    if pool is None:
        print("EXPLAIN gate: could not connect")
        return 1
    await apply_schema(pool)
    async with pool.acquire() as conn:
        await _seed(conn)
        plan_rows = await conn.fetch(
            f"EXPLAIN (FORMAT JSON) {Q.CLAIM}",
            ["cpu"],
            [],
            50,
            "explain-worker",
            8,
        )
        raw = plan_rows[0][0]
        plan = json.loads(raw) if isinstance(raw, str) else raw
    await pool.close()

    root = plan[0]["Plan"] if isinstance(plan, list) else plan["Plan"]
    print(json.dumps(root, indent=2))

    def _is_hot_path_seq_scan(node: dict, under_modify_table: bool) -> bool:
        """A Seq Scan on `jobs` that is the UNGROUPED candidate scan (the O(n)
        regression we guard). Grouped-path scans (filtered by group_key IS NOT
        NULL) and terminal UPDATE joins (under a ModifyTable) are exempt."""
        if node.get("Node Type") != "Seq Scan" or node.get("Relation Name") != "jobs":
            return False
        if under_modify_table:
            return False  # UPDATE ... FROM candidate join by id — tiny set
        return "group_key IS NOT NULL" not in node.get("Filter", "")

    def walk(node: dict, under_modify_table: bool = False) -> tuple[bool, bool]:
        """Return (hot_path_seq_scan, uses_claim_index)."""
        seq_bad = _is_hot_path_seq_scan(node, under_modify_table)
        uses_idx = node.get("Index Name") == "ix_jobs_claimable"
        child_under_modify = under_modify_table or node.get("Node Type") == "ModifyTable"
        for child in node.get("Plans", []):
            child_seq, child_idx = walk(child, child_under_modify)
            seq_bad = seq_bad or child_seq
            uses_idx = uses_idx or child_idx
        return seq_bad, uses_idx

    seq_scan_on_jobs, uses_claim_index = walk(root)

    if seq_scan_on_jobs:
        print("EXPLAIN gate FAILED: Seq Scan on the ungrouped claim candidate (should use ix_jobs_claimable)")
        return 1
    if not uses_claim_index:
        print("EXPLAIN gate FAILED: claim plan does not use ix_jobs_claimable")
        return 1

    print("EXPLAIN gate OK: ungrouped claim uses ix_jobs_claimable, no hot-path Seq Scan")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
