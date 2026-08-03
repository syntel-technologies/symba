#!/usr/bin/env python3
"""Dump the engine's OpenAPI schema to frontend/openapi.json.

The UI's TypeScript client is generated from THIS file; a CI drift gate
(check_openapi.py) regenerates it and fails if the committed copy is stale, so the
UI can never silently diverge from the API.

    why we can build the app without a database
    -------------------------------------------
    FastAPI's .openapi() only inspects route signatures + Pydantic DTOs — it never
    executes a handler, so no pool connection is needed. EngineState.build() wires
    service objects around the pools but opens nothing at construction time
    (db/pool.py: pools connect lazily in create_pools, not here). We therefore hand
    it a pair of placeholder pool objects purely to satisfy the dataclass; they are
    never touched during schema generation.

Usage:
    python tools/dump_openapi.py            # write frontend/openapi.json
    python tools/dump_openapi.py --check    # print schema to stdout (drift gate uses this)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, cast

from symba.config import load_config
from symba.db.pool import Pools
from symba.transport.http_server import build_app
from symba.transport.state import EngineState

_OUT = Path(__file__).resolve().parent.parent / "frontend" / "openapi.json"


def build_schema() -> dict[str, Any]:
    # Placeholder pools: never connected to, only stored on the dataclass. cast keeps
    # the type checker honest without pulling asyncpg into a schema-only tool.
    placeholder = cast(Any, object())
    pools = Pools(hot=placeholder, general=placeholder)
    app = build_app(EngineState.build(load_config(), pools))
    return app.openapi()


def _canonical(schema: dict[str, Any]) -> str:
    # Stable key order + trailing newline so the committed file is diff-friendly and
    # the drift gate compares bytes deterministically across machines.
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def main() -> int:
    schema = _canonical(build_schema())
    if "--check" in sys.argv:
        sys.stdout.write(schema)
        return 0
    _OUT.write_text(schema)
    sys.stderr.write(f"Wrote {_OUT.relative_to(_OUT.parent.parent)} ({len(schema)} bytes)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
