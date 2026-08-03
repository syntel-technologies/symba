#!/usr/bin/env python3
"""Drift gate: fail if frontend/openapi.json is stale.

The UI's generated TypeScript client is only as trustworthy as the committed
schema. This gate regenerates the schema from the live app and byte-compares it to
the committed frontend/openapi.json. If they differ, someone changed a route or DTO
without running `make openapi` — the UI would silently drift from the API. Fix by
running `python tools/dump_openapi.py` and committing the result.

Exit 0 = in sync; exit 1 = drift (prints a unified diff).
"""

from __future__ import annotations

import difflib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dump_openapi import _canonical, build_schema  # noqa: E402  (sibling script, not a package)

_COMMITTED = Path(__file__).resolve().parent.parent / "frontend" / "openapi.json"


def main() -> int:
    generated = _canonical(build_schema())
    if not _COMMITTED.exists():
        sys.stderr.write("frontend/openapi.json is missing; run: python tools/dump_openapi.py\n")
        return 1
    committed = _COMMITTED.read_text()
    if committed == generated:
        sys.stderr.write("OpenAPI schema in sync\n")
        return 0
    diff = difflib.unified_diff(
        committed.splitlines(keepends=True),
        generated.splitlines(keepends=True),
        fromfile="frontend/openapi.json (committed)",
        tofile="regenerated",
    )
    sys.stderr.write("OpenAPI drift detected; run: python tools/dump_openapi.py && commit\n\n")
    sys.stderr.writelines(diff)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
