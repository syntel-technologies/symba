"""Structural import gate.

Two rules, enforced in CI:
  1. src/symba/core/ imports nothing but stdlib + pydantic. No asyncpg, redis,
     grpc, fastapi, or any symba.services/db/transport module.
  2. `import logging` appears only in src/symba/observability/logging.py.

Exit non-zero (naming the offending file + line) on any violation.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src" / "symba"

_CORE_ALLOWED_TOP = {"pydantic", "symba"}  # symba only for symba.core.* siblings
_CORE_BANNED = {"asyncpg", "redis", "grpc", "fastapi", "uvicorn"}


def _module_top(name: str) -> str:
    return name.split(".", 1)[0]


def _check_core(path: Path, tree: ast.Module) -> list[str]:
    errors: list[str] = []
    for node in ast.walk(tree):
        names: list[tuple[str, int]] = []
        if isinstance(node, ast.Import):
            names = [(alias.name, node.lineno) for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [(node.module, node.lineno)]
        for name, lineno in names:
            top = _module_top(name)
            if top in _CORE_BANNED:
                errors.append(f"{path}:{lineno}: core/ may not import {name!r}")
            if name.startswith(("symba.services", "symba.db", "symba.transport")):
                errors.append(f"{path}:{lineno}: core/ may not import {name!r} (I/O layer)")
    return errors


def _check_logging(path: Path, tree: ast.Module) -> list[str]:
    allowed = SRC / "observability" / "logging.py"
    if path == allowed:
        return []
    errors: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _module_top(alias.name) == "logging":
                    errors.append(
                        f"{path}:{node.lineno}: 'import logging' forbidden outside "
                        f"observability/logging.py (import the shared `logger`)"
                    )
        elif isinstance(node, ast.ImportFrom) and node.module and _module_top(node.module) == "logging":
            errors.append(f"{path}:{node.lineno}: 'from logging import' forbidden outside observability/logging.py")
    return errors


def main() -> int:
    errors: list[str] = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        if "core" in path.relative_to(SRC).parts:
            errors.extend(_check_core(path, tree))
        errors.extend(_check_logging(path, tree))

    if errors:
        print("Import gate FAILED:")
        for err in errors:
            print(f"  {err}")
        return 1
    print("Import gate OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
