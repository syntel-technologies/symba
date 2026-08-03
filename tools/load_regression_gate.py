"""Load-throughput regression gate.

After a nightly k6 run overwrites tests/load/baseline/submit_throughput.summary.json
with fresh numbers, this compares the fresh submit throughput (`http_reqs.rate`)
against the PREVIOUS committed baseline (read from git HEAD) and fails the run on a
regression worse than the allowed fraction (default 20% -> current must be >= 0.8x).

First-run semantics: if the committed baseline is a placeholder (`placeholder: true`)
or unreadable, we RECORD (pass) rather than compare — there is nothing meaningful to
diff against yet. The freshly written summary should then be committed to seed the
baseline.

Exit 0 = pass/record, exit 1 = regression (or a hard error reading the fresh run).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
BASELINE = ROOT / "tests" / "load" / "baseline" / "submit_throughput.summary.json"
BASELINE_REPO_PATH = "tests/load/baseline/submit_throughput.summary.json"
MIN_RATIO = 0.80  # >20% drop fails


def _rate(summary: dict[str, Any]) -> float | None:
    try:
        return float(summary["metrics"]["http_reqs"]["values"]["rate"])
    except (KeyError, TypeError, ValueError):
        return None


def _committed_baseline() -> dict[str, Any] | None:
    """The baseline as it was BEFORE this run overwrote the file (git HEAD)."""
    try:
        raw = subprocess.check_output(
            ["git", "show", f"HEAD:{BASELINE_REPO_PATH}"], cwd=ROOT, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def main() -> int:
    if not BASELINE.exists():
        print("load gate: no fresh summary found (did k6 run?)", file=sys.stderr)
        return 1

    fresh = json.loads(BASELINE.read_text())
    fresh_rate = _rate(fresh)
    if fresh_rate is None or fresh.get("placeholder"):
        print("load gate: fresh run produced no http_reqs.rate — nothing to gate")
        return 1

    prior = _committed_baseline()
    if prior is None or prior.get("placeholder"):
        print(f"load gate: RECORD (no prior baseline). submit rate={fresh_rate:.1f}/s — commit to seed baseline.")
        return 0

    prior_rate = _rate(prior)
    if prior_rate is None or prior_rate == 0:
        print(f"load gate: RECORD (prior baseline had no rate). submit rate={fresh_rate:.1f}/s")
        return 0

    ratio = fresh_rate / prior_rate
    if ratio < MIN_RATIO:
        print(
            f"load gate FAILED: submit throughput regressed {(1 - ratio) * 100:.0f}% "
            f"({prior_rate:.1f}/s -> {fresh_rate:.1f}/s; floor is {MIN_RATIO:.0%} of baseline)"
        )
        return 1

    print(f"load gate OK: submit throughput {fresh_rate:.1f}/s vs baseline {prior_rate:.1f}/s ({ratio:.2f}x)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
