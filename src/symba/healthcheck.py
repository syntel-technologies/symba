"""Container HEALTHCHECK entrypoint.

Exits 0 when the engine's HTTP health endpoint reports ready, non-zero otherwise.
Kept dependency-light (stdlib urllib) so it works in the lean runtime image.
"""

from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    port = os.environ.get("SYMBA_SERVER__HTTP_PORT", "7300")
    url = f"http://127.0.0.1:{port}/healthz"
    try:
        with urllib.request.urlopen(url, timeout=2.5) as resp:  # noqa: S310 - loopback only
            return 0 if resp.status == 200 else 1
    except (urllib.error.URLError, TimeoutError, OSError):
        return 1


if __name__ == "__main__":
    sys.exit(main())
