"""Exercise the prebuilt local quickstart without installing Python dependencies."""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def main() -> None:
    base = f"http://127.0.0.1:{os.environ.get('UI_HOST_PORT', '8080')}"
    with urlopen(base, timeout=10) as response:
        assert response.status == 200
        assert b"<html" in response.read().lower()

    payload = json.dumps(
        {"tenant": "default", "specs": [{"task_name": "demo.echo", "payload": {"hello": "docker"}}]}
    ).encode()
    request = Request(f"{base}/v1/jobs", data=payload, headers={"Content-Type": "application/json"})
    try:
        urlopen(request, timeout=10)
    except HTTPError as exc:
        assert exc.code == 401, f"Unexpected unauthenticated status: {exc.code}"
    else:
        raise AssertionError("Unauthenticated submission was accepted")

    request.add_header("Authorization", "Bearer symba-local-dev-token")
    with urlopen(request, timeout=10) as response:
        assert response.status in (200, 201, 202)
        result = json.load(response)
        assert len(result["job_ids"]) == 1, f"Unexpected submission response: {result}"
    request = Request(
        f"{base}/v1/jobs/{result['job_ids'][0]}",
        headers={"Authorization": "Bearer symba-local-dev-token"},
    )
    with urlopen(request, timeout=10) as response:
        job = json.load(response)
        assert job["task_name"] == "demo.echo", job
        assert job["tenant"] == "default", job
    print("Quickstart smoke passed: console, authentication, submission, and persisted job lookup")


if __name__ == "__main__":
    main()
