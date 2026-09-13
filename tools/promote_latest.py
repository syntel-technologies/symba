"""Promote a validated, current stable release after its smoke checks pass."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

from tools.mirror_dockerhub import inspect


def main() -> None:
    manifest = json.loads(Path(sys.argv[1]).read_text())
    version = manifest["version"]
    if not re.fullmatch(r"v\d+\.\d+\.\d+", version):
        raise ValueError("Stable version required")
    namespace = os.environ.get("DOCKERHUB_NAMESPACE", "")
    if namespace and not re.fullmatch(r"[a-z0-9]{4,30}", namespace):
        raise ValueError("Invalid Docker Hub namespace")
    expected = {"symba", "symba-frontend", "symba-flyway"}
    plan = []
    for item in manifest["images"]:
        name = item["image"].rsplit("/", 1)[-1]
        if name not in expected or item["image"] != f"ghcr.io/syntel-technologies/{name}":
            raise ValueError("Invalid or duplicate source image")
        expected.remove(name)
        if item["tag"] != version or not re.fullmatch(r"sha256:[0-9a-f]{64}", item["digest"]):
            raise ValueError("Invalid image identity")
        repositories = [] if os.environ.get("HUB_ONLY") == "true" else [item["image"]]
        if namespace:
            hub = f"docker.io/{namespace}/{name}"
            if item.get("dockerhub_image") != hub:
                raise ValueError("Docker Hub inventory mismatch")
            repositories.append(hub)
        for repository in repositories:
            source = f"{repository}@{item['digest']}"
            raw = inspect(source)
            if raw != inspect(f"{repository}:{version}"):
                raise ValueError("Version tag no longer matches the verified digest")
            plan.append((source, f"{repository}:latest", raw))
    if expected or not plan:
        raise ValueError("Incomplete release inventory")
    # Validate every source before moving any alias. A retry repairs partial
    # registry failures; version tags and digest-pinned assets never change.
    for source, target, raw in plan:
        subprocess.run(["docker", "buildx", "imagetools", "create", "--tag", target, source], check=True)
        if inspect(target) != raw:
            raise ValueError(f"Latest manifest mismatch: {target}")
        print(f"Verified {target} -> {source}")


if __name__ == "__main__":
    main()
