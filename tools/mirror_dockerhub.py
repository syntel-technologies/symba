"""Copy a verified release inventory to Docker Hub without rebuilding images.

The caller must validate release checks and the inventory checksum first. The
backfill workflow does that before giving this script registry credentials.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def inspect(reference: str, *, allow_missing: bool = False) -> bytes | None:
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", "--raw", reference], capture_output=True, check=False
    )
    if result.returncode:
        error = result.stderr.decode()
        if allow_missing and ("not found" in error.lower() or "manifest unknown" in error.lower()):
            return None
        raise RuntimeError(f"Cannot inspect {reference}: {error}")
    return result.stdout


def main() -> None:
    namespace = os.environ["DOCKERHUB_NAMESPACE"]
    if not re.fullmatch(r"[a-z0-9]{4,30}", namespace):
        raise ValueError("Invalid Docker Hub namespace")
    manifest = json.loads(Path(sys.argv[1]).read_text())
    tag = manifest["version"]
    if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
        raise ValueError("A stable release version is required")
    expected = {"symba", "symba-frontend", "symba-flyway"}
    plan = []
    for image in manifest["images"]:
        name = image["image"].rsplit("/", 1)[-1]
        if name not in expected or image["image"] != f"ghcr.io/syntel-technologies/{name}":
            raise ValueError("Invalid or duplicate source image")
        expected.remove(name)
        if image["tag"] != tag or not re.fullmatch(r"sha256:[0-9a-f]{64}", image["digest"]):
            raise ValueError("Invalid source tag or digest")
        source = f"{image['image']}@{image['digest']}"
        target = f"docker.io/{namespace}/{name}:{tag}"
        raw = inspect(source)
        assert raw is not None
        platforms = {
            f"{item['platform']['os']}/{item['platform']['architecture']}" for item in json.loads(raw)["manifests"]
        }
        if not {"linux/amd64", "linux/arm64"}.issubset(platforms):
            raise ValueError(f"Missing release platforms: {source}")
        existing = inspect(target, allow_missing=True)
        if existing is not None and json.loads(existing) != json.loads(raw):
            raise ValueError(f"Refusing to replace a different published image: {target}")
        plan.append((image, source, target, raw, existing))
    if expected:
        raise ValueError(f"Missing release images: {sorted(expected)}")

    compose = Path("compose.quickstart.yml").read_text()
    for image, source, target, raw, existing in plan:
        if existing is None:
            subprocess.run(["docker", "buildx", "imagetools", "create", "--tag", target, source], check=True)
        mirrored = inspect(target)
        # Looking up the destination by the ORIGINAL digest proves byte identity.
        digest_reference = f"{target.rsplit(':', 1)[0]}@{image['digest']}"
        if inspect(digest_reference) != raw or mirrored != raw:
            raise ValueError(f"Mirrored digest or manifest mismatch: {target}")
        image["dockerhub_image"] = target.rsplit(":", 1)[0]
        name = image["image"].rsplit("/", 1)[-1]
        template = "${SYMBA_IMAGE_PREFIX:?Set SYMBA_IMAGE_PREFIX}/" + name + ":${SYMBA_VERSION:?Set SYMBA_VERSION}"
        if template not in compose:
            raise ValueError(f"Missing quickstart image: {name}")
        compose = compose.replace(template, digest_reference)
        print(f"Verified {target} at {image['digest']}")
    compose = compose.replace("${SYMBA_VERSION:?Set SYMBA_VERSION}", tag)
    output = Path("dockerhub-assets")
    output.mkdir(exist_ok=True)
    (output / "compose.quickstart.yml").write_text(compose)
    (output / "images-dockerhub.json").write_text(json.dumps(manifest, indent=2) + "\n")
    sums = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in sorted(output.iterdir())
        if path.name != "DOCKERHUB-SHA256SUMS"
    )
    (output / "DOCKERHUB-SHA256SUMS").write_text(sums)


if __name__ == "__main__":
    main()
