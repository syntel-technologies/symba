"""Promotion never rebuilds images or changes aliases before validating sources."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import promote_latest


class PromoteLatestTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.inventory = Path(self.directory.name) / "inventory.json"
        self.manifest = {
            "version": "v0.1.0",
            "images": [
                {
                    "image": f"ghcr.io/syntel-technologies/{name}",
                    "dockerhub_image": f"docker.io/synteltechnologies/{name}",
                    "tag": "v0.1.0",
                    "digest": "sha256:" + "a" * 64,
                }
                for name in ("symba", "symba-frontend", "symba-flyway")
            ],
        }
        self.addCleanup(patch.stopall)
        patch("sys.argv", ["promote_latest", str(self.inventory)]).start()
        patch.dict("os.environ", {"DOCKERHUB_NAMESPACE": "synteltechnologies", "HUB_ONLY": "false"}).start()
        self.inspect = patch.object(promote_latest, "inspect", return_value=b"verified-manifest").start()
        self.docker_run = patch.object(promote_latest.subprocess, "run").start()

    def promote(self):
        self.inventory.write_text(json.dumps(self.manifest))
        promote_latest.main()

    def test_promotes_all_verified_digests_in_both_registries(self):
        self.promote()
        self.assertEqual(self.docker_run.call_count, 6)
        for call in self.docker_run.call_args_list:
            arguments = call.args[0]
            self.assertEqual(arguments[:5], ["docker", "buildx", "imagetools", "create", "--tag"])
            self.assertTrue(arguments[5].endswith(":latest"))
            self.assertTrue(arguments[6].endswith("@sha256:" + "a" * 64))

    def test_backfill_only_promotes_docker_hub(self):
        with patch.dict("os.environ", {"HUB_ONLY": "true"}):
            self.promote()
        self.assertEqual(self.docker_run.call_count, 3)
        for call in self.docker_run.call_args_list:
            self.assertTrue(call.args[0][5].startswith("docker.io/synteltechnologies/"))

    def test_rejects_missing_image_before_moving_any_alias(self):
        self.manifest["images"].pop()
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            self.promote()
        self.docker_run.assert_not_called()

    def test_rejects_changed_version_tag_before_moving_any_alias(self):
        self.inspect.side_effect = lambda reference: b"digest" if "@" in reference else b"changed"
        with self.assertRaisesRegex(ValueError, "Version tag"):
            self.promote()
        self.docker_run.assert_not_called()

    def test_rejects_unexpected_destination_before_moving_any_alias(self):
        self.manifest["images"][-1]["dockerhub_image"] = "docker.io/other/symba-flyway"
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            self.promote()
        self.docker_run.assert_not_called()

    def test_verifies_alias_after_publication(self):
        self.inspect.side_effect = lambda reference: b"wrong" if reference.endswith(":latest") else b"verified"
        with self.assertRaisesRegex(ValueError, "Latest manifest mismatch"):
            self.promote()


if __name__ == "__main__":
    unittest.main()
