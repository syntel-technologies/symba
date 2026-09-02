"""Behavioral tests for fail-closed production admission scripts."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FINAL_MANIFEST = "a" * 64


def _run_shell(
    script: Path,
    *,
    environment: dict[str, str],
    arguments: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sh", str(script), *arguments],
        cwd=ROOT,
        env={"PATH": os.environ.get("PATH", ""), **environment},
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )


def _write_provenance(directory: Path, *, manifest: str = FINAL_MANIFEST) -> None:
    directory.mkdir()
    (directory / "release-id").write_text("release-2026-09-02\n", encoding="utf-8")
    (directory / "release-manifest-sha256").write_text(
        f"{manifest}\n", encoding="utf-8"
    )


def _postgres_environment(pgdata: Path | None = None) -> dict[str, str]:
    environment = {
        "POSTGRES_USER": "postgres",
        "POSTGRES_DB": "postgres",
        "POSTGRES_INITDB_ARGS": "--auth-host=scram-sha-256 --auth-local=peer",
        "POSTGRES_HOST_AUTH_METHOD": "scram-sha-256",
        "POSTGRES_PASSWORD": "admin-password-01234567890123456789",
        "SYMBA_POSTGRES_MIGRATION_PASSWORD": (
            "migration-password-0123456789012345"
        ),
        "SYMBA_POSTGRES_RUNTIME_PASSWORD": "runtime-password-012345678901234567",
    }
    if pgdata is not None:
        environment["PGDATA"] = str(pgdata)
    return environment


def test_release_verifier_accepts_exact_image_and_desired_identity(
    tmp_path: Path,
) -> None:
    provenance = tmp_path / "provenance"
    _write_provenance(provenance)
    environment = {
        "KNOR_RELEASE_ID": "release-2026-09-02",
        "KNOR_RELEASE_MANIFEST_SHA256": FINAL_MANIFEST,
        "SYMBA_RELEASE_REQUIRE_FINAL": "true",
    }

    completed = _run_shell(
        ROOT / "scripts" / "release" / "verify_provenance.sh",
        environment=environment,
        arguments=(str(provenance),),
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == ""
    assert completed.stderr == ""


def test_release_verifier_rejects_mismatch_sentinel_and_symlink(
    tmp_path: Path,
) -> None:
    script = ROOT / "scripts" / "release" / "verify_provenance.sh"
    provenance = tmp_path / "provenance"
    _write_provenance(provenance)
    environment = {
        "KNOR_RELEASE_ID": "release-2026-09-02",
        "KNOR_RELEASE_MANIFEST_SHA256": "b" * 64,
        "SYMBA_RELEASE_REQUIRE_FINAL": "true",
    }
    mismatch = _run_shell(
        script,
        environment=environment,
        arguments=(str(provenance),),
    )
    assert mismatch.returncode == 78
    assert "manifest_sha256_mismatch" in mismatch.stderr
    assert FINAL_MANIFEST not in mismatch.stderr

    unreleased = tmp_path / "unreleased"
    _write_provenance(unreleased, manifest="unreleased")
    sentinel = _run_shell(
        script,
        environment={
            **environment,
            "KNOR_RELEASE_MANIFEST_SHA256": "unreleased",
        },
        arguments=(str(unreleased),),
    )
    assert sentinel.returncode == 78
    assert "final_image_provenance_required" in sentinel.stderr

    symlinked = tmp_path / "symlinked"
    _write_provenance(symlinked)
    (symlinked / "release-id").unlink()
    (symlinked / "release-id").symlink_to(provenance / "release-id")
    symlink = _run_shell(
        script,
        environment={**environment, "KNOR_RELEASE_MANIFEST_SHA256": FINAL_MANIFEST},
        arguments=(str(symlinked),),
    )
    assert symlink.returncode == 78
    assert "image_provenance_symlink" in symlink.stderr


def test_postgres_contract_rejects_wrong_identity_before_database_start(
    tmp_path: Path,
) -> None:
    script = ROOT / "scripts" / "postgres" / "assert_contract.sh"
    valid = _run_shell(script, environment=_postgres_environment(tmp_path))
    assert valid.returncode == 0, valid.stderr

    for variable, invalid, reason in (
        ("POSTGRES_USER", "symba", "bootstrap_user_invalid"),
        ("POSTGRES_DB", "symba", "bootstrap_database_invalid"),
        ("POSTGRES_INITDB_ARGS", "--auth-host=trust", "initdb_auth_contract_invalid"),
        ("POSTGRES_HOST_AUTH_METHOD", "trust", "host_auth_contract_invalid"),
    ):
        environment = _postgres_environment(tmp_path)
        environment[variable] = invalid
        completed = _run_shell(script, environment=environment)
        assert completed.returncode == 78
        assert reason in completed.stderr


def test_hba_hardener_atomically_prefixes_exact_bootstrap_rejects(
    tmp_path: Path,
) -> None:
    pgdata = tmp_path / "pgdata"
    pgdata.mkdir()
    hba = pgdata / "pg_hba.conf"
    original = (
        "local all all peer\n"
        "local replication all peer\n"
        "host all all 127.0.0.1/32 scram-sha-256\n"
        "host all all ::1/128 scram-sha-256\n"
        "host all all all scram-sha-256\n"
    )
    hba.write_text(original, encoding="utf-8")
    hba.chmod(0o640)

    completed = _run_shell(
        ROOT / "scripts" / "postgres" / "harden_hba.sh",
        environment=_postgres_environment(pgdata),
    )

    assert completed.returncode == 0, completed.stderr
    assert hba.read_text(encoding="utf-8") == (
        "host replication postgres all reject\n"
        "host all postgres all reject\n"
        f"{original}"
    )
    assert stat.S_IMODE(hba.stat().st_mode) == 0o640
    assert list(pgdata.glob("pg_hba.conf.symba.*")) == []


def test_hba_hardener_failure_preserves_original_policy(tmp_path: Path) -> None:
    pgdata = tmp_path / "pgdata"
    pgdata.mkdir()
    hba = pgdata / "pg_hba.conf"
    original = "local all all peer\nhost all all all trust\n"
    hba.write_text(original, encoding="utf-8")

    invalid_auth = _run_shell(
        ROOT / "scripts" / "postgres" / "harden_hba.sh",
        environment=_postgres_environment(pgdata),
    )
    assert invalid_auth.returncode == 78
    assert "generic_scram_rule_invalid" in invalid_auth.stderr
    assert hba.read_text(encoding="utf-8") == original


def test_all_production_shell_entrypoints_parse() -> None:
    scripts = (
        ROOT / "scripts" / "release" / "verify_provenance.sh",
        ROOT / "scripts" / "engine" / "entrypoint.sh",
        ROOT / "scripts" / "flyway" / "entrypoint.sh",
        ROOT / "scripts" / "postgres" / "assert_contract.sh",
        ROOT / "scripts" / "postgres" / "harden_hba.sh",
        ROOT / "scripts" / "postgres" / "entrypoint.sh",
        ROOT / "scripts" / "postgres" / "init_roles.sh",
        ROOT / "scripts" / "tls" / "preflight.sh",
        ROOT / "frontend" / "release-entrypoint.sh",
    )
    completed = subprocess.run(
        ["sh", "-n", *(str(script) for script in scripts)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
