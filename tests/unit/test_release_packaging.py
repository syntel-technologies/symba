"""Static release contracts for deterministic production artifacts."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")


def _assert_digest_pinned_dockerfile(path: Path) -> str:
    source = path.read_text(encoding="utf-8")
    assert DIGEST.search(source.splitlines()[0])
    for line in source.splitlines():
        if line.startswith("FROM "):
            assert DIGEST.search(line.split()[1]), line
        if line.startswith("COPY --from="):
            image = line.removeprefix("COPY --from=").split()[0]
            if "/" in image:
                assert DIGEST.search(image), line
    return source


def _compose_service_block(source: str, service_name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service_name)}:\n(?P<body>.*?)(?=^  [a-z0-9-]+:\n|\Z)",
        source,
    )
    assert match is not None
    return match.group("body")


def test_engine_image_uses_only_locked_python_resolution() -> None:
    dockerfile = _assert_digest_pinned_dockerfile(ROOT / "Dockerfile")

    assert "COPY pyproject.toml uv.lock ./" in dockerfile
    sync_commands = [
        line.strip() for line in dockerfile.splitlines() if "uv sync" in line and not line.lstrip().startswith("#")
    ]
    assert len(sync_commands) == 3
    assert all("--frozen" in command for command in sync_commands)
    assert any("--group codegen" in command for command in sync_commands)
    assert "uv pip install" not in dockerfile
    assert "uv pip uninstall" not in dockerfile
    assert 'ENTRYPOINT ["/usr/local/bin/symba-engine-entrypoint"]' in dockerfile
    assert "scripts/release/verify_provenance.sh" in dockerfile
    assert "scripts/engine/entrypoint.sh" in dockerfile

    runtime_stage = dockerfile.rsplit("FROM ", maxsplit=1)[-1]
    assert "uv sync" not in runtime_stage
    assert "uv run" not in runtime_stage
    assert "COPY --from=build --chown=symba:symba /app /app" in runtime_stage

    manifest = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert manifest["dependency-groups"]["codegen"] == ["grpcio-tools==1.82.1"]
    assert manifest["build-system"]["requires"] == ["hatchling==1.32.0"]


def test_frontend_image_uses_committed_npm_lock() -> None:
    dockerfile = _assert_digest_pinned_dockerfile(ROOT / "frontend" / "Dockerfile")

    assert "COPY package.json package-lock.json ./" in dockerfile
    assert "RUN npm ci --ignore-scripts" in dockerfile
    assert "npm install" not in dockerfile
    lock = json.loads((ROOT / "frontend" / "package-lock.json").read_text(encoding="utf-8"))
    assert lock["lockfileVersion"] == 3

    assert "ENV NGINX_LISTEN_PORT=8080" in dockerfile
    assert "chown -R 101:101 /etc/nginx/conf.d /var/cache/nginx /run" in dockerfile
    assert "USER 101:101" in dockerfile
    assert "EXPOSE 8080" in dockerfile
    assert 'ENTRYPOINT ["/usr/local/bin/symba-frontend-entrypoint"]' in dockerfile
    assert 'CMD ["nginx", "-g", "daemon off;"]' in dockerfile
    assert "release-provenance.json" in dockerfile
    nginx_template = (ROOT / "frontend" / "nginx.conf").read_text(encoding="utf-8")
    assert "listen ${NGINX_LISTEN_PORT};" in nginx_template
    assert "location = /release-provenance.json" in nginx_template
    assert 'Cache-Control "no-store" always' in nginx_template
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert '${UI_HOST_PORT:-8080}:8080"' in compose
    assert '${UI_HOST_PORT:-8080}:80"' not in compose


def test_built_images_expose_exact_coordinated_release_provenance() -> None:
    fragments = (
        "ARG KNOR_RELEASE_ID=development",
        "ARG KNOR_RELEASE_MANIFEST_SHA256=unreleased",
        'org.opencontainers.image.version="${KNOR_RELEASE_ID}"',
        'org.opencontainers.image.revision="${KNOR_RELEASE_MANIFEST_SHA256}"',
        'com.amplior.release.id="${KNOR_RELEASE_ID}"',
        'com.amplior.release.manifest-sha256="${KNOR_RELEASE_MANIFEST_SHA256}"',
        'ENV KNOR_IMAGE_RELEASE_ID="${KNOR_RELEASE_ID}"',
        'KNOR_IMAGE_RELEASE_MANIFEST_SHA256="${KNOR_RELEASE_MANIFEST_SHA256}"',
    )
    for path in (
        ROOT / "Dockerfile",
        ROOT / "Dockerfile.flyway",
        ROOT / "Dockerfile.postgres",
        ROOT / "Dockerfile.tls-preflight",
        ROOT / "frontend" / "Dockerfile",
    ):
        source = path.read_text(encoding="utf-8")
        for fragment in fragments:
            assert source.count(fragment) == 1, f"{path}: {fragment}"


def test_compose_wires_release_inputs_and_production_requires_them() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    for service_name in ("symba-flyway", "symba-engine", "symba-frontend"):
        service = _compose_service_block(compose, service_name)
        assert "KNOR_RELEASE_ID: ${KNOR_RELEASE_ID:-development}" in service
        assert (
            "KNOR_RELEASE_MANIFEST_SHA256: "
            "${KNOR_RELEASE_MANIFEST_SHA256:-unreleased}"
        ) in service

    production = (ROOT / "docker-compose.production.yml").read_text(encoding="utf-8")
    for service_name in (
        "symba-postgres",
        "symba-flyway",
        "symba-runtime-preflight",
        "symba-engine",
        "symba-frontend",
    ):
        service = _compose_service_block(production, service_name)
        assert "KNOR_RELEASE_ID must be set for production" in service
        assert "KNOR_RELEASE_MANIFEST_SHA256 must be set for production" in service
        assert 'SYMBA_RELEASE_REQUIRE_FINAL: "true"' in service


def test_compose_runtime_images_are_digest_pinned() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    image_lines = [
        line.strip().split("#", maxsplit=1)[0].rstrip()
        for line in compose.splitlines()
        if line.strip().startswith("image:")
    ]
    assert len(image_lines) == 3
    assert "image: symba-flyway:${KNOR_RELEASE_MANIFEST_SHA256:-unreleased}" in image_lines
    for line in image_lines:
        if line.startswith("image: symba-flyway:"):
            continue
        assert DIGEST.search(line.removeprefix("image: ")), line


def test_production_overlay_requires_auth_database_and_tls_secrets() -> None:
    production = (ROOT / "docker-compose.production.yml").read_text(encoding="utf-8")

    assert "SYMBA_POSTGRES_ADMIN_PASSWORD must be set for production" in production
    assert "SYMBA_POSTGRES_MIGRATION_PASSWORD must be set for production" in production
    assert "SYMBA_POSTGRES_RUNTIME_PASSWORD must be set for production" in production
    assert "SYMBA_DEV_TOKEN must be set for production" in production
    assert "SYMBA_REDIS_PASSWORD must be set for production" in production
    assert "SYMBA_AUTH__MODE: token" in production
    assert 'SYMBA_SERVER__GRPC_TLS_ENABLED: "true"' in production
    assert "SYMBA_GRPC_TLS_CERT_FILE_HOST must be set" in production
    assert "SYMBA_GRPC_TLS_KEY_FILE_HOST must be set" in production
    assert "SYMBA_GRPC_TLS_CA_FILE_HOST must be set" in production
    assert "SYMBA_GRPC_TLS_EXPECTED_DNS_NAME must be set for production" in production
    assert "SYMBA_GRPC_TLS_EXPECTED_IP must be set for production" in production
    assert "SYMBA_GRPC_TLS_EXPECTED_CERT_SHA256 must be set for production" in production
    assert "SYMBA_GRPC_TLS_EXPECTED_CA_SHA256 must be set for production" in production
    assert "SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS must be set for production" in production
    suffix = (
        "${KNOR_RELEASE_MANIFEST_SHA256:?"
        "KNOR_RELEASE_MANIFEST_SHA256 must be set for production}"
    )
    assert f"image: symba-postgres:{suffix}" in production
    assert f"image: symba-flyway:{suffix}" in production
    assert f"image: symba-tls-preflight:{suffix}" in production
    assert f"image: symba-engine:{suffix}" in production
    assert f"image: symba-frontend:{suffix}" in production


def test_production_overlay_requires_authenticated_ephemeral_redis() -> None:
    production = (ROOT / "docker-compose.production.yml").read_text(encoding="utf-8")
    redis = _compose_service_block(production, "symba-redis")
    assert "--requirepass" in redis
    assert "--appendonly no" in redis
    assert "--save ''" in redis
    assert 'REDISCLI_AUTH="$$REDIS_PASSWORD"' in redis

    preflight_script = (ROOT / "scripts" / "tls" / "preflight.sh").read_text(
        encoding="utf-8"
    )
    assert '${#SYMBA_REDIS_PASSWORD}' in preflight_script
    assert '${#SYMBA_DEV_TOKEN}' in preflight_script
    assert '${#SYMBA_POSTGRES_ADMIN_PASSWORD}' in preflight_script
    assert '${#SYMBA_POSTGRES_MIGRATION_PASSWORD}' in preflight_script
    assert '${#SYMBA_POSTGRES_RUNTIME_PASSWORD}' in preflight_script
    assert "*[!A-Za-z0-9_-]*" in preflight_script
    assert '"$SYMBA_REDIS_PASSWORD" != "$SYMBA_DEV_TOKEN"' in preflight_script

    preflight = _compose_service_block(production, "symba-runtime-preflight")
    assert "dockerfile: Dockerfile.tls-preflight" in preflight
    assert "SYMBA_REDIS_PASSWORD:" in preflight
    assert "SYMBA_DEV_TOKEN:" in preflight
    assert "SYMBA_POSTGRES_ADMIN_PASSWORD:" in preflight
    assert "SYMBA_POSTGRES_MIGRATION_PASSWORD:" in preflight
    assert "SYMBA_POSTGRES_RUNTIME_PASSWORD:" in preflight

    assert "symba-runtime-preflight:" in redis
    assert "condition: service_completed_successfully" in redis

    engine = _compose_service_block(production, "symba-engine")
    assert "SYMBA_REDIS__URL: redis://:${SYMBA_REDIS_PASSWORD:" in engine


def test_production_postgres_separates_admin_migration_and_runtime_roles() -> None:
    dockerfile = _assert_digest_pinned_dockerfile(ROOT / "Dockerfile.postgres")
    assert "scripts/postgres/init_roles.sh" in dockerfile
    assert "scripts/postgres/entrypoint.sh" in dockerfile
    assert 'ENTRYPOINT ["/usr/local/bin/symba-postgres-entrypoint"]' in dockerfile
    assert 'CMD ["postgres"]' in dockerfile

    entrypoint = (ROOT / "scripts" / "postgres" / "entrypoint.sh").read_text(
        encoding="utf-8"
    )
    assert "/usr/local/bin/symba-verify-release" in entrypoint
    assert "/usr/local/bin/symba-assert-postgres-contract" in entrypoint
    assert 'exec /usr/local/bin/docker-entrypoint.sh "$@"' in entrypoint

    contract = (ROOT / "scripts" / "postgres" / "assert_contract.sh").read_text(
        encoding="utf-8"
    )
    assert "database_secret_too_short" in contract
    assert "database_secrets_not_independent" in contract
    assert '"${POSTGRES_USER:-}" = "postgres"' in contract
    assert '"${POSTGRES_DB:-}" = "postgres"' in contract
    assert "--auth-host=scram-sha-256 --auth-local=peer" in contract
    assert '"${POSTGRES_HOST_AUTH_METHOD:-}" = "scram-sha-256"' in contract

    init = (ROOT / "scripts" / "postgres" / "init_roles.sh").read_text(
        encoding="utf-8"
    )
    for role in ("symba_migrator", "symba_runtime"):
        assert role in init
    assert "/usr/local/bin/symba-assert-postgres-contract" in init
    assert "/usr/local/bin/symba-harden-pg-hba" in init
    assert init.count("NOSUPERUSER") >= 4
    assert init.count("NOCREATEDB") >= 4
    assert init.count("NOCREATEROLE") >= 4
    assert init.count("NOREPLICATION") >= 4
    assert init.count("NOBYPASSRLS") >= 4
    assert "ALTER DEFAULT PRIVILEGES FOR ROLE symba_migrator" in init
    assert "REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC" in init
    assert "REVOKE USAGE ON TYPES FROM PUBLIC" in init
    assert "GRANT EXECUTE ON FUNCTIONS TO symba_runtime" in init
    assert "GRANT USAGE ON TYPES TO symba_runtime" in init
    assert "GRANT USAGE ON SCHEMA symba TO symba_runtime" in init
    assert "REVOKE ALL ON DATABASE symba FROM PUBLIC" in init
    assert "REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC" in init
    assert "REVOKE CONNECT, TEMPORARY ON DATABASE template1 FROM PUBLIC" in init
    assert "\\getenv migration_password" in init
    assert "\\getenv runtime_password" in init

    hba = (ROOT / "scripts" / "postgres" / "harden_hba.sh").read_text(
        encoding="utf-8"
    )
    assert "host all postgres all reject" in hba
    assert "host replication postgres all reject" in hba
    assert "generic_scram_rule_invalid" in hba
    assert "local_peer_rule_invalid" in hba
    assert "local_replication_peer_rule_invalid" in hba
    assert "loopback_scram_rules_invalid" in hba
    assert "mktemp \"$PGDATA/pg_hba.conf.symba.XXXXXX\"" in hba
    assert 'mv -f "$hba_tmp" "$hba"' in hba
    assert "sed -i" not in hba

    production = (ROOT / "docker-compose.production.yml").read_text(encoding="utf-8")
    postgres = _compose_service_block(production, "symba-postgres")
    flyway = _compose_service_block(production, "symba-flyway")
    engine = _compose_service_block(production, "symba-engine")
    assert "dockerfile: Dockerfile.postgres" in postgres
    assert "POSTGRES_USER: postgres" in postgres
    assert "POSTGRES_INITDB_ARGS: --auth-host=scram-sha-256 --auth-local=peer" in postgres
    assert "POSTGRES_HOST_AUTH_METHOD: scram-sha-256" in postgres
    assert "exec gosu postgres pg_isready -U postgres -d postgres" in postgres
    assert "FLYWAY_USER: symba_migrator" in flyway
    assert "SYMBA_POSTGRES__USER: symba_runtime" in engine
    assert "symba-runtime-preflight:" in postgres
    assert "condition: service_completed_successfully" in postgres
    assert "symba-runtime-preflight:" in flyway
    assert "condition: service_completed_successfully" in flyway
    assert "synchronous_commit=off" not in production


def test_flyway_migrations_are_baked_into_the_release_image() -> None:
    dockerfile = _assert_digest_pinned_dockerfile(ROOT / "Dockerfile.flyway")
    assert "flyway/flyway:12.0.1@sha256:" in dockerfile
    assert "COPY flyway.toml /opt/symba/flyway/flyway.toml" in dockerfile
    assert "COPY database/symba/ /opt/symba/flyway/sql/symba/" in dockerfile
    assert "find /opt/symba/flyway -type d -exec chmod 0555" in dockerfile
    assert "find /opt/symba/flyway -type f -exec chmod 0444" in dockerfile
    assert "scripts/release/verify_provenance.sh" in dockerfile
    assert "scripts/flyway/entrypoint.sh" in dockerfile

    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    flyway = _compose_service_block(compose, "symba-flyway")
    assert "dockerfile: Dockerfile.flyway" in flyway
    assert "volumes:" not in flyway
    assert "./flyway.toml" not in compose
    assert "./database/symba" not in compose
    assert 'FLYWAY_CONFIG_FILES: "/opt/symba/flyway/flyway.toml"' in flyway
    assert "filesystem:/opt/symba/flyway/sql/symba" in flyway


def test_production_overlay_statically_locks_runtime_identities_and_filesystems() -> None:
    production = (ROOT / "docker-compose.production.yml").read_text(encoding="utf-8")
    common = (
        'user: "10001:10001"',
        "read_only: true",
        "cap_drop:\n      - ALL",
        "security_opt:\n      - no-new-privileges:true",
    )

    for service_name in ("symba-flyway", "symba-engine"):
        block = _compose_service_block(production, service_name)
        for fragment in common:
            assert fragment in block
        assert "size=256m" in block
        assert "uid=10001,gid=10001" in block

    preflight = _compose_service_block(production, "symba-runtime-preflight")
    assert "network_mode: none" in preflight
    assert all(fragment in preflight for fragment in common)
    assert 'SYMBA_TLS_KEY_EXPECTED_OWNER: "10001:10001"' in preflight
    assert "SYMBA_GRPC_TLS_EXPECTED_DNS_NAME:" in preflight
    assert "SYMBA_GRPC_TLS_EXPECTED_IP:" in preflight
    assert "SYMBA_GRPC_TLS_EXPECTED_CERT_SHA256:" in preflight
    assert "SYMBA_GRPC_TLS_EXPECTED_CA_SHA256:" in preflight
    assert "SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS:" in preflight
    assert "/run/secrets/symba/server.crt:ro" in preflight
    assert "/run/secrets/symba/server.key:ro" in preflight
    assert "/run/secrets/symba/ca.crt:ro" in preflight
    assert "size=1m" in preflight
    assert 'restart: "no"' in preflight

    preflight_image = _assert_digest_pinned_dockerfile(ROOT / "Dockerfile.tls-preflight")
    assert "alpine/openssl:3.5.4@sha256:" in preflight_image
    assert 'ENTRYPOINT ["/usr/local/bin/symba-runtime-preflight"]' in preflight_image

    preflight_script = (ROOT / "scripts" / "tls" / "preflight.sh").read_text(
        encoding="utf-8"
    )
    for contract in (
        "tls_certificate_private_key_mismatch",
        "tls_certificate_fingerprint_mismatch",
        "tls_ca_fingerprint_mismatch",
        "tls_dns_chain_verification_failed",
        "tls_ip_chain_verification_failed",
        "tls_server_auth_eku_missing",
        "tls_public_key_algorithm_not_allowed",
        "tls_rsa_key_too_weak",
        "tls_signature_algorithm_not_allowed",
        "tls_certificate_expired_or_expiring",
        "tls_ca_expired_or_expiring",
        "tls_file_mount_not_read_only",
    ):
        assert contract in preflight_script

    engine = _compose_service_block(production, "symba-engine")
    assert "symba-runtime-preflight:" in engine
    assert "condition: service_completed_successfully" in engine

    frontend = _compose_service_block(production, "symba-frontend")
    assert 'user: "101:101"' in frontend
    assert "read_only: true" in frontend
    assert "cap_drop:\n      - ALL" in frontend
    assert "security_opt:\n      - no-new-privileges:true" in frontend
    assert "/etc/nginx/conf.d:" in frontend
    assert "/var/cache/nginx:" in frontend
    assert "/run:" in frontend
    assert "/tmp:" in frontend


def test_production_overlay_renders_immutable_nonroot_runtime_contracts(
    tmp_path: Path,
) -> None:
    docker = shutil.which("docker")
    if docker is None:
        return

    empty_environment_file = tmp_path / "empty.env"
    empty_environment_file.write_text("# intentionally empty\n", encoding="utf-8")
    process_environment = {
        "HOME": os.environ.get("HOME", str(tmp_path)),
        "PATH": os.environ.get("PATH", ""),
        "SYMBA_POSTGRES_ADMIN_PASSWORD": "test-admin-password-0123456789",
        "SYMBA_POSTGRES_MIGRATION_PASSWORD": "test-migration-password-0123456789",
        "SYMBA_POSTGRES_RUNTIME_PASSWORD": "test-runtime-password-0123456789",
        "SYMBA_DEV_TOKEN": "test-token-0123456789012345678901",
        "SYMBA_REDIS_PASSWORD": "test-redis-password-012345678901",
        "KNOR_RELEASE_ID": "test-release",
        "KNOR_RELEASE_MANIFEST_SHA256": "a" * 64,
        "SYMBA_GRPC_TLS_CERT_FILE_HOST": "/tmp/symba-test-server.crt",
        "SYMBA_GRPC_TLS_KEY_FILE_HOST": "/tmp/symba-test-server.key",
        "SYMBA_GRPC_TLS_CA_FILE_HOST": "/tmp/symba-test-ca.crt",
        "SYMBA_GRPC_TLS_EXPECTED_DNS_NAME": "symba.example.test",
        "SYMBA_GRPC_TLS_EXPECTED_IP": "100.64.0.4",
        "SYMBA_GRPC_TLS_EXPECTED_CERT_SHA256": "b" * 64,
        "SYMBA_GRPC_TLS_EXPECTED_CA_SHA256": "c" * 64,
        "SYMBA_GRPC_TLS_MIN_REMAINING_SECONDS": "86400",
    }
    completed = subprocess.run(
        [
            docker,
            "compose",
            "--env-file",
            str(empty_environment_file),
            "-p",
            "symba-production-render-test",
            "-f",
            str(ROOT / "docker-compose.yml"),
            "-f",
            str(ROOT / "docker-compose.production.yml"),
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env=process_environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    services = json.loads(completed.stdout)["services"]

    expected_runtime = {
        "symba-flyway": (
            "10001:10001",
            [
                "/tmp:rw,noexec,nosuid,nodev,size=256m,mode=1770,uid=10001,gid=10001"
            ],
        ),
        "symba-engine": (
            "10001:10001",
            [
                "/tmp:rw,noexec,nosuid,nodev,size=256m,mode=1770,uid=10001,gid=10001"
            ],
        ),
        "symba-frontend": (
            "101:101",
            [
                "/etc/nginx/conf.d:rw,noexec,nosuid,nodev,size=1m,mode=0770,uid=101,gid=101",
                "/var/cache/nginx:rw,noexec,nosuid,nodev,size=32m,mode=0770,uid=101,gid=101",
                "/run:rw,noexec,nosuid,nodev,size=1m,mode=0770,uid=101,gid=101",
                "/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1770,uid=101,gid=101",
            ],
        ),
    }
    for service_name, (user, tmpfs) in expected_runtime.items():
        service = services[service_name]
        assert service["user"] == user
        assert service["read_only"] is True
        assert service["tmpfs"] == tmpfs
        assert service["cap_drop"] == ["ALL"]
        assert service["security_opt"] == ["no-new-privileges:true"]

    assert services["symba-flyway"]["environment"] | {
        "HOME": "/tmp",
        "XDG_CACHE_HOME": "/tmp/.cache",
    } == services["symba-flyway"]["environment"]
    assert services["symba-engine"]["environment"] | {
        "HOME": "/tmp",
        "XDG_CACHE_HOME": "/tmp/.cache",
        "PYTHONDONTWRITEBYTECODE": "1",
    } == services["symba-engine"]["environment"]
    assert services["symba-redis"]["environment"]["REDIS_PASSWORD"] == (
        "test-redis-password-012345678901"
    )
    assert "--requirepass" in services["symba-redis"]["command"][2]
    assert services["symba-redis"]["healthcheck"]["test"] == [
        "CMD-SHELL",
        'REDISCLI_AUTH="$$REDIS_PASSWORD" redis-cli --no-auth-warning ping',
    ]
    assert services["symba-postgres"]["environment"]["POSTGRES_USER"] == "postgres"
    assert services["symba-postgres"]["environment"]["POSTGRES_DB"] == "postgres"
    assert services["symba-flyway"]["environment"]["FLYWAY_USER"] == "symba_migrator"
    assert services["symba-flyway"]["image"] == f"symba-flyway:{'a' * 64}"
    assert "volumes" not in services["symba-flyway"]
    assert services["symba-engine"]["environment"]["SYMBA_POSTGRES__USER"] == (
        "symba_runtime"
    )
    assert all(
        mount["read_only"]
        for mount in services["symba-engine"]["volumes"]
        if mount["target"].startswith("/run/secrets/symba/")
    )
    assert services["symba-frontend"]["ports"] == [
        {
            "mode": "ingress",
            "host_ip": "127.0.0.1",
            "target": 8080,
            "published": "8080",
            "protocol": "tcp",
        }
    ]

    tls_preflight = services["symba-runtime-preflight"]
    assert tls_preflight["image"] == f"symba-tls-preflight:{'a' * 64}"
    assert tls_preflight["network_mode"] == "none"
    assert tls_preflight["user"] == "10001:10001"
    assert tls_preflight["read_only"] is True
    assert tls_preflight["cap_drop"] == ["ALL"]
    assert tls_preflight["security_opt"] == ["no-new-privileges:true"]
    assert tls_preflight["restart"] == "no"
    assert "ports" not in tls_preflight
    assert all(mount["read_only"] for mount in tls_preflight["volumes"])
    assert tls_preflight["tmpfs"] == [
        "/tmp:rw,noexec,nosuid,nodev,size=1m,mode=1770,uid=10001,gid=10001"
    ]
    assert tls_preflight["environment"]["SYMBA_TLS_KEY_EXPECTED_OWNER"] == (
        "10001:10001"
    )
    assert tls_preflight["environment"]["SYMBA_GRPC_TLS_EXPECTED_DNS_NAME"] == (
        "symba.example.test"
    )
    assert tls_preflight["environment"]["SYMBA_GRPC_TLS_EXPECTED_IP"] == (
        "100.64.0.4"
    )
    assert {mount["target"] for mount in tls_preflight["volumes"]} == {
        "/run/secrets/symba/server.crt",
        "/run/secrets/symba/server.key",
        "/run/secrets/symba/ca.crt",
    }
    assert services["symba-engine"]["depends_on"]["symba-runtime-preflight"] == {
        "condition": "service_completed_successfully",
        "required": True,
    }
    assert services["symba-redis"]["depends_on"]["symba-runtime-preflight"] == {
        "condition": "service_completed_successfully",
        "required": True,
    }


def test_engine_build_context_excludes_non_runtime_and_secret_inputs() -> None:
    entries = {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert {
        ".git",
        ".venv",
        "frontend",
        "tests",
        "docs",
        "examples",
        "tools",
        "symba-sdk-python",
        ".env*",
    } <= entries
    assert {"Dockerfile", "pyproject.toml", "uv.lock", "proto", "src"}.isdisjoint(entries)

    frontend_entries = {
        line.strip()
        for line in (ROOT / "frontend" / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert {"node_modules", "dist", "tests", ".env*", ".git"} <= frontend_entries
    assert {"package.json", "package-lock.json", "src"}.isdisjoint(frontend_entries)
