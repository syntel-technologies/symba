"""Typed deployment configuration boundaries."""

from __future__ import annotations

import pytest

from symba.config import AppConfig, AuthConfig, PostgresConfig, ServerConfig, SymbaConfig


def test_postgres_discrete_credentials_are_uri_encoded() -> None:
    cfg = PostgresConfig(
        host="db.internal",
        port=5433,
        user="symba+worker",
        password="p@ss:/?#[]!",
        database="symba jobs",
    )

    assert cfg.resolved_dsn == ("postgresql://symba%2Bworker:p%40ss%3A%2F%3F%23%5B%5D%21@db.internal:5433/symba%20jobs")


def test_postgres_dsn_remains_backward_compatible() -> None:
    cfg = PostgresConfig(dsn="postgresql://legacy:secret@db/legacy")

    assert cfg.resolved_dsn == "postgresql://legacy:secret@db/legacy"


def test_postgres_discrete_fields_fail_when_incomplete() -> None:
    cfg = PostgresConfig(host="db.internal")

    with pytest.raises(ValueError, match="user, password, database"):
        _ = cfg.resolved_dsn


def test_production_rejects_authenticated_plaintext_grpc() -> None:
    with pytest.raises(ValueError, match="require gRPC TLS"):
        SymbaConfig(
            app=AppConfig(environment="production"),
            auth=AuthConfig(mode="token", tokens={"secret": "default"}),
        )


def test_mtls_requires_transport_client_authentication() -> None:
    with pytest.raises(ValueError, match="auth.mode=mtls"):
        SymbaConfig(
            auth=AuthConfig(mode="mtls"),
            server=ServerConfig(
                grpc_tls_enabled=True,
                grpc_tls_cert_file="/run/secrets/server.crt",
                grpc_tls_key_file="/run/secrets/server.key",
            ),
        )


def test_tls_material_cannot_be_silently_ignored() -> None:
    with pytest.raises(ValueError, match="grpc_tls_enabled=true"):
        SymbaConfig(server=ServerConfig(grpc_tls_cert_file="/run/secrets/server.crt"))
