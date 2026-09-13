"""gRPC server transport-security wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import grpc

from symba.config import ServerConfig, SymbaConfig
from symba.transport.grpc_server import _add_listening_port
from symba.transport.state import EngineState


class _Server:
    def __init__(self) -> None:
        self.insecure_addresses: list[str] = []
        self.secure_addresses: list[str] = []

    def add_insecure_port(self, address: str) -> int:
        self.insecure_addresses.append(address)
        return 7233

    def add_secure_port(self, address: str, _credentials: grpc.ServerCredentials) -> int:
        self.secure_addresses.append(address)
        return 7233


def _state(server_config: ServerConfig) -> EngineState:
    return cast(EngineState, cast(Any, type("State", (), {"config": SymbaConfig(server=server_config)})()))


def test_development_defaults_to_insecure_listener() -> None:
    server = _Server()

    assert _add_listening_port(cast(grpc.aio.Server, cast(Any, server)), _state(ServerConfig())) == 7233

    assert server.insecure_addresses == ["0.0.0.0:7233"]
    assert server.secure_addresses == []


def test_tls_listener_reads_runtime_certificate_and_key(tmp_path: Path) -> None:
    certificate = tmp_path / "server.crt"
    private_key = tmp_path / "server.key"
    certificate.write_bytes(b"test certificate")
    private_key.write_bytes(b"test private key")
    server = _Server()
    config = ServerConfig(
        grpc_tls_enabled=True,
        grpc_tls_cert_file=str(certificate),
        grpc_tls_key_file=str(private_key),
    )

    assert _add_listening_port(cast(grpc.aio.Server, cast(Any, server)), _state(config)) == 7233

    assert server.insecure_addresses == []
    assert server.secure_addresses == ["0.0.0.0:7233"]
