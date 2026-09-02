# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false
"""gRPC data-plane server.

grpc.aio server on the gRPC port. At M0 it registers the standard gRPC health
service (so LBs and `grpc_health_probe`-style checks work) and server
reflection (so `grpcurl` can introspect during development). The data-plane
servicers (Claim stream, Heartbeat, Complete, Fail, Wait, checkpoints,
GetResult) are registered here in M1 as services land.

Keepalive/connection-age options come straight from config; the
`max_connection_age` recycling is intentional: Claim streams churn by design
and reconnect storms are a tested path, not an incident.
"""

from __future__ import annotations

from pathlib import Path

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from symba.observability.logging import logger
from symba.transport.admin_service import AdminServicer
from symba.transport.auth import build_grpc_interceptor
from symba.transport.client_service import ClientServicer
from symba.transport.state import EngineState
from symba.transport.worker_service import WorkerServicer
from symba.v1 import admin_pb2_grpc as admin_grpc
from symba.v1 import control_plane_pb2_grpc as cp_grpc
from symba.v1 import data_plane_pb2_grpc as dp_grpc

logger = logger.bind(service="grpc_server", context="engine/transport")

_DATA_PLANE_SERVICE = "symba.v1.DataPlane"
_CONTROL_PLANE_SERVICE = "symba.v1.ClientService"
_ADMIN_SERVICE = "symba.v1.AdminService"


def _read_tls_material(path: str, *, label: str) -> bytes:
    """Read one TLS input without ever including its contents in diagnostics."""
    try:
        value = Path(path).read_bytes()
    except OSError as exc:
        raise RuntimeError(f"unable to read configured {label}") from exc
    if not value.strip():
        raise RuntimeError(f"configured {label} is empty")
    return value


def _add_listening_port(server: grpc.aio.Server, state: EngineState) -> int:
    """Bind exactly one gRPC listener using the configured transport security."""
    cfg = state.config.server
    address = f"0.0.0.0:{cfg.grpc_port}"
    if not cfg.grpc_tls_enabled:
        return server.add_insecure_port(address)  # noqa: S104 - container-internal dev only

    private_key = _read_tls_material(cfg.grpc_tls_key_file, label="gRPC TLS private key")
    certificate_chain = _read_tls_material(
        cfg.grpc_tls_cert_file,
        label="gRPC TLS certificate chain",
    )
    root_certificates = None
    if cfg.grpc_tls_require_client_auth:
        root_certificates = _read_tls_material(
            cfg.grpc_tls_client_ca_file,
            label="gRPC TLS client CA",
        )
    credentials = grpc.ssl_server_credentials(
        [(private_key, certificate_chain)],
        root_certificates=root_certificates,
        require_client_auth=cfg.grpc_tls_require_client_auth,
    )
    return server.add_secure_port(address, credentials)


def _server_options(state: EngineState) -> list[tuple[str, int]]:
    cfg = state.config.server
    max_bytes = cfg.grpc_max_message_mb * 1024 * 1024
    return [
        ("grpc.keepalive_time_ms", cfg.grpc_keepalive_time_ms),
        ("grpc.keepalive_timeout_ms", cfg.grpc_keepalive_timeout_ms),
        # Accept the SDK's idle-stream keepalive pings instead of GOAWAY-ing them.
        # min_ping_interval says "the soonest a client may ping without data"; set it
        # <= the SDK keepalive so a well-behaved client is never a "strike". permit=1
        # lets a client ping while it holds a stream but has no in-flight RPC (the
        # Claim / StreamEvents idle case).
        ("grpc.http2.min_ping_interval_without_data_ms", cfg.grpc_min_ping_interval_ms),
        ("grpc.http2.min_time_between_pings_ms", cfg.grpc_min_ping_interval_ms),
        ("grpc.keepalive_permit_without_calls", 1 if cfg.grpc_permit_keepalive_without_calls else 0),
        ("grpc.max_connection_age_ms", cfg.grpc_max_connection_age_s * 1000),
        ("grpc.max_connection_age_grace_ms", cfg.grpc_max_connection_age_grace_s * 1000),
        ("grpc.max_send_message_length", max_bytes),
        ("grpc.max_receive_message_length", max_bytes),
    ]


async def serve_grpc(state: EngineState) -> None:
    cfg = state.config.server
    # Auth interceptor runs before every servicer; health + reflection
    # are also intercepted, but under mode=none (dev) they pass, and under token/mtls
    # a probe client must present a credential like any other caller.
    server = grpc.aio.server(
        options=_server_options(state),
        interceptors=[build_grpc_interceptor(state.authenticator)],
    )

    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    # Data-plane worker servicer: Claim stream + Heartbeat/Complete/Fail/GetResult.
    dp_grpc.add_WorkerServiceServicer_to_server(
        WorkerServicer(state.registry, state.jobs, state.signals, state.checkpoints),
        server,
    )

    # Control-plane client servicer (ENG-1): the SDK's Engine dials ClientServiceStub
    # for Submit/FanOut/Query/GetJob/AwaitJob/Cancel/Signal/Resubmit/StreamEvents, so
    # the gRPC server must register a servicer for it — not only the HTTP mirror.
    cp_grpc.add_ClientServiceServicer_to_server(
        ClientServicer(
            state.submit,
            state.fanout,
            state.cancel,
            state.signals,
            state.resubmit,
            state.query,
            state.events,
        ),
        server,
    )

    # Admin servicer: the SDK's engine.admin dials AdminServiceStub for rate-class,
    # cron, and fleet ops. Without this registration every engine.admin.* call
    # returned UNIMPLEMENTED (only the HTTP cron mirror existed).
    admin_grpc.add_AdminServiceServicer_to_server(
        AdminServicer(state.query, state.rate_limiter),
        server,
    )

    reflection.enable_server_reflection(
        (
            health.SERVICE_NAME,
            _DATA_PLANE_SERVICE,
            _CONTROL_PLANE_SERVICE,
            _ADMIN_SERVICE,
            reflection.SERVICE_NAME,
        ),
        server,
    )

    bound_port = _add_listening_port(server, state)
    if bound_port == 0:
        raise RuntimeError("gRPC server failed to bind its configured port")
    await server.start()

    # Advertise SERVING for both the overall server ("") and our data-plane name.
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set(_DATA_PLANE_SERVICE, health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set(_CONTROL_PLANE_SERVICE, health_pb2.HealthCheckResponse.SERVING)
    await health_servicer.set(_ADMIN_SERVICE, health_pb2.HealthCheckResponse.SERVING)
    logger.info(
        "[serve_grpc] gRPC server started",
        port=cfg.grpc_port,
        tls_enabled=cfg.grpc_tls_enabled,
        client_auth_required=cfg.grpc_tls_require_client_auth,
    )

    try:
        await server.wait_for_termination()
    finally:
        logger.info("[serve_grpc] Draining gRPC server", grace_s=cfg.shutdown_drain_s)
        await health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        await server.stop(grace=cfg.shutdown_drain_s)
