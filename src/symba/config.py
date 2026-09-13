"""Engine configuration.

pydantic-settings model with clean, typed ergonomics: loads from symba.toml
plus a .env file plus `SYMBA_<SECTION>__<KEY>` environment overrides (nested
delimiter `__` with matching section layout). This stays typed and fail-fast:
a bad key aborts at boot with a pydantic error naming the exact field, and the
model doubles as documentation.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Self
from urllib.parse import quote

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from symba import __version__


class AppConfig(BaseModel):
    name: str = "symba"
    description: str = "Symba job execution engine"
    environment: str = "development"  # development | testing | production
    version_major: str = __version__.split(".")[0]
    version_minor: str = __version__.split(".")[1]
    version_patch: str = __version__.split(".")[2]


class ServerConfig(BaseModel):
    grpc_port: int = 7233
    http_port: int = 7300
    roles: list[str] = Field(default_factory=lambda: ["all"])
    grpc_max_message_mb: int = 4
    grpc_keepalive_time_ms: int = 20000
    grpc_keepalive_timeout_ms: int = 5000
    # The SDK client pings every ~10s with keepalive_permit_without_calls=1 so that
    # idle Claim / StreamEvents streams stay warm. gRPC's server defaults reject that
    # as ENHANCE_YOUR_CALM ("too many pings") after 2 strikes because the default
    # min recv-ping interval is 5min. These two knobs relax the server to accept a
    # client that pings on idle streams; keep them <= the SDK keepalive_time_s.
    grpc_min_ping_interval_ms: int = 5000
    grpc_permit_keepalive_without_calls: bool = True
    grpc_max_connection_age_s: int = 1800
    grpc_max_connection_age_grace_s: int = 60
    shutdown_drain_s: int = 30
    # Production gRPC is TLS-only. Certificate/key paths are mounted runtime
    # secrets, never image contents. Client authentication remains optional for
    # token mode and is mandatory when auth.mode=mtls.
    grpc_tls_enabled: bool = False
    grpc_tls_cert_file: str = ""
    grpc_tls_key_file: str = ""
    grpc_tls_client_ca_file: str = ""
    grpc_tls_require_client_auth: bool = False

    @field_validator("roles")
    @classmethod
    def _valid_roles(cls, value: list[str]) -> list[str]:
        allowed = {"api", "sweeper", "all"}
        bad = [role for role in value if role not in allowed]
        if bad:
            raise ValueError(f"unknown role(s) {bad}; allowed: {sorted(allowed)}")
        return value


class PostgresConfig(BaseModel):
    dsn: str = "postgresql://symba:symba@localhost:5432/symba"
    # Prefer discrete fields in deployments so credentials containing URI
    # delimiters are encoded safely. ``dsn`` remains supported for existing
    # installations and local configuration.
    host: str = ""
    port: int = Field(default=5432, ge=1, le=65535)
    user: str = ""
    password: str = ""
    database: str = ""
    # Flyway-managed application schema. Named `schema_name` (not `schema`) to
    # avoid clashing with pydantic's deprecated BaseModel.schema classmethod.
    schema_name: str = "symba"
    hot_pool_size: int = Field(default=20, ge=2)
    hot_command_timeout_s: int = 5
    general_pool_size: int = Field(default=20, ge=2)
    general_command_timeout_s: int = 30

    @property
    def resolved_dsn(self) -> str:
        """Return an asyncpg DSN with discrete credentials safely encoded."""
        if any((self.host, self.user, self.password, self.database)):
            missing = [
                name
                for name, value in (
                    ("host", self.host),
                    ("user", self.user),
                    ("password", self.password),
                    ("database", self.database),
                )
                if not value
            ]
            if missing:
                raise ValueError("postgres discrete connection fields are incomplete: " + ", ".join(missing))
            host = self.host
            if ":" in host and not host.startswith("["):
                host = f"[{host}]"
            return (
                "postgresql://"
                f"{quote(self.user, safe='')}:{quote(self.password, safe='')}"
                f"@{host}:{self.port}/{quote(self.database, safe='')}"
            )
        return self.dsn


class RedisConfig(BaseModel):
    url: str = ""  # empty -> degraded mode
    socket_timeout_s: float = 2.0

    @property
    def enabled(self) -> bool:
        return bool(self.url)


class DefaultsConfig(BaseModel):
    lease_ttl_s: int = 60
    timeout_s: int = 600
    max_attempts: int = 5
    backoff_base_s: float = 1.0
    backoff_factor: float = 2.0
    backoff_max_s: float = 300.0
    jitter: bool = True


class LimitsConfig(BaseModel):
    max_result_kb: int = 64
    max_payload_kb: int = 256
    max_chain_len: int = 50
    max_fanout_children: int = 100000
    tenant_queued_cap: int = 0  # 0 = unlimited
    query_max_page: int = 1000


class SweeperConfig(BaseModel):
    interval_s: int = 5
    batch_size: int = 1000  # rows reclaimed/expired per statement per pass
    worker_stale_after_heartbeats: int = 3
    # The engine does not know a worker's own heartbeat cadence (it lives in the
    # SDK, default 15s), so staleness is threshold = stale_after * this interval.
    # last_seen is refreshed on every Claim-stream slot frame.
    worker_heartbeat_interval_s: int = 15


class RetentionConfig(BaseModel):
    job_events_days: int = 90
    succeeded_jobs_days: int = 30  # DEAD jobs are NEVER auto-pruned (DLQ contract)
    checkpoints_hours: int = 72
    consumed_signals_days: int = 7


class DispatcherConfig(BaseModel):
    min_tick_ms: int = 10
    max_tick_ms: int = 250
    max_per_group_per_batch: int = 0  # 0 -> GREATEST(2, limit/8)


class MatcherConfig(BaseModel):
    strict_group_caps: bool = False
    max_upstream_inline_kb: int = 256


class AuthConfig(BaseModel):
    # Default "none" is safe because it fail-closes on any peer that is neither
    # loopback nor an explicitly trusted proxy (transport/auth.py): local dev + tests
    # work out of the box, while a routable deployment MUST opt into "token" or "mtls"
    # or it refuses remote callers.
    mode: str = "none"  # "token" | "mtls" | "none"
    token_jwks_url: str = ""
    tokens: dict[str, str] = Field(default_factory=dict)  # shared-secret -> tenant
    # CIDRs whose peers are trusted as "local" under mode=none. Empty keeps the
    # loopback-only behavior. This exists so the dev/compose stack works when the
    # browser reaches the engine THROUGH the SPA's nginx reverse proxy (the engine
    # then sees the nginx container IP, not 127.0.0.1). Set it to the internal Docker
    # network CIDR — NEVER to a public range, or mode=none becomes an open control
    # plane. mode=none still refuses anything outside loopback + these CIDRs.
    trusted_proxy_cidrs: list[str] = Field(default_factory=list)

    @field_validator("mode")
    @classmethod
    def _valid_mode(cls, value: str) -> str:
        if value not in {"token", "mtls", "none"}:
            raise ValueError(f"auth.mode must be token|mtls|none, got {value!r}")
        return value

    @field_validator("trusted_proxy_cidrs")
    @classmethod
    def _valid_cidrs(cls, value: list[str]) -> list[str]:
        import ipaddress

        for cidr in value:
            try:
                ipaddress.ip_network(cidr, strict=False)
            except ValueError as exc:
                raise ValueError(f"invalid trusted_proxy_cidr {cidr!r}: {exc}") from exc
        return value


class LogConfig(BaseModel):
    level: str = "INFO"
    format: str = "json"  # "json" | "console"
    file_path: str = ""
    enable_stdout: bool = True


class ObservabilityConfig(BaseModel):
    metrics_enabled: bool = True
    otlp_endpoint: str = ""  # empty -> tracing off


class CronConfig(BaseModel):
    tick_s: float = 1.0


class SymbaConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SYMBA_",
        env_nested_delimiter="__",
        toml_file="symba.toml",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app: AppConfig = Field(default_factory=AppConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    sweeper: SweeperConfig = Field(default_factory=SweeperConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    dispatcher: DispatcherConfig = Field(default_factory=DispatcherConfig)
    matcher: MatcherConfig = Field(default_factory=MatcherConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    log: LogConfig = Field(default_factory=LogConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    cron: CronConfig = Field(default_factory=CronConfig)

    @model_validator(mode="after")
    def _validate_deployment_security(self) -> Self:
        """Fail closed for production auth configuration mistakes."""
        tls_material = (
            self.server.grpc_tls_cert_file.strip(),
            self.server.grpc_tls_key_file.strip(),
        )
        if self.server.grpc_tls_enabled and not all(tls_material):
            raise ValueError("gRPC TLS requires both grpc_tls_cert_file and grpc_tls_key_file")
        if not self.server.grpc_tls_enabled and any(tls_material):
            raise ValueError("gRPC TLS material requires grpc_tls_enabled=true")
        if self.server.grpc_tls_require_client_auth:
            if not self.server.grpc_tls_enabled:
                raise ValueError("gRPC client authentication requires grpc_tls_enabled=true")
            if not self.server.grpc_tls_client_ca_file.strip():
                raise ValueError("gRPC client authentication requires grpc_tls_client_ca_file")
        if self.auth.mode == "mtls" and not (
            self.server.grpc_tls_enabled
            and self.server.grpc_tls_require_client_auth
            and self.server.grpc_tls_client_ca_file.strip()
        ):
            raise ValueError("auth.mode=mtls requires TLS with client authentication and a client CA")

        if self.app.environment.strip().lower() != "production":
            return self
        if self.auth.mode == "none":
            raise ValueError("production deployments require auth.mode=token or mtls")
        if self.auth.mode == "token":
            has_shared_secret = any(
                str(secret).strip() and str(tenant).strip() for secret, tenant in self.auth.tokens.items()
            )
            if not has_shared_secret and not self.auth.token_jwks_url.strip():
                raise ValueError("production token auth requires a non-empty shared secret or token_jwks_url")
        if not self.server.grpc_tls_enabled:
            raise ValueError("production deployments require gRPC TLS")
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Precedence: init > process env > .env file > symba.toml > defaults.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            TomlConfigSettingsSource(settings_cls),
        )


def load_config(path: str | None = None) -> SymbaConfig:
    """Load config, optionally from an explicit TOML path (used by `symba config check`)."""
    if path:
        raw: dict[str, Any] = tomllib.loads(Path(path).read_text())
        return SymbaConfig(**raw)
    return SymbaConfig()
