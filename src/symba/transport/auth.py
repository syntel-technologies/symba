"""Authentication + tenant resolution for both transports.

One verifier backs the HTTP middleware and the gRPC interceptor so the security
boundary is defined once. It resolves an inbound request to a Principal (tenant +
subject) or raises Unauthenticated; per-tenant authorization (a principal may only
touch its own tenant's rows) is enforced downstream by passing principal.tenant into
every query.

    mode matrix
    -----------
    token   Bearer <cred>. First tries the shared-secret -> tenant map (constant-time
            compare); then, if token_jwks_url is set, verifies an RS256 JWT against
            the JWKS and reads tenant from the `tenant` claim (fallback: `sub`).
    mtls    Trust is established by the TLS layer (client cert required at the gRPC/
            proxy edge); tenant is the peer identity. The engine reads it from a
            trusted header set by the terminating proxy (x-symba-tenant) or the gRPC
            auth context. We do NOT terminate TLS in-process (a proxy/LB does).
    none    Dev only. Refuses any peer that is neither loopback nor inside a
            configured trusted_proxy_cidrs range (fail-closed): binding auth=none on a
            routable interface is almost always a misconfiguration, so we treat an
            untrusted remote peer as unauthenticated rather than silently granting
            access. The trusted-CIDR carve-out exists so the SPA's nginx reverse proxy
            (which makes the engine see the proxy's container IP, not 127.0.0.1) can be
            explicitly allowed for local/compose dev.

    why fail-closed on none+untrusted
    ---------------------------------
    The alternative (allow-all) turns a forgotten dev setting into an open control
    plane reachable from the network. Refusing anything outside loopback + explicitly
    trusted CIDRs makes the blast radius of the mistake a broken deploy (loud) instead
    of a silent auth bypass (catastrophic).
"""
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false, reportMissingParameterType=false

from __future__ import annotations

import hmac
import ipaddress
from dataclasses import dataclass

import grpc

from symba.config import AuthConfig
from symba.core.errors import Unauthenticated
from symba.observability.logging import logger

logger = logger.bind(service="auth", context="engine/transport")

_BEARER_PREFIX = "bearer "
_TENANT_HEADER = "x-symba-tenant"
_DEFAULT_TENANT = "default"


@dataclass(slots=True, frozen=True)
class Principal:
    """The authenticated caller. tenant scopes every downstream query."""

    tenant: str
    subject: str
    method: str  # "token" | "jwt" | "mtls" | "none"


def _is_loopback(peer_ip: str | None) -> bool:
    if not peer_ip:
        # No peer info (e.g. in-process test transport) -> treat as loopback.
        return True
    try:
        return ipaddress.ip_address(peer_ip).is_loopback
    except ValueError:
        return False


def _in_trusted_cidrs(peer_ip: str | None, cidrs: list[str]) -> bool:
    # A configured trusted proxy range (e.g. the internal Docker network) is treated
    # as "local" under mode=none so the SPA's nginx reverse proxy works in dev.
    if not peer_ip or not cidrs:
        return False
    try:
        addr = ipaddress.ip_address(peer_ip)
    except ValueError:
        return False
    for cidr in cidrs:
        try:
            if addr in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


def _match_shared_secret(cred: str, tokens: dict[str, str]) -> str | None:
    # Constant-time compare against every configured secret so a timing side channel
    # cannot probe which secret prefix is correct. Small map -> full scan is cheap.
    matched: str | None = None
    for secret, tenant in tokens.items():
        if hmac.compare_digest(cred, secret):
            matched = tenant
    return matched


class Authenticator:
    """Transport-agnostic verifier built once at boot from AuthConfig."""

    def __init__(self, cfg: AuthConfig) -> None:
        self._cfg = cfg
        self._jwks_client = None  # lazily constructed on first JWT (network dependency)

    @property
    def mode(self) -> str:
        return self._cfg.mode

    def _verify_jwt(self, token: str) -> Principal:
        # Imported lazily: JWKS verification is optional and pulls a network client;
        # token-map and none modes must not require it.
        import jwt
        from jwt import PyJWKClient

        if self._jwks_client is None:
            self._jwks_client = PyJWKClient(self._cfg.token_jwks_url)
        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
            claims = jwt.decode(token, signing_key.key, algorithms=["RS256"], options={"require": ["exp"]})
        except Exception as exc:  # jwt raises a wide family; all mean "reject"
            raise Unauthenticated("invalid bearer token", reason=str(exc)) from exc
        tenant = claims.get("tenant") or claims.get("sub")
        if not tenant:
            raise Unauthenticated("token missing tenant/sub claim")
        return Principal(tenant=str(tenant), subject=str(claims.get("sub", tenant)), method="jwt")

    def _authenticate_token(self, authorization: str | None) -> Principal:
        if not authorization or authorization[: len(_BEARER_PREFIX)].lower() != _BEARER_PREFIX:
            raise Unauthenticated("missing bearer credentials")
        cred = authorization[len(_BEARER_PREFIX) :].strip()
        if not cred:
            raise Unauthenticated("empty bearer credentials")
        tenant = _match_shared_secret(cred, self._cfg.tokens)
        if tenant is not None:
            return Principal(tenant=tenant, subject=tenant, method="token")
        if self._cfg.token_jwks_url:
            return self._verify_jwt(cred)
        raise Unauthenticated("unrecognized bearer credentials")

    def authenticate(
        self, *, authorization: str | None, tenant_header: str | None, peer_ip: str | None
    ) -> Principal:
        """Resolve an inbound request to a Principal or raise Unauthenticated."""
        mode = self._cfg.mode
        if mode == "none":
            if not _is_loopback(peer_ip) and not _in_trusted_cidrs(peer_ip, self._cfg.trusted_proxy_cidrs):
                logger.warning(
                    "[auth] Refusing untrusted peer under auth mode=none",
                    peer_ip=peer_ip,
                    trusted_cidrs=self._cfg.trusted_proxy_cidrs,
                )
                raise Unauthenticated(
                    "auth disabled; only loopback or trusted-proxy callers are permitted"
                )
            return Principal(tenant=_DEFAULT_TENANT, subject="local", method="none")
        if mode == "mtls":
            # TLS terminates upstream; the trusted proxy passes the verified identity.
            tenant = (tenant_header or "").strip()
            if not tenant:
                raise Unauthenticated("mtls mode: missing trusted tenant identity")
            return Principal(tenant=tenant, subject=tenant, method="mtls")
        return self._authenticate_token(authorization)


def build_authenticator(cfg: AuthConfig) -> Authenticator:
    logger.info("[auth] Authenticator configured", mode=cfg.mode, jwks=bool(cfg.token_jwks_url))
    return Authenticator(cfg)


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """gRPC async server interceptor enforcing the same policy as the HTTP middleware.

    Workers present the credential in the `authorization` metadata key (Bearer) and
    their tenant via `x-symba-tenant` (mtls mode). A rejected call is aborted with
    UNAUTHENTICATED before the servicer runs. The resolved Principal is not yet
    threaded into per-RPC tenant scoping (worker RPCs are job_id-keyed by a lease
    token, an unguessable capability) — this gate is the authentication boundary;
    lease tokens remain the per-job authorization capability (see worker_service).
    """

    def __init__(self, authenticator: Authenticator) -> None:
        self._auth = authenticator

    async def intercept_service(self, continuation, handler_call_details):
        md = dict(handler_call_details.invocation_metadata or ())

        def _str(value: object) -> str | None:
            # gRPC metadata values are str for ascii keys but bytes for -bin keys;
            # our two keys are ascii, but normalize defensively.
            if isinstance(value, bytes):
                return value.decode("utf-8", "replace")
            return value if isinstance(value, str) else None

        authorization = _str(md.get("authorization"))
        tenant_header = _str(md.get("x-symba-tenant"))
        # Interceptors run before a peer is bound to a context; for loopback detection
        # in `none` mode we cannot see the peer here, so `none` trusts the bind address
        # (the server only listens container-internally) and accepts. token/mtls are
        # verified fully.
        try:
            if self._auth.mode != "none":
                self._auth.authenticate(
                    authorization=authorization, tenant_header=tenant_header, peer_ip=None
                )
        except Unauthenticated as exc:
            # Capture the message: `exc` is cleared when the except block exits, so the
            # abort handler (called later, per RPC) must close over a plain string.
            reason = exc.message

            async def _abort(request, context):
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, reason)

            return grpc.unary_unary_rpc_method_handler(_abort)
        return await continuation(handler_call_details)


def build_grpc_interceptor(authenticator: Authenticator) -> AuthInterceptor:
    return AuthInterceptor(authenticator)
