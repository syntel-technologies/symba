"""L1 unit tests for the transport authenticator (N9).

Pure logic — no transport, no infra. Covers the full mode matrix and the
fail-closed rule that makes mode=none safe as a default.

    matrix under test
    -----------------
                    loopback peer      remote peer
    none            -> default tenant  -> Unauthenticated (fail-closed)
    token (secret)  -> mapped tenant   -> mapped tenant
    token (bad)     -> Unauthenticated -> Unauthenticated
    mtls (+tenant)  -> that tenant      -> that tenant
    mtls (no tenant)-> Unauthenticated  -> Unauthenticated
"""

from __future__ import annotations

import pytest

from symba.config import AuthConfig
from symba.core.errors import Unauthenticated
from symba.transport.auth import build_authenticator


def _auth(**kwargs: object):
    return build_authenticator(AuthConfig(**kwargs))  # type: ignore[arg-type]


class TestNoneMode:
    def test_loopback_grants_default_tenant(self) -> None:
        p = _auth(mode="none").authenticate(authorization=None, tenant_header=None, peer_ip="127.0.0.1")
        assert p.tenant == "default"
        assert p.method == "none"

    def test_no_peer_treated_as_loopback(self) -> None:
        # In-process ASGI transport (tests) exposes no client host -> peer_ip=None.
        p = _auth(mode="none").authenticate(authorization=None, tenant_header=None, peer_ip=None)
        assert p.tenant == "default"

    def test_ipv6_loopback_grants(self) -> None:
        p = _auth(mode="none").authenticate(authorization=None, tenant_header=None, peer_ip="::1")
        assert p.tenant == "default"

    def test_remote_peer_refused(self) -> None:
        with pytest.raises(Unauthenticated):
            _auth(mode="none").authenticate(authorization=None, tenant_header=None, peer_ip="10.0.0.5")

    def test_unparseable_peer_refused(self) -> None:
        # A hostname we cannot parse as an IP is not provably loopback -> refuse.
        with pytest.raises(Unauthenticated):
            _auth(mode="none").authenticate(authorization=None, tenant_header=None, peer_ip="evil.example")

    def test_trusted_proxy_cidr_grants(self) -> None:
        # The SPA's nginx reverse proxy peers from the internal Docker subnet; trusting
        # it lets the dev stack work under mode=none without a login.
        a = _auth(mode="none", trusted_proxy_cidrs=["172.31.0.0/24"])
        p = a.authenticate(authorization=None, tenant_header=None, peer_ip="172.31.0.9")
        assert p.tenant == "default"
        assert p.method == "none"

    def test_peer_outside_trusted_cidr_refused(self) -> None:
        # Trusting one subnet must not accidentally admit a different one.
        a = _auth(mode="none", trusted_proxy_cidrs=["172.31.0.0/24"])
        with pytest.raises(Unauthenticated):
            a.authenticate(authorization=None, tenant_header=None, peer_ip="10.0.0.5")

    def test_public_peer_still_refused_with_trusted_cidr(self) -> None:
        # A public IP must never be admitted even when a private CIDR is trusted.
        a = _auth(mode="none", trusted_proxy_cidrs=["172.31.0.0/24"])
        with pytest.raises(Unauthenticated):
            a.authenticate(authorization=None, tenant_header=None, peer_ip="8.8.8.8")


class TestTokenMode:
    def test_shared_secret_maps_to_tenant(self) -> None:
        a = _auth(mode="token", tokens={"s3cret": "acme"})
        p = a.authenticate(authorization="Bearer s3cret", tenant_header=None, peer_ip="10.0.0.5")
        assert p.tenant == "acme"
        assert p.method == "token"

    def test_case_insensitive_bearer_prefix(self) -> None:
        a = _auth(mode="token", tokens={"s3cret": "acme"})
        p = a.authenticate(authorization="bearer s3cret", tenant_header=None, peer_ip="10.0.0.5")
        assert p.tenant == "acme"

    def test_missing_credentials_refused(self) -> None:
        with pytest.raises(Unauthenticated):
            _auth(mode="token", tokens={"s3cret": "acme"}).authenticate(
                authorization=None, tenant_header=None, peer_ip="10.0.0.5"
            )

    def test_empty_bearer_refused(self) -> None:
        with pytest.raises(Unauthenticated):
            _auth(mode="token", tokens={"s3cret": "acme"}).authenticate(
                authorization="Bearer   ", tenant_header=None, peer_ip="10.0.0.5"
            )

    def test_wrong_secret_refused(self) -> None:
        with pytest.raises(Unauthenticated):
            _auth(mode="token", tokens={"s3cret": "acme"}).authenticate(
                authorization="Bearer nope", tenant_header=None, peer_ip="10.0.0.5"
            )

    def test_no_tokens_and_no_jwks_refuses_any_bearer(self) -> None:
        with pytest.raises(Unauthenticated):
            _auth(mode="token").authenticate(
                authorization="Bearer anything", tenant_header=None, peer_ip="10.0.0.5"
            )


class TestMtlsMode:
    def test_trusted_tenant_header_grants(self) -> None:
        p = _auth(mode="mtls").authenticate(authorization=None, tenant_header="acme", peer_ip="10.0.0.5")
        assert p.tenant == "acme"
        assert p.method == "mtls"

    def test_missing_tenant_identity_refused(self) -> None:
        with pytest.raises(Unauthenticated):
            _auth(mode="mtls").authenticate(authorization=None, tenant_header=None, peer_ip="10.0.0.5")

    def test_blank_tenant_identity_refused(self) -> None:
        with pytest.raises(Unauthenticated):
            _auth(mode="mtls").authenticate(authorization=None, tenant_header="   ", peer_ip="10.0.0.5")
