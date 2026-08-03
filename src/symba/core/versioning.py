"""Protocol version handshake — pure compatibility logic.

the compatibility contract
---------------------------------------
Wire protocol version = engine MAJOR.MINOR. The published rule is:

    "SDK N.x supports engine protocol N-1 and N."

Read from the ENGINE's side (what it accepts on a Claim handshake), the dual is:
accept a client whose protocol MAJOR is the engine's major or exactly one behind,
and never a client from the FUTURE (a newer major may speak fields this engine
cannot honor). MINOR is additive-only (buf breaking gate) so a MINOR skew
within an accepted major is always safe.

    engine major = E, client major = C
    ┌──────────────┬─────────────────────────────────────────────┐
    │  C  >  E     │  REJECT — client is newer than the engine     │
    │  C == E      │  ACCEPT — same major, any minor               │
    │  C == E-1    │  ACCEPT — one major behind (the N-1 window)   │
    │  C  <  E-1   │  REJECT — too old, past the support window    │
    └──────────────┴─────────────────────────────────────────────┘

A rejected handshake raises ProtocolVersionUnsupported naming BOTH versions so the
operator sees exactly what to upgrade. An unparseable/empty client version is
treated as unsupported rather than silently allowed — fail closed on the wire
contract.

This module is I/O-free (core purity, checked by tools/check_imports.py); the
transport calls check_supported() on the first Claim frame.
"""

from __future__ import annotations

from symba import PROTOCOL_VERSION
from symba.core.errors import ProtocolVersionUnsupported


def parse_version(raw: str) -> tuple[int, int]:
    """Parse a client version token into (major, minor).

    Accepts three shapes so the engine tolerates the SDK's real handshake header
    without a lockstep release:

        "0.1"                        bare MAJOR.MINOR (the engine's own PROTOCOL_VERSION)
        "0.1.0" / "v0.1.0"           MAJOR.MINOR.PATCH, optional leading "v"
        "symba/0.1.0 proto/v0.1.0"   composite user-agent — the "proto/<ver>" token wins

    The wire contract is keyed on MAJOR.MINOR only; patch/pre-release and the
    package half of the user-agent are ignored. Raises ValueError on anything
    unparseable so callers fail closed.
    """
    token = _extract_version_token(raw)
    parts = token.split(".")
    if len(parts) < 2:
        raise ValueError(f"protocol version {raw!r} is not MAJOR.MINOR")
    return int(parts[0]), int(parts[1])


def _extract_version_token(raw: str) -> str:
    """Pull the bare MAJOR.MINOR[.PATCH] token out of a possibly-composite header.

    The SDK sends ``sdk_version = "symba/<pkg> proto/<stub-tag>"`` (spec 4.3), where
    the protocol half is the authoritative wire version. Prefer the ``proto/`` token
    when present; otherwise fall back to the last whitespace-separated token. A
    leading ``v`` (from a git tag like ``v0.1.0``) is stripped.
    """
    text = raw.strip()
    proto_token = next(
        (part.split("/", 1)[1] for part in text.split() if part.startswith("proto/")),
        None,
    )
    token = proto_token if proto_token is not None else (text.split()[-1] if text else "")
    return token[1:] if token[:1] == "v" else token


def is_supported(client_version: str, engine_version: str = PROTOCOL_VERSION) -> bool:
    """True iff the engine accepts a client speaking client_version (see module table)."""
    try:
        client_major, _ = parse_version(client_version)
        engine_major, _ = parse_version(engine_version)
    except ValueError:
        return False  # fail closed on a malformed/empty client version
    # Not from the future, and within one major of the engine (N and N-1).
    return engine_major - 1 <= client_major <= engine_major


def check_supported(client_version: str, engine_version: str = PROTOCOL_VERSION) -> None:
    """Raise ProtocolVersionUnsupported (naming both versions) if incompatible."""
    if is_supported(client_version, engine_version):
        return
    raise ProtocolVersionUnsupported(
        f"client protocol {client_version or '(unset)'} is not supported by engine "
        f"protocol {engine_version} (supported: {_supported_range(engine_version)})",
        client_version=client_version,
        engine_version=engine_version,
    )


def _supported_range(engine_version: str) -> str:
    """Human-readable accepted-major window for the reject message."""
    engine_major, _ = parse_version(engine_version)
    low = max(0, engine_major - 1)
    return f"protocol majors {low}.x–{engine_major}.x"
