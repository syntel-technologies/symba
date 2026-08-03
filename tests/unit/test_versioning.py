"""L1: protocol version handshake compatibility (N10).

Walks the acceptance table in core/versioning.py from the engine's perspective:
accept same-major and one-major-behind (the N-1 window), reject anything newer than
the engine or more than one major behind, and fail closed on garbage.
"""

from __future__ import annotations

import pytest

from symba.core.errors import ProtocolVersionUnsupported
from symba.core.versioning import check_supported, is_supported, parse_version

pytestmark = pytest.mark.l1


def test_parse_version_major_minor():
    assert parse_version("2.5") == (2, 5)
    assert parse_version("2.5.9") == (2, 5)  # patch ignored
    assert parse_version(" 3.0 ") == (3, 0)  # whitespace tolerated


def test_parse_version_tolerates_v_prefix_and_composite_user_agent():
    # The SDK's real ClaimRequest.sdk_version is a composite user-agent whose
    # authoritative half is the "proto/<ver>" token, and the proto tag carries a
    # leading "v" from the git tag (spec 4.3). The engine must parse the wire
    # version out of it rather than fail closed on "int('symba/0')".
    assert parse_version("v0.1.0") == (0, 1)
    assert parse_version("symba/0.1.0 proto/v0.1.0") == (0, 1)
    assert parse_version("symba/2.3.1 proto/2.5") == (2, 5)  # proto token wins over pkg


@pytest.mark.parametrize("bad", ["", "3", "x.y", "3.", "symba/0.1.0 proto/nope"])
def test_parse_version_rejects_garbage(bad: str):
    with pytest.raises(ValueError):
        parse_version(bad)


@pytest.mark.parametrize(
    "client,engine,expected",
    [
        ("2.0", "2.5", True),  # same major, older minor
        ("2.9", "2.5", True),  # same major, newer minor (additive-only) still ok
        ("1.4", "2.5", True),  # one major behind: the N-1 window
        ("3.0", "2.5", False),  # newer major than engine -> reject (from the future)
        ("0.9", "2.5", False),  # two majors behind -> outside the window
        ("", "2.5", False),  # unset/garbage -> fail closed
        ("nope", "2.5", False),
    ],
)
def test_is_supported_matrix(client: str, engine: str, expected: bool):
    assert is_supported(client, engine) is expected


def test_check_supported_accepts_in_window():
    check_supported("2.0", "2.5")  # no raise


def test_check_supported_rejects_and_names_both_versions():
    with pytest.raises(ProtocolVersionUnsupported) as exc:
        check_supported("3.0", "2.5")
    err = exc.value
    # The reject must name both versions so the operator knows what to upgrade.
    assert err.context["client_version"] == "3.0"
    assert err.context["engine_version"] == "2.5"
    assert "3.0" in err.message and "2.5" in err.message
    assert err.grpc_code == "FAILED_PRECONDITION"


def test_check_supported_unset_version_fails_closed():
    with pytest.raises(ProtocolVersionUnsupported):
        check_supported("", "2.5")
