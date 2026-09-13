from __future__ import annotations

import math

from symba.core.failure import build_error_entry, normalize_retry_after


def test_legacy_worker_message_fails_closed() -> None:
    raw = "Error 429: {'error': {'message': 'secret provider body', 'api_key': 'sk-test-secret'}}"

    entry = build_error_entry("RateLimitError", raw, "abc123", True)

    assert entry["message"] == "Worker task failed"
    assert "secret" not in str(entry)


def test_safe_message_is_redacted_again_at_engine_boundary() -> None:
    entry = build_error_entry(
        "ValueError",
        "bad input; authorization=Bearer abcdefghijklmnop",
        "abc123",
        False,
        message_safe=True,
    )

    assert "abcdefghijklmnop" not in entry["message"]
    assert "[redacted]" in entry["message"]


def test_structured_body_is_rejected_even_when_marked_safe() -> None:
    entry = build_error_entry(
        "ProviderError",
        'response body: {"prompt": "private"}',
        "abc123",
        False,
        message_safe=True,
    )

    assert entry["message"] == "Worker task failed"
    assert "private" not in str(entry)


def test_metadata_keeps_only_allowlisted_bounded_scalars() -> None:
    entry = build_error_entry(
        "RateLimitError",
        "External service rate limited the request",
        "abc123",
        True,
        message_safe=True,
        metadata={
            "module": "openai._exceptions",
            "status_code": 429,
            "error_code": "rate_limit_exceeded",
            "request_id": "req-123",
            "retry_after_s": 12.5,
            "body": {"secret": "must-not-survive"},
            "headers": {"authorization": "must-not-survive"},
            "prompt": "must-not-survive",
        },
    )

    assert entry["metadata"] == {
        "status_code": 429,
        "error_code": "rate_limit_exceeded",
        "request_id": "req-123",
        "module": "openai._exceptions",
        "retry_after_s": 12.5,
    }
    assert "must-not-survive" not in str(entry)


def test_retry_after_rejects_non_finite_or_unbounded_values() -> None:
    assert normalize_retry_after(math.nan) is None
    assert normalize_retry_after(math.inf) is None
    assert normalize_retry_after(-1) is None
    assert normalize_retry_after(86_401) is None
    assert normalize_retry_after(30) == 30.0


def test_retry_hint_rejects_strings_and_booleans():
    from symba.core.failure import normalize_retry_after

    assert normalize_retry_after("12") is None
    assert normalize_retry_after(True) is None
    assert normalize_retry_after(None) is None
