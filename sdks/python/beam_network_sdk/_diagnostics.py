"""Allowlisted diagnostics for SDK logs."""

from __future__ import annotations

import hashlib
import hmac
import os
from typing import Any

from beam_network_sdk.exceptions import BeamAPIError

_SAFE_PROVIDER_CODES = (
    "NoSuchUpload",
    "InvalidPart",
    "InvalidPartOrder",
    "AccessDenied",
    "RequestTimeout",
    "SlowDown",
    "ServiceUnavailable",
    "InternalError",
    "ECONNRESET",
    "ETIMEDOUT",
    "EAI_AGAIN",
)


def opaque_correlation_token(operation: str, *values: object) -> str:
    key = os.environ.get("BEAM_LOG_CORRELATION_KEY", "beam-sdk-runtime").encode("utf-8")
    fingerprint = "\0".join((operation, *(str(value) for value in values)))
    return hmac.new(key, fingerprint.encode("utf-8"), hashlib.sha256).hexdigest()[:24]


def safe_error_diagnostic(error: BaseException, *, retryable: bool) -> dict[str, Any]:
    """Return fields safe for logs without exposing provider request material."""
    message = str(error)
    explicit_code = str(getattr(error, "code", "") or "")
    provider_code = next(
        (code for code in _SAFE_PROVIDER_CODES if code == explicit_code or code in message),
        type(error).__name__,
    )
    status = error.status_code if isinstance(error, BeamAPIError) else None
    fingerprint = f"{type(error).__name__}\0{explicit_code}\0{status or ''}\0{message}"
    diagnostic: dict[str, Any] = {
        "provider_error_code": provider_code,
        "retryable": retryable,
        "correlation_token": opaque_correlation_token("sdk_error", fingerprint),
    }
    if isinstance(status, int) and 100 <= status <= 599:
        diagnostic["http_status"] = status
    return diagnostic
