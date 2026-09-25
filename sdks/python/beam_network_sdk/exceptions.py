"""BEAM SDK exceptions."""

from __future__ import annotations

from typing import Any


class BeamError(Exception):
    """Base exception for all BEAM SDK errors."""


class BeamAuthError(BeamError):
    """Authentication or signing failure."""


class BeamAPIError(BeamError):
    """HTTP error returned by a BEAM endpoint."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        *,
        url: str | None = None,
        body: Any | None = None,
    ):
        self.status_code = status_code
        self.detail = detail
        self.url = url
        self.body = body
        super().__init__(f"HTTP {status_code}: {detail}")


class BeamTimeoutError(BeamError):
    """Request timed out."""


class BeamTransferError(BeamError):
    """Error related to a specific transfer."""

    def __init__(self, transfer_id: str, message: str):
        self.transfer_id = transfer_id
        super().__init__(f"Transfer {transfer_id}: {message}")


class BeamRouteRecoveryPendingError(BeamTransferError):
    """The transfer is prepared and its in-memory route recovery lease is active."""

    def __init__(self, transfer_id: str, cause: BaseException):
        self.cause = cause
        super().__init__(transfer_id, "route recovery is continuing in the background")


class BeamChecksumError(BeamError):
    """Checksum verification failure."""

    def __init__(self, expected: str, actual: str, context: str = ""):
        self.expected = expected
        self.actual = actual
        self.context = context
        msg = f"Checksum mismatch: expected {expected[:16]}..., got {actual[:16]}..."
        if context:
            msg = f"{context}: {msg}"
        super().__init__(msg)
