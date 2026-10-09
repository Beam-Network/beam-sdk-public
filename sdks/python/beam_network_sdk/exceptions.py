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


STORAGE_ACCESS_ERROR_CODES = ("source_access_denied", "destination_access_denied")


class BeamTransferFailedError(BeamTransferError):
    """BeamCore reported the transfer as failed.

    ``error_message`` is BeamCore's message for the transfer, verbatim (``None`` when it
    sent none).
    """

    def __init__(self, transfer_id: str, error_message: str | None):
        self.error_message = error_message
        self.transfer_id = transfer_id
        BeamError.__init__(
            self, f"Transfer {transfer_id} failed: {error_message or 'unknown error'}"
        )


class BeamStorageAccessError(BeamTransferFailedError):
    """The source or destination storage refused Beam's requests.

    ``code`` is ``source_access_denied`` or ``destination_access_denied`` and
    ``error_message`` carries BeamCore's explanation verbatim.
    """

    def __init__(self, transfer_id: str, error_message: str, code: str):
        self.code = code
        super().__init__(transfer_id, error_message)


def transfer_failed_error(transfer_id: str, error_message: str | None) -> BeamTransferFailedError:
    """Build the error for a failed transfer from its ``error_message``.

    Returns a :class:`BeamStorageAccessError` when the message starts with a storage access
    code (the text before the first ``:``), otherwise a :class:`BeamTransferFailedError`.
    """
    if error_message is not None:
        code = error_message.split(":", 1)[0].strip()
        if code in STORAGE_ACCESS_ERROR_CODES:
            return BeamStorageAccessError(transfer_id, error_message, code)
    return BeamTransferFailedError(transfer_id, error_message)


class BeamRouteRecoveryPendingError(BeamTransferError):
    """The transfer is prepared and its in-memory route recovery lease is active."""

    def __init__(self, transfer_id: str, cause: BaseException):
        self.cause = cause
        super().__init__(transfer_id, "route recovery is continuing in the background")


class BeamProviderTransferError(BeamTransferError):
    """A provider-backed transfer failed without a recoverable route stream.

    The SDK has already asked BeamCore to cancel the transfer and aborted the
    multipart uploads it created. ``errors`` lists the original failure followed by
    any cancellation or cleanup failure; the two flags report whether those steps
    succeeded. Messages carry only error types and status codes, never signed URLs.
    """

    def __init__(
        self,
        transfer_id: str,
        cause: BaseException,
        *,
        cancel_error: BaseException | None = None,
        cleanup_error: BaseException | None = None,
    ):
        self.cause = cause
        self.errors: list[BaseException] = [cause]
        if cancel_error is not None:
            self.errors.append(cancel_error)
        if cleanup_error is not None:
            self.errors.append(cleanup_error)
        self.transfer_cancelled = cancel_error is None
        self.multipart_cleanup_complete = cleanup_error is None
        self.transfer_id = transfer_id
        BeamError.__init__(
            self,
            f"provider transfer failed for {transfer_id} "
            f"(transfer_cancelled={str(self.transfer_cancelled).lower()}, "
            f"multipart_cleanup_complete={str(self.multipart_cleanup_complete).lower()})",
        )


class BeamCancelledError(BeamError):
    """An operation stopped because its cancellation token or ownership fence fired."""


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
