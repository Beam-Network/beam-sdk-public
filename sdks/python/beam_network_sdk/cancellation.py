"""Cancellation tokens: the Python counterpart of the TypeScript SDK's ``AbortSignal``."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from beam_network_sdk.exceptions import BeamCancelledError

logger = logging.getLogger("beam_network_sdk.cancellation")


class BeamCancellationToken:
    """A thread-safe, one-shot cancellation signal.

    Pass one as ``ownership=`` to ``prepare_provider_transfer`` /
    ``resume_provider_transfer`` to fence a transfer's owner, or as
    ``cancellation=`` to the provider helpers. ``cancel()`` may be called from any
    thread; callbacks run synchronously in the cancelling thread.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reason: BaseException | None = None
        self._callbacks: list[Callable[[], None]] = []

    @property
    def cancelled(self) -> bool:
        return self._reason is not None

    @property
    def reason(self) -> BaseException | None:
        """The exception ``raise_if_cancelled`` raises, once cancelled."""
        return self._reason

    def cancel(self, reason: BaseException | str | None = None) -> None:
        """Cancel once; later calls are ignored. ``reason`` becomes the raised error."""
        if isinstance(reason, BaseException):
            error = reason
        else:
            error = BeamCancelledError(reason or "operation cancelled")
        with self._lock:
            if self._reason is not None:
                return
            self._reason = error
            callbacks = self._callbacks
            self._callbacks = []
        for callback in callbacks:
            try:
                callback()
            except Exception:
                logger.exception("beam cancellation callback failed")

    def raise_if_cancelled(self) -> None:
        reason = self._reason
        if reason is not None:
            raise reason

    def add_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Run ``callback`` on cancellation (immediately if already cancelled).

        Returns a function that unregisters the callback.
        """
        with self._lock:
            if self._reason is None:
                self._callbacks.append(callback)
                registered = True
            else:
                registered = False
        if not registered:
            callback()

        def remove() -> None:
            with self._lock:
                if callback in self._callbacks:
                    self._callbacks.remove(callback)

        return remove


def raise_if_cancelled(token: BeamCancellationToken | None) -> None:
    if token is not None:
        token.raise_if_cancelled()
