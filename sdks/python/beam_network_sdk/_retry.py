"""Async retry helper with exponential backoff and jitter."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from beam_network_sdk._diagnostics import safe_error_diagnostic
from beam_network_sdk.exceptions import BeamAPIError, BeamTimeoutError

logger = logging.getLogger("beam_network_sdk.retry")

T = TypeVar("T")

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


async def retry_async(
    fn: Callable[..., Awaitable[T]],
    *args: Any,
    max_retries: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    **kwargs: Any,
) -> T:
    """Call *fn* with retries on transient failures.

    Retries on:
      - ``BeamAPIError`` with status 429/500/502/503/504
      - ``BeamTimeoutError``

    Does NOT retry on client errors (400/401/403/404) or other exceptions.
    """
    last_exc: BaseException | None = None
    for attempt in range(1 + max_retries):
        try:
            return await fn(*args, **kwargs)
        except BeamAPIError as exc:
            if exc.status_code not in _RETRYABLE_STATUS_CODES:
                raise
            last_exc = exc
        except BeamTimeoutError as exc:
            last_exc = exc

        if attempt < max_retries:
            delay = min(base_delay * (2**attempt), max_delay)
            delay *= 0.5 + random.random()  # jitter: 50%-150% of computed delay
            logger.warning(
                "retry attempt=%d max_attempts=%d delay_s=%.1f diagnostic=%s",
                attempt + 1,
                max_retries,
                delay,
                safe_error_diagnostic(last_exc, retryable=True),
            )
            await asyncio.sleep(delay)

    if last_exc is None:
        raise BeamAPIError(500, "Retry logic error: no exception captured")
    logger.error(
        "retries_exhausted attempts=%d diagnostic=%s",
        max_retries,
        safe_error_diagnostic(last_exc, retryable=False),
    )
    raise last_exc
