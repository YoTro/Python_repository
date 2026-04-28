import asyncio
import functools
import logging
import random
from typing import Any, Callable, Optional, Tuple, Type

import requests

logger = logging.getLogger(__name__)

_SENTINEL = object()  # marks "no early-return value"


def exponential_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on_status: Tuple[int, ...] = (429,),
    retry_on_exceptions: Tuple[Type[Exception], ...] = (requests.RequestException,),
    jitter: bool = True,
    is_retryable: Optional[Callable[[requests.Response], bool]] = None,
    response_hook: Optional[Callable[[requests.Response], Any]] = None,
):
    """
    Decorator for async functions to perform exponential backoff retries.
    Specifically useful for Amazon SP-API and Ads-API rate limiting.

    The wrapped function should return a requests.Response object or raise.

    Extra parameters
    ----------------
    is_retryable(response) -> bool
        Called when response.status_code is NOT in retry_on_status.
        Return True to treat the response as retryable anyway (e.g. HTTP 200
        with {"code":"425"} in the body).

    response_hook(response) -> Any | _SENTINEL
        Called on every response before the retry/return decision.
        • Return a non-_SENTINEL value  → use it as the final result immediately
          (skips retry logic entirely — useful for extracting a duplicate ID).
        • Return _SENTINEL (default)    → proceed with normal retry/return logic.
        Import the sentinel as ``from src.core.utils.decorators import EARLY_RETURN``.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    result = await func(*args, **kwargs)

                    if isinstance(result, requests.Response):
                        # Optional hook: may return an early value (e.g. duplicate reportId)
                        if response_hook is not None:
                            hook_val = response_hook(result)
                            if hook_val is not _SENTINEL:
                                return hook_val

                        # Determine whether this response should trigger a retry
                        should_retry = result.status_code in retry_on_status
                        if not should_retry and is_retryable is not None:
                            should_retry = is_retryable(result)

                        if should_retry:
                            if attempt < max_retries:
                                delay = min(max_delay, base_delay * (2 ** attempt))
                                if jitter:
                                    delay *= 0.5 + random.random()
                                logger.warning(
                                    f"SP-API/Ads-API retryable response on {func.__name__} "
                                    f"(attempt {attempt + 1}/{max_retries}), retrying in {delay:.2f}s…"
                                )
                                await asyncio.sleep(delay)
                                continue
                            else:
                                logger.error(f"Max retries reached for {func.__name__}")
                                result.raise_for_status()

                        return result

                    return result

                except retry_on_exceptions as e:
                    last_exception = e

                    if isinstance(e, requests.HTTPError) and e.response is not None:
                        if e.response.status_code not in retry_on_status:
                            raise

                    if attempt == max_retries:
                        logger.error(f"Max retries reached for {func.__name__} after exception: {e}")
                        raise

                    delay = min(max_delay, base_delay * (2 ** attempt))
                    if jitter:
                        delay *= 0.5 + random.random()
                    logger.warning(
                        f"Retrying {func.__name__} due to {type(e).__name__}: {e} "
                        f"(attempt {attempt + 1}/{max_retries}, waiting {delay:.2f}s)"
                    )
                    await asyncio.sleep(delay)

            if last_exception:
                raise last_exception

        return wrapper
    return decorator


# Public sentinel so callers can signal "no early return" from response_hook
EARLY_RETURN = _SENTINEL
