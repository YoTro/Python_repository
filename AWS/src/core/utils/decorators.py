import asyncio
import functools
import logging
import random
from collections.abc import Callable

import requests

from src.core.errors import RetryableError

logger = logging.getLogger(__name__)


def exponential_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on_exceptions: tuple[type[Exception], ...] = (
        requests.ConnectionError,
        requests.Timeout,
    ),
    jitter: bool = True,
):
    """
    Decorator for async functions that perform exponential backoff retries.

    The wrapped function signals a transient failure by raising RetryableError
    (from src.core.errors). Any other AWSBaseError propagates immediately.
    Pure network-level failures matching retry_on_exceptions are also retried.
    """

    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)

                except RetryableError as e:
                    last_exception = e
                    if attempt == max_retries:
                        raise
                    delay = min(max_delay, base_delay * (2**attempt))
                    if jitter:
                        delay *= 0.5 + random.random()
                    logger.warning(
                        f"Retrying {func.__name__} [{e.code}] "
                        f"(attempt {attempt + 1}/{max_retries}, waiting {delay:.2f}s)"
                    )
                    await asyncio.sleep(delay)

                except retry_on_exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        raise
                    delay = min(max_delay, base_delay * (2**attempt))
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
