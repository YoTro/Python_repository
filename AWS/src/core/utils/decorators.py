import asyncio
import functools
import logging
import random
from typing import Any, Callable, Type, Tuple, Optional

import requests

logger = logging.getLogger(__name__)

def exponential_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on_status: Tuple[int, ...] = (429,),
    retry_on_exceptions: Tuple[Type[Exception], ...] = (requests.RequestException,),
    jitter: bool = True
):
    """
    Decorator for async functions to perform exponential backoff retries.
    Specifically useful for Amazon SP-API and Ads-API 429 rate limiting.
    
    The wrapped function should ideally return a requests.Response object
    or raise an exception.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    
                    # If the function returns a Response, check for retryable status codes
                    if isinstance(result, requests.Response):
                        if result.status_code in retry_on_status:
                            if attempt < max_retries:
                                delay = min(max_delay, base_delay * (2 ** attempt))
                                if jitter:
                                    delay = delay * (0.5 + random.random())
                                
                                logger.warning(
                                    f"SP-API/Ads-API {result.status_code} on {func.__name__}, "
                                    f"retrying {attempt + 1}/{max_retries} in {delay:.2f}s..."
                                )
                                await asyncio.sleep(delay)
                                continue
                            else:
                                logger.error(f"Max retries reached for {func.__name__} with status {result.status_code}")
                                result.raise_for_status()
                        
                        # If we reached here, it's either not a retryable status or we want to return it
                        return result
                    
                    return result
                    
                except retry_on_exceptions as e:
                    last_exception = e
                    
                    # If it's an HTTPError, only retry if the status code matches
                    if isinstance(e, requests.HTTPError) and e.response is not None:
                        if e.response.status_code not in retry_on_status:
                            raise
                    
                    if attempt == max_retries:
                        logger.error(f"Max retries reached for {func.__name__} after exception: {e}")
                        raise
                    
                    delay = min(max_delay, base_delay * (2 ** attempt))
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.warning(
                        f"Retrying {func.__name__} due to {type(e).__name__}: {e} "
                        f"(attempt {attempt + 1}/{max_retries}, waiting {delay:.2f}s)"
                    )
                    await asyncio.sleep(delay)
                    
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator
