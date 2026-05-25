"""LLM retry helpers — SECURITY §22 (retry) / §23 (typed degradation events).

Wraps GigaChat invocations in `tenacity` so transport-level failures (429,
5xx, timeout, transport) are retried with exponential-jitter backoff and each
attempt is classified into a `gigachat_*` audit event. The outer `except`
blocks in calling nodes preserve existing fallback behaviour after exhaustion.
"""

from __future__ import annotations

import asyncio

import logging

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from .config import settings

logger = logging.getLogger(__name__)


# Retry only transient categories — 429, 5xx, timeout, transport.
# Validation / 4xx-non-429 / programming errors fall straight through to the
# node-level fallback so we don't waste backoff on permanent failures.
_RETRYABLE_EVENTS = {
    "gigachat_rate_limited",
    "gigachat_5xx_failed",
    "gigachat_timeout",
    "gigachat_transport_error",
}


def classify_gigachat_error(exc: BaseException) -> str:
    """Map a GigaChat / transport exception to a §23 audit event name."""
    status = getattr(exc, "status_code", None)
    if status is None:
        response = getattr(exc, "response", None)
        if response is not None:
            status = getattr(response, "status_code", None)

    if status == 429:
        return "gigachat_rate_limited"
    if isinstance(status, int) and 500 <= status < 600:
        return "gigachat_5xx_failed"
    if isinstance(status, int) and 400 <= status < 500:
        return "gigachat_response_error"

    if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError, TimeoutError)):
        return "gigachat_timeout"
    if isinstance(exc, (httpx.ConnectError, httpx.RemoteProtocolError, ConnectionError, OSError)):
        return "gigachat_transport_error"

    return "gigachat_unknown_error"


def _log_llm_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    event = classify_gigachat_error(exc) if exc else "gigachat_unknown_error"
    next_wait_sec = round(retry_state.next_action.sleep, 3) if retry_state.next_action else None
    exc_type = type(exc).__name__ if exc else None
    exc_str = str(exc) if exc else None
    logger.warning(
        f"{event}. attempt={retry_state.attempt_number} "
        f"next_wait_sec={next_wait_sec} exc_type={exc_type} exc={exc_str} will_retry=True"
    )


def log_llm_exhausted(exc: BaseException, *, purpose: str) -> None:
    """Emit a typed §23 audit event after retries are exhausted.

    Call this from the outer `except` block before falling back to a
    deterministic default — gives observability without altering control flow.
    """
    logger.error(
        f"{classify_gigachat_error(exc)}. purpose={purpose} "
        f"exc_type={type(exc).__name__} exc={exc} will_retry=False"
    )


def _should_retry_llm(exc: BaseException) -> bool:
    return classify_gigachat_error(exc) in _RETRYABLE_EVENTS


def llm_retrying_async() -> AsyncRetrying:
    return AsyncRetrying(
        retry=retry_if_exception(_should_retry_llm),
        stop=stop_after_attempt(settings.llm_max_retries),
        wait=wait_exponential_jitter(
            initial=settings.llm_retry_base,
            max=settings.llm_retry_max,
        ),
        before_sleep=_log_llm_retry,
        reraise=True,
    )


def llm_retrying_sync() -> Retrying:
    return Retrying(
        retry=retry_if_exception(_should_retry_llm),
        stop=stop_after_attempt(settings.llm_max_retries),
        wait=wait_exponential_jitter(
            initial=settings.llm_retry_base,
            max=settings.llm_retry_max,
        ),
        before_sleep=_log_llm_retry,
        reraise=True,
    )
