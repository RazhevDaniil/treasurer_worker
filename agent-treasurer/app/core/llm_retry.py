"""LLM retry helpers — SECURITY §22 (retry) / §23 (typed degradation events).

Wraps GigaChat invocations in `tenacity` so transport-level failures,
timeout and HTTP 500/502/503/504 are retried with exponential-jitter backoff
and each attempt is classified into a `gigachat_*` audit event. The outer `except`
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
from .tracing import record_hop

logger = logging.getLogger(__name__)

GIGAPLATFORM_STOP_MESSAGE = (
    "The service is temporarily unavailable due to technical reasons."
)
GIGAPLATFORM_STOP_EVENT = "gigaplatform_stop_event"


# Retry only transient categories — 500/502/503/504, timeout, transport.
# Validation / 4xx-non-429 / programming errors fall straight through to the
# node-level fallback so we don't waste backoff on permanent failures.
_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}
_RETRYABLE_EVENTS = {
    "gigachat_5xx_failed",
    "gigachat_timeout",
    "gigachat_transport_error",
}


def _status_code(exc: BaseException) -> int | None:
    status = getattr(exc, "status_code", None)
    if status is None:
        response = getattr(exc, "response", None)
        if response is not None:
            status = getattr(response, "status_code", None)
    return status if isinstance(status, int) else None


def _exception_text(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    parts = [str(exc)]
    if response is not None:
        for attr in ("text", "content"):
            value = getattr(response, attr, None)
            if value is None:
                continue
            if isinstance(value, bytes):
                try:
                    value = value.decode("utf-8")
                except UnicodeDecodeError:
                    continue
            parts.append(str(value))
        try:
            parts.append(str(response.json()))
        except Exception:
            pass
    return " ".join(parts)


def is_gigaplatform_stop_event(exc: BaseException) -> bool:
    """True when GigaPlatform deliberately blocks this agent traffic class."""
    return _status_code(exc) == 403 and GIGAPLATFORM_STOP_MESSAGE in _exception_text(exc)


def classify_gigachat_error(exc: BaseException) -> str:
    """Map a GigaChat / transport exception to a §23 audit event name."""
    status = _status_code(exc)

    if is_gigaplatform_stop_event(exc):
        return GIGAPLATFORM_STOP_EVENT
    if status == 429:
        return "gigachat_rate_limited"
    if status in _RETRYABLE_STATUS_CODES:
        return "gigachat_5xx_failed"
    if isinstance(status, int) and 500 <= status < 600:
        return "gigachat_response_error"
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
        f"llm_retry_wait. event={event} attempt={retry_state.attempt_number} "
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
            exp_base=settings.llm_retry_exp_base,
            jitter=settings.llm_retry_jitter,
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
            exp_base=settings.llm_retry_exp_base,
            jitter=settings.llm_retry_jitter,
        ),
        before_sleep=_log_llm_retry,
        reraise=True,
    )


async def llm_ainvoke_with_retry(
    runnable,
    messages,
    *,
    purpose: str,
    trace_span=None,
):
    """Invoke a LangChain/GigaChat runnable through the shared LLM retry policy."""
    async for attempt in llm_retrying_async():
        attempt_number = attempt.retry_state.attempt_number
        logger.info(
            f"llm_retry_attempt_start. purpose={purpose} attempt={attempt_number}"
        )
        if trace_span is not None:
            hop = record_hop()
            trace_span.add_span_attributes(**{
                "aef.hops_used": hop,
                "aef.llm_attempt": attempt_number,
            })
        with attempt:
            try:
                result = await runnable.ainvoke(messages)
            except Exception as exc:
                event = classify_gigachat_error(exc)
                if trace_span is not None and is_gigaplatform_stop_event(exc):
                    trace_span.add_span_attributes(**{"aef.stop_event": GIGAPLATFORM_STOP_EVENT})
                logger.warning(
                    f"llm_retry_attempt_error. purpose={purpose} attempt={attempt_number} "
                    f"event={event} exc_type={type(exc).__name__} retryable={_should_retry_llm(exc)}"
                )
                raise
            logger.info(
                f"llm_retry_attempt_success. purpose={purpose} attempt={attempt_number}"
            )
            return result

    raise RuntimeError(f"LLM retry loop exited without result: {purpose}")
