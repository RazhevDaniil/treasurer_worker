"""Shared outbound HTTP retry/backoff helper.

RR-AI-5 / ТН05_retry: retry policy lives in the infrastructural HTTP layer,
not in business services or graph nodes. Only transient transport failures
and HTTP 500/502/503/504 are retried; 4xx statuses, including 408/429, are
returned to the caller without retry so business code can handle them.
"""

import logging
from collections.abc import Mapping
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from .config import settings
from .tracing import record_hop, safe_trace_json

logger = logging.getLogger(__name__)

RETRYABLE_HTTP_STATUS_CODES = {500, 502, 503, 504}


def _status_code(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None) if response is not None else None
    return status if isinstance(status, int) else None


def _is_retryable_http_exception(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return _status_code(exc) in RETRYABLE_HTTP_STATUS_CODES
    return isinstance(exc, httpx.RequestError)


def _log_http_wait(retry_state: RetryCallState, *, operation_name: str) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    next_wait_sec = round(retry_state.next_action.sleep, 3) if retry_state.next_action else None
    status_code = _status_code(exc) if exc else None
    logger.warning(
        f"http_retry_wait. operation={operation_name} attempt={retry_state.attempt_number} "
        f"next_wait_sec={next_wait_sec} exc_type={type(exc).__name__ if exc else None} "
        f"status_code={status_code} will_retry=True"
    )


def _response_trace_payload(response: httpx.Response) -> dict[str, Any]:
    try:
        body: Any = response.json()
    except Exception:
        body = response.text
    return {
        "status_code": response.status_code,
        "body": body,
    }


def _raise_for_retryable_status(response: httpx.Response) -> None:
    if response.status_code not in RETRYABLE_HTTP_STATUS_CODES:
        return
    raise httpx.HTTPStatusError(
        f"retryable HTTP status {response.status_code}",
        request=response.request,
        response=response,
    )


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    operation_name: str,
    trace_span: Any = None,
    headers: Mapping[str, str] | None = None,
    **kwargs: Any,
) -> httpx.Response:
    """Perform one outbound HTTP call through the shared bounded retry policy."""
    retrying = AsyncRetrying(
        retry=retry_if_exception(_is_retryable_http_exception),
        stop=stop_after_attempt(settings.http_max_retries),
        wait=wait_exponential_jitter(
            initial=settings.http_retry_base,
            max=settings.http_retry_max,
            exp_base=settings.http_retry_exp_base,
            jitter=settings.http_retry_jitter,
        ),
        before_sleep=lambda state: _log_http_wait(state, operation_name=operation_name),
        reraise=True,
    )

    async for attempt in retrying:
        with attempt:
            return await _request_once(
                client,
                method,
                url,
                operation_name=operation_name,
                trace_span=trace_span,
                attempt_number=attempt.retry_state.attempt_number,
                headers=headers,
                **kwargs,
            )

    raise RuntimeError(f"HTTP retry loop exited without response: {operation_name}")


async def _request_once(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    operation_name: str,
    trace_span: Any = None,
    attempt_number: int,
    headers: Mapping[str, str] | None = None,
    **kwargs: Any,
) -> httpx.Response:
    hop = record_hop()
    if trace_span is not None:
        trace_span.add_span_attributes(**{
            "aef.hops_used": hop,
            "aef.http_attempt": attempt_number,
        })
    logger.info(
        f"http_retry_attempt_start. operation={operation_name} "
        f"attempt={attempt_number} method={method}"
    )

    try:
        response = await client.request(method, url, headers=headers, **kwargs)
        if trace_span is not None:
            trace_span.add_span_attributes(**{
                "aef.http_status_code": response.status_code,
                "aef.response_payload": safe_trace_json(_response_trace_payload(response)),
            })
        _raise_for_retryable_status(response)
    except httpx.HTTPError as exc:
        retryable = _is_retryable_http_exception(exc)
        logger.warning(
            f"http_retry_attempt_error. operation={operation_name} "
            f"attempt={attempt_number} exc_type={type(exc).__name__} "
            f"status_code={_status_code(exc)} retryable={retryable}"
        )
        raise

    logger.info(
        f"http_retry_attempt_success. operation={operation_name} "
        f"attempt={attempt_number} status_code={response.status_code}"
    )
    return response
