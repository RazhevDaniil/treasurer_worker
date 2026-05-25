"""Trace propagation helpers for approve_app.

approve_app is a Kafka/HTTP orchestrator, so it must preserve the parent
operation UID while it calls db-service, CalcFundCost, agent_treasurer and the
external result topic. The canonical transport key is `x-trace-id` and the
value is always a UUID v4.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

from structlog.contextvars import bind_contextvars, unbind_contextvars

TRACE_HEADER_NAME = "x-trace-id"
_trace_id_cvar: ContextVar[str | None] = ContextVar("approve_trace_id", default=None)


def _decode_header_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return str(value)


def _iter_headers(headers: Any) -> Iterable[tuple[str, Any]]:
    if headers is None:
        return ()
    if isinstance(headers, Mapping):
        return headers.items()
    return headers


def normalize_uuid4(value: Any) -> str | None:
    """Return a canonical UUID v4 string or None for missing/invalid values."""
    raw = _decode_header_value(value)
    if not raw:
        return None
    try:
        parsed = uuid.UUID(raw.strip())
    except (AttributeError, TypeError, ValueError):
        return None
    if parsed.version != 4:
        return None
    return str(parsed)


def extract_trace_id(headers: Any) -> str | None:
    """Extract and validate `x-trace-id` from HTTP/Kafka headers."""
    for key, value in _iter_headers(headers):
        if str(key).lower() == TRACE_HEADER_NAME:
            return normalize_uuid4(value)
    return None


def resolve_trace_id(
    value: Any = None,
    *,
    headers: Any = None,
    fallback: Any = None,
) -> str:
    """Resolve a valid operation UID, preferring explicit value then headers."""
    return (
        normalize_uuid4(value)
        or extract_trace_id(headers)
        or normalize_uuid4(fallback)
        or str(uuid.uuid4())
    )


def current_trace_id() -> str | None:
    return normalize_uuid4(_trace_id_cvar.get())


def ensure_current_trace_id(fallback: Any = None) -> str:
    trace_id = current_trace_id() or resolve_trace_id(fallback=fallback)
    _trace_id_cvar.set(trace_id)
    return trace_id


def trace_header_dict(
    extra: Mapping[str, str] | None = None,
    *,
    trace_id: Any = None,
) -> dict[str, str]:
    """HTTP headers carrying the current operation UID."""
    headers = dict(extra or {})
    for key in list(headers):
        if str(key).lower() == TRACE_HEADER_NAME:
            headers.pop(key, None)
    headers[TRACE_HEADER_NAME] = resolve_trace_id(trace_id or current_trace_id())
    return headers


def trace_header_list(
    extra: Iterable[tuple[str, Any]] | Mapping[str, Any] | None = None,
    *,
    trace_id: Any = None,
) -> list[tuple[str, bytes]]:
    """Kafka headers carrying the current operation UID."""
    headers: list[tuple[str, bytes]] = []
    for key, value in _iter_headers(extra):
        key_str = str(key)
        if key_str.lower() == TRACE_HEADER_NAME:
            continue
        decoded = _decode_header_value(value)
        headers.append((key_str, (decoded or "").encode("utf-8")))
    headers.append((
        TRACE_HEADER_NAME,
        resolve_trace_id(trace_id or current_trace_id()).encode("utf-8"),
    ))
    return headers


@contextmanager
def bound_trace(**kw: str) -> Iterator[None]:
    """Temporarily bind values to contextvars; unbind on exit.

    Consumers обрабатывают сообщения последовательно в одном потоке — без явного
    unbind ID одного сообщения утечёт в логи следующего.
    """
    token = None
    if "trace_id" in kw:
        trace_id = resolve_trace_id(kw["trace_id"])
        kw["trace_id"] = trace_id
        token = _trace_id_cvar.set(trace_id)

    bind_contextvars(**kw)
    try:
        yield
    finally:
        unbind_contextvars(*kw.keys())
        if token is not None:
            _trace_id_cvar.reset(token)
