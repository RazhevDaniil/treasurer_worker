"""Helpers for cross-service `x-trace-id` propagation."""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from typing import Any

TRACE_HEADER_NAME = "x-trace-id"
TRACE_PAYLOAD_KEYS = ("x_trace_id", "trace_id", "run_id")


def _decode(value: Any) -> str | None:
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
    raw = _decode(value)
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
    for key, value in _iter_headers(headers):
        if str(key).lower() == TRACE_HEADER_NAME:
            return normalize_uuid4(value)
    return None


def trace_id_from_payload(payload: Mapping[str, Any] | None) -> str | None:
    if not payload:
        return None
    for key in TRACE_PAYLOAD_KEYS:
        if trace_id := normalize_uuid4(payload.get(key)):
            return trace_id
    headers_json = payload.get("headers_json")
    if isinstance(headers_json, Mapping):
        return extract_trace_id(headers_json)
    return None


def resolve_trace_id(
    value: Any = None,
    *,
    headers: Any = None,
    payload: Mapping[str, Any] | None = None,
) -> str:
    return (
        normalize_uuid4(value)
        or extract_trace_id(headers)
        or trace_id_from_payload(payload)
        or str(uuid.uuid4())
    )


def http_trace_headers(
    trace_id: str,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    headers = dict(extra or {})
    for key in list(headers):
        if str(key).lower() == TRACE_HEADER_NAME:
            headers.pop(key, None)
    headers[TRACE_HEADER_NAME] = trace_id
    return headers


def kafka_trace_headers(
    trace_id: str,
    extra: Iterable[tuple[str, bytes]] | None = None,
) -> list[tuple[str, bytes]]:
    headers: list[tuple[str, bytes]] = []
    for key, value in extra or ():
        if str(key).lower() == TRACE_HEADER_NAME:
            continue
        headers.append((str(key), value))
    headers.append((TRACE_HEADER_NAME, trace_id.encode("utf-8")))
    return headers


def payload_with_trace(payload: Mapping[str, Any], trace_id: str) -> dict:
    traced = dict(payload)
    traced["x_trace_id"] = trace_id
    traced["run_id"] = trace_id
    return traced
