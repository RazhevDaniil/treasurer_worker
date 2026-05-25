"""AEF Tracing SDK bootstrap and re-exports for the treasurer agent.

Replaces the prior structlog-based `tool_call`/`llm_call`/`trace_headers`
layer. The SDK is a hard dependency — missing import means the agent
must not start (see SECURITY_COMPLIANCE_SDK.md §1).

Module surface:
- `init_tracing()`        — build provider/sender/exporter/handler once
                            inside the FastAPI `lifespan` startup hook.
- `get_aef_handler()`     — return the singleton `AEFHandler` to plug
                            into `graph.ainvoke(config={"callbacks": [...]})`
                            and `GigaChat(callbacks=[...])`.
- `aef_input_request`, `aef_agent_start`, `aef_kafka_produce`,
  `aef_kafka_consume`, `aef_custom_span`, `aef_observation` —
  re-exported context managers / decorator (see docs_for_SDK/).
- `session_id_cvar`       — contextvar carrying the request-scoped
                            session_id (chat_id / thread_id) into every
                            span produced inside the request.
"""

import logging
import uuid
from contextvars import ContextVar
from collections.abc import Mapping
from typing import Any, Iterable

from aef_tracing import (
    AEFBatchSpanProcessor,
    AEFHandler,
    AEFTracerProvider,
    aef_agent_start,
    aef_custom_span,
    aef_input_request,
    aef_kafka_consume,
    aef_kafka_produce,
    aef_observation,
)
from aef_tracing.exporters import AEFKafkaSender, AEFProtobufSenderExporter
from aef_tracing.span_processors import session_id_cvar

from .config import settings

# The AEF SDK itself writes the `sdk-list` Kafka header — its own entry for
# aef-tracing is added automatically (docs_for_SDK/instructions/tracing_usage.md
# §2.3). We do not enumerate langchain/langgraph here: those are informational
# inventory entries, not part of the obligatory contract for prototype agents.
# If ПСИ/ПРОМ audit requests them, add `sdk_list_headers=[SDKEntry(...), ...]`
# to the AEFKafkaSender(...) call.

logger = logging.getLogger(__name__)

_HANDLER: AEFHandler | None = None
X_TRACE_ID_HEADER = "x-trace-id"
x_trace_id_cvar: ContextVar[str | None] = ContextVar("x_trace_id", default=None)


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
    """Return canonical UUID v4 string or None for missing/invalid values."""
    raw = _decode_header_value(value)
    if not raw:
        return None
    try:
        parsed = uuid.UUID(raw)
    except (TypeError, ValueError, AttributeError):
        return None
    if parsed.version != 4:
        return None
    return str(parsed)


def extract_x_trace_id(headers: Any) -> str | None:
    """Extract `x-trace-id` from HTTP/Kafka headers and validate it as UUID v4."""
    for key, value in _iter_headers(headers):
        if str(key).lower() == X_TRACE_ID_HEADER:
            return normalize_uuid4(value)
    return None


def ensure_x_trace_id(value: Any = None) -> str:
    """Validate an incoming UID or create a fresh UUID v4 for this operation."""
    return normalize_uuid4(value) or str(uuid.uuid4())


def bind_x_trace_id(trace_id: Any = None, headers: Any = None) -> str:
    """Resolve and store the current operation UID in a context variable."""
    resolved = normalize_uuid4(trace_id) or extract_x_trace_id(headers) or str(uuid.uuid4())
    x_trace_id_cvar.set(resolved)
    return resolved


def current_x_trace_id() -> str:
    """Return the current operation UID, creating one if the context is empty."""
    trace_id = normalize_uuid4(x_trace_id_cvar.get())
    if trace_id is None:
        trace_id = str(uuid.uuid4())
        x_trace_id_cvar.set(trace_id)
    return trace_id


def trace_header_dict(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """HTTP headers carrying the current operation UID."""
    headers = dict(extra or {})
    for key in list(headers):
        if str(key).lower() == X_TRACE_ID_HEADER:
            headers.pop(key, None)
    headers[X_TRACE_ID_HEADER] = current_x_trace_id()
    return headers


def kafka_trace_headers(extra: Iterable[tuple[str, Any]] | Mapping[str, Any] | None = None) -> list[tuple[str, bytes]]:
    """Kafka headers carrying the current operation UID."""
    headers: list[tuple[str, bytes]] = []
    for key, value in _iter_headers(extra):
        key_str = str(key)
        if key_str.lower() == X_TRACE_ID_HEADER:
            continue
        decoded = _decode_header_value(value)
        headers.append((key_str, (decoded or "").encode("utf-8")))
    headers.append((X_TRACE_ID_HEADER, current_x_trace_id().encode("utf-8")))
    return headers


def init_tracing() -> AEFHandler:
    """Initialise the AEF tracer provider, Kafka sender, batch exporter
    and the langchain `AEFHandler`. Idempotent on repeated calls.

    Returns the handler so the caller can pass it into
    `graph.ainvoke(config={"callbacks": [handler]})` or
    `GigaChat(..., callbacks=[handler])`.
    """
    global _HANDLER
    if _HANDLER is not None:
        return _HANDLER

    logger.info(
        "aef_tracing_config. kafka_hosts=%r outbox_topic=%r "
        "security_protocol=%r max_request_size=%d "
        "agent_id=%r cluster_id=%r namespace=%r distributive=%r",
        settings.kafka_hosts,
        settings.tracing_service_kafka_outbox_topic,
        settings.aef_kafka_security_protocol,
        settings.aef_kafka_max_request_size,
        settings.aef_agent_id,
        settings.aef_cluster_id,
        settings.aef_namespace,
        settings.aef_distributive,
    )

    sender = AEFKafkaSender(
        outbox_topic=settings.tracing_service_kafka_outbox_topic,
        kafka_producer_config={
            "bootstrap_servers": settings.kafka_hosts,
            "security_protocol": settings.aef_kafka_security_protocol,
            "max_request_size": settings.aef_kafka_max_request_size,
        },
        headers={
            "agent-id": settings.aef_agent_id,
            "cluster-id": settings.aef_cluster_id,
            "namespace": settings.aef_namespace,
            "distributive": settings.aef_distributive,
        },
    )
    provider = AEFTracerProvider()
    provider.add_span_processor(
        AEFBatchSpanProcessor(AEFProtobufSenderExporter(senders=[sender]))
    )
    _HANDLER = AEFHandler(tracer=provider.get_tracer(__name__))
    logger.info("AEF Tracing initialized successfully")
    return _HANDLER


def get_aef_handler() -> AEFHandler:
    """Return the singleton handler. Raises if `init_tracing()` wasn't
    called yet — agent code must call it in the FastAPI lifespan startup."""
    if _HANDLER is None:
        raise RuntimeError(
            "AEF tracing not initialised — call init_tracing() in lifespan first"
        )
    return _HANDLER


__all__ = [
    "init_tracing",
    "get_aef_handler",
    "aef_input_request",
    "aef_agent_start",
    "aef_kafka_produce",
    "aef_kafka_consume",
    "aef_custom_span",
    "aef_observation",
    "session_id_cvar",
    "X_TRACE_ID_HEADER",
    "x_trace_id_cvar",
    "bind_x_trace_id",
    "current_x_trace_id",
    "ensure_x_trace_id",
    "extract_x_trace_id",
    "kafka_trace_headers",
    "trace_header_dict",
]
