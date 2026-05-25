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

import logging

from .config import settings

# The AEF SDK itself writes the `sdk-list` Kafka header — its own entry for
# aef-tracing is added automatically (docs_for_SDK/instructions/tracing_usage.md
# §2.3). We do not enumerate langchain/langgraph here: those are informational
# inventory entries, not part of the obligatory contract for prototype agents.
# If ПСИ/ПРОМ audit requests them, add `sdk_list_headers=[SDKEntry(...), ...]`
# to the AEFKafkaSender(...) call.

logger = logging.getLogger(__name__)

_HANDLER: AEFHandler | None = None


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
]
