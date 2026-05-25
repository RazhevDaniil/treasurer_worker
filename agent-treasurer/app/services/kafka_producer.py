"""Kafka producer for KAFKA_IN_TOPIC — publishes approval results back to approve_app."""

import json
from datetime import datetime, timezone
import threading

import logging
from confluent_kafka import KafkaException, Producer

from ..core.config import settings
from ..core.tracing import (
    aef_kafka_produce,
    kafka_trace_headers,
    record_hop,
    safe_span_attributes,
    safe_span_output,
    safe_trace_json,
)
from ..models.approve_schemas import AgentDecision, AgentResult

logger = logging.getLogger(__name__)


class AgentResultPublishError(RuntimeError):
    """Raised when Kafka does not acknowledge an approval result."""


class AgentResultProducer:
    """Publishes AgentResult to KAFKA_IN_TOPIC."""

    def __init__(self) -> None:
        # SECURITY §22: idempotent producer with bounded retry/backoff.
        # `enable.idempotence` forces acks=all and prevents duplicate delivery
        # under retries; `retries` / `retry.backoff.ms*` cap the broker-side
        # recovery attempts before the delivery callback reports failure.
        self._producer = Producer({
            "bootstrap.servers": settings.adapter_brokers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": settings.kafka_producer_retries,
            "retry.backoff.ms": settings.kafka_retry_backoff_ms,
            "retry.backoff.max.ms": settings.kafka_retry_backoff_max_ms,
        })

    def close(self) -> None:
        self._producer.flush(timeout=10)

    def send_result(
        self,
        *,
        task_id: str,
        calculation_id: str,
        decision: str,
        reason: str,
        agent_version: str,
    ) -> None:
        """Publish approval result. decision is "COMPLETED" or "REJECTED"."""
        result = AgentResult(
            task_id=task_id,
            calculation_id=calculation_id,
            decision=AgentDecision(decision),
            reason=reason,
            agent_version=agent_version,
            processed_at=datetime.now(timezone.utc).isoformat(),
        )
        body = json.dumps(result.model_dump()).encode("utf-8")
        headers = kafka_trace_headers()

        # SECURITY §20 — `kafka_produce` span for confluent-kafka (not in the
        # auto-instrumented aiokafka list). is_mutation/rollback_possible mark
        # the publish as a mutating action without a downstream rollback path.
        with aef_kafka_produce(
            span_name="produce_agent_result",
            headers=dict(headers),
            body=body,
            topic=settings.kafka_in_topic,
            kafka_cluster=settings.kafka_cluster_name,
            bootstrap_servers=settings.adapter_brokers.split(","),
        ) as kafka_span:
            hop = record_hop()
            safe_span_attributes(kafka_span, **{
                "aef.action": "approve_app.publish_result",
                "aef.call_type": "service_call",
                "aef.target_name": f"kafka.topic.{settings.kafka_in_topic}",
                "aef.agent_uid": settings.aef_agent_id,
                "aef.agent_name": settings.aef_agent_id,
                "aef.operation_uid": dict(headers).get("x-trace-id", b"").decode("utf-8"),
                "aef.parent_operation_uid": dict(headers).get("x-trace-id", b"").decode("utf-8"),
                "aef.is_mutation": True,
                "aef.rollback_possible": False,
                "aef.hops": settings.operation_max_hops,
                "aef.hops_used": hop,
                "aef.ttl": settings.operation_ttl_sec,
                "aef.stop_event": None,
                "aef.x_trace_id": dict(headers).get("x-trace-id", b"").decode("utf-8"),
                "aef.request_payload": safe_trace_json(result.model_dump()),
                "aef.executable_json": safe_trace_json(result.model_dump()),
            })
            self._produce_sync(task_id=task_id, body=body, headers=headers)
            safe_span_attributes(kafka_span, **{
                "aef.result_payload": safe_trace_json({
                    "published": True,
                    "topic": settings.kafka_in_topic,
                    "task_id": task_id,
                    "calculation_id": calculation_id,
                    "decision": decision,
                })
            })
            safe_span_output(kafka_span, {
                "published": True,
                "topic": settings.kafka_in_topic,
                "task_id": task_id,
                "calculation_id": calculation_id,
                "decision": decision,
            })

            logger.info(
                f"approve_result_sent. task_id={task_id} "
                f"calculation_id={calculation_id} decision={decision}"
            )

    def _produce_sync(self, *, task_id: str, body: bytes, headers: list[tuple[str, bytes]]) -> None:
        delivery_event = threading.Event()
        delivery_error: list[KafkaException] = []

        def _on_delivery(err, msg) -> None:
            if err is not None:
                delivery_error.append(KafkaException(err))
                logger.error(
                    f"kafka_produce_failed. error={err} "
                    f"topic={msg.topic() if msg else settings.kafka_in_topic}"
                )
            else:
                logger.debug(
                    f"kafka_produce_ok. topic={msg.topic()} "
                    f"partition={msg.partition()} offset={msg.offset()}"
                )
            delivery_event.set()

        try:
            self._producer.produce(
                topic=settings.kafka_in_topic,
                key=task_id.encode("utf-8"),
                value=body,
                headers=headers,
                callback=_on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            raise AgentResultPublishError(f"kafka enqueue failed: {exc}") from exc

        remaining = self._producer.flush(timeout=10)
        if remaining:
            raise AgentResultPublishError(
                f"kafka delivery timed out: {remaining} message(s) still queued"
            )
        if not delivery_event.is_set():
            raise AgentResultPublishError("kafka delivery callback timed out")
        if delivery_error:
            raise AgentResultPublishError(str(delivery_error[0])) from delivery_error[0]
