"""Kafka consumer for KAFKA_OUT_TOPIC — receives approval tasks from approve_app.

Runs in a background daemon thread. Confluent-kafka is synchronous,
so we call the async pipeline (approve + manager notification) via asyncio.run().
"""

import asyncio
import json
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException

from ..core.config import settings
from ..core.tracing import (
    aef_agent_start,
    aef_kafka_consume,
    bind_x_trace_id,
    ensure_x_trace_id,
    extract_x_trace_id,
    kafka_trace_headers,
    session_id_cvar,
)
from ..models.approve_schemas import AgentTask
from .approve_notifier import ApproveNotifier
from .approve_service import approve, approve_version
from .kafka_producer import AgentResultProducer

logger = logging.getLogger(__name__)


class ApproveTaskConsumer:
    """Consumes KAFKA_OUT_TOPIC, runs business validation, publishes result."""

    def __init__(
        self,
        result_producer: AgentResultProducer,
        notifier: ApproveNotifier | None = None,
    ) -> None:
        self._producer = result_producer
        self._notifier = notifier or ApproveNotifier()
        self._running = False

        self._consumer = Consumer({
            "bootstrap.servers": settings.adapter_brokers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def close(self) -> None:
        self._running = False
        self._consumer.close()

    def run(self) -> None:
        self._consumer.subscribe([settings.kafka_out_topic])
        self._running = True
        logger.info(f"approve_consumer_started. topic={settings.kafka_out_topic}")

        while self._running:
            msg = self._consumer.poll(timeout=settings.kafka_poll_timeout_seconds)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"approve_consumer_error. error={msg.error()}")
                raise KafkaException(msg.error())

            self._handle_message(msg)

    def _handle_message(self, msg) -> None:
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            task = AgentTask.model_validate(payload)
        except (json.JSONDecodeError, Exception) as exc:
            logger.error(f"approve_parse_error. error={exc} raw={msg.value()!r}")
            self._consumer.commit(message=msg)
            return

        # task_id is the natural correlation key for this approval round —
        # use it as the AEF session_id so all spans in the consume/agent_start
        # subtree share the same `attributes.aef.session_id`.
        session_id_cvar.set(task.task_id)
        incoming_headers = msg.headers() or []
        x_trace_id = extract_x_trace_id(incoming_headers) or ensure_x_trace_id(task.task_id)
        bind_x_trace_id(x_trace_id)
        trace_headers = kafka_trace_headers(incoming_headers)
        effective_ttl = max(0, min(settings.operation_ttl_sec, task.ttl_seconds))

        # SECURITY §20 — `kafka_consume` span (confluent-kafka not auto-instrumented).
        # Wrap the agent execution in `agent_start` so contract params are visible.
        with aef_kafka_consume(
            span_name="consume_agent_task",
            headers=dict(trace_headers),
            body=msg.value(),
            topic=settings.kafka_out_topic,
            kafka_cluster=settings.kafka_cluster_name,
            bootstrap_servers=settings.adapter_brokers.split(","),
            consumer_group=settings.kafka_group_id,
        ):
            with aef_agent_start(input={**task.model_dump(), "x_trace_id": x_trace_id}) as agent_span:
                agent_span.add_span_attributes(**{
                    "aef.agent_uid": settings.aef_agent_id,
                    "aef.session_id": task.task_id,
                    "aef.hops": None,
                    "aef.ttl": effective_ttl,
                    "aef.stop_event": None,
                    "aef.x_trace_id": x_trace_id,
                })

                logger.info(
                    f"approve_task_received. task_id={task.task_id} "
                    f"calculation_id={task.calculation_id} "
                    f"x_trace_id={x_trace_id} ttl_sec={effective_ttl}"
                )

                decision, reason, stop_event = asyncio.run(
                    self._process_task_async(task, effective_ttl)
                )
                if stop_event:
                    agent_span.add_span_attributes(**{"aef.stop_event": stop_event})

                try:
                    self._producer.send_result(
                        task_id=task.task_id,
                        calculation_id=task.calculation_id,
                        decision=decision,
                        reason=reason,
                        agent_version=approve_version(),
                    )
                except Exception as exc:
                    logger.exception(
                        f"approve_result_publish_failed. task_id={task.task_id} "
                        f"calculation_id={task.calculation_id} error={exc}"
                    )
                    agent_span.add_span_attributes(**{"aef.stop_event": "result_publish_failed"})
                    return

                self._consumer.commit(message=msg)
                agent_span.add_output_result(output={
                    "task_id": task.task_id,
                    "calculation_id": task.calculation_id,
                    "decision": decision,
                    "x_trace_id": x_trace_id,
                })

                if decision == "REJECTED":
                    asyncio.run(self._notify_rejection_async(task, reason))

                logger.info(
                    f"approve_task_processed. task_id={task.task_id} "
                    f"calculation_id={task.calculation_id} decision={decision}"
                )

    async def _process_task_async(
        self,
        task: AgentTask,
        ttl_seconds: int,
    ) -> tuple[str, str, str | None]:
        """Run deterministic approve validation.

        `approve()` is deterministic validation + a single get_rate HTTP call —
        no LangGraph involvement, so no callbacks plumbing is needed; httpx
        auto-instrumentation captures the outgoing request span.
        """
        if ttl_seconds <= 0:
            logger.error(
                f"approve_ttl_exceeded_before_start. task_id={task.task_id} "
                f"calculation_id={task.calculation_id} ttl_sec={ttl_seconds}"
            )
            return "REJECTED", "Превышен TTL обработки approve-задачи.", "ttl_exceeded"

        try:
            decision, reason = await asyncio.wait_for(
                approve(task.parameters),
                timeout=ttl_seconds,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"approve_ttl_exceeded. task_id={task.task_id} "
                f"calculation_id={task.calculation_id} ttl_sec={ttl_seconds}"
            )
            return "REJECTED", "Превышен TTL обработки approve-задачи.", "ttl_exceeded"
        except Exception as exc:
            logger.error(
                f"approve_unexpected_error. task_id={task.task_id} "
                f"calculation_id={task.calculation_id} error={exc}"
            )
            decision, reason = "REJECTED", f"Ошибка обработки: {exc}"

        return decision, reason, None

    async def _notify_rejection_async(self, task: AgentTask, reason: str) -> None:
        """Best-effort notification after result publication succeeds."""
        await self._notifier.notify_rejection(
            task_id=task.task_id,
            calculation_id=task.calculation_id,
            parameters=task.parameters,
            reason=reason,
        )
