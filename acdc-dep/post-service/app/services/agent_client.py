"""Kafka publisher for KAFKA_MAIL_TOPIC.

Бывший HTTP-клиент к agent_treasurer /chat заменён на confluent-kafka producer.
Ответ агента приходит асинхронно через mail_app HTTP /api/v1/send_reply, поэтому
publish — fire-and-forget на уровне бизнес-логики; нам важен лишь факт доставки
сообщения брокеру (ack=all + idempotence).
"""

from __future__ import annotations

import json
import logging
import threading

from confluent_kafka import KafkaException, Producer

from ..config import Settings

logger = logging.getLogger("services.agent_client")


class AgentPublishError(RuntimeError):
    """Wraps Kafka delivery failures so AgentDispatcher can decide retry/fail."""


class AgentKafkaPublisher:
    """Идемпотентный producer в KAFKA_MAIL_TOPIC.

    Ключ партиционирования — `thread_id`: все сообщения одного треда попадают
    в одну партицию и обрабатываются consumer'ом строго по порядку.

    Дедупликацию по `message_id` обеспечивает сам consumer (через checkpointer
    LangGraph + явные проверки); producer лишь гарантирует exactly-once delivery
    в рамках broker session за счёт `enable.idempotence=true`.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._producer = Producer({
            "bootstrap.servers": settings.adapter_brokers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": settings.kafka_producer_retries,
            "retry.backoff.ms": settings.kafka_retry_backoff_ms,
            "retry.backoff.max.ms": settings.kafka_retry_backoff_max_ms,
        })

    def close(self) -> None:
        self._producer.flush(timeout=self.settings.kafka_flush_timeout_seconds)

    def publish_incoming_email(self, payload: dict, idempotency_key: str) -> None:
        """Publish incoming email to KAFKA_MAIL_TOPIC.

        Блокирует поток до подтверждения брокером (flush). При delivery error
        бросает AgentPublishError — AgentDispatcher повторит через RETRYING.
        """
        thread_id = payload.get("thread_id") or ""
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        delivery_event = threading.Event()
        delivery_error: list[KafkaException] = []

        def _on_delivery(err, msg) -> None:
            if err is not None:
                delivery_error.append(KafkaException(err))
                logger.error(
                    "kafka_produce_failed. topic=%s key=%s error=%s",
                    msg.topic() if msg else self.settings.kafka_mail_topic,
                    idempotency_key,
                    err,
                )
            else:
                logger.debug(
                    "kafka_produce_ok. topic=%s partition=%s offset=%s key=%s",
                    msg.topic(), msg.partition(), msg.offset(), idempotency_key,
                )
            delivery_event.set()

        try:
            self._producer.produce(
                topic=self.settings.kafka_mail_topic,
                key=thread_id.encode("utf-8") if thread_id else None,
                value=body,
                headers=[("idempotency-key", idempotency_key.encode("utf-8"))],
                on_delivery=_on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            raise AgentPublishError(f"enqueue failed: {exc}") from exc

        self._producer.flush(timeout=self.settings.kafka_flush_timeout_seconds)

        if not delivery_event.is_set():
            raise AgentPublishError("delivery callback timed out")
        if delivery_error:
            raise AgentPublishError(str(delivery_error[0])) from delivery_error[0]
