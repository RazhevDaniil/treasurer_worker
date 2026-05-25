"""Small synchronous Kafka delivery helper for confluent-kafka producers."""

from __future__ import annotations

import threading
from typing import Any

from confluent_kafka import KafkaException, Producer


class KafkaDeliveryError(RuntimeError):
    """Raised when a produced message is not acknowledged by Kafka."""


def produce_sync(
    producer: Producer,
    *,
    topic: str,
    key: bytes | None,
    value: bytes,
    timeout: float = 10.0,
    log: Any = None,
) -> None:
    """Produce one message and raise if delivery callback reports failure."""
    delivered = threading.Event()
    errors: list[KafkaException] = []

    def _on_delivery(err, msg) -> None:
        if err is not None:
            errors.append(KafkaException(err))
            if log is not None:
                log.error(
                    "kafka_produce_failed",
                    error=str(err),
                    topic=msg.topic() if msg else topic,
                )
        elif log is not None:
            log.debug(
                "kafka_produce_ok",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
        delivered.set()

    try:
        producer.produce(topic=topic, key=key, value=value, callback=_on_delivery)
    except (BufferError, KafkaException) as exc:
        raise KafkaDeliveryError(f"kafka enqueue failed: {exc}") from exc

    remaining = producer.flush(timeout=timeout)
    if remaining:
        raise KafkaDeliveryError(
            f"kafka delivery timed out: {remaining} message(s) still queued"
        )
    if not delivered.is_set():
        raise KafkaDeliveryError("kafka delivery callback timed out")
    if errors:
        raise KafkaDeliveryError(str(errors[0])) from errors[0]
