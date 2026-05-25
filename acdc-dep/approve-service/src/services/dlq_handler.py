"""Обработка Dead Letter Queue — спека §6.1.

DLQ обрабатывается после основной очереди.
При неуспехе из DLQ — задача переводится в FAILED через db-service.
"""

import json

from confluent_kafka import Consumer, KafkaError, KafkaException

from ..config import settings
from ..models.task import TaskStatus
from ..services.db_client import DbClient
from ..utils.logger import get_logger
from ..utils.tracing import resolve_trace_id

log = get_logger(__name__)


class DlqHandler:
    def __init__(self, db: DbClient, process_func) -> None:
        """
        Args:
            db: клиент db-service
            process_func: функция обработки сообщения (принимает calculation_id, возвращает None).
                          Должна выбрасывать исключение при ошибке.
        """
        self._db = db
        self._process_func = process_func
        self._consumer = Consumer({
            "bootstrap.servers": settings.adapter_brokers,
            "group.id": f"{settings.kafka_group_id}-dlq",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def close(self) -> None:
        self._consumer.close()

    def process_dlq(self) -> None:
        """Читает и обрабатывает все сообщения из DLQ-топика.

        При неуспехе повторной обработки — переводит задачу в FAILED
        с error_message (спека §6.1).
        """
        self._consumer.subscribe([settings.dlq_topic])
        log.info("dlq_processing_started", topic=settings.dlq_topic)

        while True:
            msg = self._consumer.poll(timeout=settings.kafka_poll_timeout_seconds)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())

            payload: dict = {}
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                calculation_id = payload["calculation_id"]

                log.info("dlq_reprocessing", calculation_id=calculation_id)
                self._process_func(calculation_id)
                self._consumer.commit(message=msg)

            except Exception as exc:
                calculation_id = payload.get("calculation_id", "unknown")
                log.error(
                    "dlq_final_failure",
                    calculation_id=calculation_id,
                    error=str(exc),
                )
                # Финальный неуспех — переводим задачу в FAILED с error_message.
                task = self._db.get_task_by_calculation_id(calculation_id)
                if task is not None:
                    trace_id = resolve_trace_id(task.run_id, fallback=task.task_id)
                    self._db.update_task_status(
                        task.task_id,
                        TaskStatus.FAILED,
                        error_message=str(exc),
                        run_id=trace_id,
                    )
                self._consumer.commit(message=msg)

        log.info("dlq_processing_finished")
