"""Consumer для PALM_CSP_AGENT_OUT_TOPIC — спека §4.1, §4.2, §4.3.

Основной pipeline:
1. Приём calculation_id + параметров сделки из внешней системы
2. Дедупликация → получение task_id
3. Обогащение через CalcFundCost (последовательно)
4. Отправка задания агенту
"""

import json

from confluent_kafka import Consumer, KafkaError, KafkaException

from ..config import settings
from ..models.calculation import DEAL_CONDITION_FIELDS
from ..producers.agent_task_producer import AgentTaskProducer
from ..producers.reliable_kafka import produce_sync
from ..services.db_client import DbClient
from ..services.deduplication_service import DuplicateError, check_and_register
from ..services.dlq_handler import DlqHandler
from ..services.enrichment_service import EnrichmentService
from ..services.retry_handler import RetryExhausted
from ..utils.logger import get_logger
from ..utils.tracing import bound_trace

log = get_logger(__name__)


def _extract_deal_conditions(payload: dict) -> dict:
    """Извлекает параметры сделки из входящего Kafka-сообщения."""
    return {
        field: payload[field]
        for field in DEAL_CONDITION_FIELDS
        if field in payload and payload[field] is not None
    }


class CspAgentConsumer:
    """Consumer первого этапа pipeline: приём → дедупликация → обогащение → агент."""

    def __init__(
        self,
        db: DbClient,
        enrichment: EnrichmentService,
        agent_producer: AgentTaskProducer,
    ) -> None:
        self._db = db
        self._enrichment = enrichment
        self._agent_producer = agent_producer
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
        """Запускает бесконечный цикл чтения из PALM_CSP_AGENT_OUT_TOPIC.

        После обработки всех доступных сообщений — обрабатывает DLQ (спека §6.1).
        """
        self._consumer.subscribe([settings.palm_csp_agent_out_topic])
        self._running = True
        log.info("consumer_started", topic=settings.palm_csp_agent_out_topic)

        # Счётчик последовательных пустых poll — для перехода к DLQ
        idle_count = 0
        max_idle = 5

        while self._running:
            msg = self._consumer.poll(timeout=settings.kafka_poll_timeout_seconds)

            if msg is None:
                idle_count += 1
                # После серии пустых poll — обрабатываем DLQ (спека §6.1)
                if idle_count >= max_idle:
                    self._process_dlq()
                    idle_count = 0
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("consumer_error", error=str(msg.error()))
                raise KafkaException(msg.error())

            idle_count = 0
            self._handle_message(msg)

    def _handle_message(self, msg) -> None:
        """Обрабатывает одно входящее сообщение из PALM_CSP_AGENT_OUT_TOPIC.

        Входящий контракт:
          {calculation_id, inn?, term_days?, volume?, currency?,
           rate?, rate_type?, product?, basis?, optionality?}

        После дедупликации к контексту прибиндивается `thread_id = task_id`
        для локальной корреляции логов в пределах обработки одного сообщения.
        """
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            calculation_id = payload["calculation_id"]
        except (json.JSONDecodeError, KeyError) as exc:
            log.error("message_parse_error", error=str(exc), raw=msg.value())
            self._consumer.commit(message=msg)
            return

        deal_conditions = _extract_deal_conditions(payload)

        log.info(
            "message_received",
            calculation_id=calculation_id,
            action="received",
            has_deal_conditions=bool(deal_conditions),
        )

        try:
            # Шаг 1: Дедупликация → получаем стабильный task_id.
            task_id = check_and_register(self._db, calculation_id)
        except DuplicateError:
            self._consumer.commit(message=msg)
            return

        with bound_trace(thread_id=task_id):
                try:
                    # Commit offset после успешной регистрации (спека §4.1, шаг 4)
                    self._consumer.commit(message=msg)

                    # Шаг 2: Обогащение через CalcFundCost (спека §4.2).
                    # Сделочные поля вливаются в parameters и сохраняются в snapshot.
                    enriched = self._enrichment.enrich(
                        task_id,
                        calculation_id,
                        deal_conditions=deal_conditions,
                    )

                    # Шаг 3: Отправка задания агенту (спека §4.3).
                    # parameters уже содержит и финансы, и сделку.
                    self._agent_producer.send_task(
                        task_id=task_id,
                        calculation_id=calculation_id,
                        parameters=enriched.parameters,
                    )

                except RetryExhausted as exc:
                    log.error(
                        "retry_exhausted_to_dlq",
                        calculation_id=calculation_id,
                        error=str(exc),
                    )
                    self._send_to_dlq(calculation_id)
                    self._consumer.commit(message=msg)

                except Exception as exc:
                    log.error(
                        "message_processing_error",
                        calculation_id=calculation_id,
                        error=str(exc),
                    )
                    self._send_to_dlq(calculation_id)
                    self._consumer.commit(message=msg)

    def _send_to_dlq(self, calculation_id: str) -> None:
        """Перемещает сообщение в APPROVE_SERVICE_DLQ."""
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": settings.adapter_brokers})
        produce_sync(
            producer,
            topic=settings.dlq_topic,
            key=calculation_id.encode("utf-8"),
            value=json.dumps({"calculation_id": calculation_id}).encode("utf-8"),
            timeout=10,
            log=log,
        )
        log.info("sent_to_dlq", calculation_id=calculation_id)

    def _process_dlq(self) -> None:
        """Обрабатывает DLQ после опустошения основной очереди (спека §6.1).

        При повторе из DLQ пытается достать сделочные поля из ранее сохранённого
        snapshot'а (если enrichment на первой попытке отработал, а send_task — нет).
        """
        def reprocess(calc_id: str) -> None:
            # Достаём task_id (он уже создан при первом приёме calc_id)
            task = self._db.get_task_by_calculation_id(calc_id)
            if task is None:
                from src.services.deduplication_service import check_and_register
                task_id = check_and_register(self._db, calc_id)
            else:
                task_id = task.task_id

            with bound_trace(thread_id=task_id):
                # Пробуем восстановить сделочные поля из snapshot'а
                deal_conditions = self._read_deal_conditions_from_snapshot(task_id)

                enriched = self._enrichment.enrich(
                    task_id,
                    calc_id,
                    deal_conditions=deal_conditions,
                )
                self._agent_producer.send_task(
                    task_id=task_id,
                    calculation_id=calc_id,
                    parameters=enriched.parameters,
                )

        dlq = DlqHandler(self._db, reprocess)
        try:
            dlq.process_dlq()
        finally:
            dlq.close()

    def _read_deal_conditions_from_snapshot(self, task_id: str) -> dict | None:
        """Извлекает сделочные поля из snapshot'а для DLQ-повтора."""
        snap = self._db.get_snapshot_by_task(task_id)
        if snap is None:
            return None
        conditions = {
            field: snap[field]
            for field in DEAL_CONDITION_FIELDS
            if snap.get(field) is not None
        }
        return conditions or None
