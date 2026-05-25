"""Consumer для KAFKA_IN_TOPIC — спека §4.4.

Принимает результат решения AI-агента, валидирует, сохраняет ответ на самой задаче
(agent_answer + deal_status), обновляет статус и публикует результат во внешнюю систему.
"""

import json

from confluent_kafka import Consumer, KafkaError, KafkaException

from ..config import settings
from ..models.agent_response import AgentDecision, AgentResult
from ..models.task import TaskStatus
from ..producers.csp_result_producer import CspResultProducer
from ..services.db_client import DbClient
from ..utils.logger import get_logger
from ..utils.tracing import bound_trace, resolve_trace_id

log = get_logger(__name__)


class AgentResultConsumer:
    """Consumer второго этапа pipeline: приём результата агента → аудит → внешняя система."""

    def __init__(self, db: DbClient, csp_producer: CspResultProducer) -> None:
        self._db = db
        self._csp_producer = csp_producer
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
        """Запускает бесконечный цикл чтения из KAFKA_IN_TOPIC."""
        self._consumer.subscribe([settings.kafka_in_topic])
        self._running = True
        log.info("agent_result_consumer_started", topic=settings.kafka_in_topic)

        while self._running:
            msg = self._consumer.poll(timeout=settings.kafka_poll_timeout_seconds)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("consumer_error", error=str(msg.error()))
                raise KafkaException(msg.error())

            self._handle_message(msg)

    def _handle_message(self, msg) -> None:
        """Обрабатывает результат от агента (спека §4.4)."""
        trace_id = resolve_trace_id(headers=msg.headers(), fallback=msg.key())
        with bound_trace(trace_id=trace_id):
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                result = AgentResult.model_validate(payload)
            except (json.JSONDecodeError, Exception) as exc:
                log.error("agent_result_parse_error", error=str(exc), raw=msg.value())
                self._consumer.commit(message=msg)
                return

            log.info(
                "agent_result_received",
                task_id=result.task_id,
                calculation_id=result.calculation_id,
                trace_id=trace_id,
                decision=result.decision,
            )

            # Шаг 2: Валидация — лукап по task_id (внутреннему UUID), не по calc_id
            task = self._db.get_task(result.task_id)
            if task is None or task.task_status != TaskStatus.SENT_TO_AGENT:
                log.warning(
                    "agent_result_validation_failed",
                    task_id=result.task_id,
                    calculation_id=result.calculation_id,
                    reason="task not found or unexpected status",
                    current_status=task.task_status if task else None,
                )
                self._consumer.commit(message=msg)
                return

            # Шаг 3: публикация результата во внешнюю систему (спека §4.5).
            # Статус в БД переводим в terminal только после подтверждённого
            # Kafka delivery, иначе watchdog уже не сможет повторить задачу.
            new_status = (
                TaskStatus.COMPLETED
                if result.decision == AgentDecision.COMPLETED
                else TaskStatus.REJECTED
            )
            try:
                self._csp_producer.send_result(
                    result.calculation_id,
                    result.decision,
                    trace_id=trace_id,
                )
            except Exception as exc:
                log.error(
                    "external_result_publish_failed",
                    task_id=result.task_id,
                    calculation_id=result.calculation_id,
                    trace_id=trace_id,
                    error=str(exc),
                )
                return

            # Шаг 4: статус + ответ агента (reason → agent_answer, decision → deal_status).
            self._db.update_task_status(
                result.task_id,
                new_status,
                agent_answer=result.reason,
                deal_status=result.decision.value,
                run_id=trace_id,
            )
            log.info(
                "task_status_updated",
                task_id=result.task_id,
                calculation_id=result.calculation_id,
                trace_id=trace_id,
                action=result.decision.value.lower(),
                status_from=TaskStatus.SENT_TO_AGENT,
                status_to=new_status,
            )

            self._consumer.commit(message=msg)
