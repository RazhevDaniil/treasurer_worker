"""Producer для KAFKA_OUT_TOPIC — спека §4.3.

Публикует задания для AI-агента с обогащёнными параметрами расчёта.
task_id стабилен на всю историю задачи и берётся из БД, а не генерится тут.
"""

import json
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer

from ..config import settings
from ..models.agent_response import AgentTask
from ..models.task import TaskStatus
from ..services.db_client import DbClient
from ..services.retry_handler import get_agent_ttl
from ..utils.logger import get_logger
from .reliable_kafka import produce_sync

log = get_logger(__name__)


class AgentTaskProducer:
    def __init__(self, db: DbClient) -> None:
        self._db = db
        self._producer = Producer({
            "bootstrap.servers": settings.adapter_brokers,
        })

    def close(self) -> None:
        self._producer.flush(timeout=10)

    def send_task(
        self,
        *,
        task_id: str,
        calculation_id: str,
        parameters: dict[str, Any],
        attempt: int = 1,
    ) -> None:
        """Формирует и публикует задание агенту (спека §4.3)."""
        ttl = get_agent_ttl(attempt)

        task = AgentTask(
            task_id=task_id,
            calculation_id=calculation_id,
            parameters=parameters,
            created_at=datetime.now(timezone.utc).isoformat(),
            ttl_seconds=ttl,
        )

        # Trace propagation removed — AEF SDK on treasurer generates its
        # own `trace_id` per kafka_consume span (SECURITY_COMPLIANCE_SDK §3).
        produce_sync(
            self._producer,
            topic=settings.kafka_out_topic,
            key=task_id.encode("utf-8"),
            value=json.dumps(task.model_dump()).encode("utf-8"),
            timeout=10,
            log=log,
        )

        # Обновляем статус задачи → SENT_TO_AGENT (и фиксируем номер попытки).
        # attempt_count хранит «сколько раз уже отправляли», т.е. attempt - 1.
        self._db.update_task_status(
            task_id,
            TaskStatus.SENT_TO_AGENT,
            attempt_count=attempt - 1,
        )
        log.info(
            "task_sent_to_agent",
            task_id=task_id,
            calculation_id=calculation_id,
            action="sent_to_agent",
            status_from=TaskStatus.ENRICHED,
            status_to=TaskStatus.SENT_TO_AGENT,
            ttl_seconds=ttl,
            attempt=attempt,
        )
