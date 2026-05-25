"""Producer для PALM_CSP_AGENT_IN_TOPIC — спека §4.5.

Отправляет финальный результат (id + новый статус) обратно во внешнюю систему.
"""

import json

from confluent_kafka import Producer

from ..config import settings
from ..models.agent_response import AgentDecision
from ..utils.logger import get_logger
from ..utils.tracing import current_trace_id, resolve_trace_id, trace_header_list
from .reliable_kafka import produce_sync

log = get_logger(__name__)

# Маппинг внутреннего решения агента → статус для внешней системы (спека §4.5)
_DECISION_TO_STATUS = {
    AgentDecision.COMPLETED: "approved",
    AgentDecision.REJECTED: "rejected",
}


class CspResultProducer:
    def __init__(self) -> None:
        self._producer = Producer({
            "bootstrap.servers": settings.adapter_brokers,
        })

    def close(self) -> None:
        self._producer.flush(timeout=10)

    def send_result(
        self,
        calculation_id: str,
        decision: AgentDecision,
        *,
        trace_id: str | None = None,
    ) -> None:
        """Публикует результат во внешнюю систему (спека §4.5).

        Выходной контракт: { calculation_id, status: "approved" | "rejected" }
        """
        operation_trace_id = resolve_trace_id(trace_id or current_trace_id())
        status = _DECISION_TO_STATUS[decision]
        payload = {
            "calculation_id": calculation_id,
            "status": status,
        }

        produce_sync(
            self._producer,
            topic=settings.palm_csp_agent_in_topic,
            key=calculation_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            headers=trace_header_list(trace_id=operation_trace_id),
            timeout=10,
            log=log,
        )

        log.info(
            "result_sent_to_external",
            calculation_id=calculation_id,
            trace_id=operation_trace_id,
            action=status,
            status_to=status,
        )
