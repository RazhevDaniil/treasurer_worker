"""Обогащение данных через CalcFundCost API — спека §4.2.

Обработка последовательная: расчёты обогащаются один за другим.
При ошибке — retry с backoff (спека §6.1).
"""

from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from ..config import settings
from ..models.calculation import (
    DEAL_CONDITION_FIELDS,
    FINANCIAL_SNAPSHOT_FIELDS,
    SNAPSHOT_FIELD_TYPES,
    CalcSearchRequest,
    CalcSearchResponse,
    SnapshotCreate,
)
from ..models.task import TaskStatus
from ..services.db_client import DbClient
from ..services.retry_handler import retry_with_backoff
from ..utils.logger import get_logger

log = get_logger(__name__)

_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def _cast_snapshot_value(field: str, raw: Any) -> Any:
    """Приводит сырое значение к типу, ожидаемому в SnapshotCreate."""
    field_type = SNAPSHOT_FIELD_TYPES.get(field)
    if field_type is str:
        return str(raw)
    elif field_type is int:
        return int(raw)
    elif field_type is float:
        return float(raw)
    else:
        # По умолчанию — Decimal (финансовые показатели)
        return Decimal(str(raw))


def map_parameters_to_snapshot(
    *,
    calc_id: str,
    task_id: str,
    parameters: dict[str, Any],
) -> SnapshotCreate:
    """Превращает свободный dict в плоский SnapshotCreate.

    Финансовые поля → Decimal, сделочные → str/int/float согласно SNAPSHOT_FIELD_TYPES.
    """
    values: dict[str, Any] = {
        "calc_id": calc_id,
        "task_id": task_id,
    }

    for field in FINANCIAL_SNAPSHOT_FIELDS + DEAL_CONDITION_FIELDS:
        raw = parameters.get(field)
        if raw is None:
            continue
        try:
            values[field] = _cast_snapshot_value(field, raw)
        except (InvalidOperation, TypeError, ValueError):
            log.warning(
                "snapshot_field_unparseable",
                calc_id=calc_id,
                field=field,
                raw_value=raw,
            )

    return SnapshotCreate(**values)


class EnrichmentService:
    def __init__(self, db: DbClient, calc_fund_cost_url: str | None = None) -> None:
        self._db = db
        self._calc_url = (calc_fund_cost_url or settings.calc_fund_cost_url).rstrip("/")
        self._client = httpx.Client(base_url=self._calc_url, timeout=_TIMEOUT)

    def close(self) -> None:
        self._client.close()

    def enrich(
        self,
        task_id: str,
        calculation_id: str,
        deal_conditions: dict[str, Any] | None = None,
    ) -> CalcSearchResponse:
        """Обогащает расчёт: запрос в CalcFundCost → snapshot → статус ENRICHED.

        deal_conditions — параметры сделки из входящего сообщения (inn, term_days,
        volume, currency, ...), вливаются в snapshot и далее в AgentTask.
        """
        request_body = CalcSearchRequest(id=calculation_id)

        def _call_api() -> CalcSearchResponse:
            resp = self._client.post(
                "/api/v1/calculations/search",
                json=request_body.model_dump(),
            )
            resp.raise_for_status()
            return CalcSearchResponse.model_validate(resp.json())

        # Retry с backoff 60 → 120 → 180 мин (спека §6.1)
        result = retry_with_backoff(_call_api, calculation_id=calculation_id)

        # Мержим финансовые параметры CalcFundCost + сделочные из внешней системы
        merged = dict(result.parameters)
        if deal_conditions:
            merged.update(deal_conditions)

        # Сохраняем snapshot с обоими наборами полей
        snapshot = map_parameters_to_snapshot(
            calc_id=result.id,
            task_id=task_id,
            parameters=merged,
        )
        self._db.save_snapshot(snapshot)

        # Подменяем parameters в ответе на смерженные — они пойдут в AgentTask
        result.parameters = merged

        self._db.update_task_status(task_id, TaskStatus.ENRICHED)
        log.info(
            "calculation_enriched",
            task_id=task_id,
            calculation_id=calculation_id,
            calc_id=result.id,
            action="enriched",
            status_from=TaskStatus.RECEIVED,
            status_to=TaskStatus.ENRICHED,
        )
        return result
