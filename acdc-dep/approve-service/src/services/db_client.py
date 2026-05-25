"""HTTP-клиент для db-service.

Все CRUD-операции с БД проходят через db-service REST API под /v1/approve/*.
"""

import httpx

from ..config import settings
from ..models.calculation import SnapshotCreate
from ..models.task import Task, TaskCreate, TaskStatus, TaskStatusUpdate
from ..utils.logger import get_logger
from ..utils.tracing import current_trace_id, resolve_trace_id, trace_header_dict

log = get_logger(__name__)

_TIMEOUT = httpx.Timeout(10.0, connect=5.0)


class DbClient:
    """Синхронный клиент для db-service."""

    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = (base_url or settings.db_service_url).rstrip("/")
        self._client = httpx.Client(base_url=self._base_url, timeout=_TIMEOUT)

    def close(self) -> None:
        self._client.close()

    def _headers(self, trace_id: str | None = None) -> dict[str, str]:
        return trace_header_dict(trace_id=trace_id or current_trace_id())

    # --- Tasks ---

    def get_task(self, task_id: str) -> Task | None:
        """GET /v1/approve/tasks/{task_id} — поиск по внутреннему UUID."""
        resp = self._client.get(
            f"/v1/approve/tasks/{task_id}",
            headers=self._headers(),
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return Task.model_validate(resp.json())

    def get_task_by_calculation_id(self, calculation_id: str) -> Task | None:
        """GET /v1/approve/tasks/by-calculation/{calculation_id} — для дедупликации."""
        resp = self._client.get(
            f"/v1/approve/tasks/by-calculation/{calculation_id}",
            headers=self._headers(),
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return Task.model_validate(resp.json())

    def create_task(
        self,
        task_id: str,
        calculation_id: str,
        run_id: str | None = None,
    ) -> Task:
        """POST /v1/approve/tasks — регистрация новой задачи (идемпотентно по task_id)."""
        trace_id = resolve_trace_id(run_id or current_trace_id(), fallback=task_id)
        body = TaskCreate(
            task_id=task_id,
            calculation_id=calculation_id,
            task_status=TaskStatus.RECEIVED,
            attempt_count=0,
            run_id=trace_id,
        )
        resp = self._client.post(
            "/v1/approve/tasks",
            json=body.model_dump(mode="json"),
            headers=self._headers(trace_id),
        )
        resp.raise_for_status()
        return Task.model_validate(resp.json())

    def update_task_status(
        self,
        task_id: str,
        status: TaskStatus,
        *,
        deal_status: str | None = None,
        agent_answer: str | None = None,
        attempt_count: int | None = None,
        error_message: str | None = None,
        run_id: str | None = None,
    ) -> Task:
        """PATCH /v1/approve/tasks/{task_id}/status — обновление статуса и связанных полей."""
        trace_id = resolve_trace_id(run_id or current_trace_id(), fallback=task_id)
        body = TaskStatusUpdate(
            task_status=status,
            deal_status=deal_status,
            agent_answer=agent_answer,
            attempt_count=attempt_count,
            error_message=error_message,
            run_id=trace_id,
        )
        resp = self._client.patch(
            f"/v1/approve/tasks/{task_id}/status",
            json=body.model_dump(mode="json", exclude_none=True),
            headers=self._headers(trace_id),
        )
        resp.raise_for_status()
        return Task.model_validate(resp.json())

    def list_tasks_by_status(
        self,
        status: TaskStatus,
        older_than_minutes: int | None = None,
    ) -> list[Task]:
        """GET /v1/approve/tasks?task_status=...&older_than_minutes=... — для TTL watchdog'а."""
        params: dict[str, str] = {"task_status": status.value}
        if older_than_minutes is not None:
            params["older_than_minutes"] = str(older_than_minutes)
        resp = self._client.get(
            "/v1/approve/tasks",
            params=params,
            headers=self._headers(),
        )
        resp.raise_for_status()
        return [Task.model_validate(t) for t in resp.json()]

    # --- Snapshots ---

    def save_snapshot(self, snapshot: SnapshotCreate) -> None:
        """POST /v1/approve/snapshots — сохранение параметров расчёта (snapshot)."""
        trace_id = resolve_trace_id(snapshot.run_id or current_trace_id(), fallback=snapshot.task_id)
        snapshot.run_id = trace_id
        resp = self._client.post(
            "/v1/approve/snapshots",
            json=snapshot.model_dump(mode="json"),
            headers=self._headers(trace_id),
        )
        resp.raise_for_status()

    def get_snapshot_by_task(self, task_id: str) -> dict | None:
        """GET /v1/approve/snapshots/by-task/{task_id} — для retry в TTL watchdog'е."""
        resp = self._client.get(
            f"/v1/approve/snapshots/by-task/{task_id}",
            headers=self._headers(),
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
