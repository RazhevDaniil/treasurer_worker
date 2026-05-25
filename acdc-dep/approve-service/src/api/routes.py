"""HTTP API approve-service — эндпоинты мониторинга.

Включает эндпоинт из спеки §5.4 и health check.
"""

from fastapi import APIRouter, Query

from ..models.task import Task, TaskStatus
from ..services.db_client import DbClient

router = APIRouter(prefix="/api")

# DbClient инжектится при инициализации приложения (см. main.py)
_db: DbClient | None = None


def init_routes(db: DbClient) -> None:
    """Привязка DbClient к роутеру. Вызывается при старте приложения."""
    global _db
    _db = db


@router.get("/tasks/status", response_model=list[Task])
def get_tasks_by_status(
    status: TaskStatus = Query(..., description="Статус задачи для фильтрации"),
    older_than: int | None = Query(
        None, description="Минимальный возраст задачи в минутах (по updated_at)"
    ),
) -> list[Task]:
    """Возвращает задачи в указанном статусе (спека §5.4).

    Используется для обнаружения «зависших» задач.
    Пример: GET /api/tasks/status?status=SENT_TO_AGENT&older_than=5
    """
    return _db.list_tasks_by_status(status, older_than)


@router.get("/health")
def health() -> dict:
    """Health check для orchestration (Docker, k8s)."""
    return {"status": "ok"}
