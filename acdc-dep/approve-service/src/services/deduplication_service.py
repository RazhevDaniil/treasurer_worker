"""Дедупликация входящих сообщений — спека §7.

Ключ дедупликации — calculation_id (из CSP).
- Задача существует и статус != FAILED → пропустить.
- Задача существует и статус == FAILED → разрешить повторную обработку, вернуть её task_id.
- Задачи нет → создать новую запись, вернуть сгенерированный UUID task_id.
"""

import uuid

from ..models.task import TaskStatus
from ..services.db_client import DbClient
from ..utils.logger import get_logger

log = get_logger(__name__)


class DuplicateError(Exception):
    """Задача уже обрабатывается или завершена — дубликат."""
    pass


def check_and_register(db: DbClient, calculation_id: str) -> str:
    """Проверяет дедупликацию и регистрирует новую задачу.

    Returns:
        task_id (UUID) — стабильный внутренний идентификатор задачи.

    Raises:
        DuplicateError: если задача уже обрабатывается (не-FAILED статус).
    """
    existing = db.get_task_by_calculation_id(calculation_id)

    if existing is not None:
        if existing.task_status != TaskStatus.FAILED:
            log.info(
                "duplicate_skipping",
                calculation_id=calculation_id,
                task_id=existing.task_id,
                current_status=existing.task_status,
            )
            raise DuplicateError(
                f"calculation_id={calculation_id} already in status {existing.task_status}"
            )
        # FAILED → разрешаем повторную обработку: сбрасываем счётчик и статус.
        log.info(
            "reprocessing_failed_task",
            calculation_id=calculation_id,
            task_id=existing.task_id,
            action="reprocess",
            status_from=TaskStatus.FAILED,
            status_to=TaskStatus.RECEIVED,
        )
        db.update_task_status(
            existing.task_id,
            TaskStatus.RECEIVED,
            attempt_count=0,
            error_message="",
        )
        return existing.task_id

    # Новая задача — генерим стабильный task_id и создаём запись.
    task_id = str(uuid.uuid4())
    log.info(
        "task_registered",
        calculation_id=calculation_id,
        task_id=task_id,
        action="received",
        status_to=TaskStatus.RECEIVED,
    )
    db.create_task(task_id=task_id, calculation_id=calculation_id)
    return task_id
