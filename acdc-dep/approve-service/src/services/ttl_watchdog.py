"""TTL watchdog для контроля ответа агента — спека §6.2, §6.3.

Периодически проверяет задачи в статусе SENT_TO_AGENT.
Если агент не ответил в пределах TTL — повторная отправка с увеличенным TTL.
После исчерпания попыток — задача переводится в FAILED.
"""

import threading
import time
from datetime import datetime, timezone

from ..config import settings
from ..models.calculation import SNAPSHOT_FIELDS
from ..models.task import TaskStatus
from ..services.db_client import DbClient
from ..services.retry_handler import get_agent_ttl
from ..utils.logger import get_logger
from ..utils.tracing import bound_trace

log = get_logger(__name__)


class TtlWatchdog:
    """Фоновый поток, отслеживающий превышение TTL для задач в SENT_TO_AGENT."""

    def __init__(self, db: DbClient, agent_producer) -> None:
        self._db = db
        self._agent_producer = agent_producer
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, name="ttl-watchdog", daemon=True
        )
        self._thread.start()
        log.info(
            "ttl_watchdog_started",
            check_interval=settings.agent_ttl_check_interval_seconds,
        )

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        while self._running:
            try:
                self._check_expired_tasks()
            except Exception as exc:
                log.error("ttl_watchdog_error", error=str(exc))
            time.sleep(settings.agent_ttl_check_interval_seconds)

    def _check_expired_tasks(self) -> None:
        """Проверяет задачи в SENT_TO_AGENT на превышение TTL."""
        tasks = self._db.list_tasks_by_status(TaskStatus.SENT_TO_AGENT)
        now = datetime.now(timezone.utc)

        for task in tasks:
            with bound_trace(thread_id=task.task_id):
                attempt = task.attempt_count + 1  # текущая попытка (1-based)
                ttl = get_agent_ttl(attempt)
                elapsed = (now - task.updated_at).total_seconds()

                if elapsed <= ttl:
                    continue

                log.warning(
                    "agent_ttl_expired",
                    task_id=task.task_id,
                    calculation_id=task.calculation_id,
                    attempt=attempt,
                    ttl_seconds=ttl,
                    elapsed_seconds=round(elapsed),
                )

                next_attempt = attempt + 1

                if next_attempt > settings.agent_max_attempts:
                    # Все попытки исчерпаны → FAILED (спека §6.2)
                    err = (
                        f"Agent TTL expired after {settings.agent_max_attempts} attempts"
                    )
                    self._db.update_task_status(
                        task.task_id,
                        TaskStatus.FAILED,
                        error_message=err,
                    )
                    log.error(
                        "agent_ttl_all_attempts_exhausted",
                        task_id=task.task_id,
                        calculation_id=task.calculation_id,
                        action="failed",
                        status_from=TaskStatus.SENT_TO_AGENT,
                        status_to=TaskStatus.FAILED,
                        attempt=attempt,
                    )
                    continue

                # Повторная отправка с увеличенным TTL (спека §6.2).
                # Параметры берём из snapshot'а в БД — он уже сохранён на этапе ENRICHED.
                try:
                    snap = self._db.get_snapshot_by_task(task.task_id)
                    if snap is None:
                        log.warning(
                            "agent_retry_no_snapshot",
                            task_id=task.task_id,
                            calculation_id=task.calculation_id,
                        )
                        parameters: dict = {}
                    else:
                        parameters = {
                            field: snap[field]
                            for field in SNAPSHOT_FIELDS
                            if snap.get(field) is not None
                        }

                    self._agent_producer.send_task(
                        task_id=task.task_id,
                        calculation_id=task.calculation_id,
                        parameters=parameters,
                        attempt=next_attempt,
                    )
                    log.info(
                        "agent_task_resent",
                        task_id=task.task_id,
                        calculation_id=task.calculation_id,
                        action="retry",
                        attempt=next_attempt,
                        new_ttl_seconds=get_agent_ttl(next_attempt),
                    )
                except Exception as exc:
                    log.error(
                        "agent_retry_failed",
                        task_id=task.task_id,
                        calculation_id=task.calculation_id,
                        attempt=next_attempt,
                        error=str(exc),
                    )
