"""Модель задачи (approve_tasks) — спека §5."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class TaskStatus(StrEnum):
    """Статусы обработки задачи (спека §5.3)."""
    RECEIVED = "RECEIVED"
    ENRICHED = "ENRICHED"
    SENT_TO_AGENT = "SENT_TO_AGENT"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


class Task(BaseModel):
    """Полная модель задачи из db-service."""
    task_id: str
    calculation_id: str
    task_status: TaskStatus
    task_dttm: datetime
    updated_at: datetime
    attempt_count: int = 0
    agent_answer: str | None = None
    deal_status: str | None = None
    error_message: str | None = None


class TaskCreate(BaseModel):
    """Тело POST /approve/tasks — регистрация новой задачи (спека §4.1, шаг 3)."""
    task_id: str
    calculation_id: str
    task_status: TaskStatus = TaskStatus.RECEIVED
    attempt_count: int = 0


class TaskStatusUpdate(BaseModel):
    """Тело PATCH /approve/tasks/{task_id}/status."""
    task_status: TaskStatus
    deal_status: str | None = None
    agent_answer: str | None = None
    attempt_count: int | None = None
    error_message: str | None = None
