"""Модели для взаимодействия с AI-агентом — спека §4.3, §4.4."""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class AgentDecision(StrEnum):
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


class AgentTask(BaseModel):
    """Задание для агента, публикуемое в KAFKA_OUT_TOPIC (спека §4.3)."""
    task_id: str
    calculation_id: str
    parameters: dict[str, Any]
    created_at: str  # ISO-8601
    ttl_seconds: int


class AgentResult(BaseModel):
    """Результат обработки агентом из KAFKA_IN_TOPIC (спека §4.4)."""
    task_id: str
    calculation_id: str
    decision: AgentDecision
    reason: str
    agent_version: str
    processed_at: str  # ISO-8601
