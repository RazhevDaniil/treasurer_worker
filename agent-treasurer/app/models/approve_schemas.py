"""Models for approve-service Kafka integration — mirror of approve_app contracts."""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class AgentDecision(StrEnum):
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


class AgentTask(BaseModel):
    """Задание от approve_app из KAFKA_OUT_TOPIC."""
    task_id: str
    calculation_id: str
    parameters: dict[str, Any]
    created_at: str  # ISO-8601
    ttl_seconds: int


class AgentResult(BaseModel):
    """Результат, публикуемый в KAFKA_IN_TOPIC."""
    task_id: str
    calculation_id: str
    decision: AgentDecision
    reason: str
    agent_version: str
    processed_at: str  # ISO-8601
