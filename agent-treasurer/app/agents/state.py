"""Agent state definition for LangGraph."""

from datetime import datetime
from typing import Annotated, Literal, Optional

from langgraph.graph.message import add_messages
from pydantic import BaseModel, ConfigDict, Field

from ..models.schemas import (
    Deal,
    DealResult,
    DealUpdate,
    OfferRecord,
)


def replace_value(current: any, new: any) -> any:
    """Simple reducer that replaces current value with new value."""
    return new


def append_to_list(current: list, new: any) -> list:
    """Append item to list or extend if new is a list."""
    if current is None:
        current = []
    if new is None:
        return current
    if isinstance(new, list):
        return current + new
    return current + [new]


class AgentState(BaseModel):
    """
    State for the deal processing agent graph (multi-deal).

    Graph: receive → parse_message → process_deals → check_thread → compose_response → send_response → END
    with terminal routing to END on fatal errors.
    """

    # === Conversation & Messages ===
    messages: Annotated[list, add_messages] = Field(
        default_factory=list,
        description="LLM conversation history"
    )

    # === Current Message Processing ===
    incoming_message: Optional[str] = Field(
        None,
        description="Text of the current incoming message"
    )

    # === Multi-deal State ===
    deals: list[Deal] = Field(
        default_factory=list,
        description="All deals in this thread"
    )

    deal_updates: list[DealUpdate] = Field(
        default_factory=list,
        description="Per-deal actions from current message"
    )

    is_new_thread: bool = Field(
        True,
        description="First message vs reply"
    )

    thread_locked: bool = Field(
        False,
        description="Thread locked (escalation)"
    )

    # === Negotiation History ===
    message_history: Annotated[list[str], append_to_list] = Field(
        default_factory=list,
        description="All message texts in this negotiation thread"
    )

    # === Workflow Phase ===
    phase: Literal[
        "receiving",
        "parsing",
        "processing",
        "responding",
        "completed",
        "error"
    ] = Field(
        "receiving",
        description="Current workflow phase"
    )

    # === Response ===
    response_message: Optional[str] = Field(
        None,
        description="Final response text to be sent"
    )

    response_sent: bool = Field(
        False,
        description="Whether response was sent"
    )

    # === Escalation ===
    escalation_requested: bool = Field(
        False,
        description="Escalation triggered (locks entire thread)"
    )

    escalation_reason: Optional[str] = Field(
        None,
        description="Reason for escalation"
    )

    counterparty_email: Optional[str] = Field(
        None,
        description="Counterparty email address (populated from graph config)"
    )

    # === Error Handling ===
    last_error: Optional[str] = Field(
        None,
        description="Last error message if any"
    )

    warnings: Annotated[list[str], append_to_list] = Field(
        default_factory=list,
        description="Non-fatal processing warnings"
    )

    error_diagnostics: Optional[str] = Field(
        None,
        description="Machine-facing details for fatal errors"
    )

    # === Interrupt State ===
    awaiting_reply: bool = Field(
        False,
        description="Graph is interrupted, waiting for counterparty reply"
    )

    # === Human Approval (for deal_judge) ===
    requires_human_approval: bool = Field(
        False,
        description="Deal requires manual approval from employee"
    )

    approval_context: Optional[str] = Field(
        None,
        description="Context for what needs approval"
    )

    # === Metadata ===
    started_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When processing started"
    )

    # === Transient (set by process_deals, consumed by compose_response) ===
    deal_results: Optional[list[DealResult]] = Field(
        None,
        description="Per-deal processing results (transient)"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)
