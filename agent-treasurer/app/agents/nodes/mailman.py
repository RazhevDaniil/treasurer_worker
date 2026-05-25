"""
Mailman node — handles message reception and response sending.

Simplified for multi-deal graph:
- receive: validate message, determine is_new_thread
- send_response: consolidate history before interrupt/end
"""

import logging
from datetime import datetime
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig

from ..state import AgentState

logger = logging.getLogger(__name__)


async def mailman_receive_node(
    state: AgentState, config: RunnableConfig
) -> dict[str, Any]:
    """Receive and validate incoming message."""
    logger.debug(f"mailman_receive_enter. has_message={bool(state.incoming_message)}")

    message = state.incoming_message or ""
    if not message.strip():
        logger.warning("mailman_receive_empty_message")
        return {
            "phase": "error",
            "last_error": "No message to process",
        }

    is_new_thread = len(state.deals) == 0

    configurable = (config or {}).get("configurable", {}) if config else {}
    counterparty_email = state.counterparty_email or configurable.get("counterparty_email")
    if counterparty_email in (None, "", "unknown"):
        counterparty_email = state.counterparty_email

    logger.info(
        f"mailman_receive_ok. is_new_thread={is_new_thread} "
        f"existing_deals={len(state.deals)} counterparty_email={counterparty_email} "
        f"message={message}"
    )

    result: dict[str, Any] = {
        "phase": "parsing",
        "started_at": datetime.utcnow(),
        "is_new_thread": is_new_thread,
    }
    if counterparty_email:
        result["counterparty_email"] = counterparty_email
    return result


async def mailman_send_response_node(state: AgentState) -> dict[str, Any]:
    """Finalize response and update history before interrupt.

    Single convergence point for all flows before END/interrupt.
    """
    logger.debug(f"mailman_send_enter. has_response={bool(state.response_message)}")

    if not state.response_message:
        logger.warning("mailman_send_no_response")
        return {
            "phase": "error",
            "last_error": "No response to send",
        }

    result: dict[str, Any] = {
        "response_sent": True,
        "phase": "completed",
        # Clear transient fields
        "deal_results": None,
        "deal_updates": [],
    }

    if state.incoming_message:
        result["messages"] = [
            HumanMessage(content=state.incoming_message),
            AIMessage(content=state.response_message),
        ]
        result["message_history"] = [state.incoming_message]
    if state.incoming_message_id:
        result["last_processed_message_id"] = state.incoming_message_id

    logger.info(
        f"mailman_send_ok. response={state.response_message} "
        f"history_updated={bool(state.incoming_message)}"
    )

    return result
