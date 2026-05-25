"""
LangGraph StateGraph definition for the multi-deal processing agent.

Happy path:
  receive → parse_message → process_deals → check_thread → compose_response → send_response → END

Fatal errors short-circuit to END from any intermediate node.
Interrupt after send_response for iterative negotiation (pause/resume).
"""

import logging
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler
from langgraph.graph import END, START, StateGraph
from langgraph.checkpoint.memory import MemorySaver

from .state import AgentState
from .nodes import (
    mailman_receive_node,
    parse_message_node,
    process_deals_node,
    check_thread_node,
    compose_response_node,
    mailman_send_response_node,
)

logger = logging.getLogger(__name__)


def route_on_error(state: AgentState) -> str:
    """Route to compose_response on error so user always gets a reply."""
    phase = state.get("phase") if isinstance(state, dict) else state.phase
    return "error" if phase == "error" else "continue"


def route_compose_on_error(state: AgentState) -> str:
    """Route after compose_response: errors skip send_response → END."""
    phase = state.get("phase") if isinstance(state, dict) else state.phase
    return "error_end" if phase == "error" else "continue"


def create_deal_agent_graph(checkpointer: MemorySaver | None = None) -> StateGraph:
    """Create and compile the multi-deal processing agent graph.

    Graph:
      receive → parse_message → process_deals → check_thread → compose_response → send_response → END

    On error from any node before compose_response, the graph jumps to
    compose_response so that a user-facing error message is always generated.
    """
    logger.debug(f"create_deal_agent_graph. has_checkpointer={checkpointer is not None}")

    builder = StateGraph(AgentState)

    builder.add_node("receive", mailman_receive_node)
    builder.add_node("parse_message", parse_message_node)
    builder.add_node("process_deals", process_deals_node)
    builder.add_node("check_thread", check_thread_node)
    builder.add_node("compose_response", compose_response_node)
    builder.add_node("send_response", mailman_send_response_node)

    builder.set_entry_point("receive")
    builder.add_conditional_edges(
        "receive",
        route_on_error,
        {"continue": "parse_message", "error": "compose_response"},
    )
    builder.add_conditional_edges(
        "parse_message",
        route_on_error,
        {"continue": "process_deals", "error": "compose_response"},
    )
    builder.add_conditional_edges(
        "process_deals",
        route_on_error,
        {"continue": "check_thread", "error": "compose_response"},
    )
    builder.add_edge("check_thread", "compose_response")
    builder.add_conditional_edges(
        "compose_response",
        route_compose_on_error,
        {"continue": "send_response", "error_end": END},
    )
    builder.add_edge("send_response", END)

    compile_kwargs = {}
    if checkpointer is not None:
        compile_kwargs["checkpointer"] = checkpointer
        compile_kwargs["interrupt_after"] = ["send_response"]

    graph = builder.compile(**compile_kwargs)
    logger.debug("deal_agent_graph_compiled")
    return graph


# =============================================================================
# Helper functions for running the graph
# =============================================================================


async def process_message(
    message: str,
    checkpointer: MemorySaver | None = None,
    thread_id: str | None = None,
    counterparty_id: str | None = None,
    incoming_message_id: str | None = None,
    callbacks: list[BaseCallbackHandler] | None = None,
) -> AgentState:
    """Process an incoming message through the deal agent graph.

    `callbacks` is forwarded into LangGraph as `config.callbacks`. Pass
    `[get_aef_handler()]` (see core/tracing.py) so the SDK collects
    `chain` / `llm` / `retriever` / `tool` spans for SECURITY §20.
    """
    import uuid

    resolved_thread_id = thread_id or str(uuid.uuid4())

    logger.info(f"process_message_start. message={message}")

    initial_state = AgentState(
        incoming_message=message,
        incoming_message_id=incoming_message_id,
        deals=[],
        is_new_thread=True,
    )

    graph = create_deal_agent_graph(checkpointer)

    config: dict[str, Any] = {
        "configurable": {
            "thread_id": resolved_thread_id,
            "counterparty_email": counterparty_id or "unknown",
        }
    }
    if callbacks:
        config["callbacks"] = callbacks

    final_state_dict = await graph.ainvoke(initial_state, config)
    final_state = AgentState(**final_state_dict)

    logger.info(
        f"process_message_done. phase={final_state.phase} "
        f"deals_count={len(final_state.deals)} "
        f"awaiting_reply={final_state.awaiting_reply}"
    )

    return final_state


async def resume_with_message(
    message: str,
    thread_id: str,
    checkpointer: MemorySaver,
    incoming_message_id: str | None = None,
    callbacks: list[BaseCallbackHandler] | None = None,
) -> AgentState:
    """Resume interrupted graph with a new message (counterparty reply).

    `callbacks` is forwarded into LangGraph as `config.callbacks` so the
    AEF SDK collects the corresponding span tree.
    """
    logger.info(f"resume_with_message_start. message={message}")

    graph = create_deal_agent_graph(checkpointer)

    config: dict[str, Any] = {
        "configurable": {
            "thread_id": thread_id,
        }
    }
    if callbacks:
        config["callbacks"] = callbacks

    state_snapshot = await graph.aget_state(config)

    if state_snapshot.values is None:
        raise ValueError(f"No state found for thread {thread_id}")

    if not state_snapshot.values.get("awaiting_reply"):
        raise ValueError(f"Thread {thread_id} is not awaiting a reply")

    update = {
        "incoming_message": message,
        "incoming_message_id": incoming_message_id,
        "awaiting_reply": False,
        "response_message": None,
        "response_sent": False,
        "deal_updates": [],
        "warnings": [],
        "last_error": None,
        "error_diagnostics": None,
        "stop_event": None,
        "is_new_thread": False,
        "deal_results": None,
    }

    await graph.aupdate_state(config, update, as_node=START)

    final_state_dict = await graph.ainvoke(None, config)
    final_state = AgentState(**final_state_dict)

    logger.info(
        f"resume_with_message_done. phase={final_state.phase} "
        f"deals_count={len(final_state.deals)} "
        f"awaiting_reply={final_state.awaiting_reply}"
    )

    return final_state


async def get_thread_state(
    thread_id: str,
    checkpointer: MemorySaver,
) -> AgentState | None:
    """Get current state for a thread."""
    logger.debug(f"get_thread_state. thread_id={thread_id}")

    graph = create_deal_agent_graph(checkpointer)
    config = {"configurable": {"thread_id": thread_id}}

    state_snapshot = await graph.aget_state(config)

    if state_snapshot.values is None:
        return None

    return AgentState(**state_snapshot.values)


async def get_thread_history(
    thread_id: str,
    checkpointer: MemorySaver,
) -> list[AgentState]:
    """Get full state history for a thread (chronological order)."""
    logger.debug(f"get_thread_history. thread_id={thread_id}")

    graph = create_deal_agent_graph(checkpointer)
    config = {"configurable": {"thread_id": thread_id}}

    history = []
    async for snapshot in graph.aget_state_history(config):
        if snapshot.values:
            history.append(AgentState(**snapshot.values))

    logger.debug(f"get_thread_history_done. thread_id={thread_id} snapshots={len(history)}")
    return list(reversed(history))
