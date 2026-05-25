"""
LangGraph StateGraph definition for the multi-deal processing agent.

Happy path:
  receive → parse_message → process_deals → check_thread → compose_response → send_response → END

Fatal errors short-circuit to END from any intermediate node.
Interrupt after send_response for iterative negotiation (pause/resume).
"""

import logging
from inspect import signature
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler
from langgraph.graph import END, START, StateGraph
from langgraph.checkpoint.memory import MemorySaver

from ..core.config import settings
from ..core.tracing import (
    current_hops,
    current_x_trace_id,
    safe_trace_json,
    trace_action_span,
)
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


def _state_trace_payload(state: AgentState | dict | None) -> dict[str, Any] | None:
    if state is None:
        return None
    if isinstance(state, AgentState):
        data = state.model_dump()
    elif isinstance(state, dict):
        data = state
    else:
        return {"state_repr": repr(state)}

    return {
        "phase": data.get("phase"),
        "incoming_message": data.get("incoming_message"),
        "incoming_message_id": data.get("incoming_message_id"),
        "last_processed_message_id": data.get("last_processed_message_id"),
        "is_new_thread": data.get("is_new_thread"),
        "awaiting_reply": data.get("awaiting_reply"),
        "thread_locked": data.get("thread_locked"),
        "response_sent": data.get("response_sent"),
        "stop_event": data.get("stop_event"),
        "deals": data.get("deals"),
        "deal_updates": data.get("deal_updates"),
        "warnings": data.get("warnings"),
        "last_error": data.get("last_error"),
        "error_diagnostics": data.get("error_diagnostics"),
    }


def _config_trace_payload(config: Any) -> dict[str, Any]:
    if not config:
        return {}
    if isinstance(config, dict):
        return {"configurable": config.get("configurable", {})}
    return {"config_repr": repr(config)}


def _traced_node(name: str, fn, *, rollback_possible: bool = True):
    accepts_config = len(signature(fn).parameters) >= 2

    async def _wrapped(state: AgentState, config: Any = None) -> dict[str, Any]:
        with trace_action_span(
            f"graph.{name}",
            call_type="action",
            target_name=f"langgraph.{name}",
            request_payload={
                "node": name,
                "state": _state_trace_payload(state),
                "config": _config_trace_payload(config),
            },
            is_mutation=True,
            rollback_possible=rollback_possible,
            extra_attrs={
                "aef.node": name,
                "aef.hops_used": current_hops(),
            },
        ) as span:
            result = await fn(state, config) if accepts_config else await fn(state)
            span.add_span_attributes(
                **{
                    "aef.response_payload": safe_trace_json(result),
                    "aef.result_payload": safe_trace_json(result),
                    "aef.stop_event": result.get("stop_event"),
                }
            )
            span.add_output_result(result)
            return result

    _wrapped.__name__ = f"traced_{name}_node"
    return _wrapped


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

    builder.add_node("receive", _traced_node("receive", mailman_receive_node))
    builder.add_node("parse_message", _traced_node("parse_message", parse_message_node))
    builder.add_node("process_deals", _traced_node("process_deals", process_deals_node))
    builder.add_node("check_thread", _traced_node("check_thread", check_thread_node))
    builder.add_node("compose_response", _traced_node("compose_response", compose_response_node))
    builder.add_node(
        "send_response",
        _traced_node("send_response", mailman_send_response_node, rollback_possible=False),
    )

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

    with trace_action_span(
        "agent.process_message",
        call_type="agent_call",
        target_name="deal_agent_graph",
        request_payload={
            "thread_id": resolved_thread_id,
            "counterparty_id": counterparty_id,
            "incoming_message_id": incoming_message_id,
            "message": message,
            "x_trace_id": current_x_trace_id(),
        },
        is_mutation=True,
        rollback_possible=True,
        extra_attrs={
            "aef.session_id": resolved_thread_id,
            "aef.ttl": settings.operation_ttl_sec,
            "aef.hops": settings.operation_max_hops,
        },
    ) as span:
        final_state_dict = await graph.ainvoke(initial_state, config)
        span.add_span_attributes(**{
            "aef.response_payload": safe_trace_json(final_state_dict),
            "aef.result_payload": safe_trace_json(final_state_dict),
            "aef.stop_event": final_state_dict.get("stop_event"),
        })
        span.add_output_result(final_state_dict)
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

    with trace_action_span(
        "agent.resume_with_message",
        call_type="agent_call",
        target_name="deal_agent_graph",
        request_payload={
            "thread_id": thread_id,
            "incoming_message_id": incoming_message_id,
            "message": message,
            "x_trace_id": current_x_trace_id(),
        },
        is_mutation=True,
        rollback_possible=True,
        extra_attrs={
            "aef.session_id": thread_id,
            "aef.ttl": settings.operation_ttl_sec,
            "aef.hops": settings.operation_max_hops,
        },
    ) as span:
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

        with trace_action_span(
            "graph.resume_state_update",
            call_type="action",
            target_name="langgraph.update_state",
            request_payload={"thread_id": thread_id, "update": update},
            is_mutation=True,
            rollback_possible=True,
        ) as update_span:
            await graph.aupdate_state(config, update, as_node=START)
            update_span.add_output_result({"updated": True, "thread_id": thread_id})

        final_state_dict = await graph.ainvoke(None, config)
        span.add_span_attributes(**{
            "aef.response_payload": safe_trace_json(final_state_dict),
            "aef.result_payload": safe_trace_json(final_state_dict),
            "aef.stop_event": final_state_dict.get("stop_event"),
        })
        span.add_output_result(final_state_dict)
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
