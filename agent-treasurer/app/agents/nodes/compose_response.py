"""
Compose response node — assembles one email from per-deal sections.

Reads deal_results from state (set by process_deals),
builds numbered sections, adds signature.
"""

import logging
from typing import Any

from ..state import AgentState

logger = logging.getLogger(__name__)


async def compose_response_node(state: AgentState) -> dict[str, Any]:
    """Assemble response from per-deal sections."""
    results = state.deal_results or []
    logger.debug(
        f"compose_response_enter. phase={state.phase} "
        f"results_count={len(results)} thread_locked={state.thread_locked}"
    )

    # Early nodes hit phase="error" → generate user-facing error and keep phase
    if state.phase == "error":
        logger.warning(f"compose_response_error_phase. last_error={state.last_error}")
        return {
            "response_message": _error_response(),
            "phase": "error",
        }

    warnings = state.warnings or []

    if not results:
        if any(w.startswith("Unknown deal number referenced:") for w in warnings):
            logger.warning(f"compose_response_unknown_deal_number. warnings={warnings}")
            return {
                "response_message": (
                    "Добрый день!\n\n"
                    "Я не нашел сделки по указанному номеру. "
                    "Пожалуйста, уточните номер сделки и повторите ответ.\n\n"
                    "С уважением,\nкоманда Казначейства"
                ),
                "phase": "responding",
            }
        logger.warning("compose_response_no_results")
        return {
            "response_message": _error_response(),
            "phase": "responding",
        }

    sections: list[str] = []
    single_deal = len(results) == 1

    for result in results:
        if single_deal:
            sections.append(result.response_section)
        else:
            sections.append(f"Сделка {result.deal.deal_number}:\n{result.response_section}")

    body = "\n\n".join(sections)

    # Log escalation info (not shown to client)
    if state.thread_locked:
        employee = next(
            (d.assigned_employee for d in state.deals if d.assigned_employee),
            None,
        )
        if employee:
            logger.info(f"compose_response_thread_escalated. employee={employee}")

    response = f"Добрый день!\n\n{body}\n\nС уважением,\nкоманда Казначейства"

    logger.info(
        f"compose_response_ok. sections_count={len(sections)} "
        f"thread_locked={state.thread_locked} response_length={len(response)}"
    )

    return {
        "response_message": response,
        "phase": "responding",
    }


def _error_response() -> str:
    return (
        "Добрый день!\n\n"
        "К сожалению, я не смог обработать ваш запрос.\n"
        "Пожалуйста, уточните условия сделки или обратитесь к уполномоченному сотруднику Казначейства\n\n"
        "С уважением,\nкоманда Казначейства"
    )
