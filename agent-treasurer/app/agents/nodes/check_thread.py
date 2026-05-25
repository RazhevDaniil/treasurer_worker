"""
Check thread node — determines thread-level state after processing all deals.

Rules:
- Any deal escalated → lock entire thread (all non-terminal deals become escalated)
- All deals terminal → thread complete, no interrupt
- Otherwise → await reply
"""

import logging
from typing import Any

from ..state import AgentState
from ...models.schemas import DealStatus

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {DealStatus.ACCEPTED, DealStatus.ESCALATED}


async def check_thread_node(state: AgentState) -> dict[str, Any]:
    """Determine thread-level state after processing all deals."""
    deals = state.deals

    logger.debug(f"check_thread_enter. deals_count={len(deals)}")

    if not deals:
        logger.warning("check_thread_no_deals")
        return {
            "phase": "error",
            "last_error": "No deals to check",
        }

    # Any escalation → lock entire thread
    escalated = [d for d in deals if d.status == DealStatus.ESCALATED]
    if escalated:
        employee = escalated[0].assigned_employee
        updated = []
        for d in deals:
            if d.status not in TERMINAL_STATUSES:
                d = d.model_copy(update={
                    "status": DealStatus.ESCALATED,
                    "assigned_employee": employee,
                })
            updated.append(d)

        escalated_deals = [d.deal_number for d in escalated]
        total_deals_locked = len([d for d in updated if d.status == DealStatus.ESCALATED])
        logger.info(
            f"check_thread_escalation. escalated_deals={escalated_deals} "
            f"employee={employee} total_deals_locked={total_deals_locked}"
        )

        return {
            "deals": updated,
            "thread_locked": True,
            "escalation_requested": True,
            "escalation_reason": f"Сделка эскалирована → весь тред заблокирован. Сотрудник: {employee}",
            "awaiting_reply": False,
        }

    # All deals in terminal state → thread complete
    if all(d.status in TERMINAL_STATUSES for d in deals):
        logger.info(f"check_thread_all_terminal. deals_count={len(deals)}")
        return {
            "awaiting_reply": False,
            "phase": "completed",
        }

    # Some deals still active → await reply
    active = [d.deal_number for d in deals if d.status not in TERMINAL_STATUSES]
    logger.info(f"check_thread_awaiting_reply. active_deal_numbers={active}")
    return {
        "awaiting_reply": True,
        "phase": "responding",
    }
