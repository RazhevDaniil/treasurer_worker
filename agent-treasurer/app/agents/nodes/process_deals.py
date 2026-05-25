"""
Process deals node — dispatches each DealUpdate to the appropriate handler.

Absorbs logic from:
- deal_negotiator.py (negotiate, generate offer, escalate)
- mailman.py (missing fields check)
"""

import logging
from datetime import datetime
from typing import Any, Optional

from langchain_core.runnables import RunnableConfig

from .parse_message import REQUIRED_DEAL_FIELDS, _merge_conditions
from ..state import AgentState
from ...models.schemas import (
    Deal,
    DealConditions,
    DealResult,
    DealStatus,
    DealUpdate,
    OfferRecord,
    RateError,
    ValidationResult,
)
from ...services.deal_service import DealService

logger = logging.getLogger(__name__)


def _build_offer_text(
    deal: "Deal",
    suggestions: list[str] | None = None,
) -> str:
    """Build human-readable offer from deal conditions."""
    c = deal.conditions
    lines: list[str] = []
    lines.append(f"Продукт: {c.product}")
    if c.inn:
        lines.append(f"ИНН: {c.inn}")
    if c.term_days:
        lines.append(f"Срок: {c.term_days} дней")
    if c.volume:
        lines.append(f"Сумма: {c.volume:,.0f} {c.currency}")
    lines.append(f"Тип ставки: {_format_rate_type(c.rate_type)}")
    if c.basis is not None:
        lines.append(f"Базис начисления: {_format_basis(c.basis)}")
    if c.optionality:
        lines.append(f"Опциональность: {_format_optionality(c.optionality)}")

    if suggestions:
        lines.append("")
        for s in suggestions:
            lines.append(f"* {s}")

    if c.rate is not None:
        lines.append("")
        lines.append(f"\nНаше предложение по ставке: {c.rate:.2f}%")
        if c.rate_type == "FLOAT":
            lines.append("")
            lines.append(
                "Ставка считается как КС + спред. "
                "Если интересует расчёт от других финансовых показателей — "
                "обратитесь к менеджеру."
            )
    return "\n".join(lines)


def _format_optionality(optionality: Optional[str]) -> str:
    """Format optionality field for display."""
    optionality_map = {
        "POP": "пополнение",
        "OTZ": "отзывность",
        "POP_OTZ": "пополнение и отзывность",
    }
    return optionality_map.get(optionality, "нет") if optionality else "нет"



def _format_rate_type(rate_type: str) -> str:
    """Format rate type for display."""
    return "фиксированная" if rate_type == "FIX" else "плавающая"


def _format_basis(basis: Optional[str]) -> str:
    """Format interest accrual basis for display."""
    basis_map = {
        "MONTH": "ежемесячно",
        "QUARTAL": "ежеквартально",
        "SEMIANNUAL": "раз в полгода",
        "ANNUAL": "ежегодно",
        "END": "в конце срока",
    }
    return basis_map.get(basis, "не указан") if basis else "не указан"


# ============================================================================
# Per-deal handlers
# ============================================================================

async def _handle_new_deal(
    deal: Deal,
    update: DealUpdate,
    suggestions: list[str] | None = None,
) -> DealResult:
    """Handle new deal: negotiate."""
    return await _negotiate_deal(deal, update, suggestions)


async def _negotiate_deal(
    deal: Deal,
    update: DealUpdate,
    suggestions: list[str] | None = None,
) -> DealResult:
    """Generate offer for a deal using rate ladder.

    Rate ladder flow:
    1. First call / conditions changed → fetch rates, store ladder, offer first (lowest)
    2. Client unhappy, conditions same → advance position, offer next rate
    3. Ladder exhausted → escalate
    """
    logger.debug(
        f"negotiate_deal_start. deal_number={deal.deal_number} "
        f"iteration={deal.iteration_count} max_iterations={deal.max_iterations} "
        f"ladder_len={len(deal.rate_ladder)} ladder_pos={deal.rate_ladder_position}"
    )

    # Check iteration limit
    if deal.iteration_count >= deal.max_iterations:
        logger.info(
            f"negotiate_iteration_limit_reached. deal_number={deal.deal_number} "
            f"iteration_count={deal.iteration_count}"
        )
        return await _escalate_deal(deal, "Превышен лимит итераций переговоров")

    # Detect whether structural deal parameters changed (not just rate).
    # Rate is the subject of negotiation — changing it should not reset the ladder.
    # Compare update.conditions against previous offer (last proposed conditions),
    # because parse_message already merged update into deal.conditions.
    _STRUCTURAL_FIELDS = ("product", "inn", "term_days", "volume", "currency", "rate_type", "basis", "optionality")
    structural_changed = False
    if update.conditions is not None and deal.offers_history:
        prev = deal.offers_history[-1].proposed
        if prev is not None:
            for field in _STRUCTURAL_FIELDS:
                new_val = getattr(update.conditions, field, None)
                if new_val is not None and new_val != getattr(prev, field):
                    structural_changed = True
                    break

    # Merge new conditions if provided (may be redundant after parse_message,
    # but needed for first turn and for handlers that bypass parse_message)
    if update.conditions is not None:
        merged = _merge_conditions(deal.conditions, update.conditions)
        deal = deal.model_copy(update={"conditions": merged, "updated_at": datetime.utcnow()})

    # Reset ladder only when structural parameters changed
    if structural_changed:
        deal = deal.model_copy(update={
            "rate_ladder": [],
            "rate_ladder_position": 0,
        })
        logger.debug(f"negotiate_structural_change_ladder_reset. deal_number={deal.deal_number}")

    # Fetch rate ladder if empty (first request or conditions changed)
    if not deal.rate_ladder:
        deal_service = DealService()
        rate_ladder, rate_error = await deal_service.get_rate(deal.conditions)

        if rate_ladder is None:
            logger.warning(
                f"negotiate_no_rate. deal_number={deal.deal_number} "
                f"rate_error={rate_error if rate_error else None}"
            )
            match rate_error:
                case RateError.CURRENCY_NOT_SUPPORTED:
                    message = (
                        "К сожалению, агент пока не имеет возможности вести расчёты "
                        "по валютным сделкам. Обратитесь к менеджеру."
                    )
                case RateError.RUB_CALCULATION_FAILED:
                    message = (
                        "К сожалению, агент пока не имеет возможности вести расчёты "
                        "по рублёвым сделкам. Обратитесь к менеджеру."
                    )
                case _:
                    message = (
                        "К сожалению, сервис расчёта ставок пока недоступен. "
                        "Не можем сформировать предложение. Попробуйте позже."
                    )
            return DealResult(deal=deal, response_section=message)

        deal = deal.model_copy(update={
            "rate_ladder": rate_ladder,
            "rate_ladder_position": 0,
        })
        logger.debug(
            f"negotiate_ladder_fetched. deal_number={deal.deal_number} "
            f"ladder={rate_ladder}"
        )
    else:
        # Ladder exists, structural params didn't change.
        # Advance position only if client is actually asking for more (or just rejecting).
        # If client requests a rate <= current ladder rate, they're being favorable — no advance.
        if not structural_changed:
            _, current_rate = deal.rate_ladder[deal.rate_ladder_position]
            client_rate = update.conditions.rate if update.conditions else None
            if client_rate is None or client_rate > current_rate:
                deal = deal.model_copy(update={
                    "rate_ladder_position": deal.rate_ladder_position + 1,
                })

    # Check if ladder exhausted → escalate
    if deal.rate_ladder_position >= len(deal.rate_ladder):
        logger.info(
            f"negotiate_ladder_exhausted. deal_number={deal.deal_number} "
            f"ladder={deal.rate_ladder} ladder_len={len(deal.rate_ladder)}"
        )
        return await _escalate_deal(
            deal, "Исчерпаны все доступные варианты ставок", ladder_exhausted=True,
        )

    # Pick rate at current position
    source_key, ladder_rate = deal.rate_ladder[deal.rate_ladder_position]

    # If client requested a specific rate in this turn, offer the better of the two.
    # Otherwise use the ladder rate (previous deal.conditions.rate is our old offer, not client's request).
    requested_rate = update.conditions.rate if update.conditions else None
    if requested_rate is not None:
        final_rate = min(requested_rate, ladder_rate)
    else:
        final_rate = ladder_rate

    logger.debug(
        f"negotiate_rate_selected. deal_number={deal.deal_number} "
        f"source={source_key} ladder_rate={ladder_rate} "
        f"requested_rate={requested_rate} final_rate={final_rate} "
        f"position={deal.rate_ladder_position}"
    )

    # Set final rate on deal
    deal = deal.model_copy(update={
        "conditions": deal.conditions.model_copy(update={"rate": final_rate}),
    })

    # Format offer
    response_section = _build_offer_text(deal, suggestions)

    # Update deal history and iteration
    deal = deal.model_copy(update={
        "iteration_count": deal.iteration_count + 1,
        "negotiation_history": deal.negotiation_history + [
            f"[Предложение #{deal.iteration_count + 1} ({source_key})]: {response_section}"
        ],
        "updated_at": datetime.utcnow(),
    })

    # Record offer
    offer_record = OfferRecord(
        incoming=update.conditions,
        proposed=deal.conditions,
        iteration=deal.iteration_count,
    )
    deal = deal.model_copy(update={
        "offers_history": deal.offers_history + [offer_record],
    })

    logger.info(
        f"negotiate_offer_generated. deal_number={deal.deal_number} "
        f"rate={final_rate} source={source_key} "
        f"position={deal.rate_ladder_position} iteration={deal.iteration_count}"
    )

    return DealResult(deal=deal, response_section=response_section)


async def _accept_deal(deal: Deal, update: DealUpdate) -> DealResult:
    """Handle client agreement — validate rate against ladder, then respond with deal link."""
    # If conditions changed or rate ladder was never fetched — re-negotiate
    if update.conditions is not None or not deal.rate_ladder:
        logger.info(
            f"accept_needs_negotiation. deal_number={deal.deal_number} "
            f"has_new_conditions={update.conditions is not None} "
            f"has_ladder={bool(deal.rate_ladder)}"
        )
        return await _negotiate_deal(deal, update)

    # Validate rate doesn't exceed current ladder position
    _, current_max_rate = deal.rate_ladder[deal.rate_ladder_position]
    if deal.conditions.rate is not None and deal.conditions.rate > current_max_rate:
        logger.info(
            f"accept_rate_exceeds_ladder. deal_number={deal.deal_number} "
            f"deal_rate={deal.conditions.rate} ladder_rate={current_max_rate}"
        )
        return await _negotiate_deal(deal, update)

    deal = deal.model_copy(update={
        "status": DealStatus.ACCEPTED,
        "updated_at": datetime.utcnow(),
    })

    logger.info(f"deal_accepted. deal_number={deal.deal_number}")

    c = deal.conditions

    return DealResult(
        deal=deal,
        response_section=(
            f"Отлично! Фиксируем условия:\n"
            f"  {c.product}, {c.term_days} дн., "
            f"{c.volume:,.0f} {c.currency}, {c.rate}%\n\n"
            f"Оформите сделку в системе ЕФС и отправьте на одобрение в казначейство."
        ),
    )


async def _escalate_deal(
    deal: Deal, reason: str, *, ladder_exhausted: bool = False,
) -> DealResult:
    """Escalate deal to human employee."""
    deal_service = DealService()
    employee_email = await deal_service.get_available_employee()

    deal = deal.model_copy(update={
        "status": DealStatus.ESCALATED,
        "assigned_employee": employee_email,
        "updated_at": datetime.utcnow(),
    })

    logger.info(
        f"deal_escalated. deal_number={deal.deal_number} "
        f"reason={reason} employee={employee_email}"
    )

    if ladder_exhausted:
        response = (
            f"Пока не могу предложить условий лучше, "
            f"обсудите с менеджером: {employee_email}"
        )
    else:
        reasons_list = "\n".join(f"  - {r}" for r in reason.split("; "))
        response = (
            f"К сожалению, агент не имеет возможности работать с такой сделкой:\n"
            f"{reasons_list}\n\n"
            f"По этой сделке следует обратиться к менеджеру: {employee_email}"
        )

    return DealResult(deal=deal, response_section=response)


async def _provide_data(
    deal: Deal,
    update: DealUpdate,
    suggestions: list[str] | None = None,
) -> DealResult:
    """Handle user providing missing data."""
    logger.debug(f"provide_data_start. deal_number={deal.deal_number}")

    if update.conditions is not None:
        merged = _merge_conditions(deal.conditions, update.conditions)
        deal = deal.model_copy(update={"conditions": merged, "updated_at": datetime.utcnow()})

    # Re-check required fields
    missing = [
        label for field, label in REQUIRED_DEAL_FIELDS.items()
        if getattr(deal.conditions, field) is None
    ]

    if missing:
        logger.info(f"provide_data_still_missing. deal_number={deal.deal_number} missing={missing}")
        missing_list = "\n".join(f"  - {m}" for m in missing)
        return DealResult(
            deal=deal,
            response_section=f"Всё ещё не хватает:\n{missing_list}",
        )

    logger.debug(f"provide_data_complete_proceeding_to_negotiate. deal_number={deal.deal_number}")
    # All fields present — generate offer
    return await _negotiate_deal(deal, update, suggestions)


# ============================================================================
# Node function
# ============================================================================

async def process_deals_node(state: AgentState, config: RunnableConfig) -> dict[str, Any]:
    """Process each DealUpdate, dispatching to the appropriate handler."""
    logger.debug(f"process_deals_enter. updates_count={len(state.deal_updates)}")

    results: list[DealResult] = []
    warnings: list[str] = []
    updated_deals = list(state.deals)
    deals_by_number = {d.deal_number: d for d in updated_deals}
    processed_updates = 0

    for update in state.deal_updates:
        logger.debug(
            f"process_deal_dispatch. deal_number={update.deal_number} "
            f"intent={update.intent}"
        )

        deal = deals_by_number.get(update.deal_number)
        if deal is None:
            logger.warning(f"process_deal_unknown_number. deal_number={update.deal_number}")
            warnings.append(f"Unknown deal number referenced: {update.deal_number}")
            continue

        # Universal required fields check (before any business logic)
        missing = [
            label for field, label in REQUIRED_DEAL_FIELDS.items()
            if getattr(deal.conditions, field) is None
        ]
        if missing:
            logger.info(
                f"process_deal_missing_fields. deal_number={deal.deal_number} "
                f"missing={missing}"
            )
            missing_list = "\n".join(f"  - {m}" for m in missing)
            result = DealResult(
                deal=deal,
                response_section=f"Для оформления сделки {deal.deal_number} необходимо указать:\n{missing_list}",
            )
            deals_by_number[update.deal_number] = result.deal
            results.append(result)
            processed_updates += 1
            continue

        # Business rules validation (currency, term_days, volume ranges)
        validation = deal.conditions.validate_business_rules()

        if validation.escalation_reasons:
            reason = "; ".join(validation.escalation_reasons)
            logger.info(
                f"process_deal_escalation. deal_number={deal.deal_number} "
                f"reasons={validation.escalation_reasons}"
            )
            result = await _escalate_deal(deal, reason)
            deals_by_number[update.deal_number] = result.deal
            results.append(result)
            processed_updates += 1
            continue

        # Apply adjusted conditions (e.g. FLOAT → FIX)
        if validation.adjusted_conditions:
            deal = deal.model_copy(update={
                "conditions": deal.conditions.model_copy(
                    update=validation.adjusted_conditions
                ),
                "rate_ladder": [],
                "rate_ladder_position": 0,
            })
            # Also patch update.conditions so merge in _negotiate_deal
            # doesn't overwrite the adjustment
            if update.conditions is not None:
                update = update.model_copy(update={
                    "conditions": update.conditions.model_copy(
                        update=validation.adjusted_conditions
                    ),
                })
            logger.info(
                f"process_deal_conditions_adjusted. deal_number={deal.deal_number} "
                f"adjustments={validation.adjusted_conditions}"
            )

        # Suggestions without adjustments (e.g. min volume info) —
        # return immediately, no rate calculation needed
        if validation.suggestions and not validation.adjusted_conditions:
            message = "\n".join(f"* {s}" for s in validation.suggestions)
            result = DealResult(deal=deal, response_section=message)
            deals_by_number[update.deal_number] = result.deal
            results.append(result)
            processed_updates += 1
            continue

        # Collect suggestions for response text (only with adjustments, e.g. FIX suggestion)
        suggestions = validation.suggestions or None

        match update.intent:
            case "new":
                result = await _handle_new_deal(deal, update, suggestions)
            case "negotiate":
                result = await _negotiate_deal(deal, update, suggestions)
            case "approve":
                result = await _accept_deal(deal, update)
            case "provide_data":
                result = await _provide_data(deal, update, suggestions)
            case "escalate":
                result = await _escalate_deal(deal, "Запрос пользователя")
            case _:
                result = await _negotiate_deal(deal, update, suggestions)

        logger.debug(
            f"process_deal_result. deal_number={update.deal_number} "
            f"intent={update.intent} status={result.deal.status.value}"
        )

        deals_by_number[update.deal_number] = result.deal
        results.append(result)
        processed_updates += 1

    final_deals = sorted(deals_by_number.values(), key=lambda d: d.deal_number)

    if state.deal_updates and processed_updates == 0:
        logger.error(f"process_deals_no_valid_updates. updates_count={len(state.deal_updates)}")
        return {
            "deals": final_deals,
            "deal_results": results,
            "warnings": warnings,
            "phase": "error",
            "last_error": "No valid deals matched in reply",
        }

    logger.info(
        f"process_deals_done. processed={processed_updates} "
        f"total_deals={len(final_deals)} warnings_count={len(warnings)}"
    )

    return {
        "deals": final_deals,
        "deal_results": results,
        "warnings": warnings,
        "phase": "processing",
    }
