"""Approve service — deterministic deal approval without LLM/graph/negotiation.

Receives CalcFundCost-enriched parameters, validates business rules,
optionally checks rate feasibility, returns a decision.
"""

import logging
from datetime import datetime, timezone

from ..models.schemas import DealConditions, RateError
from .deal_service import DealService

logger = logging.getLogger(__name__)

# Fields that can be extracted from parameters to build DealConditions
_CONDITION_FIELDS = (
    "product", "inn", "term_days", "volume", "currency",
    "rate", "rate_type", "basis", "optionality",
)


def _build_conditions(parameters: dict) -> DealConditions | None:
    """Extract DealConditions from enrichment parameters dict.

    Returns None if critical fields (inn, term_days, volume) are all missing.
    """
    extracted = {}
    for field in _CONDITION_FIELDS:
        if field in parameters and parameters[field] is not None:
            extracted[field] = parameters[field]

    # Accept currency string values from CalcFundCost (e.g. "USD" → "OTHER")
    if "currency" in extracted and extracted["currency"] not in ("RUB", "CNY", "INR", "OTHER"):
        extracted["currency"] = "OTHER"

    if not extracted:
        return None

    try:
        return DealConditions(**extracted)
    except Exception:
        logger.warning(f"approve_build_conditions_failed. extracted_keys={list(extracted.keys())}")
        return None


async def approve(parameters: dict) -> tuple[str, str]:
    """Run business validation and return (decision, reason).

    decision is "COMPLETED" or "REJECTED" (matching AgentDecision enum).

    Steps:
    1. Extract DealConditions from parameters
    2. Validate business rules (per-currency limits, INN format)
    3. If passed: optionally check rate via agent_tools_app
    4. Return decision
    """

    conditions = _build_conditions(parameters)

    if conditions is None:
        return "REJECTED", (
            "Недостаточно параметров для проверки условий сделки. "
            "Ожидаются: inn, term_days, volume, currency."
        )

    # Check required fields
    missing = []
    if conditions.inn is None:
        missing.append("ИНН")
    if conditions.term_days is None:
        missing.append("срок (term_days)")
    if conditions.volume is None:
        missing.append("объём (volume)")

    if missing:
        return "REJECTED", f"Отсутствуют обязательные параметры: {', '.join(missing)}"

    # Business rules validation (per-currency limits)
    validation = conditions.validate_business_rules()

    if validation.escalation_reasons:
        reason = "; ".join(validation.escalation_reasons)
        logger.info(f"approve_escalation. reasons={validation.escalation_reasons}")
        return "REJECTED", reason

    if validation.suggestions and not validation.adjusted_conditions:
        reason = "; ".join(validation.suggestions)
        logger.info(f"approve_suggestions. suggestions={validation.suggestions}")
        return "REJECTED", reason

    # Apply any adjustments (e.g. FLOAT → FIX) for rate check
    if validation.adjusted_conditions:
        conditions = conditions.model_copy(update=validation.adjusted_conditions)

    # Try rate calculation
    deal_service = DealService()
    rate_ladder, rate_error = await deal_service.get_rate(conditions)

    if rate_ladder is None:
        err_msg = {
            RateError.SERVICE_UNAVAILABLE: "Сервис расчёта ставок недоступен",
            RateError.CURRENCY_NOT_SUPPORTED: "Валюта не поддерживается расчётным сервисом",
            RateError.RUB_CALCULATION_FAILED: "Ошибка расчёта рублёвой ставки",
        }.get(rate_error, "Неизвестная ошибка расчёта ставки")
        logger.warning(f"approve_rate_failed. rate_error={rate_error}")
        return "REJECTED", err_msg

    # Rate is feasible → approved
    lowest_rate = min(rate for _, rate in rate_ladder)
    reason_parts = [f"Параметры сделки в допустимых пределах."]

    if conditions.rate is not None:
        if conditions.rate >= lowest_rate:
            reason_parts.append(
                f"Запрошенная ставка ({conditions.rate:.2f}%) выше минимальной доступной ({lowest_rate:.2f}%)."
            )
        else:
            reason_parts.append(
                f"Запрошенная ставка ({conditions.rate:.2f}%) ниже минимальной доступной ({lowest_rate:.2f}%), "
                "но условия в допустимых пределах."
            )
    else:
        reason_parts.append(f"Доступная ставка от {lowest_rate:.2f}%.")

    if validation.suggestions:
        reason_parts.extend(validation.suggestions)

    logger.info(f"approve_completed. conditions={conditions}")
    return "COMPLETED", " ".join(reason_parts)


def approve_version() -> str:
    return "agent_treasurer_app-approve-v1.0.0"
