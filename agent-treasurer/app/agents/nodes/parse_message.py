"""
Parse message node — splits incoming message into per-deal fragments and extracts conditions.

Two-phase pipeline:
  Phase A: Regex splitter (deterministic, no LLM)
  Phase B: Per-fragment LLM extraction → DealUpdate objects
"""

import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from ..state import AgentState
from ...core.config import settings
from ...core.llm import get_llm_with_config
from ...core.llm_retry import (
    GIGAPLATFORM_STOP_EVENT,
    is_gigaplatform_stop_event,
    llm_retrying_async,
    log_llm_exhausted,
)
from ...models.schemas import (
    Deal,
    DealConditions,
    DealStatus,
    DealUpdate,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Fresh start detection
# ============================================================================

def _detect_fresh_start(text: str, active_deals: list[Deal]) -> bool:
    """Detect if user is starting fresh deals instead of continuing existing ones.

    Extracts volume and term_days from text via regex, compares against every
    active deal. If both are found and none of the active deals match — fresh start.
    INN is used as an additional distinguishing factor when available.
    """
    volume = DealConditions._extract_volume(text)
    term_days = DealConditions._extract_term_days(text)

    # Need both mandatory parameters to make a decision
    if volume is None or term_days is None:
        return False

    inn = DealConditions._extract_inn(text)

    for deal in active_deals:
        dc = deal.conditions

        # If INN extracted and deal has INN — mismatch means definitely not this deal
        if inn is not None and dc.inn is not None and inn != dc.inn:
            continue

        # Volume match: ±1% tolerance
        volume_match = (
            dc.volume is not None
            and abs(volume - dc.volume) / max(volume, dc.volume) < 0.01
        )

        # Term match: exact
        term_match = dc.term_days is not None and term_days == dc.term_days

        if volume_match and term_match:
            # Found a matching active deal — not a fresh start
            return False

    # No active deal matched
    return True


def _drop_active_deals(deals: list[Deal]) -> list[Deal]:
    """Remove all NEGOTIATING deals, keep only terminal ones."""
    return [d for d in deals if d.status != DealStatus.NEGOTIATING]


# ============================================================================
# Phase A: Fragment splitter
# ============================================================================

@dataclass
class Fragment:
    text: str
    deal_number_hint: int | None = None


# Delimiter patterns: (name, regex, has_number_group, is_separator)
# has_number: True if regex has a capture group with deal number
# is_separator: True for patterns that split BETWEEN fragments (like semicolon)
#   vs patterns that mark the START of each fragment
DELIMITER_PATTERNS: list[tuple[str, str, bool, bool]] = [
    # 1. "Сделка N:" / "Deal N:"
    ("sdelka", r"(?:сделка|deal)\s*(\d+)\s*[:.;\-—]", True, False),
    # 2. "По N —" (reply references)
    ("po", r"(?:^|(?<=[\.\!\?\n]))\s*по\s+(\d+)\s*[:.;\-—]", True, False),
    # 3. Numbered: "1)" / "1." / "1:" / "1 -" (inline or line-start)
    ("numbered", r"(?:^|\n|\s)(\d+)\s*[).:\-—]\s", True, False),
    # 4. Lettered cyrillic: "а)" / "б." (inline or line-start)
    ("cyrillic_letter", r"(?:^|\n|\s)([а-яё])\s*[).]\s", False, False),
    # 5. Lettered latin: "a)" / "B." (inline or line-start)
    ("latin_letter", r"(?:^|\n|\s)([a-zA-Z])\s*[).]\s", False, False),
    # 6. Bullet: "•" / "* "
    ("bullet", r"(?:^|\n)\s*[•\*]\s+", False, False),
    # 7. Numbered with #: "#1" / "# 1"
    ("hash", r"#\s*(\d+)", True, False),
    # 8. Semicolon-separated (separator: splits between fragments)
    ("semicolon", r"\s*;\s*", False, True),
]


def _looks_like_deal(text: str) -> bool:
    """Check if text fragment contains deal-like markers."""
    text_lower = text.lower()
    has_product = bool(re.search(r"деп|нсо|nso|размест|депозит", text_lower))
    has_volume = DealConditions._extract_volume(text) is not None
    has_term = DealConditions._extract_term_days(text) is not None
    has_rate = DealConditions._extract_rate(text) is not None
    has_inn = DealConditions._extract_inn(text) is not None
    has_intent = bool(re.search(r"согласен|одобр|подтвержд|ставк|поменя|измени|инн\s", text_lower))
    return any([has_product, has_volume, has_term, has_rate, has_inn, has_intent])


def _split_at_delimiters(
    text: str,
    matches: list[re.Match],
    has_number: bool,
) -> list[Fragment]:
    """Split text at delimiter positions (each match marks START of a fragment)."""
    fragments: list[Fragment] = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        fragment_text = text[start:end].strip()
        if not fragment_text:
            continue

        deal_number_hint = None
        if has_number and match.lastindex and match.lastindex >= 1:
            try:
                deal_number_hint = int(match.group(1))
            except (ValueError, IndexError):
                pass

        fragments.append(Fragment(text=fragment_text, deal_number_hint=deal_number_hint))

    return fragments


def _split_at_separators(
    text: str,
    matches: list[re.Match],
) -> list[Fragment]:
    """Split text at separator positions (each match is BETWEEN fragments)."""
    fragments: list[Fragment] = []
    prev_end = 0
    for match in matches:
        chunk = text[prev_end:match.start()].strip()
        if chunk:
            fragments.append(Fragment(text=chunk))
        prev_end = match.end()
    # Remainder after last separator
    chunk = text[prev_end:].strip()
    if chunk:
        fragments.append(Fragment(text=chunk))
    return fragments


def split_into_fragments(text: str) -> list[Fragment]:
    """Split message into per-deal fragments using delimiter detection.

    Tries patterns in priority order. Returns [Fragment(text)] as-is
    if no delimiters found (single deal).
    """
    for pattern_name, regex, has_number, is_separator in DELIMITER_PATTERNS:
        flags = re.IGNORECASE | re.MULTILINE
        matches = list(re.finditer(regex, text, flags))

        if is_separator:
            # Separators: 1 match → 2 fragments, 2 matches → 3, etc.
            if len(matches) >= 1:
                fragments = _split_at_separators(text, matches)
                if len(fragments) >= 2 and all(_looks_like_deal(f.text) for f in fragments):
                    logger.debug(
                        f"split_pattern_matched. pattern={pattern_name} "
                        f"fragments_count={len(fragments)}"
                    )
                    return fragments
        else:
            # Delimiters: each match marks start of fragment
            if len(matches) >= 2:
                fragments = _split_at_delimiters(text, matches, has_number)
                if len(fragments) >= 2 and all(_looks_like_deal(f.text) for f in fragments):
                    logger.debug(
                        f"split_pattern_matched. pattern={pattern_name} "
                        f"fragments_count={len(fragments)}"
                    )
                    return fragments

    # Fallback: try newline split
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if len(lines) >= 2 and all(_looks_like_deal(line) for line in lines):
        logger.debug(
            f"split_pattern_matched. pattern=newline_fallback fragments_count={len(lines)}"
        )
        return [Fragment(text=line) for line in lines]

    # No delimiters found → single fragment
    logger.debug(f"split_no_delimiter. text_length={len(text)}")
    return [Fragment(text=text)]


# ============================================================================
# Phase B: Per-fragment LLM extraction
# ============================================================================

CONDITIONS_EXTRACTION_PROMPT = """Ты - AI-ассистент, извлекающий условия сделки из текста.

Извлеки следующие параметры сделки из фрагмента:
- product: тип продукта сделки. "Depo" (депозит) или "NSO". По умолчанию "Depo"
- inn: ИНН контрагента (10 или 12 цифр), null если не указан
- term_days: срок сделки в днях, null если не указан. Конвертируй в дни (1 месяц=30, 1 год=365 и т.д.)
- volume: объем/сумма сделки (число, от 500 млн до 15 млрд), null если не указана
- currency: валюта (RUB, CNY, INR), по умолчанию RUB. Если указана другая валюта (USD, EUR, GBP и т.д.) — верни "OTHER"
- rate: желаемая ставка (%), null если не указана
- rate_type: тип ставки — "FIX" (фиксированная) или "FLOAT" (плавающая). По умолчанию "FIX"
- basis: базис начисления процентов — "MONTH" (ежемесячно), "QUARTAL" (ежеквартально), "SEMIANNUAL" (раз в полгода), "ANNUAL" (ежегодно), "END" (в конце срока). null если не указан
- optionality: "POP" / "OTZ" / "POP_OTZ" / null

Если параметр не указан явно — верни null.
"""

REPLY_EXTRACTION_PROMPT = """Ты — AI-ассистент, анализирующий ответ по сделке.

Текущие сделки в треде:
{deals_context}

Фрагмент ответа:
{fragment_text}

Определи:
- deal_number: номер сделки (из списка выше или max+1 если новая)
- intent: "approve" / "negotiate" / "provide_data" / "escalate" / "new"
  - approve: пользователь согласен с текущими условиями
  - negotiate: пользователь не согласен с предложением, хочет лучше или хочет изменить условия (ставку, срок и т.д.)
  - provide_data: пользователь дополняет недостающие данные (ИНН, срок и т.д.)
  - escalate: пользователь явно просит передать сделку сотруднику или менеджеру
  - new: новая сделка (не связана с существующими)
- conditions: изменённые параметры (null если approve без изменений или просто отказ без новых условий)

Верни JSON с полями deal_number, intent, conditions.
"""

_DEFAULT_VALUES = {
    "product": "Depo",
    "currency": "RUB",
    "rate_type": "FIX",
}


def _has_nondefault_conditions(conditions: DealConditions) -> bool:
    """Check if parsed conditions contain at least one explicit non-default field."""
    for field, value in conditions.model_dump().items():
        if value is None:
            continue
        if field in _DEFAULT_VALUES and value == _DEFAULT_VALUES[field]:
            continue
        return True
    return False


def _extract_reply_conditions_fallback(fragment_text: str) -> DealConditions | None:
    """Extract conditions from reply text via deterministic fallback."""
    conditions = DealConditions.model_validate(
        {},
        context={"source_text": fragment_text},
    )
    return conditions if _has_nondefault_conditions(conditions) else None


def _fallback_reply_update(fragment: Fragment) -> DealUpdate:
    """Build a resilient fallback update when LLM reply extraction fails."""
    text_lower = fragment.text.lower()
    fallback_conditions = _extract_reply_conditions_fallback(fragment.text)

    if re.search(r"соглас|одобр|подтвержд|принима[юе]м|\bок\b", text_lower) and fallback_conditions is None:
        intent = "approve"
    elif re.search(r"сотрудник|менеджер|эскал|переда(й|йте|ть)", text_lower):
        intent = "escalate"
    elif fallback_conditions is not None:
        intent = "provide_data"
    else:
        intent = "negotiate"

    return DealUpdate(
        deal_number=fragment.deal_number_hint or 1,
        intent=intent,
        conditions=fallback_conditions,
    )


def _merge_conditions(current: DealConditions, new: DealConditions) -> DealConditions:
    """Merge new conditions into current. Non-None, non-default new values overwrite."""
    merged = current.model_dump()
    for field, value in new.model_dump().items():
        if value is None:
            continue
        if field in _DEFAULT_VALUES and value == _DEFAULT_VALUES[field]:
            continue
        merged[field] = value
    return DealConditions(**merged)


async def _extract_deal_conditions(message_text: str) -> DealConditions:
    """Extract deal conditions from fragment text using structured output (temperature=0)."""
    logger.debug(f"extract_conditions_llm_call. text_length={len(message_text)}")
    llm = (
        get_llm_with_config(temperature=0)
        .with_structured_output(DealConditions)
    )
    messages = [
        SystemMessage(content=CONDITIONS_EXTRACTION_PROMPT),
        HumanMessage(content=message_text),
    ]
    try:
        async for attempt in llm_retrying_async():
            with attempt:
                result = await llm.ainvoke(messages)
    except Exception as exc:
        log_llm_exhausted(exc, purpose="extract_deal_conditions")
        raise
    if result is None:
        logger.warning("extract_conditions_llm_returned_none")
        result_payload = {}
    elif isinstance(result, DealConditions):
        result_payload = result.model_dump()
    elif isinstance(result, dict):
        result_payload = result
    elif hasattr(result, "model_dump"):
        result_payload = result.model_dump()
    else:
        logger.warning(f"extract_conditions_llm_unexpected_type. result_type={type(result).__name__}")
        result_payload = {}

    conditions = DealConditions.model_validate(
        result_payload,
        context={"source_text": message_text},
    )
    logger.debug(
        f"extract_conditions_done. product={conditions.product} "
        f"volume={conditions.volume} term_days={conditions.term_days} "
        f"currency={conditions.currency} rate={conditions.rate} inn={conditions.inn}"
    )
    return conditions


async def _extract_reply_update(
    fragment: Fragment,
    existing_deals: list[Deal],
) -> DealUpdate:
    """Extract DealUpdate from a reply fragment using LLM."""
    deals_context = "\n".join(
        f"Сделка {d.deal_number}: {d.conditions.product}, "
        f"ИНН={d.conditions.inn or '?'}, "
        f"срок={d.conditions.term_days or '?'} дн., "
        f"сумма={d.conditions.volume or '?'}, "
        f"ставка={d.conditions.rate or '?'}%, "
        f"тип ставки={d.conditions.rate_type}, "
        f"базис={d.conditions.basis or '?'}, "
        f"опциональность={d.conditions.optionality or 'нет'}, "
        f"статус={d.status.value}"
        for d in existing_deals
    )

    prompt_text = REPLY_EXTRACTION_PROMPT.format(
        deals_context=deals_context,
        fragment_text=fragment.text,
    )

    logger.debug(
        f"extract_reply_update_llm_call. fragment_text={fragment.text!r} "
        f"deal_number_hint={fragment.deal_number_hint} "
        f"existing_deals_count={len(existing_deals)}"
    )

    llm = (
        get_llm_with_config(temperature=0)
        .with_structured_output(DealUpdate)
    )
    messages = [
        SystemMessage(content=prompt_text),
        HumanMessage(content=fragment.text),
    ]

    try:
        async for attempt in llm_retrying_async():
            with attempt:
                raw_update = await llm.ainvoke(messages)
    except Exception as exc:
        log_llm_exhausted(exc, purpose="extract_reply_update")
        raise
    if raw_update is None:
        logger.warning("extract_reply_update_llm_returned_none")
        update = _fallback_reply_update(fragment)
    elif isinstance(raw_update, DealUpdate):
        update = raw_update
    elif isinstance(raw_update, dict):
        update = DealUpdate.model_validate(raw_update)
    elif hasattr(raw_update, "model_dump"):
        update = DealUpdate.model_validate(raw_update.model_dump())
    else:
        logger.warning(
            f"extract_reply_update_llm_unexpected_type. result_type={type(raw_update).__name__}"
        )
        update = _fallback_reply_update(fragment)

    # Validate deal_number_hint from splitter
    if fragment.deal_number_hint is not None and update.deal_number != fragment.deal_number_hint:
        update = update.model_copy(update={"deal_number": fragment.deal_number_hint})

    # Re-validate conditions against fragment text.
    # If LLM returned empty/default-only conditions, treat it as no conditions and
    # try deterministic fallback extraction below.
    if update.conditions is not None:
        validated = DealConditions.model_validate(
            update.conditions.model_dump(),
            context={"source_text": fragment.text},
        )
        if _has_nondefault_conditions(validated):
            update = update.model_copy(update={"conditions": validated})
        else:
            update = update.model_copy(update={"conditions": None})

    # Deterministic fallback for replies when LLM missed conditions.
    if update.conditions is None:
        fallback_conditions = _extract_reply_conditions_fallback(fragment.text)
        if fallback_conditions is not None:
            fallback_intent = "provide_data" if update.intent == "approve" else update.intent
            update = update.model_copy(update={
                "intent": fallback_intent,
                "conditions": fallback_conditions,
            })

    logger.debug(
        f"extract_reply_update_done. deal_number={update.deal_number} intent={update.intent}"
    )
    return update


REQUIRED_DEAL_FIELDS: dict[str, str] = {
    "inn": "ИНН клиента (обязательно начните со слова ИНН, пример: ИНН 7707083893)",
    "term_days": "срок сделки (в днях)",
    "volume": "сумма/объём сделки",
}


def _gigaplatform_stop_result(exc: BaseException) -> dict[str, Any]:
    return {
        "phase": "error",
        "last_error": "GigaPlatform temporarily disabled GigaChat requests for this agent class",
        "error_diagnostics": f"{type(exc).__name__}: {exc}",
        "stop_event": GIGAPLATFORM_STOP_EVENT,
    }


def _apply_updates(
    existing_deals: list[Deal],
    deal_updates: list[DealUpdate],
    config: RunnableConfig,
    is_new_thread: bool,
) -> list[Deal]:
    """Create/update Deal objects from DealUpdate list."""
    deals = list(existing_deals)
    deals_by_number = {d.deal_number: d for d in deals}

    counterparty_email = config["configurable"].get("counterparty_email", "unknown")
    thread_id = config["configurable"].get("thread_id", str(uuid.uuid4()))

    for update in deal_updates:
        if update.deal_number in deals_by_number:
            # Update existing deal
            deal = deals_by_number[update.deal_number]
            if update.conditions is not None:
                merged = _merge_conditions(deal.conditions, update.conditions)
                deal = deal.model_copy(update={
                    "conditions": merged,
                    "updated_at": datetime.utcnow(),
                })
            deals_by_number[update.deal_number] = deal
        else:
            if not is_new_thread and update.intent != "new":
                # In replies, do not auto-create deals for unknown deal numbers
                # unless the parser explicitly marked this fragment as a new deal.
                continue
            # Create new deal
            conditions = update.conditions or DealConditions()
            deal = Deal(
                id=str(uuid.uuid4()),
                thread_id=thread_id,
                counterparty_email=counterparty_email,
                conditions=conditions,
                status=DealStatus.NEGOTIATING,
                deal_number=update.deal_number,
                max_iterations=settings.max_negotiation_iterations,
            )
            deals_by_number[update.deal_number] = deal

    # Return sorted by deal_number
    return sorted(deals_by_number.values(), key=lambda d: d.deal_number)


# ============================================================================
# Node function
# ============================================================================

async def parse_message_node(state: AgentState, config: RunnableConfig) -> dict[str, Any]:
    """Parse incoming message into per-deal updates and create/update Deal objects."""
    logger.debug(
        f"parse_message_enter. is_new_thread={state.is_new_thread} "
        f"existing_deals={len(state.deals)}"
    )

    text = state.incoming_message or ""
    if not text.strip():
        logger.warning("parse_message_empty_text")
        return {
            "phase": "error",
            "last_error": "No message to parse",
        }

    # Phase A: Split
    try:
        fragments = split_into_fragments(text)
    except Exception as exc:
        logger.warning(
            f"split_into_fragments_failed. exc_type={type(exc).__name__} exc={exc}"
        )
        return {
            "phase": "error",
            "last_error": "Failed to split incoming message",
            "error_diagnostics": f"{type(exc).__name__}: {exc}",
        }

    logger.debug(f"parse_message_phase_a_done. fragments_count={len(fragments)}")

    # --- Level 1: Heuristic fresh start detection (before LLM) ---
    is_new_thread = state.is_new_thread
    fresh_start = False

    if not is_new_thread:
        active_deals = [d for d in state.deals if d.status == DealStatus.NEGOTIATING]
        if active_deals and _detect_fresh_start(text, active_deals):
            logger.info(
                f"fresh_start_detected_heuristic. active_deals_count={len(active_deals)}"
            )
            fresh_start = True
            is_new_thread = True

    # Phase B: Extract per fragment
    deal_updates: list[DealUpdate] = []
    warnings: list[str] = []
    successful_extractions = 0
    if is_new_thread:
        for i, fragment in enumerate(fragments):
            logger.debug(f"extract_fragment_new. fragment_index={i + 1} text={fragment.text!r}")
            try:
                conditions = await _extract_deal_conditions(fragment.text)
                successful_extractions += 1
            except Exception as exc:
                if is_gigaplatform_stop_event(exc):
                    logger.error(
                        f"gigaplatform_stop_event. fragment_index={i + 1} "
                        f"exc_type={type(exc).__name__} exc={exc}"
                    )
                    return _gigaplatform_stop_result(exc)
                logger.error(
                    f"extract_fragment_failed. fragment_index={i + 1} "
                    f"exc_type={type(exc).__name__} exc={exc}"
                )
                conditions = DealConditions.model_validate(
                    {}, context={"source_text": fragment.text}
                )
                warnings.append(
                    f"Fragment {i + 1}: extraction failed: {type(exc).__name__}: {exc}"
                )
            deal_updates.append(DealUpdate(
                deal_number=i + 1,
                intent="new",
                conditions=conditions,
            ))
    else:
        for i, fragment in enumerate(fragments):
            logger.debug(
                f"extract_fragment_reply. fragment_index={i + 1} "
                f"deal_number_hint={fragment.deal_number_hint} text={fragment.text!r}"
            )
            try:
                update = await _extract_reply_update(fragment, state.deals)
                successful_extractions += 1
            except Exception as exc:
                if is_gigaplatform_stop_event(exc):
                    logger.error(
                        f"gigaplatform_stop_event. fragment_index={i + 1} "
                        f"deal_number_hint={fragment.deal_number_hint} "
                        f"exc_type={type(exc).__name__} exc={exc}"
                    )
                    return _gigaplatform_stop_result(exc)
                logger.error(
                    f"extract_reply_fragment_failed. fragment_index={i + 1} "
                    f"deal_number_hint={fragment.deal_number_hint} "
                    f"exc_type={type(exc).__name__} exc={exc}"
                )
                update = _fallback_reply_update(fragment)
                hint = f"deal hint {fragment.deal_number_hint}" if fragment.deal_number_hint is not None else "no deal hint"
                warnings.append(
                    f"Fragment {i + 1} ({hint}): extraction failed: {type(exc).__name__}: {exc}"
                )
            deal_updates.append(update)

    # --- Level 2: LLM fallback fresh start detection ---
    if not is_new_thread and deal_updates and all(u.intent == "new" for u in deal_updates):
        logger.info("fresh_start_detected_llm_fallback")
        fresh_start = True
        is_new_thread = True
        # Re-number deal updates starting from 1
        for i, update in enumerate(deal_updates):
            deal_updates[i] = update.model_copy(update={"deal_number": i + 1})

    if fragments and successful_extractions == 0:
        logger.error(
            f"parse_message_all_extractions_failed. fragments_count={len(fragments)} "
            f"warnings={warnings}"
        )
        return {
            "phase": "error",
            "last_error": "Failed to extract deal data from message",
            "error_diagnostics": "; ".join(warnings) if warnings else None,
            "warnings": warnings,
        }

    # On fresh start: drop active deals, keep only terminal ones
    base_deals = _drop_active_deals(state.deals) if fresh_start else state.deals
    updated_deals = _apply_updates(
        base_deals,
        deal_updates,
        config,
        is_new_thread=is_new_thread,
    )

    for deal in updated_deals:
        c = deal.conditions
        update = next((u for u in deal_updates if u.deal_number == deal.deal_number), None)
        intent_value = update.intent if update else None
        logger.info(
            f"parsed_deal. deal_number={deal.deal_number} intent={intent_value} "
            f"product={c.product} volume={c.volume} currency={c.currency} "
            f"term_days={c.term_days} rate={c.rate} inn={c.inn} "
            f"optionality={c.optionality} status={deal.status.value}"
        )

    logger.info(
        f"parse_message_done. fragments_count={len(fragments)} "
        f"successful_extractions={successful_extractions} "
        f"total_deals={len(updated_deals)} fresh_start={fresh_start} "
        f"warnings_count={len(warnings)}"
    )

    return {
        "deals": updated_deals,
        "deal_updates": deal_updates,
        "warnings": warnings,
        "phase": "parsing",
    }
