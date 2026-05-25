"""Модель расчёта и контракт CalcFundCost API — спека §8.1."""

from decimal import Decimal
from typing import Any

from pydantic import BaseModel


class CalcSearchRequest(BaseModel):
    """Тело запроса POST /api/v1/calculations/search."""
    id: str
    status: str = "signOffRequiresFromTreasurer"


class CalcSearchResponse(BaseModel):
    """Ответ CalcFundCost API."""
    id: str
    status: str
    parameters: dict[str, Any]


# Финансовые показатели из CalcFundCost (Decimal-поля)
FINANCIAL_SNAPSHOT_FIELDS: tuple[str, ...] = (
    "policy_rate",
    "ets",
    "for_rate",
    "eva_target",
    "eva_indicative",
    "asv",
    "option_price",
    "crl",
    "cfc_so",
    "cfc_ets",
    "cfc_for_rate",
    "cfc_eva_target",
    "cfc_eva_indicative",
    "cfc_asv",
    "cfc_option_price",
    "cfc_k_liq_fund",
)

# Параметры сделки из входящего сообщения внешней системы (не-Decimal)
DEAL_CONDITION_FIELDS: tuple[str, ...] = (
    "inn",
    "term_days",
    "volume",
    "currency",
    "rate",
    "rate_type",
    "product",
    "basis",
    "optionality",
)

# Все поля snapshot'а (финансовые + сделочные)
SNAPSHOT_FIELDS: tuple[str, ...] = FINANCIAL_SNAPSHOT_FIELDS + DEAL_CONDITION_FIELDS

# Типы для не-Decimal полей (остальные считаются Decimal)
SNAPSHOT_FIELD_TYPES: dict[str, type] = {
    "inn": str,
    "term_days": int,
    "volume": float,
    "currency": str,
    "rate": float,
    "rate_type": str,
    "product": str,
    "basis": str,
    "optionality": str,
}


class SnapshotCreate(BaseModel):
    """Тело POST /approve/snapshots — создание snapshot'а с привязкой к task_id."""
    calc_id: str
    task_id: str
    run_id: str | None = None
    # Финансовые показатели (Decimal)
    policy_rate: Decimal | None = None
    ets: Decimal | None = None
    for_rate: Decimal | None = None
    eva_target: Decimal | None = None
    eva_indicative: Decimal | None = None
    asv: Decimal | None = None
    option_price: Decimal | None = None
    crl: Decimal | None = None
    cfc_so: Decimal | None = None
    cfc_ets: Decimal | None = None
    cfc_for_rate: Decimal | None = None
    cfc_eva_target: Decimal | None = None
    cfc_eva_indicative: Decimal | None = None
    cfc_asv: Decimal | None = None
    cfc_option_price: Decimal | None = None
    cfc_k_liq_fund: Decimal | None = None
    # Параметры сделки
    inn: str | None = None
    term_days: int | None = None
    volume: float | None = None
    currency: str | None = None
    rate: float | None = None
    rate_type: str | None = None
    product: str | None = None
    basis: str | None = None
    optionality: str | None = None
