"""Pydantic models for agent data structures."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Literal, Optional, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

# Russian number words for parsing compound numerals
NUMBERS_WORDS: dict[str, int] = {
    # Units
    "один": 1, "два": 2, "три": 3, "четыре": 4, "пять": 5,
    "шесть": 6, "семь": 7, "восемь": 8, "девять": 9,
    # 10-19
    "десять": 10, "одиннадцать": 11, "двенадцать": 12, "тринадцать": 13,
    "четырнадцать": 14, "пятнадцать": 15, "шестнадцать": 16,
    "семнадцать": 17, "восемнадцать": 18, "девятнадцать": 19,
    # Tens
    "двадцать": 20, "тридцать": 30, "сорок": 40, "пятьдесят": 50,
    "шестьдесят": 60, "семьдесят": 70, "восемьдесят": 80, "девяносто": 90,
    # Hundreds
    "сто": 100, "двести": 200, "триста": 300, "четыреста": 400,
    "пятьсот": 500, "шестьсот": 600, "семьсот": 700,
    "восемьсот": 800, "девятьсот": 900,
}

# Volume multipliers (billions, millions, thousands, slang)
VOLUME_MULTIPLIERS: dict[str, int] = {
    "миллиард": 1_000_000_000, "млрд": 1_000_000_000, "ярд": 1_000_000_000,
    "миллион": 1_000_000, "млн": 1_000_000, "лям": 1_000_000,
    "тысяча": 1_000, "тысяч": 1_000, "тыс": 1_000,
}


class BusinessValidationError(Exception):
    """Значения распарсились, но вне допустимых бизнес-диапазонов."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("\n".join(errors))


@dataclass
class ValidationResult:
    """Результат валидации бизнес-правил по условиям сделки."""

    escalation_reasons: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    adjusted_conditions: dict = field(default_factory=dict)


class RateError(str, Enum):
    """Причина, по которой ставка не была получена от расчётного сервиса."""

    SERVICE_UNAVAILABLE = "service_unavailable"       # Сервис недоступен (сеть/таймаут)
    RUB_CALCULATION_FAILED = "rub_calculation_failed"  # Сервис доступен, RUB, но ошибка
    CURRENCY_NOT_SUPPORTED = "currency_not_supported"  # Сервис доступен, CNY/INR — не поддерживается


class DealStatus(str, Enum):
    """Status of a deal."""

    NEGOTIATING = "negotiating"  # В процессе обсуждения
    ACCEPTED = "accepted"  # Клиент согласился, ждём оформления по ссылке
    ESCALATED = "escalated"  # Передана сотруднику


class DealConditions(BaseModel):
    """Deal conditions (structured output for extraction, temperature=0)."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "input": "Предлагаем разместить 500 млн на овернайт под 18%",
                    "output": {
                        "product": "Depo",
                        "term_days": 1,
                        "volume": 500_000_000.0,
                        "rate": 18.0,
                        "currency": "RUB",
                        "rate_type": "FIX",
                    },
                },
                {
                    "input": "ИНН 7707083893, депозит 1 млрд RUB на 90 дней, хотим ставку 16.5%, ежеквартальное начисление",
                    "output": {
                        "product": "Depo",
                        "inn": "7707083893",
                        "term_days": 90,
                        "volume": 1_000_000_000.0,
                        "rate": 16.5,
                        "currency": "RUB",
                        "rate_type": "FIX",
                        "basis": "QUARTAL",
                    },
                },
                {
                    "input": "Запрашиваем плавающую ставку по депозиту на 30 дней, сумма 2 миллиарда, ежемесячные выплаты",
                    "output": {
                        "product": "Depo",
                        "term_days": 30,
                        "volume": 2_000_000_000.0,
                        "rate": None,
                        "currency": "RUB",
                        "rate_type": "FLOAT",
                        "basis": "MONTH",
                    },
                },
                {
                    "input": "Готовы разместить overnight 800М под 17.25%, выплата в конце срока",
                    "output": {
                        "product": "Depo",
                        "term_days": 1,
                        "volume": 800_000_000.0,
                        "rate": 17.25,
                        "currency": "RUB",
                        "rate_type": "FIX",
                        "basis": "END",
                    },
                },
            ]
        }
    )

    product: Literal["Depo", "NSO"] = Field(
        "Depo", description="Тип продукта сделки, депозит по умолчанию 'Depo'"
    )
    inn: Optional[str] = Field(None, description="ИНН контрагента (10 или 12 цифр)")
    term_days: Optional[int] = Field(None, description="Срок сделки в днях (овернайт = 1)")
    volume: Optional[float] = Field(None, description="Объем/сумма сделки")
    currency: Literal["RUB", "CNY", "INR", "OTHER"] = Field(
        "RUB", description="Валюта (RUB, CNY, INR), OTHER если указана неподдерживаемая валюта, по умолчанию RUB"
    )
    rate: Optional[float] = Field(
        None, description="Процентная ставка в процентах (None если запрашивают, а не предлагают)"
    )
    rate_type: Literal["FIX", "FLOAT"] = Field(
        "FIX",
        description="Тип ставки: фиксированная (FIX) или плавающая (FLOAT). По умолчанию FIX",
    )
    basis: Optional[Literal["MONTH", "QUARTAL", "SEMIANNUAL", "ANNUAL", "END"]] = Field(
        None,
        description="Базис начисления процентов: MONTH (ежемесячно), QUARTAL (ежеквартально), "
        "SEMIANNUAL (раз в полгода), ANNUAL (ежегодно), END (в конце срока). None если не указан",
    )
    optionality: Optional[Literal["POP", "OTZ", "POP_OTZ"]] = Field(
        None,
        description="Опциональность сделки: возможность пополнения (POP), отзывности (OTZ), обе (POP_OTZ) или нет (None)",
    )

    @field_validator("inn")
    @classmethod
    def normalize_inn(cls, v: str | None) -> str | None:
        """Normalize INN format (pad with leading zeros if needed)."""
        if v is None:
            return None
        digits = "".join(c for c in v if c.isdigit())
        if len(digits) == 9:
            digits = "0" + digits
        elif len(digits) == 11:
            digits = "0" + digits
        if len(digits) not in (10, 12):
            return None  # Invalid format, will be handled in model_validator
        return digits

    @model_validator(mode="after")
    def validate_and_fallback(self, info: ValidationInfo) -> Self:
        """Validate LLM values against source text, fallback to regex if invalid."""
        source_text = info.context.get("source_text", "") if info.context else ""
        if not source_text:
            return self

        text_lower = source_text.lower()
        errors: list[str] = []

        # --- INN validation ---
        if self.inn is not None:
            if self.inn not in source_text and self.inn.lstrip("0") not in source_text:
                # LLM hallucinated, try regex fallback
                fallback_inn = self._extract_inn(source_text)
                if fallback_inn is not None:
                    self.inn = fallback_inn
                else:
                    # Keep original for retry context, just report error
                    errors.append(
                        f"Галлюцинация ИНН: '{self.inn}' отсутствует в тексте. "
                        f"Укажи только ИНН который явно присутствует, иначе None."
                    )
        else:
            self.inn = self._extract_inn(source_text)

        # --- Rate validation ---
        if self.rate is not None:
            if not self._is_valid_rate(self.rate, text_lower):
                # LLM hallucinated, try regex fallback
                fallback_rate = self._extract_rate(source_text)
                if fallback_rate is not None:
                    self.rate = fallback_rate
                else:
                    # Keep original for retry context, just report error
                    errors.append(
                        f"Галлюцинация ставки: {self.rate}% не найдено с маркером процента (%, проц, под, ставка). "
                        f"Укажи rate только если явно указана процентная ставка, иначе None."
                    )
        else:
            self.rate = self._extract_rate(source_text)

        # --- Term days validation ---
        if self.term_days is not None:
            term_markers = r"дн|день|дня|дней|days?|сут|суток|сутки|месяц|мес|month|овернайт|overnight|o/n|недел|квартал|год|лет|года|полгода|р\.?\s*д\.?"
            if not re.search(term_markers, text_lower):
                # LLM hallucinated, try regex fallback
                fallback_term = self._extract_term_days(source_text)
                if fallback_term is not None:
                    self.term_days = fallback_term
                else:
                    # Keep original for retry context, just report error
                    errors.append(
                        f"Галлюцинация срока: {self.term_days} дней указано, но нет маркеров срока в тексте. "
                        f"Укажи term_days только если срок явно указан, иначе None."
                    )
        else:
            self.term_days = self._extract_term_days(source_text)

        # --- Volume validation ---
        if self.volume is not None:
            if not self._is_valid_volume(self.volume, source_text):
                # LLM hallucinated, try regex fallback
                fallback_volume = self._extract_volume(source_text)
                if fallback_volume is not None:
                    self.volume = fallback_volume
                else:
                    # Keep original for retry context, just report error
                    errors.append(
                        f"Галлюцинация объёма: {self.volume} не найдено в тексте. "
                        f"Укажи volume только если сумма явно указана, иначе None."
                    )
        else:
            self.volume = self._extract_volume(source_text)

        # --- Product validation (Depo is default) ---
        has_nso = self._has_nso_markers(text_lower)
        if self.product == "NSO" and not has_nso:
            # LLM hallucinated NSO → return to default Depo
            self.product = "Depo"
        elif self.product == "Depo" and has_nso:
            # LLM missed NSO → fix it
            self.product = "NSO"

        # --- Currency validation ---
        detected_currency = self._detect_currency(text_lower)
        if detected_currency and detected_currency != self.currency:
            # Auto-correct currency silently (not a critical error)
            self.currency = detected_currency

        # --- Rate type validation ---
        detected_rate_type = self._extract_rate_type(source_text)
        if detected_rate_type is not None and detected_rate_type != self.rate_type:
            self.rate_type = detected_rate_type

        # --- Basis validation ---
        detected_basis = self._extract_basis(source_text)
        if detected_basis is not None:
            if self.basis is None or self.basis != detected_basis:
                self.basis = detected_basis

        # --- Optionality validation ---
        detected_optionality = self._extract_optionality(source_text)
        if detected_optionality is not None:
            if self.optionality is None or self.optionality != detected_optionality:
                self.optionality = detected_optionality

        # Raise combined error only if regex fallback also failed
        if errors:
            raise ValueError(
                f"Ошибки валидации (текст: '{source_text[:150]}...'):\n" + "\n".join(f"- {e}" for e in errors)
            )

        return self

    @staticmethod
    def _is_valid_rate(rate: float, text_lower: str) -> bool:
        """Check if rate value has percent marker in text."""
        rate_int = str(int(rate))
        rate_patterns = [
            rf"{rate_int}\s*%",
            rf"{rate_int}\s*проц",
            rf"(?:под|ставк\w*)\s*{rate_int}",
            rf"{rate_int}\s*годовых",
            rf"годовых\s*{rate_int}",
            rf"{rate_int}\s*п\.?\s*п\.?",
        ]
        if rate != int(rate):
            rate_str = str(rate).replace(".", "[.,]")
            rate_patterns.extend([
                rf"{rate_str}\s*%",
                rf"{rate_str}\s*проц",
                rf"(?:под|ставк\w*)\s*{rate_str}",
                rf"{rate_str}\s*годовых",
                rf"годовых\s*{rate_str}",
                rf"{rate_str}\s*п\.?\s*п\.?",
            ])
        return any(re.search(p, text_lower) for p in rate_patterns)

    @staticmethod
    def _detect_currency(text_lower: str) -> str | None:
        """Detect currency from text markers. Returns 'OTHER' for unsupported currencies."""
        has_rub = bool(re.search(r"р\b|руб|rub|рубл", text_lower))
        has_cny = bool(re.search(r"¥|юан|元|cny", text_lower))
        has_inr = bool(re.search(r"₹|рупи|inr", text_lower))
        has_other = bool(re.search(r"\$|usd|долл|евро|eur|€|фунт|gbp|£|йен|jpy|¥", text_lower))

        supported = [has_rub, has_cny, has_inr]
        if sum(supported) == 1:
            if has_cny:
                return "CNY"
            if has_inr:
                return "INR"
            if has_rub:
                return "RUB"

        # Unsupported currency detected and no supported one
        if has_other and not any(supported):
            return "OTHER"

        return None

    @staticmethod
    def _extract_term_days(text: str) -> int | None:
        text_lower = text.lower()
        # Overnight variants
        if re.search(r"овернайт|overnight|o/n", text_lower):
            return 1

        # Half-year ("полгода")
        if re.search(r"полгода", text_lower):
            return 182

        # N days / N суток / N working days
        if m := re.search(r"(\d+)\s*(?:рабочих\s*)?(?:дн|день|дня|days?|сут|суток|сутки)", text_lower):
            return int(m.group(1))
        # N р.д. (рабочих дней abbreviated)
        if m := re.search(r"(\d+)\s*р\.?\s*д\.?", text_lower):
            return int(m.group(1))

        # Single week ("на неделю")
        if re.search(r"на\s+недел[юь]|одн[уа]\s+недел", text_lower):
            return 7
        # N weeks
        if m := re.search(r"(\d+)\s*недел", text_lower):
            return int(m.group(1)) * 7
        # "через N недель" pattern
        if m := re.search(r"через\s+(\d+)\s*недел", text_lower):
            return int(m.group(1)) * 7

        # Single quarter ("на квартал")
        if re.search(r"на\s+квартал\b|один\s+квартал", text_lower):
            return 90
        # N quarters
        if m := re.search(r"(\d+)\s*квартал", text_lower):
            return int(m.group(1)) * 90

        # Single year ("на год")
        if re.search(r"на\s+год\b|один\s+год", text_lower):
            return 365
        # N years
        if m := re.search(r"(\d+)\s*(?:год|лет|года)", text_lower):
            return int(m.group(1)) * 365

        # N months
        if m := re.search(r"(\d+)\s*(?:месяц|мес|month)", text_lower):
            return int(m.group(1)) * 30

        return None

    @staticmethod
    def _extract_volume(text: str) -> float | None:
        text_lower = text.lower()

        # Priority 0: Fractional words ("полтора миллиона", "полмиллиарда", "полмиллиона")
        if re.search(r"полтора\s*(?:миллиард|млрд)", text_lower):
            return 1_500_000_000
        if re.search(r"полтора\s*(?:миллион|млн)", text_lower):
            return 1_500_000
        if re.search(r"полмиллиарда", text_lower):
            return 500_000_000
        if re.search(r"полмиллиона", text_lower):
            return 500_000

        # Priority 1: Words + multiplier ("сто двадцать три миллиона")
        number_words_pattern = "|".join(NUMBERS_WORDS.keys())
        words_multiplier_pattern = rf"((?:(?:{number_words_pattern})\s*)+)\s*(миллиард\w*|млрд|ярд\w*|миллион\w*|млн|лям\w*|тысяч\w*|тыс\.?)"
        if m := re.search(words_multiplier_pattern, text_lower):
            words = m.group(1).split()
            multiplier_text = m.group(2)
            number = DealConditions._parse_number_words(words)
            if number:
                if multiplier_text.startswith(("миллиард", "млрд", "ярд")):
                    return float(number) * 1_000_000_000
                elif multiplier_text.startswith(("миллион", "млн", "лям")):
                    return float(number) * 1_000_000
                else:  # тысяч, тыс
                    return float(number) * 1_000

        # Priority 2: Digit + multiplier with slang ("50 млрд", "3 ляма", "5 ярдов")
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*(?:миллиард\w*|млрд|ярд\w*)", text_lower):
            return float(m.group(1).replace(",", ".")) * 1_000_000_000
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*(?:миллион\w*|млн|лям\w*)", text_lower):
            return float(m.group(1).replace(",", ".")) * 1_000_000
        # 50М (uppercase M only to avoid confusion with lowercase 'm')
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*М\b", text):
            return float(m.group(1).replace(",", ".")) * 1_000_000
        # Thousands: 500 тыс, 500 тысяч, 500к, 500K
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*(?:тысяч\w*|тыс\.?)", text_lower):
            return float(m.group(1).replace(",", ".")) * 1_000
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*[кkКK]\b", text):
            return float(m.group(1).replace(",", ".")) * 1_000

        # Priority 3: Numbers with spaces ("3 000 000")
        if m := re.search(r"\b(\d{1,3}(?:\s\d{3})+)\b", text):
            return float(m.group(1).replace(" ", ""))

        # Priority 4: Numbers with commas as thousands separator ("3,000,000")
        if m := re.search(r"\b(\d{1,3}(?:,\d{3})+)\b", text):
            return float(m.group(1).replace(",", ""))

        # Priority 5: Currency markers (check before raw digits to avoid grabbing INN)
        # RUB: 50р, 100 руб, 500 рублей, 50 RUB
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*(?:р\b|руб\w*|rub)", text_lower):
            return float(m.group(1).replace(",", "."))
        # CNY: 50¥, 100 юаней, 200 元, 50 CNY, 50 юань
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*(?:¥|юан\w*|元|cny)", text_lower):
            return float(m.group(1).replace(",", "."))
        # INR: 50₹, 100 рупий, 50 INR, 50 рупия
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*(?:₹|рупи\w*|inr)", text_lower):
            return float(m.group(1).replace(",", "."))

        # Priority 6: Volume markers ("сумма", "объем", "на сумму") + number
        if m := re.search(r"(?:сумм\w*|объ[её]м\w*|на\s+сумму)\s*:?\s*(\d+(?:[.,]\d+)?)", text_lower):
            return float(m.group(1).replace(",", "."))

        # Priority 7: Large number 6+ digits (fallback, EXCLUDING INN pattern)
        # Remove INN from text before searching for large numbers
        text_no_inn = re.sub(
            r"\bинн\b(?:\s*(?:контрагента|клиента))?\s*(?:(?:на|это)\s*)?(?:[:=№\-—]\s*)?\d{9,12}\b",
            "",
            text_lower,
        )
        if m := re.search(r"(\d{6,})", text_no_inn.replace(" ", "")):
            digits = m.group(1)
            # Unmarked 10/12-digit numbers are ambiguous with INN in replies.
            # Require explicit amount markers for such values.
            if len(digits) in (10, 12):
                return None
            return float(digits)

        return None

    @staticmethod
    def _extract_rate(text: str) -> float | None:
        text_lower = text.lower()
        # "под 18%" / "ставка 16.5%" / "rate 15,5%"
        if m := re.search(r"(?:под|ставк\w*|rate)\s*(\d+(?:[.,]\d+)?)\s*%?", text_lower):
            return float(m.group(1).replace(",", "."))
        # Just N% in context
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*%", text):
            return float(m.group(1).replace(",", "."))
        # N годовых (without %)
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*годовых", text_lower):
            return float(m.group(1).replace(",", "."))
        # годовых N (reversed order)
        if m := re.search(r"годовых\s*(\d+(?:[.,]\d+)?)", text_lower):
            return float(m.group(1).replace(",", "."))
        # N п.п. (процентных пунктов)
        if m := re.search(r"(\d+(?:[.,]\d+)?)\s*п\.?\s*п\.?", text_lower):
            return float(m.group(1).replace(",", "."))
        return None

    @staticmethod
    def _extract_inn(text: str) -> str | None:
        # ИНН marker is mandatory; supports forms like:
        # "ИНН 770...", "ИНН: 770...", "ИНН на 770...", "ИНН клиента на: 770..."
        if m := re.search(
            r"\bинн\b(?:\s*(?:контрагента|клиента))?\s*(?:(?:на|это)\s*)?(?:[:=№\-—]\s*)?(\d{9,12})\b",
            text.lower(),
        ):
            digits = m.group(1)
            if len(digits) == 9:
                return "0" + digits
            elif len(digits) == 11:
                return "0" + digits
            elif len(digits) in (10, 12):
                return digits
        return None

    @staticmethod
    def _parse_number_words(words: list[str]) -> int | None:
        """Parse compound Russian numeral from word list.

        Examples:
            ["сто", "двадцать", "три"] → 123
            ["шестьдесят", "шесть"] → 66
        """
        total = 0
        for word in words:
            if word in NUMBERS_WORDS:
                total += NUMBERS_WORDS[word]
            else:
                return None  # unknown word
        return total if total > 0 else None

    @staticmethod
    def _has_nso_markers(text_lower: str) -> bool:
        """Check for NSO (non-reducible balance) markers in text."""
        return bool(re.search(r"\bнсо\b|\bnso\b|неснижаем", text_lower))

    @staticmethod
    def _extract_rate_type(text: str) -> str | None:
        """Extract rate type from text.

        Returns:
            "FIX" - фиксированная ставка
            "FLOAT" - плавающая ставка
            None - не указано (по умолчанию FIX)
        """
        text_lower = text.lower()
        if re.search(r"плавающ|float|переменн", text_lower):
            return "FLOAT"
        if re.search(r"фиксирован|fix(?:ed)?(?:\b)", text_lower):
            return "FIX"
        return None

    @staticmethod
    def _extract_optionality(text: str) -> str | None:
        """Extract optionality markers from text.

        Returns:
            "POP" - с пополнением
            "OTZ" - с отзывностью
            "POP_OTZ" - оба варианта
            None - без опций или явно указано "без"
        """
        text_lower = text.lower()

        # Check for explicit negation first
        has_bez_popolneniya = bool(re.search(r"без\s+пополнен", text_lower))
        has_bezotzyvny = bool(re.search(r"безотзывн", text_lower))

        # Check for positive markers
        has_popolnenie = bool(
            re.search(r"с\s+пополнен|пополняем|возможност\w+\s+пополнен", text_lower)
        ) and not has_bez_popolneniya
        has_otzyvnost = bool(
            re.search(r"отзывн|с\s+отзывност", text_lower)
        ) and not has_bezotzyvny

        if has_popolnenie and has_otzyvnost:
            return "POP_OTZ"
        if has_popolnenie:
            return "POP"
        if has_otzyvnost:
            return "OTZ"
        return None

    @staticmethod
    def _extract_basis(text: str) -> str | None:
        """Extract interest accrual basis from text.

        Returns:
            "MONTH" - ежемесячно
            "QUARTAL" - ежеквартально
            "SEMIANNUAL" - раз в полгода
            "ANNUAL" - ежегодно
            "END" - в конце срока
            None - не указано
        """
        text_lower = text.lower()

        # End of term
        if re.search(r"в\s+конце\s+срока|на\s+конец\s+период|по\s+окончани", text_lower):
            return "END"

        # Monthly
        if re.search(r"ежемесячн|помесячн", text_lower):
            return "MONTH"

        # Quarterly
        if re.search(r"ежекварталь|поквартальн|раз\s+в\s+квартал", text_lower):
            return "QUARTAL"

        # Semiannual
        if re.search(r"полугод|раз\s+в\s+полгода|раз\s+в\s+полугод", text_lower):
            return "SEMIANNUAL"

        # Annual
        if re.search(r"ежегодн|раз\s+в\s+год", text_lower):
            return "ANNUAL"

        return None

    @staticmethod
    def _is_valid_volume(volume: float, text: str) -> bool:
        """Check that volume is not an LLM hallucination."""
        extracted = DealConditions._extract_volume(text)
        if extracted is not None:
            # Allow ±1% tolerance for rounding errors
            return abs(volume - extracted) / max(volume, extracted) < 0.01

        # _extract_volume found nothing (e.g. unknown currency marker like "баксов", "евро").
        # Fallback: check that the numeric value is literally present in the text
        # and does NOT belong to another known parameter (rate, term, INN, date).
        # Strip patterns mirror _is_valid_rate and _extract_term_days exactly.
        text_clean = text.lower()

        # INN
        text_clean = re.sub(
            r"\bинн\b(?:\s*(?:контрагента|клиента))?\s*(?:(?:на|это)\s*)?(?:[:=№\-—]\s*)?\d{9,12}\b",
            "",
            text_clean,
        )
        # Dates
        text_clean = re.sub(r"\d{1,2}[./]\d{1,2}[./]\d{2,4}", "", text_clean)

        # Rate: mirrors _is_valid_rate — "N%", "N проц", "N годовых", "N п.п.",
        #       "под N", "ставка N", "годовых N"
        text_clean = re.sub(r"\d+(?:[.,]\d+)?\s*(?:%|проц\w*|годовых|п\.?\s*п\.?)", "", text_clean)
        text_clean = re.sub(r"(?:под|ставк\w*)\s*\d+(?:[.,]\d+)?", "", text_clean)
        text_clean = re.sub(r"годовых\s*\d+(?:[.,]\d+)?", "", text_clean)

        # Term: mirrors _extract_term_days digit-based patterns
        text_clean = re.sub(r"\d+\s*(?:рабочих\s*)?(?:дн|день|дня|days?|сут|суток|сутки)", "", text_clean)
        text_clean = re.sub(r"\d+\s*р\.?\s*д\.?", "", text_clean)
        text_clean = re.sub(r"через\s+\d+\s*недел\w*", "", text_clean)
        text_clean = re.sub(r"\d+\s*недел\w*", "", text_clean)
        text_clean = re.sub(r"\d+\s*квартал\w*", "", text_clean)
        text_clean = re.sub(r"\d+\s*(?:год|лет|года)", "", text_clean)
        text_clean = re.sub(r"\d+\s*(?:месяц|мес|month)", "", text_clean)

        volume_str = str(int(volume)) if volume == int(volume) else str(volume)
        has_explicit_volume_markers = bool(re.search(
            r"сумм\w*|объ[её]м\w*|на\s+сумму|руб\w*|rub|юан\w*|cny|рупи\w*|inr|¥|₹|元|"
            r"миллиард\w*|млрд|ярд\w*|миллион\w*|млн|лям\w*|тысяч\w*|тыс\.?|[кk]\b",
            text.lower(),
        ))
        if volume == int(volume) and len(volume_str) in (10, 12) and not has_explicit_volume_markers:
            return False
        return bool(re.search(rf"\b{re.escape(volume_str)}\b", text_clean))

    def validate_business_rules(self) -> ValidationResult:
        """Validate parsed values against per-currency business constraints.

        Returns ValidationResult with escalation reasons, suggestions,
        and adjusted conditions instead of raising exceptions.
        Limits are read from settings (agent_treasurer_app/app/core/config.py).
        """
        from agent_treasurer_app.app.core.config import settings as s

        result = ValidationResult()
        term = self.term_days
        vol = self.volume

        # INN format check
        if self.inn and len(self.inn) not in (10, 12):
            result.suggestions.append("ИНН должен содержать 10 или 12 цифр")

        if self.currency == "OTHER":
            result.escalation_reasons.append(
                "Указанная валюта не поддерживается агентом"
            )
            return result

        if self.currency == "RUB":
            if self.rate_type == "FLOAT":
                if term is not None and term > s.rub_float_max_term:
                    result.escalation_reasons.append(
                        f"Срок {term} дн. превышает максимум для плавающей ставки ({s.rub_float_max_term} дней)"
                    )
                if vol is not None and vol > s.rub_float_max_volume:
                    result.escalation_reasons.append(
                        f"Объём {vol:,.0f} ₽ превышает максимум ({s.rub_float_max_volume:,.0f} ₽)"
                    )
                # Срок < min или объём < min → предложить FIX
                if not result.escalation_reasons:
                    needs_fix = False
                    if term is not None and term < s.rub_float_min_term:
                        needs_fix = True
                    if vol is not None and vol < s.rub_float_min_volume:
                        needs_fix = True
                    if needs_fix:
                        result.suggestions.append(
                            "По данным условиям плавающая ставка недоступна. "
                            "Предлагаем фиксированную."
                        )
                        result.adjusted_conditions["rate_type"] = "FIX"
            else:
                if term is not None and not (s.rub_fix_min_term <= term <= s.rub_fix_max_term):
                    result.escalation_reasons.append(
                        f"Срок {term} дн. вне допустимого диапазона ({s.rub_fix_min_term}–{s.rub_fix_max_term})"
                    )
                if vol is not None:
                    if vol < s.rub_fix_min_volume:
                        result.escalation_reasons.append(
                            f"Объём {vol:,.0f} ₽ ниже минимума ({s.rub_fix_min_volume:,.0f} ₽)"
                        )
                    elif vol > s.rub_fix_max_volume:
                        result.escalation_reasons.append(
                            f"Объём {vol:,.0f} ₽ превышает максимум ({s.rub_fix_max_volume:,.0f} ₽)"
                        )

        elif self.currency == "CNY":
            if term is not None and not (s.cny_min_term <= term <= s.cny_max_term):
                result.escalation_reasons.append(
                    f"Срок {term} дн. вне допустимого диапазона для юаней ({s.cny_min_term}–{s.cny_max_term})"
                )
            if vol is not None:
                if vol > s.cny_max_volume:
                    result.escalation_reasons.append(
                        f"Объём {vol:,.0f} ¥ превышает максимум ({s.cny_max_volume:,.0f} ¥)"
                    )
                elif vol < s.cny_min_volume:
                    result.suggestions.append(
                        f"Готовы прокотировать сумму от {s.cny_min_volume:,.0f} юаней"
                    )

        elif self.currency == "INR":
            if term is not None and not (s.inr_min_term <= term <= s.inr_max_term):
                result.escalation_reasons.append(
                    f"Срок {term} дн. вне допустимого диапазона для рупий ({s.inr_min_term}–{s.inr_max_term})"
                )
            if vol is not None:
                if vol > s.inr_max_volume:
                    result.escalation_reasons.append(
                        f"Объём {vol:,.0f} ₹ превышает максимум ({s.inr_max_volume:,.0f} ₹)"
                    )
                elif vol < s.inr_min_volume:
                    result.suggestions.append(
                        f"Готовы прокотировать сумму от {s.inr_min_volume:,.0f} рупий"
                    )

        return result


class IncomingEmail(BaseModel):
    """Incoming email structure."""

    message_id: str = Field(..., description="Unique message ID")
    from_address: str = Field(..., description="Sender email")
    to_address: str = Field(..., description="Recipient email")
    subject: str = Field(..., description="Email subject")
    body: str = Field(..., description="Email body text")
    html_body: Optional[str] = Field(None, description="HTML body if present")
    received_at: datetime = Field(default_factory=datetime.utcnow)
    thread_id: Optional[str] = Field(None, description="Thread/conversation ID")
    in_reply_to: Optional[str] = Field(None, description="ID of message being replied to")


class OutgoingEmail(BaseModel):
    """Outgoing email to be sent."""

    to_address: str = Field(..., description="Recipient email")
    subject: str = Field(..., description="Email subject")
    body: str = Field(..., description="Email body text")
    html_body: Optional[str] = Field(None, description="HTML body")
    in_reply_to: Optional[str] = Field(None, description="Message ID being replied to")
    cc: Optional[list[str]] = Field(None, description="CC recipients")


class OfferRecord(BaseModel):
    """Record of an offer exchange in a negotiation iteration."""

    incoming: Optional[DealConditions] = Field(None, description="Условия, предложенные контрагентом")
    proposed: Optional[DealConditions] = Field(None, description="Условия, предложенные нами в ответ")
    iteration: int = Field(0, description="Номер итерации переговоров")


class Deal(BaseModel):
    """Deal entity."""

    id: str = Field(..., description="Unique deal ID")
    thread_id: str = Field(..., description="Email thread ID")
    counterparty_email: str = Field(..., description="Counterparty email address")
    conditions: DealConditions = Field(..., description="Deal conditions")
    status: DealStatus = Field(default=DealStatus.NEGOTIATING)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    assigned_employee: Optional[str] = Field(None, description="Employee email if escalated")
    negotiation_history: list[str] = Field(default_factory=list, description="History of negotiation messages")
    deal_number: int = Field(1, description="Fixed number in thread (1-indexed)")
    iteration_count: int = Field(0, description="Negotiation iterations for this deal")
    max_iterations: int = Field(10, description="Max iterations before auto-escalation")
    offers_history: list[OfferRecord] = Field(default_factory=list, description="Per-deal offer history")
    rate_ladder: list[tuple[str, float]] = Field(
        default_factory=list,
        description="Sorted list of (source, rate) from pricing service; reset on condition change",
    )
    rate_ladder_position: int = Field(0, description="Current position in rate_ladder (0 = lowest/starting rate)")


class DealUpdate(BaseModel):
    """Result of parsing one deal fragment from incoming message."""

    deal_number: int = Field(..., description="Fixed deal number in thread")
    intent: Literal[
        "new",
        "negotiate",
        "approve",
        "provide_data",
        "escalate",
    ] = Field(..., description="Per-deal intent")
    conditions: Optional[DealConditions] = Field(None, description="Updated/new conditions")


class DealResult(BaseModel):
    """Result of processing one deal in process_deals node."""

    deal: Deal = Field(..., description="Updated deal object")
    response_section: str = Field(..., description="Text for this deal's section in email")
