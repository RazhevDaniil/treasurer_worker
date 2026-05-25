"""Retry-логика с нарастающим backoff — спека §6.1, §6.2.

CalcFundCost / db-service: 3 попытки, backoff 60 → 120 → 180 минут.
Агент (TTL): 3 попытки, TTL 90 → 180 → 270 секунд.
"""

import time

from ..config import settings
from ..utils.logger import get_logger

log = get_logger(__name__)


class RetryExhausted(Exception):
    """Все попытки retry исчерпаны."""
    pass


def retry_with_backoff(
    func,
    *,
    max_attempts: int = settings.enrichment_retry_count,
    backoff_intervals_minutes: list[int] | None = None,
    calculation_id: str = "",
):
    """Выполняет func() с retry и нарастающим backoff.

    При исчерпании попыток выбрасывает RetryExhausted.
    backoff_intervals_minutes по умолчанию берётся из конфига (60, 120, 180 мин).
    """
    if backoff_intervals_minutes is None:
        backoff_intervals_minutes = settings.backoff_intervals_minutes

    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as exc:
            last_error = exc
            log.warning(
                "retry_attempt_failed",
                calculation_id=calculation_id,
                attempt=attempt,
                max_attempts=max_attempts,
                error=str(exc),
            )
            if attempt < max_attempts:
                wait_minutes = backoff_intervals_minutes[attempt - 1]
                log.info(
                    "retry_backoff",
                    calculation_id=calculation_id,
                    wait_minutes=wait_minutes,
                )
                time.sleep(wait_minutes * 60)

    raise RetryExhausted(
        f"All {max_attempts} attempts exhausted for calculation_id={calculation_id}: {last_error}"
    )


def get_agent_ttl(attempt: int) -> int:
    """TTL для n-й попытки отправки задания агенту (спека §6.2).

    attempt=1 → 90s, attempt=2 → 180s, attempt=3 → 270s.
    """
    return settings.agent_ttl_seconds * attempt
