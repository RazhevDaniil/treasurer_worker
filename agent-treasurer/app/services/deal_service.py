"""Service layer for deal-related business logic."""

import logging
from typing import Optional

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ..core.config import settings
from ..core.tracing import trace_header_dict
from ..models.schemas import Deal, DealConditions, RateError

logger = logging.getLogger(__name__)


def _log_http_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    next_wait_sec = round(retry_state.next_action.sleep, 3) if retry_state.next_action else None
    exc_type = type(exc).__name__ if exc else None
    exc_str = str(exc) if exc else None
    logger.info(
        f"http_retry. attempt={retry_state.attempt_number} "
        f"next_wait_sec={next_wait_sec} exc_type={exc_type} exc={exc_str}"
    )


class DealService:
    """
    Service for deal-related business logic.

    Methods:
        get_rate — get interest rate from external pricing system
        get_available_employee — get employee for escalation
        get_deal_by_thread — load deal by thread ID (stub)
    """

    async def get_rate(
        self, conditions: DealConditions,
    ) -> tuple[list[tuple[str, float]] | None, RateError | None]:
        """
        Get rate ladder from external pricing system (tool API).

        Returns:
            Tuple of (rates, error):
            - (list[(source, rate)], None) on success — sorted ascending by rate
            - (None, RateError) on failure with specific reason
        """
        try:
            # Map agent_app fields to agent_tools_app GetRateRequest format
            product_map = {"Depo": "DEPO", "NSO": "NSO"}
            payload = {
                "inn": conditions.inn,
                "ccy": conditions.currency if conditions.currency != "OTHER" else "RUB",
                "product": product_map.get(conditions.product, "DEPO"),
                "term": conditions.term_days,
                "vol": conditions.volume,
                "rate_type": conditions.rate_type,
                "basis": conditions.basis if conditions.basis is not None else "END",
                "optionality": conditions.optionality or "",
            }

            async with httpx.AsyncClient(timeout=10.0) as client:
                async for attempt in AsyncRetrying(
                    retry=retry_if_exception_type((
                        httpx.TimeoutException,
                        httpx.ConnectError,
                        httpx.RemoteProtocolError,
                        httpx.HTTPStatusError,
                    )),
                    stop=stop_after_attempt(settings.http_max_retries),
                    wait=wait_exponential_jitter(
                        initial=settings.http_retry_base,
                        max=settings.http_retry_max,
                    ),
                    before_sleep=_log_http_retry,
                    reraise=True,
                ):
                    with attempt:
                        response = await client.post(
                            f"{settings.tool_api_url}/api/get_rate",
                            json=payload,
                            headers=trace_header_dict(),
                        )
                        # Retry only on transport-level recoverable statuses;
                        # 4xx (except 429) flows to the business-error branch below.
                        if response.status_code >= 500 or response.status_code == 429:
                            response.raise_for_status()
                if response.is_error:
                    if conditions.currency in ("CNY", "INR"):
                        logger.warning(
                            f"get_rate_currency_not_supported. "
                            f"currency={conditions.currency} status_code={response.status_code}"
                        )
                        return None, RateError.CURRENCY_NOT_SUPPORTED
                    logger.warning(
                        f"get_rate_rub_calculation_failed. status_code={response.status_code}"
                    )
                    return None, RateError.RUB_CALCULATION_FAILED
                data = response.json()
                rates = data.get("rates")
                if not rates:
                    logger.warning(
                        "get_rate_data_unavailable. detail=сервис вернул пустой список ставок"
                    )
                    return None, RateError.SERVICE_UNAVAILABLE
                # Ensure list[tuple[str, float]] format
                rate_ladder = [(str(k), float(v)) for k, v in rates]
                logger.debug(f"get_rate_ok. rate_ladder={rate_ladder}")
                return rate_ladder, None
        except (httpx.ConnectError, httpx.TimeoutException, httpx.ConnectTimeout) as e:
            logger.error(f"get_rate_connection_error. exc_type={type(e).__name__} exc={e}")
            return None, RateError.SERVICE_UNAVAILABLE
        except Exception as e:
            logger.error(f"get_rate_unexpected_error. exc_type={type(e).__name__} exc={e}")
            return None, RateError.SERVICE_UNAVAILABLE

    async def get_deal_by_thread(self, thread_id: str) -> Optional[Deal]:
        """
        Load deal by thread ID.

        STUB: Always returns None.
        """
        return None

    async def get_available_employee(self) -> str:
        """
        Get an available employee for deal escalation.

        STUB: Returns the default employee email from settings.
        """
        return settings.default_employee_email
