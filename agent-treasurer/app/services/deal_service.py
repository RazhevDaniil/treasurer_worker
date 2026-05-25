"""Service layer for deal-related business logic."""

import logging
from typing import Optional

import httpx

from ..core.config import settings
from ..core.http_retry import request_with_retry
from ..core.tracing import safe_trace_json, trace_action_span, trace_header_dict
from ..models.schemas import Deal, DealConditions, RateError

logger = logging.getLogger(__name__)


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

        with trace_action_span(
            "agent_tools_app.get_rate",
            call_type="service_call",
            target_name="agent_tools_app.POST /api/get_rate",
            request_payload=payload,
            is_mutation=False,
            rollback_possible=None,
        ) as span:
            try:
                response: httpx.Response | None = None

                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await request_with_retry(
                        client,
                        "POST",
                        f"{settings.tool_api_url}/api/get_rate",
                        operation_name="agent_tools_app.get_rate",
                        trace_span=span,
                        json=payload,
                        headers=trace_header_dict(),
                    )
                if response is None:
                    span.add_span_attributes(**{
                        "aef.result_payload": safe_trace_json({"error": "empty_response"}),
                    })
                    return None, RateError.SERVICE_UNAVAILABLE
                if response.is_error:
                    if response.status_code in (408, 429):
                        logger.warning(
                            f"get_rate_temporary_http_error_no_retry. "
                            f"status_code={response.status_code}"
                        )
                        span.add_output_result({
                            "rates": None,
                            "error": RateError.SERVICE_UNAVAILABLE.value,
                        })
                        return None, RateError.SERVICE_UNAVAILABLE
                    if conditions.currency in ("CNY", "INR"):
                        logger.warning(
                            f"get_rate_currency_not_supported. "
                            f"currency={conditions.currency} status_code={response.status_code}"
                        )
                        span.add_output_result({
                            "rates": None,
                            "error": RateError.CURRENCY_NOT_SUPPORTED.value,
                        })
                        return None, RateError.CURRENCY_NOT_SUPPORTED
                    logger.warning(
                        f"get_rate_rub_calculation_failed. status_code={response.status_code}"
                    )
                    span.add_output_result({
                        "rates": None,
                        "error": RateError.RUB_CALCULATION_FAILED.value,
                    })
                    return None, RateError.RUB_CALCULATION_FAILED
                data = response.json()
                rates = data.get("rates")
                if not rates:
                    logger.warning(
                        "get_rate_data_unavailable. detail=сервис вернул пустой список ставок"
                    )
                    span.add_output_result({
                        "rates": None,
                        "error": RateError.SERVICE_UNAVAILABLE.value,
                    })
                    return None, RateError.SERVICE_UNAVAILABLE
                # Ensure list[tuple[str, float]] format
                rate_ladder = [(str(k), float(v)) for k, v in rates]
                logger.debug(f"get_rate_ok. rate_ladder={rate_ladder}")
                span.add_span_attributes(**{
                    "aef.result_payload": safe_trace_json({"rates": rate_ladder, "error": None}),
                })
                span.add_output_result({"rates": rate_ladder, "error": None})
                return rate_ladder, None
            except (httpx.ConnectError, httpx.TimeoutException, httpx.ConnectTimeout) as e:
                span.record_error(e)
                logger.error(f"get_rate_connection_error. exc_type={type(e).__name__} exc={e}")
                return None, RateError.SERVICE_UNAVAILABLE
            except Exception as e:
                span.record_error(e)
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
