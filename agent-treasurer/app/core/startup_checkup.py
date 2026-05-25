"""SECURITY §19 — readiness probes for the treasurer agent.

Defines three startup checks (GigaChat / mail_app / agent_tools_app),
runs them sequentially with fail-fast semantics, and exposes a background
re-check loop that refreshes the ready flag every N seconds so the service
recovers from transient dependency outages without a pod restart.

The lifespan in `app.py` is the only caller; routes read state via the
`is_ready()` / `get_failures()` accessors.
"""

import asyncio
import logging
from typing import Awaitable, Callable

import httpx

from .config import settings
from .http_retry import request_with_retry
from .llm import get_llm
from .llm_retry import llm_ainvoke_with_retry
from .tracing import bind_x_trace_id, trace_header_dict
from ..models.schemas import DealConditions
from ..services.deal_service import DealService

logger = logging.getLogger(__name__)


# --- Module state (private; routes read via accessors below) -----------------
_is_ready: bool = False
_failures: list[dict] = []


def is_ready() -> bool:
    return _is_ready


def get_failures() -> list[dict]:
    return list(_failures)


# --- Individual probes -------------------------------------------------------
async def _check_gigachat() -> None:
    """Synthetic LLM ping. Raises on timeout or empty content."""
    llm = get_llm()
    response = await asyncio.wait_for(
        llm_ainvoke_with_retry(
            llm,
            [{"role": "user", "content": "Ответь одним словом: OK"}],
            purpose="readiness_gigachat",
        ),
        timeout=settings.readiness_gigachat_timeout_sec,
    )
    content = getattr(response, "content", None) or ""
    if not str(content).strip():
        raise RuntimeError("gigachat returned empty content")


async def _check_mail_app() -> None:
    """Probe mail_app liveness."""
    async with httpx.AsyncClient(timeout=settings.readiness_mail_timeout_sec) as client:
        resp = await request_with_retry(
            client,
            "GET",
            f"{settings.mail_server_api_url}/health/live",
            operation_name="readiness.mail_app",
            headers=trace_header_dict(),
        )
        resp.raise_for_status()


async def _check_agent_tools() -> None:
    """Sanity-check `get_rate` on three synthetic deals; every call must
    return a non-empty rate ladder."""
    inn = settings.readiness_test_inn
    test_conditions = [
        DealConditions(
            product="Depo", inn=inn, currency="RUB",
            term_days=1, volume=500_000_000.0, rate_type="FIX", basis="END",
        ),
        DealConditions(
            product="Depo", inn=inn, currency="RUB",
            term_days=90, volume=1_000_000_000.0, rate_type="FLOAT", basis="MONTH",
        ),
        DealConditions(
            product="Depo", inn=inn, currency="CNY",
            term_days=30, volume=1_000_000.0, rate_type="FIX", basis="END",
        ),
    ]
    service = DealService()
    for i, cond in enumerate(test_conditions):
        rates, err = await asyncio.wait_for(
            service.get_rate(cond),
            timeout=settings.readiness_tools_timeout_sec,
        )
        if err is not None or not rates:
            raise RuntimeError(
                f"agent_tools_app deal #{i+1} ({cond.currency}/{cond.rate_type}/"
                f"{cond.term_days}d) returned empty ladder (err={err})"
            )


CHECKS: list[tuple[str, Callable[[], Awaitable[None]]]] = [
    ("gigachat", _check_gigachat),
    ("mail_app", _check_mail_app),
    ("agent_tools_app", _check_agent_tools),
]


# --- Orchestration -----------------------------------------------------------
async def run_checks(*, source: str) -> tuple[bool, list[dict]]:
    """Run all probes sequentially with fail-fast semantics. Updates module
    state and returns (ok, failures). `source` (`startup`/`recheck`) labels
    logs so operators can tell boot probes from background refreshes."""
    global _is_ready, _failures

    bind_x_trace_id()
    failures: list[dict] = []
    for name, fn in CHECKS:
        try:
            await fn()
            logger.info(f"readiness_check_ok. check={name} source={source}")
        except Exception as e:
            failures.append({"check": name, "error": str(e), "exc_type": type(e).__name__})
            logger.error(
                f"startup_check_failed. check={name} source={source} "
                f"exc_type={type(e).__name__} error={e}"
            )
            for skipped, _ in CHECKS[len(failures):]:
                logger.info(f"readiness_check_skipped. check={skipped} source={source}")
            _is_ready = False
            _failures = failures
            return False, failures

    _is_ready = True
    _failures = []
    return True, []


async def recheck_loop() -> None:
    """Background loop: re-runs the checks every
    `settings.readiness_recheck_interval_sec` and flips ready state on
    transition. Cancellation-safe."""
    while True:
        try:
            await asyncio.sleep(settings.readiness_recheck_interval_sec)
            prev = _is_ready
            ok, failures = await run_checks(source="recheck")
            if ok and not prev:
                logger.info("readiness_recheck_recovered")
            elif not ok and prev:
                logger.warning(f"readiness_recheck_degraded. failures={failures}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"readiness_recheck_loop_error. error={e}")
