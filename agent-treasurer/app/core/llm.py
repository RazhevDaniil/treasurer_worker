"""LLM configuration and initialization.

SECURITY §26: `get_llm_with_config` picks Main vs PreView installation
per call via `random.random() < settings.preview_ratio`. The installation
choice is:
  1. Logged at info level for an out-of-band audit trail.
  2. Attached to the langchain `AEFHandler` callbacks list — the SDK
     surfaces the resulting model id on the `llm` span automatically
     (visible in AEF Manager Traces).
"""

import logging
import random
from functools import lru_cache

from langchain_gigachat.chat_models import GigaChat

from .config import settings
from .tracing import get_aef_handler

logger = logging.getLogger(__name__)


def _pick_model() -> tuple[str, str]:
    """Return (model_name, installation). `installation` ∈ {"main", "preview"}."""
    if (
        settings.preview_ratio > 0.0
        and settings.preview_model
        and random.random() < settings.preview_ratio
    ):
        return settings.preview_model, "preview"
    return settings.main_model, "main"


@lru_cache
def get_llm() -> GigaChat:
    """Main-installation client for startup probes. PreView routing is
    per-invocation, so the readiness check stays on Main deliberately.

    No AEF handler attached, by design:
      1. `@lru_cache` resolves on first call. The first caller may be
         Streamlit UI / unit tests, where `init_tracing()` has never run —
         `get_aef_handler()` would raise. Capturing the handler later via
         `__call__`-time lookup wouldn't help because GigaChat keeps the
         callback list it was built with.
      2. Synthetic readiness probes intentionally stay out of the AEF trace
         stream — otherwise every restart sprays the AEF Manager with
         probe-shaped spans and skews per-agent latency dashboards."""
    return GigaChat(
        model=settings.main_model,
        temperature=settings.llm_temperature,
        max_tokens=settings.max_tokens,
        timeout=settings.timeout,
        profanity_check=settings.profanity_check,
        base_url=settings.llm_base_url,
        verify_ssl_certs=settings.verify_ssl_certs,
        cert_file=settings.cert_file,
        key_file=settings.key_file,
    )


def _gigachat_kwargs() -> dict:
    """Common GigaChat constructor kwargs including the AEF callback handler.

    The AEF handler is attached at the client level so `llm` spans (model
    name, tokens, latency) are emitted automatically per docs_for_SDK/gen_span/langchain.md §1.
    """
    return dict(
        max_tokens=settings.max_tokens,
        timeout=settings.timeout,
        profanity_check=settings.profanity_check,
        base_url=settings.llm_base_url,
        verify_ssl_certs=settings.verify_ssl_certs,
        cert_file=settings.cert_file,
        key_file=settings.key_file,
        callbacks=[get_aef_handler()],
    )


def get_llm_with_config(
    temperature: float | None = None,
    model: str | None = None,
) -> GigaChat:
    """Build a fresh GigaChat per call. When `model` is not given,
    Main / PreView is picked by `settings.preview_ratio`. The chosen
    installation is logged so the trace correlates with §26 ratio rollout."""
    if model is not None:
        chosen_model, installation = model, "main"
    else:
        chosen_model, installation = _pick_model()

    logger.info(f"gigachat_installation_picked. installation={installation} model={chosen_model}")

    return GigaChat(
        model=chosen_model,
        temperature=temperature if temperature is not None else settings.llm_temperature,
        **_gigachat_kwargs(),
    )
