"""Core configuration and utilities."""

from .config import settings


def get_llm(*args, **kwargs):
    from .llm import get_llm as _get_llm

    return _get_llm(*args, **kwargs)

__all__ = ["settings", "get_llm"]
