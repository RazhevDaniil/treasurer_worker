"""Structured-logging helpers for approve_app.

approve_app — Kafka-оркестратор, не AI-агент. Cross-service trace
propagation удалена вместе с AEF SDK миграцией; модуль держит только
`bound_trace` для per-message корреляции stdout-логов внутри одной
итерации consumer'а.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from structlog.contextvars import bind_contextvars, unbind_contextvars


@contextmanager
def bound_trace(**kw: str) -> Iterator[None]:
    """Temporarily bind values to contextvars; unbind on exit.

    Consumers обрабатывают сообщения последовательно в одном потоке — без явного
    unbind ID одного сообщения утечёт в логи следующего.
    """
    bind_contextvars(**kw)
    try:
        yield
    finally:
        unbind_contextvars(*kw.keys())
