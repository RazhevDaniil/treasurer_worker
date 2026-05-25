import random
from datetime import datetime, timedelta
from .time import utcnow


def compute_delay_seconds(attempt: int, min_s: int, max_s: int) -> int:
    """
    attempt: 1..N
    Внутреннее правило: возрастающий интервал от min..max + небольшой jitter.
    """
    # мягкий рост; можно поменять формулу без изменения контрактов
    base = min(max_s, max(min_s, int(attempt ** 1.7)))
    jitter = random.randint(0, 3)
    return min(max_s, base + jitter)


def next_retry_time(attempt: int, min_s: int, max_s: int) -> datetime:
    return utcnow() + timedelta(seconds=compute_delay_seconds(attempt, min_s, max_s))
