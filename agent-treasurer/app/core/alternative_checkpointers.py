from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from langgraph.checkpoint.memory import MemorySaver

from .config import settings


# ---------------------------------------------------------------------------
# 1. In-memory dict checkpointer (MemorySaver)
# ---------------------------------------------------------------------------

def create_memory_checkpointer() -> MemorySaver:
    """
    Create an in-memory dict-based checkpointer.

    Data lives only in the process memory and is lost on restart.
    Useful for tests, local development, and short-lived sessions.

    Returns:
        Configured MemorySaver instance (ready to use, no setup needed).
    """
    return MemorySaver()


@asynccontextmanager
async def get_memory_checkpointer() -> AsyncGenerator[MemorySaver, None]:
    """Context manager matching the interface of get_checkpointer()."""
    yield create_memory_checkpointer()


# ---------------------------------------------------------------------------
# 2. Redis checkpointer
# ---------------------------------------------------------------------------
# Requires:  pip install langgraph-checkpoint-redis
# ---------------------------------------------------------------------------

async def create_redis_checkpointer(
    redis_url: Optional[str] = None,
):
    """
    Create and initialize a Redis-based async checkpointer.

    Args:
        redis_url: Redis connection string.
                   Falls back to settings.redis_url if not provided.

    Returns:
        Configured AsyncRedisSaver instance.
    """
    from langgraph.checkpoint.redis.aio import AsyncRedisSaver

    url = redis_url or settings.redis_url
    checkpointer = AsyncRedisSaver(conn=url)
    await checkpointer.setup()
    return checkpointer


@asynccontextmanager
async def get_redis_checkpointer(
    redis_url: Optional[str] = None,
) -> AsyncGenerator:
    """
    Async context manager for the Redis checkpointer lifecycle.

    Usage:
        async with get_redis_checkpointer() as checkpointer:
            graph = create_deal_agent_graph(checkpointer)
            await graph.ainvoke(...)
    """
    checkpointer = await create_redis_checkpointer(redis_url)
    try:
        yield checkpointer
    finally:
        await checkpointer.conn.aclose()
