"""PostgreSQL checkpointer setup for LangGraph state persistence."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
import asyncpg

from .db_session import db_info, db_url

logger = logging.getLogger(__name__)

async def create_checkpointer() -> AsyncPostgresSaver:
    """
    Create and initialize PostgreSQL checkpointer.

    Creates necessary tables if they don't exist.

    Returns:
        Configured AsyncPostgresSaver instance
    """
    logger.info(
        f"create_checkpointer_start. host={db_info.host} "
        f"port={db_info.port} db={db_info.database}"
    )

    checkpointer = AsyncPostgresSaver.from_conn_string(db_url)

    # Setup tables (creates if not exists)
    await checkpointer.setup()

    logger.info("create_checkpointer_ok")
    return checkpointer


@asynccontextmanager
async def get_checkpointer() -> AsyncGenerator[AsyncPostgresSaver, None]:
    """
    Context manager for checkpointer lifecycle.

    Usage:
        async with get_checkpointer() as checkpointer:
            graph = create_deal_agent_graph(checkpointer)
            await graph.ainvoke(...)
    """
    checkpointer = await create_checkpointer()
    try:
        yield checkpointer
    finally:
        # Cleanup if needed
        pass


async def init_database() -> None:
    """
    Initialize database with required tables.

    Creates:
    - LangGraph checkpoint tables (via checkpointer.setup())
    - Custom tables for deals if needed
    """
    logger.info("init_database_start")

    # Create checkpointer tables
    checkpointer = await create_checkpointer()

    # Create custom tables for deals
    conn = await asyncpg.connect(db_url)

    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                id UUID PRIMARY KEY,
                thread_id VARCHAR(255) NOT NULL,
                counterparty_email VARCHAR(255) NOT NULL,
                inn VARCHAR(12),
                term_days INTEGER,
                volume DECIMAL(15, 2),
                currency VARCHAR(10) DEFAULT 'RUB',
                status VARCHAR(50) NOT NULL,
                assigned_employee VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
            );

            CREATE INDEX IF NOT EXISTS idx_deals_thread_id ON deals(thread_id);
            CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status);
            CREATE INDEX IF NOT EXISTS idx_deals_counterparty ON deals(counterparty_email);
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS negotiation_history (
                id SERIAL PRIMARY KEY,
                deal_id UUID REFERENCES deals(id),
                message_type VARCHAR(20) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_negotiation_deal_id ON negotiation_history(deal_id);
        """)

    finally:
        await conn.close()

    logger.info("init_database_ok")


async def reset_database() -> None:
    """
    Reset database (drop and recreate tables).

    WARNING: This will delete all data!
    """
    logger.warning("reset_database_start. Dropping all tables — all data will be lost")

    conn = await asyncpg.connect(db_url)

    try:
        # Drop custom tables
        await conn.execute("""
            DROP TABLE IF EXISTS negotiation_history CASCADE;
            DROP TABLE IF EXISTS deals CASCADE;
        """)

        # Drop LangGraph tables
        await conn.execute("""
            DROP TABLE IF EXISTS checkpoints CASCADE;
            DROP TABLE IF EXISTS checkpoint_blobs CASCADE;
            DROP TABLE IF EXISTS checkpoint_writes CASCADE;
        """)

    finally:
        await conn.close()

    # Recreate
    await init_database()
    logger.info("reset_database_ok")
