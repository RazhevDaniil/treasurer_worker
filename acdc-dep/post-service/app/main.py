from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, Request

from .config import settings
from .logging_config import setup_logging
from .api.routes import router as mail_router
from .api.health import router as health_router
from .services.db_client import DbClient
from .services.in_memory_db import InMemoryDbClient
from .utils.tracing import TRACE_HEADER_NAME, resolve_trace_id
from .workers.imap_worker import ImapWorker
from .workers.agent_dispatcher import AgentDispatcher
from .workers.smtp_worker import SmtpWorker

logger = logging.getLogger("app")

setup_logging(settings.log_level)

app = FastAPI(title=settings.app_name)
app.include_router(health_router)
app.include_router(mail_router)


@app.middleware("http")
async def trace_context_middleware(request: Request, call_next):
    trace_id = resolve_trace_id(headers=request.headers)
    request.state.x_trace_id = trace_id
    response = await call_next(request)
    response.headers[TRACE_HEADER_NAME] = trace_id
    return response


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("Starting mail_service...")

    if settings.db_app_url and settings.db_app_url.startswith("http"):
        # если не залетел бред в переменную тоже проверили
        logger.info(f"Using DbClient (db-service): {settings.db_app_url}")
        db_client = DbClient(base_url=settings.db_app_url)   
    else:
        logger.info(f"Using InMemoryDbClient (only for testing: {settings.db_app_url})")
        db_client = InMemoryDbClient()
    app.state.db_client = db_client
    app.state.settings = settings

    app.state.imap_worker = ImapWorker(settings, db_client)
    app.state.agent_dispatcher = AgentDispatcher(settings, db_client)
    app.state.smtp_worker = SmtpWorker(settings, db_client)

    app.state.tasks = [
        asyncio.create_task(app.state.imap_worker.run(), name="imap_worker"),
        asyncio.create_task(app.state.agent_dispatcher.run(), name="agent_dispatcher"),
        asyncio.create_task(app.state.smtp_worker.run(), name="smtp_worker"),
    ]
    logger.info("mail_service started")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("Shutting down mail_service...")
    for w in [app.state.imap_worker, app.state.agent_dispatcher, app.state.smtp_worker]:
        w.stop()

    tasks: list[asyncio.Task] = app.state.tasks
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("mail_service stopped")
