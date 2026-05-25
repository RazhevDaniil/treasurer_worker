"""Точка входа approve-service — FastAPI-приложение с lifespan.

При старте запускает фоновые потоки:
1. CspAgentConsumer — приём из внешней системы, обогащение, отправка агенту
2. AgentResultConsumer — приём результатов агента, публикация во внешнюю систему
3. TtlWatchdog — контроль TTL ответа агента, повторные отправки (спека §6.2)

При остановке — graceful shutdown всех компонентов.
"""

import threading
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.api.routes import init_routes, router
from src.config import settings
from src.consumers.agent_result_consumer import AgentResultConsumer
from src.consumers.csp_agent_consumer import CspAgentConsumer
from src.producers.agent_task_producer import AgentTaskProducer
from src.producers.csp_result_producer import CspResultProducer
from src.services.db_client import DbClient
from src.services.enrichment_service import EnrichmentService
from src.services.ttl_watchdog import TtlWatchdog
from src.utils.logger import get_logger, setup_logging

log = get_logger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan: инициализация зависимостей и запуск фоновых потоков."""
    setup_logging()
    log.info("approve_service_starting")

    # Инициализация зависимостей
    db = DbClient()
    enrichment = EnrichmentService(db)
    agent_producer = AgentTaskProducer(db)
    csp_producer = CspResultProducer()

    csp_consumer = CspAgentConsumer(db, enrichment, agent_producer)
    agent_result_consumer = AgentResultConsumer(db, csp_producer)
    ttl_watchdog = TtlWatchdog(db, agent_producer)

    # Инжектим DbClient в API-роутер
    init_routes(db)

    # Запуск consumer'ов в фоновых потоках
    t1 = threading.Thread(
        target=csp_consumer.run, name="csp-agent-consumer", daemon=True
    )
    t2 = threading.Thread(
        target=agent_result_consumer.run, name="agent-result-consumer", daemon=True
    )
    t1.start()
    t2.start()

    # Запуск TTL watchdog (спека §6.2)
    ttl_watchdog.start()

    log.info("approve_service_started")

    yield

    # Graceful shutdown
    log.info("approve_service_shutting_down")
    csp_consumer.close()
    agent_result_consumer.close()
    ttl_watchdog.stop()
    enrichment.close()
    agent_producer.close()
    csp_producer.close()
    db.close()
    log.info("approve_service_stopped")


app = FastAPI(
    title="approve-service",
    description="Микросервис-оркестратор согласования расчётов казначейством",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.server_host,
        port=settings.server_port,
    )
