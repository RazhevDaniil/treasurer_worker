import asyncio
import logging

from ..config import Settings
from ..services.in_memory_db import InMemoryDbClient
from ..services.agent_client import AgentKafkaPublisher, AgentPublishError
from ..utils.backoff import next_retry_time

logger = logging.getLogger("workers.agent_dispatcher")


class AgentDispatcher:
    def __init__(self, settings: Settings, db_client: InMemoryDbClient):
        self.settings = settings
        self.db_client = db_client
        self.publisher = AgentKafkaPublisher(settings)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()
        try:
            self.publisher.close()
        except Exception as exc:
            logger.warning("kafka_producer_close_error: %s", exc)

    async def run(self) -> None:
        logger.info(
            "Agent dispatcher started (topic=%s)",
            self.settings.kafka_mail_topic,
        )
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception as e:
                logger.exception("Agent dispatcher error: %s", e)
            await asyncio.sleep(self.settings.agent_poll_seconds)
        logger.info("Agent dispatcher stopped")

    async def _tick(self) -> None:
        tasks = await self.db_client.dequeue(
            task_type="PROCESS_INCOMING",
            limit=self.settings.agent_batch_size,
        )
        for task in tasks:
            await self._process_one(task)

    async def _process_one(self, task: dict) -> None:
        task_id = task["id"]
        claim_token = task["claim_token"]
        payload = task.get("payload_json") or {}
        attempt = task.get("attempt", 1)

        message_id = payload.get("message_id", "")
        thread_id = payload.get("thread_id", "")
        sender_email = payload.get("sender_email", "")

        try:
            await asyncio.to_thread(
                self.publisher.publish_incoming_email,
                payload,
                task_id,
            )

            await self.db_client.update_task_status(task_id, "DONE", claim_token)
            logger.info(
                "Agent dispatched message_id=%s thread_id=%s sender=%s task_id=%s",
                message_id, thread_id, sender_email, task_id,
            )
        except AgentPublishError as e:
            msg = str(e)
            logger.error(
                "Kafka publish failed message_id=%s thread_id=%s task_id=%s: %s",
                message_id, thread_id, task_id, msg,
            )
            if attempt >= self.settings.max_attempts:
                await self.db_client.update_task_status(task_id, "FAILED", claim_token, error=msg)
                return
            next_retry = next_retry_time(
                attempt, self.settings.backoff_min_seconds, self.settings.backoff_max_seconds
            )
            await self.db_client.update_task_status(
                task_id, "RETRYING", claim_token, error=msg, next_retry_at=next_retry
            )

        except Exception as e:
            msg = str(e)
            logger.exception(
                "Agent dispatch unexpected error message_id=%s thread_id=%s task_id=%s: %s",
                message_id, thread_id, task_id, msg,
            )
            if attempt >= self.settings.max_attempts:
                await self.db_client.update_task_status(task_id, "FAILED", claim_token, error=msg)
                return
            next_retry = next_retry_time(
                attempt, self.settings.backoff_min_seconds, self.settings.backoff_max_seconds
            )
            await self.db_client.update_task_status(
                task_id, "RETRYING", claim_token, error=msg, next_retry_at=next_retry
            )
