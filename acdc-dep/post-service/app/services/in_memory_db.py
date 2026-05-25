"""In-memory replacement for DbClient while db_app is not deployed."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime

from cachetools import TTLCache

logger = logging.getLogger("services.in_memory_db")

_TTL = 86_400  # 24 hours
_MAX_SIZE = 10_000


class InMemoryDbClient:
    def __init__(self, ttl: int = _TTL, maxsize: int = _MAX_SIZE) -> None:
        self._messages: TTLCache[str, dict] = TTLCache(maxsize=maxsize, ttl=ttl)
        self._tasks: TTLCache[str, dict] = TTLCache(maxsize=maxsize, ttl=ttl)
        self._queues: dict[str, asyncio.Queue[str]] = {
            "PROCESS_INCOMING": asyncio.Queue(),
            "SEND_SMTP": asyncio.Queue(),
        }

    async def ingest_message(
        self,
        *,
        message_id: str,
        author_email: str,
        sender_name: str | None = None,
        reply_to_email: str | None = None,
        subject: str | None = None,
        body_text: str | None = None,
        body_html: str | None = None,
        headers_json: dict | None = None,
        thread_id: str,
        task_key: str,
    ) -> dict:
        if message_id in self._messages:
            logger.info("in_memory dedup: message_id=%s already exists", message_id)
            return self._messages[message_id]

        record = {
            "id": str(uuid.uuid4()),
            "message_id": message_id,
            "author_email": author_email,
            "sender_name": sender_name,
            "reply_to_email": reply_to_email,
            "subject": subject,
            "body_text": body_text,
            "body_html": body_html,
            "headers_json": headers_json,
            "thread_id": thread_id,
        }
        self._messages[message_id] = record

        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "task_type": "PROCESS_INCOMING",
            "status": "PENDING",
            "attempt": 1,
            "claim_token": str(uuid.uuid4()),
            "payload_json": {
                "message_id": message_id,
                "sender_email": author_email,
                "sender_name": sender_name,
                "reply_to_email": reply_to_email,
                "subject": subject,
                "body_text": body_text,
                "body_html": body_html,
                "headers_json": headers_json,
                "thread_id": thread_id,
            },
        }
        self._tasks[task_id] = task
        await self._queues["PROCESS_INCOMING"].put(task_id)

        logger.info("in_memory ingested message_id=%s thread_id=%s task_id=%s", message_id, thread_id, task_id)
        return record

    async def save_outgoing(
        self,
        *,
        message_id: str,
        author_email: str,
        subject: str | None = None,
        body_text: str | None = None,
        headers_json: dict | None = None,
        thread_id: str,
        smtp_payload: dict | None = None,
        task_key: str,
    ) -> dict:
        record = {
            "id": str(uuid.uuid4()),
            "message_id": message_id,
            "author_email": author_email,
            "subject": subject,
            "body_text": body_text,
            "headers_json": headers_json,
            "thread_id": thread_id,
        }
        self._messages[message_id] = record

        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "task_type": "SEND_SMTP",
            "status": "PENDING",
            "attempt": 1,
            "claim_token": str(uuid.uuid4()),
            "payload_json": smtp_payload or {},
        }
        self._tasks[task_id] = task
        await self._queues["SEND_SMTP"].put(task_id)

        logger.info("in_memory save_outgoing message_id=%s thread_id=%s task_id=%s", message_id, thread_id, task_id)
        return {"task_id": task_id, **record}

    async def dequeue(
        self,
        task_type: str,
        limit: int = 10,
        claim_ttl_seconds: int = 300,
    ) -> list[dict]:
        queue = self._queues.get(task_type)
        if queue is None:
            return []

        results: list[dict] = []
        for _ in range(limit):
            try:
                task_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            task = self._tasks.get(task_id)
            if task is None:
                continue

            if task["status"] not in ("PENDING", "RETRYING"):
                continue

            task["status"] = "CLAIMED"
            task["claim_token"] = str(uuid.uuid4())
            results.append(task)

        return results

    async def update_task_status(
        self,
        task_id: str,
        status: str,
        claim_token: str,
        error: str | None = None,
        next_retry_at: datetime | None = None,
    ) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            logger.warning("in_memory update_task_status: unknown task_id=%s", task_id)
            return

        task["status"] = status
        if error:
            task["error"] = error

        if status == "RETRYING":
            task["attempt"] = task.get("attempt", 1) + 1
            queue = self._queues.get(task["task_type"])
            if queue is not None:
                await queue.put(task_id)

        logger.debug("in_memory task %s → %s", task_id, status)