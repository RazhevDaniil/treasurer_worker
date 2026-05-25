import asyncio
import logging
import socket

import httpx

from ..config import Settings
from ..services.in_memory_db import InMemoryDbClient
from ..services.smtp_client import SmtpClient, SmtpPermanentError
from ..services.email_builder import build_reply_email
from ..utils.backoff import next_retry_time
from ..utils.tracing import resolve_trace_id

logger = logging.getLogger("workers.smtp")


class SmtpWorker:
    def __init__(self, settings: Settings, db_client: InMemoryDbClient):
        self.settings = settings
        self.db_client = db_client
        self.smtp = SmtpClient(settings)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        logger.info("SMTP worker started")
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception as e:
                logger.exception("SMTP worker error: %s", e)
            await asyncio.sleep(self.settings.smtp_poll_seconds)
        logger.info("SMTP worker stopped")

    async def _tick(self) -> None:
        tasks = await self.db_client.dequeue(
            task_type="SEND_SMTP",
            limit=self.settings.smtp_batch_size,
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
        recipient_email = payload.get("recipient_email")
        cc = payload.get("cc") or None
        subject = payload.get("subject", "")
        body_text = payload.get("body_text", "")
        in_reply_to = payload.get("in_reply_to")
        references = payload.get("references")
        trace_id = resolve_trace_id(payload=payload)

        try:
            if not recipient_email:
                raise SmtpPermanentError("recipient_email is empty")

            msg = build_reply_email(
                from_addr=self.settings.smtp_from_address,
                to_addr=recipient_email,
                subject=subject,
                body_text=body_text,
                original_message_id=in_reply_to,
                original_references=references,
                cc_addrs=cc,
            )

            result = await asyncio.to_thread(lambda: self.smtp.send_message_starttls(msg))

            if result.ok:
                await self.db_client.update_task_status(
                    task_id,
                    "DONE",
                    claim_token,
                    run_id=trace_id,
                )
                logger.info(
                    "SMTP sent message_id=%s thread_id=%s to=%s task_id=%s trace_id=%s",
                    message_id, thread_id, recipient_email, task_id, trace_id,
                )
                return

            code = result.smtp_code or 0
            msg_err = f"SMTP error {code}: {result.smtp_response}"

            if 400 <= code < 500:
                if attempt >= self.settings.max_attempts:
                    await self.db_client.update_task_status(
                        task_id,
                        "FAILED",
                        claim_token,
                        error=msg_err,
                        run_id=trace_id,
                    )
                    return
                next_retry = next_retry_time(
                    attempt, self.settings.backoff_min_seconds, self.settings.backoff_max_seconds
                )
                await self.db_client.update_task_status(
                    task_id,
                    "RETRYING",
                    claim_token,
                    error=msg_err,
                    next_retry_at=next_retry,
                    run_id=trace_id,
                )
                return

            if 500 <= code < 600:
                await self.db_client.update_task_status(
                    task_id,
                    "FAILED",
                    claim_token,
                    error=msg_err,
                    run_id=trace_id,
                )
                return

            # Unknown code → retry
            if attempt >= self.settings.max_attempts:
                await self.db_client.update_task_status(
                    task_id,
                    "FAILED",
                    claim_token,
                    error=msg_err,
                    run_id=trace_id,
                )
                return
            next_retry = next_retry_time(
                attempt, self.settings.backoff_min_seconds, self.settings.backoff_max_seconds
            )
            await self.db_client.update_task_status(
                task_id,
                "RETRYING",
                claim_token,
                error=msg_err,
                next_retry_at=next_retry,
                run_id=trace_id,
            )

        except (EOFError, OSError, socket.error) as e:
            msg_err = f"Network/EOF SMTP error: {e}"
            logger.error(
                "SMTP network error message_id=%s thread_id=%s task_id=%s: %s",
                message_id, thread_id, task_id, msg_err,
            )
            if attempt >= self.settings.max_attempts:
                await self.db_client.update_task_status(
                    task_id,
                    "FAILED",
                    claim_token,
                    error=msg_err,
                    run_id=trace_id,
                )
                return
            next_retry = next_retry_time(
                attempt, self.settings.backoff_min_seconds, self.settings.backoff_max_seconds
            )
            await self.db_client.update_task_status(
                task_id,
                "RETRYING",
                claim_token,
                error=msg_err,
                next_retry_at=next_retry,
                run_id=trace_id,
            )

        except SmtpPermanentError as e:
            msg_err = str(e)
            await self.db_client.update_task_status(
                task_id,
                "FAILED",
                claim_token,
                error=msg_err,
                run_id=trace_id,
            )

        except Exception as e:
            msg_err = str(e)
            logger.error(
                "SMTP worker unexpected error message_id=%s thread_id=%s task_id=%s: %s",
                message_id, thread_id, task_id, msg_err,
            )
            if attempt >= self.settings.max_attempts:
                await self.db_client.update_task_status(
                    task_id,
                    "FAILED",
                    claim_token,
                    error=msg_err,
                    run_id=trace_id,
                )
                return
            next_retry = next_retry_time(
                attempt, self.settings.backoff_min_seconds, self.settings.backoff_max_seconds
            )
            await self.db_client.update_task_status(
                task_id,
                "RETRYING",
                claim_token,
                error=msg_err,
                next_retry_at=next_retry,
                run_id=trace_id,
            )
