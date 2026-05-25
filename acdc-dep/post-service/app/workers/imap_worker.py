import asyncio
import logging
import uuid

import httpx

from ..config import Settings
from ..services.in_memory_db import InMemoryDbClient
from ..services.imap_client import ImapClient, ImapAuthError
from ..services.email_parser import parse_email_bytes
from ..utils.tracing import TRACE_HEADER_NAME, resolve_trace_id

logger = logging.getLogger("workers.imap")

_DOMAIN_FALLBACK = "mail.local"


def _ensure_message_id(message_id: str | None, domain: str) -> str:
    if message_id:
        return message_id
    return f"<{uuid.uuid4()}@{domain}>"


class ImapWorker:
    def __init__(self, settings: Settings, db_client: InMemoryDbClient):
        self.settings = settings
        self.db_client = db_client
        self.imap = ImapClient(settings)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        logger.info(
            "IMAP worker started (poll=%ss, mailbox=%s)",
            self.settings.imap_poll_seconds,
            self.settings.imap_mailbox,
        )
        while not self._stop.is_set():
            try:
                envelopes = await asyncio.to_thread(lambda: list(self.imap.fetch_unseen()))
                if envelopes:
                    logger.info("IMAP fetched %d unseen messages", len(envelopes))
                for env in envelopes:
                    await self._handle_one(env.uid, env.uidvalidity, env.raw_bytes)
            except ImapAuthError as e:
                logger.error("IMAP auth error: %s", e)
            except Exception as e:
                logger.exception("IMAP worker error: %s", e)

            await asyncio.sleep(self.settings.imap_poll_seconds)

        logger.info("IMAP worker stopped")

    async def _handle_one(self, uid: int, uidvalidity: int, raw_bytes: bytes) -> None:
        parsed, thread_root = parse_email_bytes(raw_bytes)
        trace_id = resolve_trace_id()

        domain = (
            self.settings.smtp_from_address.split("@")[-1]
            if "@" in self.settings.smtp_from_address
            else _DOMAIN_FALLBACK
        )
        message_id = _ensure_message_id(parsed.message_id, domain)

        headers: dict = {}
        if parsed.in_reply_to:
            headers["In-Reply-To"] = parsed.in_reply_to
        if parsed.references:
            headers["References"] = parsed.references
        if parsed.recipient_email:
            headers["To"] = parsed.recipient_email
        if parsed.reply_to_email:
            headers["Reply-To"] = parsed.reply_to_email
        headers[TRACE_HEADER_NAME] = trace_id

        try:
            await self.db_client.ingest_message(
                message_id=message_id,
                author_email=parsed.sender_email or "",
                sender_name=parsed.sender_name,
                reply_to_email=parsed.reply_to_email,
                subject=parsed.subject,
                body_text=parsed.body_text,
                body_html=parsed.body_html,
                headers_json=headers or None,
                thread_id=thread_root or message_id,
                task_key=message_id,
                run_id=trace_id,
            )
            logger.info(
                "IMAP ingested message_id=%s thread_id=%s uid=%s trace_id=%s",
                message_id,
                thread_root or message_id,
                uid,
                trace_id,
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                logger.info("IMAP dedup: message_id=%s already processed", message_id)
            else:
                logger.error("db_app ingest failed uid=%s: %s", uid, e)
                return
        except Exception as e:
            logger.error("db_app ingest error uid=%s: %s", uid, e)
            return

        # Mark seen on IMAP after successful ingest
        try:
            await asyncio.to_thread(lambda: self.imap.mark_seen(uid))
            logger.info("IMAP marked seen uid=%s", uid)
        except Exception as e:
            logger.error("Failed to mark seen on IMAP uid=%s: %s", uid, e)
