"""HTTP client for db_app REST API."""

from __future__ import annotations

import logging
from datetime import datetime

import httpx

from ..utils.tracing import http_trace_headers, resolve_trace_id

logger = logging.getLogger("services.db_client")


class DbClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout)

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
        run_id: str | None = None,
    ) -> dict:
        """POST /v1/messages/ingest → IngestOut."""
        trace_id = resolve_trace_id(run_id, payload={"headers_json": headers_json})
        payload = {
            "message_id": message_id,
            "author_email": author_email,
            "sender_name": sender_name,
            "reply_to_email": reply_to_email,
            "subject": subject,
            "body_text": body_text,
            "body_html": body_html,
            "headers_json": headers_json,
            "thread_id": thread_id,
            "task_key": task_key,
            "run_id": trace_id,
        }
        async with self._client() as client:
            resp = await client.post(
                "/v1/messages/ingest",
                json=payload,
                headers=http_trace_headers(trace_id),
            )
            resp.raise_for_status()
            return resp.json()

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
        run_id: str | None = None,
    ) -> dict:
        """POST /v1/messages/outgoing → OutgoingOut."""
        trace_id = resolve_trace_id(run_id, payload=smtp_payload)
        payload = {
            "message_id": message_id,
            "author_email": author_email,
            "subject": subject,
            "body_text": body_text,
            "headers_json": headers_json,
            "thread_id": thread_id,
            "smtp_payload": smtp_payload,
            "task_key": task_key,
            "run_id": trace_id,
        }
        async with self._client() as client:
            resp = await client.post(
                "/v1/messages/outgoing",
                json=payload,
                headers=http_trace_headers(trace_id),
            )
            resp.raise_for_status()
            return resp.json()

    async def dequeue(
        self,
        task_type: str,
        limit: int = 10,
        claim_ttl_seconds: int = 300,
    ) -> list[dict]:
        """POST /v1/outbox/dequeue → list[OutboxTaskOut]."""
        payload = {
            "task_type": task_type,
            "limit": limit,
            "claim_ttl_seconds": claim_ttl_seconds,
        }
        async with self._client() as client:
            resp = await client.post("/v1/outbox/dequeue", json=payload)
            resp.raise_for_status()
            return resp.json()

    async def update_task_status(
        self,
        task_id: str,
        status: str,
        claim_token: str,
        error: str | None = None,
        next_retry_at: datetime | None = None,
        run_id: str | None = None,
    ) -> None:
        """PATCH /v1/outbox/{task_id}/status."""
        payload: dict = {"status": status, "claim_token": claim_token}
        if error is not None:
            payload["error"] = error
        if next_retry_at is not None:
            payload["next_retry_at"] = next_retry_at.isoformat()
        async with self._client() as client:
            resp = await client.patch(
                f"/v1/outbox/{task_id}/status",
                json=payload,
                headers=http_trace_headers(resolve_trace_id(run_id)),
            )
            resp.raise_for_status()
