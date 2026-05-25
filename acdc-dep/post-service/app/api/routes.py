from __future__ import annotations

import uuid
from hashlib import sha256

from fastapi import APIRouter, HTTPException, Request

from ..schemas.api import SendReplyRequest, SendReplyResponse

router = APIRouter(prefix="/api/v1", tags=["mail"])


def _thread_root(references: str | None) -> str | None:
    """Extract the first Message-ID from a References header (= thread root)."""
    if not references:
        return None
    first = references.strip().split()[0]
    return first or None


@router.post("/send_reply", response_model=SendReplyResponse)
async def send_reply(req: SendReplyRequest, request: Request) -> SendReplyResponse:
    db_client = request.app.state.db_client
    settings = request.app.state.settings

    domain = (
        settings.smtp_from_address.split("@")[-1]
        if "@" in settings.smtp_from_address
        else "mail.local"
    )
    if req.idempotency_key:
        digest = sha256(req.idempotency_key.encode("utf-8")).hexdigest()[:32]
        message_id = f"<reply-{digest}@{domain}>"
        task_key = req.idempotency_key
    else:
        message_id = f"<{uuid.uuid4()}@{domain}>"
        task_key = message_id

    headers: dict = {}
    if req.in_reply_to:
        headers["In-Reply-To"] = req.in_reply_to
    if req.references:
        headers["References"] = req.references

    thread_id = req.thread_id or _thread_root(req.references) or req.in_reply_to or message_id

    smtp_payload = {
        "message_id": message_id,
        "thread_id": thread_id,
        "recipient_email": req.recipient_email,
        "cc": req.cc,
        "subject": req.subject,
        "body_text": req.reply_body,
        "in_reply_to": req.in_reply_to,
        "references": req.references,
    }

    result = await db_client.save_outgoing(
        message_id=message_id,
        author_email=settings.smtp_from_address,
        subject=req.subject,
        body_text=req.reply_body,
        headers_json=headers or None,
        thread_id=thread_id,
        smtp_payload=smtp_payload,
        task_key=task_key,
    )

    return SendReplyResponse(
        status="queued",
        message_id=message_id,
        task_id=result["task_id"],
    )
