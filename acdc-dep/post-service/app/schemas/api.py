from pydantic import BaseModel, Field


class SendReplyRequest(BaseModel):
    recipient_email: str
    subject: str
    reply_body: str
    in_reply_to: str | None = Field(None, description="RFC Message-ID of the message being replied to")
    references: str | None = Field(None, description="References header (space-separated Message-IDs)")
    thread_id: str | None = Field(None, description="Root Message-ID of the thread")
    cc: list[str] | None = Field(None, description="Carbon-copy recipients")
    idempotency_key: str | None = Field(None, description="Stable key for retry-safe reply enqueue")


class SendReplyResponse(BaseModel):
    status: str
    message_id: str
    task_id: str


class HealthResponse(BaseModel):
    status: str
