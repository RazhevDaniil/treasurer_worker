"""
Email service for sending/receiving emails via the mail server API.

This service connects to the mail server (app/) to send and receive emails
on behalf of the deal agent.
"""

import logging

import httpx
from typing import Optional

from ..core.config import settings
from ..core.http_retry import request_with_retry
from ..core.tracing import safe_trace_json, trace_action_span, trace_header_dict

logger = logging.getLogger(__name__)


class EmailService:
    """
    Service for email operations via the mail server API.

    The agent runs as a separate service and communicates
    with the mail server via its REST API.
    """

    def __init__(self):
        self.base_url = settings.mail_server_api_url
        self.account_id = settings.mail_account_id

    def _client(self) -> httpx.AsyncClient:
        """Create an HTTP client bound to the current event loop."""
        return httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
        )

    async def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        in_reply_to: Optional[str] = None,
        references: Optional[str] = None,
        thread_id: Optional[str] = None,
        cc: Optional[list[str]] = None,
        idempotency_key: Optional[str] = None,
    ) -> bool:
        """
        Send an email via the mail server API (POST /api/v1/send_reply).

        For standalone notifications (not a reply), pass to/subject/body only —
        mail_app will generate a fresh Message-ID. Pass `cc` to put extra
        addresses in the Cc header (used by escalation: manager в To, клиент в Cc).
        """
        payload = {
            "recipient_email": to,
            "subject": subject,
            "reply_body": body,
            "in_reply_to": in_reply_to,
            "references": references,
            "thread_id": thread_id,
            "cc": cc,
            "idempotency_key": idempotency_key,
        }

        with trace_action_span(
            "mail_app.send_reply",
            call_type="api_call",
            target_name="mail_app.POST /api/v1/send_reply",
            request_payload=payload,
            is_mutation=True,
            rollback_possible=False,
        ) as span:
            try:
                response: httpx.Response | None = None
                async with self._client() as client:
                    response = await request_with_retry(
                        client,
                        "POST",
                        "/api/v1/send_reply",
                        operation_name="mail_app.send_reply",
                        trace_span=span,
                        json=payload,
                        headers=trace_header_dict({"X-Account-ID": self.account_id}),
                    )
                if response is None:
                    span.add_output_result({"sent": False, "error": "empty_response"})
                    return False
                response.raise_for_status()
                logger.info(f"send_email_ok. to={to} subject={subject}")
                span.add_span_attributes(**{
                    "aef.result_payload": safe_trace_json({"sent": True}),
                })
                span.add_output_result({"sent": True})
                return True

            except httpx.HTTPError as e:
                span.record_error(e)
                span.add_output_result({"sent": False, "error": str(e)})
                logger.error(f"send_email_failed. to={to} subject={subject} error={e}")
                return False

    async def fetch_new_emails(
        self,
        folder: str = "INBOX",
        limit: int = 10,
    ) -> list[dict]:
        """
        Fetch new unread emails from the mail server.

        Args:
            folder: IMAP folder to fetch from
            limit: Maximum number of emails to fetch

        Returns:
            List of email dictionaries

        Note:
            This is a STUB implementation.
            Real implementation should call the mail server's IMAP endpoint.
        """
        # TODO: Implement actual API call to mail server
        # GET /api/v1/imap/emails

        request_payload = {
            "folder": folder,
            "limit": limit,
            "unread_only": True,
        }
        with trace_action_span(
            "mail_app.fetch_new_emails",
            call_type="api_call",
            target_name="mail_app.GET /api/v1/imap/emails",
            request_payload=request_payload,
            is_mutation=False,
            rollback_possible=None,
        ) as span:
            try:
                response: httpx.Response | None = None
                async with self._client() as client:
                    response = await request_with_retry(
                        client,
                        "GET",
                        "/api/v1/imap/emails",
                        operation_name="mail_app.fetch_new_emails",
                        trace_span=span,
                        params=request_payload,
                        headers=trace_header_dict({"X-Account-ID": self.account_id}),
                    )
                if response is None:
                    span.add_output_result({"emails": [], "error": "empty_response"})
                    return []
                response.raise_for_status()
                data = response.json()
                emails = data.get("emails", [])
                logger.info(f"fetch_emails_ok. folder={folder} count={len(emails)}")
                span.add_span_attributes(**{
                    "aef.result_payload": safe_trace_json({"emails_count": len(emails)}),
                })
                span.add_output_result({"emails_count": len(emails), "emails": emails})
                return emails

            except httpx.HTTPError as e:
                span.record_error(e)
                span.add_output_result({"emails": [], "error": str(e)})
                logger.error(f"fetch_emails_failed. folder={folder} error={e}")
                return []

    async def mark_as_read(self, folder: str, message_uids: list[int]) -> bool:
        """
        Mark emails as read.

        Args:
            folder: IMAP folder
            message_uids: List of message UIDs to mark as read

        Returns:
            True if successful
        """
        # TODO: Implement actual API call
        # POST /api/v1/imap/mark-read

        payload = {
            "folder": folder,
            "uids": message_uids,
        }
        with trace_action_span(
            "mail_app.mark_read",
            call_type="api_call",
            target_name="mail_app.POST /api/v1/imap/mark-read",
            request_payload=payload,
            is_mutation=True,
            rollback_possible=False,
        ) as span:
            try:
                response: httpx.Response | None = None
                async with self._client() as client:
                    response = await request_with_retry(
                        client,
                        "POST",
                        "/api/v1/imap/mark-read",
                        operation_name="mail_app.mark_read",
                        trace_span=span,
                        json=payload,
                        headers=trace_header_dict({"X-Account-ID": self.account_id}),
                    )
                if response is None:
                    span.add_output_result({"marked": False, "error": "empty_response"})
                    return False
                response.raise_for_status()
                logger.debug(f"mark_as_read_ok. folder={folder} count={len(message_uids)}")
                span.add_span_attributes(**{
                    "aef.result_payload": safe_trace_json({"marked": True}),
                })
                span.add_output_result({"marked": True})
                return True

            except httpx.HTTPError as e:
                span.record_error(e)
                span.add_output_result({"marked": False, "error": str(e)})
                logger.error(f"mark_as_read_failed. folder={folder} error={e}")
                return False

    async def close(self):
        """Close HTTP client."""
        return None
