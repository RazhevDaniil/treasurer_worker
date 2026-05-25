"""
Email service for sending/receiving emails via the mail server API.

This service connects to the mail server (app/) to send and receive emails
on behalf of the deal agent.
"""

import logging

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from typing import Optional

from ..core.config import settings
from ..core.tracing import record_hop, safe_trace_json, trace_action_span, trace_header_dict

logger = logging.getLogger(__name__)


def _log_http_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    next_wait_sec = round(retry_state.next_action.sleep, 3) if retry_state.next_action else None
    exc_type = type(exc).__name__ if exc else None
    exc_str = str(exc) if exc else None
    logger.info(
        f"http_retry. attempt={retry_state.attempt_number} "
        f"next_wait_sec={next_wait_sec} exc_type={exc_type} exc={exc_str}"
    )


_RETRYABLE_HTTP_EXC = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
    httpx.HTTPStatusError,
)


def _retryable_status(status_code: int) -> bool:
    return status_code >= 500 or status_code == 429


def _http_response_payload(response: httpx.Response) -> dict:
    try:
        body = response.json()
    except Exception:
        body = response.text
    return {
        "status_code": response.status_code,
        "body": body,
    }


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
                    async for attempt in AsyncRetrying(
                        retry=retry_if_exception_type(_RETRYABLE_HTTP_EXC),
                        stop=stop_after_attempt(settings.http_max_retries),
                        wait=wait_exponential_jitter(
                            initial=settings.http_retry_base,
                            max=settings.http_retry_max,
                        ),
                        before_sleep=_log_http_retry,
                        reraise=True,
                    ):
                        with attempt:
                            hop = record_hop()
                            span.add_span_attributes(**{
                                "aef.hops_used": hop,
                                "aef.http_attempt": attempt.retry_state.attempt_number,
                            })
                            response = await client.post(
                                "/api/v1/send_reply",
                                json=payload,
                                headers=trace_header_dict({"X-Account-ID": self.account_id}),
                            )
                            span.add_span_attributes(**{
                                "aef.http_status_code": response.status_code,
                                "aef.response_payload": safe_trace_json(_http_response_payload(response)),
                            })
                            if _retryable_status(response.status_code):
                                response.raise_for_status()
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
                    async for attempt in AsyncRetrying(
                        retry=retry_if_exception_type(_RETRYABLE_HTTP_EXC),
                        stop=stop_after_attempt(settings.http_max_retries),
                        wait=wait_exponential_jitter(
                            initial=settings.http_retry_base,
                            max=settings.http_retry_max,
                        ),
                        before_sleep=_log_http_retry,
                        reraise=True,
                    ):
                        with attempt:
                            hop = record_hop()
                            span.add_span_attributes(**{
                                "aef.hops_used": hop,
                                "aef.http_attempt": attempt.retry_state.attempt_number,
                            })
                            response = await client.get(
                                f"/api/v1/imap/emails",
                                params=request_payload,
                                headers=trace_header_dict({"X-Account-ID": self.account_id}),
                            )
                            span.add_span_attributes(**{
                                "aef.http_status_code": response.status_code,
                                "aef.response_payload": safe_trace_json(_http_response_payload(response)),
                            })
                            if _retryable_status(response.status_code):
                                response.raise_for_status()
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
                    async for attempt in AsyncRetrying(
                        retry=retry_if_exception_type(_RETRYABLE_HTTP_EXC),
                        stop=stop_after_attempt(settings.http_max_retries),
                        wait=wait_exponential_jitter(
                            initial=settings.http_retry_base,
                            max=settings.http_retry_max,
                        ),
                        before_sleep=_log_http_retry,
                        reraise=True,
                    ):
                        with attempt:
                            hop = record_hop()
                            span.add_span_attributes(**{
                                "aef.hops_used": hop,
                                "aef.http_attempt": attempt.retry_state.attempt_number,
                            })
                            response = await client.post(
                                f"/api/v1/imap/mark-read",
                                json=payload,
                                headers=trace_header_dict({"X-Account-ID": self.account_id}),
                            )
                            span.add_span_attributes(**{
                                "aef.http_status_code": response.status_code,
                                "aef.response_payload": safe_trace_json(_http_response_payload(response)),
                            })
                            if _retryable_status(response.status_code):
                                response.raise_for_status()
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
