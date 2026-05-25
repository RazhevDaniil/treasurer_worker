from __future__ import annotations

import logging
import smtplib
import ssl
import socket
from dataclasses import dataclass
from email.message import EmailMessage

from ..config import Settings

logger = logging.getLogger("services.smtp_client")


class SmtpPermanentError(RuntimeError):
    pass


@dataclass
class SmtpSendResult:
    ok: bool
    smtp_code: int | None = None
    smtp_response: str | None = None


class SmtpClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def send_message_starttls(self, msg: EmailMessage) -> SmtpSendResult:
        """
        Одна попытка отправки:
          - port 25
          - EHLO
          - STARTTLS
          - LOGIN
          - send_message
        """
        try:
            with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=30) as server:
                server.ehlo()
                if not server.has_extn("starttls"):
                    raise SmtpPermanentError("SMTP сервер не поддерживает STARTTLS (по требованиям TLS обязателен)")

                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                server.starttls(context=ctx)
                server.ehlo()

                server.login(self.settings.username, self.settings.password_value)

                server.send_message(msg)
                return SmtpSendResult(ok=True)

        except smtplib.SMTPResponseException as e:
            # e.smtp_code / e.smtp_error
            try:
                resp = e.smtp_error.decode("utf-8", errors="replace") if isinstance(e.smtp_error, (bytes, bytearray)) else str(e.smtp_error)
            except Exception:
                resp = str(e.smtp_error)
            return SmtpSendResult(ok=False, smtp_code=int(e.smtp_code), smtp_response=resp)

        except (EOFError, OSError, socket.error) as e:
            # network / EOF -> caller должен retry + “перерезолвить” (новое подключение = новый DNS resolve)
            raise e
