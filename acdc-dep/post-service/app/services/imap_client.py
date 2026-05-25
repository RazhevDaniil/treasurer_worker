from __future__ import annotations

import imaplib
import ssl
import logging
from dataclasses import dataclass
from typing import Iterable, Tuple

from ..config import Settings

logger = logging.getLogger("services.imap_client")


@dataclass(frozen=True)
class ImapMessageEnvelope:
    uid: int
    uidvalidity: int
    raw_bytes: bytes


class ImapAuthError(RuntimeError):
    pass


class ImapClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _connect(self) -> imaplib.IMAP4_SSL:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        client = imaplib.IMAP4_SSL(self.settings.imap_host, self.settings.imap_port, ssl_context=ctx)
        logger.info(f"Connected to {self.settings.imap_host}, {self.settings.imap_port}, {self.settings.username} - {self.settings.password_value}")
        client.login(self.settings.username, self.settings.password_value)
        return client

    def fetch_unseen(self) -> Iterable[ImapMessageEnvelope]:
        """
        Возвращает набор непрочитанных писем как (uid, uidvalidity, raw_bytes).
        """
        client = None
        try:
            client = self._connect()
            typ, _ = client.select(self.settings.imap_mailbox, readonly=False)
            if typ != "OK":
                raise RuntimeError(f"IMAP select failed: {typ}")

            # UIDVALIDITY
            uidvalidity = int(client.response("UIDVALIDITY")[1][0])  # type: ignore

            # Search unseen by UID
            typ, data = client.uid("search", None, "UNSEEN")
            if typ != "OK":
                return []

            uids = [int(x) for x in (data[0] or b"").split() if x]
            for uid in uids:
                typ, msg_data = client.uid("fetch", str(uid), "(RFC822)")
                if typ != "OK" or not msg_data or not msg_data[0]:
                    continue
                raw = msg_data[0][1]
                if not raw:
                    continue
                yield ImapMessageEnvelope(uid=uid, uidvalidity=uidvalidity, raw_bytes=raw)

        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    pass

    def mark_seen(self, uid: int) -> None:
        client = None
        try:
            client = self._connect()
            client.select(self.settings.imap_mailbox, readonly=False)
            # Mark as \Seen
            client.uid("store", str(uid), "+FLAGS", r"(\Seen)")
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    pass
