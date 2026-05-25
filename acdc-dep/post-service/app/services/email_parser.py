from __future__ import annotations

import re
from dataclasses import dataclass
from email import policy
from email.message import Message
from email.parser import BytesParser
from email.header import decode_header
from email.utils import parseaddr, getaddresses
from html.parser import HTMLParser
from typing import Optional


class _TextExtractor(HTMLParser):
    """Extract visible text from HTML, stopping at reply/forward delimiters."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._skip = False
        self._stop = False

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if self._stop:
            return
        attrs_dict = dict(attrs)
        if tag in ("script", "style", "head"):
            self._skip = True
        elif self._is_reply_delimiter(tag, attrs_dict):
            self._stop = True

    def handle_endtag(self, tag: str) -> None:
        if self._stop:
            return
        if tag in ("script", "style", "head"):
            self._skip = False
        if tag in ("p", "br", "div", "li", "tr"):
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip or self._stop:
            return
        self._parts.append(data)

    def text(self) -> str:
        return "".join(self._parts).strip()

    @staticmethod
    def _is_reply_delimiter(tag: str, attrs: dict) -> bool:
        if tag == "div":
            aid = attrs.get("id", "")
            aclass = attrs.get("class", "")
            if aid == "divRplyFwdMsg":
                return True
            if "gmail_quote" in aclass:
                return True
        if tag == "blockquote":
            atype = attrs.get("type", "")
            aclass = attrs.get("class", "")
            if atype == "cite" or "quote" in aclass:
                return True
        return False


@dataclass
class ParsedEmail:
    message_id: Optional[str]
    in_reply_to: Optional[str]
    references: Optional[str]
    subject: Optional[str]

    sender_name: Optional[str]
    sender_email: Optional[str]

    recipient_email: Optional[str]
    reply_to_email: Optional[str]

    body_text: Optional[str]
    body_html: Optional[str]


def _decode_mime_header(value: str | None) -> str | None:
    if not value:
        return None
    parts = decode_header(value)
    out: list[str] = []
    for content, enc in parts:
        if isinstance(content, bytes):
            out.append(content.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(str(content))
    return "".join(out).strip() or None


def _extract_text_and_html(msg: Message) -> tuple[str | None, str | None]:
    text_part = None
    html_part = None

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disp:
                continue

            payload = part.get_payload(decode=True)
            if payload is None:
                continue

            charset = part.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except Exception:
                decoded = payload.decode("utf-8", errors="replace")

            if ctype == "text/plain" and text_part is None:
                text_part = decoded
            elif ctype == "text/html" and html_part is None:
                html_part = decoded
    else:
        payload = msg.get_payload(decode=True)
        if payload is not None:
            charset = msg.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except Exception:
                decoded = payload.decode("utf-8", errors="replace")

            ctype = msg.get_content_type()
            if ctype == "text/plain":
                text_part = decoded
            elif ctype == "text/html":
                html_part = decoded
            else:
                text_part = decoded

    # cleanup
    if text_part:
        text_part = text_part.strip()
    if html_part:
        html_part = html_part.strip()

    # fallback: extract text from HTML when no text/plain part
    if not text_part and html_part:
        extractor = _TextExtractor()
        extractor.feed(html_part)
        text_part = extractor.text() or None

    return text_part or None, html_part or None


def _clean_msgid(v: str | None) -> str | None:
    if not v:
        return None
    return v.strip()


def _compute_thread_root(message_id: str | None, in_reply_to: str | None, references: str | None) -> str | None:
    """
    Логика:
      - если References есть → берем самый первый msgid как root
      - иначе если In-Reply-To есть → он root
      - иначе message_id
    """
    if references:
        # вытащим все <...>
        msgids = re.findall(r"<[^>]+>", references)
        if msgids:
            return msgids[0].strip()
    if in_reply_to:
        return in_reply_to.strip()
    if message_id:
        return message_id.strip()
    return None


def parse_email_bytes(raw: bytes) -> tuple[ParsedEmail, str | None]:
    """
    Возвращает (ParsedEmail, thread_root_message_id)
    """
    msg = BytesParser(policy=policy.default).parsebytes(raw)

    subject = _decode_mime_header(msg.get("Subject"))
    message_id = _clean_msgid(msg.get("Message-ID"))
    in_reply_to = _clean_msgid(msg.get("In-Reply-To"))
    references = _decode_mime_header(msg.get("References"))  # может быть длинной строкой

    from_name, from_email = parseaddr(_decode_mime_header(msg.get("From")) or "")
    reply_to_name, reply_to_email = parseaddr(_decode_mime_header(msg.get("Reply-To")) or "")

    # recipient: берём первый адрес из To (можно расширить)
    to_raw = _decode_mime_header(msg.get("To")) or ""
    tos = getaddresses([to_raw])
    recipient_email = tos[0][1] if tos and tos[0][1] else None

    body_text, body_html = _extract_text_and_html(msg)

    parsed = ParsedEmail(
        message_id=message_id,
        in_reply_to=in_reply_to,
        references=references,
        subject=subject,
        sender_name=from_name or None,
        sender_email=from_email or None,
        recipient_email=recipient_email,
        reply_to_email=reply_to_email or None,
        body_text=body_text,
        body_html=body_html,
    )
    thread_root = _compute_thread_root(message_id, in_reply_to, references)
    return parsed, thread_root