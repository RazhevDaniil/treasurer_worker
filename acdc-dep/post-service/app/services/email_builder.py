from __future__ import annotations

from email.message import EmailMessage


def build_reply_email(
    from_addr: str,
    to_addr: str,
    subject: str,
    body_text: str,
    original_message_id: str | None,
    original_references: str | None,
    cc_addrs: list[str] | None = None,
) -> EmailMessage:
    """
    Формирует корректный email reply:
      - In-Reply-To
      - References (original + original msgid)
      - Cc (если задан)
    """
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg["Subject"] = subject
    msg.set_content(body_text or "")

    if original_message_id:
        msg["In-Reply-To"] = original_message_id
        if original_references:
            msg["References"] = f"{original_references} {original_message_id}".strip()
        else:
            msg["References"] = original_message_id

    return msg
