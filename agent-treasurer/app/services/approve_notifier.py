"""Best-effort notification of the treasury manager when an approve task is rejected.

Operates on Kafka-driven approve tasks where we don't have a graph state — only
raw parameters from approve_app and the rejection reason from approve_service.
"""

import logging
from typing import Any

from ..core.config import settings
from .email_service import EmailService

logger = logging.getLogger(__name__)


class ApproveNotifier:
    """Sends rejection notifications for Kafka-driven approve tasks.

    Best-effort: send failure never raises — manager notification must not block
    the result publication back to approve_app.
    """

    CURRENCY_SYMBOL = {
        "RUB": "₽",
        "CNY": "¥",
        "INR": "₹",
    }

    # (parameters key, russian label) — fields shown in the rejection notification
    PARAMETER_FIELDS_RU: list[tuple[str, str]] = [
        ("product", "Продукт"),
        ("inn", "ИНН"),
        ("currency", "Валюта"),
        ("volume", "Объём"),
        ("term_days", "Срок (дн.)"),
        ("rate", "Ставка (%)"),
        ("rate_type", "Тип ставки"),
        ("basis", "Базис"),
        ("optionality", "Опциональность"),
    ]

    def __init__(self, recipient: str | None = None) -> None:
        self._recipient = recipient or settings.default_employee_email

    async def notify_rejection(
        self,
        *,
        task_id: str,
        calculation_id: str,
        parameters: dict[str, Any],
        reason: str,
    ) -> bool:
        """Send a rejection notification to the treasury manager.

        Returns True on success, False on any send failure. Never raises.
        """
        subject = self._build_subject(task_id, parameters)
        body = self._build_body(task_id, calculation_id, parameters, reason)

        logger.info(
            f"approve_notify_sending. task_id={task_id} "
            f"calculation_id={calculation_id} recipient={self._recipient}"
        )

        email_service = EmailService()
        try:
            sent = await email_service.send_email(
                to=self._recipient,
                subject=subject,
                body=body,
                idempotency_key=f"approve-rejection:{task_id}",
            )
        except Exception as exc:
            logger.error(
                f"approve_notify_unexpected_error. task_id={task_id} "
                f"calculation_id={calculation_id} recipient={self._recipient} "
                f"exc_type={type(exc).__name__} exc={exc}"
            )
            return False
        finally:
            await email_service.close()

        if not sent:
            logger.error(
                f"approve_notify_send_failed. task_id={task_id} "
                f"calculation_id={calculation_id} recipient={self._recipient}"
            )
            return False

        logger.info(
            f"approve_notify_ok. task_id={task_id} "
            f"calculation_id={calculation_id} recipient={self._recipient}"
        )
        return True

    def _build_subject(self, task_id: str, parameters: dict[str, Any]) -> str:
        inn = parameters.get("inn")
        if inn:
            return f"[Отказ approve] Задача {task_id} — ИНН {inn}"
        return f"[Отказ approve] Задача {task_id}"

    def _build_body(
        self,
        task_id: str,
        calculation_id: str,
        parameters: dict[str, Any],
        reason: str,
    ) -> str:
        lines: list[str] = ["Добрый день!", ""]
        lines.append(
            "Approve-задача отклонена автоматической проверкой. "
            "Требуется ручная работа Казначейства."
        )
        lines.append("")
        lines.append(f"task_id: {task_id}")
        lines.append(f"calculation_id: {calculation_id}")
        lines.append("")
        lines.append("Параметры сделки:")
        lines.append(self._format_parameters(parameters))
        lines.append("")
        lines.append(f"Причина отказа: {reason}")
        lines.append("")
        lines.append("—")
        lines.append("Автоматическое уведомление — Казначейство")
        return "\n".join(lines)

    def _format_parameters(self, parameters: dict[str, Any]) -> str:
        currency = parameters.get("currency")
        sym = self.CURRENCY_SYMBOL.get(currency, currency) if currency else None

        rows: list[str] = []
        for field, label in self.PARAMETER_FIELDS_RU:
            value = parameters.get(field)
            if value is None:
                continue
            if field == "volume" and sym is not None:
                try:
                    rows.append(f"  {label}: {float(value):,.0f} {sym}")
                    continue
                except (TypeError, ValueError):
                    pass
            rows.append(f"  {label}: {value}")

        if not rows:
            return "  (нет извлекаемых параметров)"
        return "\n".join(rows)
