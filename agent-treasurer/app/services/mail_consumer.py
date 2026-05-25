"""Kafka consumer for KAFKA_MAIL_TOPIC — receives incoming emails from mail_app.

Зеркалит ApproveTaskConsumer (kafka_consumer.py), но для канала переговоров
по сделкам: парсит входящее письмо → гоняет LangGraph через
`process_message` / `resume_with_message` → собирает reply и отправляет его
обратно в mail_app синхронным HTTP POST /api/v1/send_reply.

`process_message` и `resume_with_message` импортируются из `agents.graph` —
тот же entry-point, что и у HTTP `/chat`. Сам checkpointer передаётся
снаружи (из `app.py`), чтобы не плодить копий MemorySaver.
"""

import asyncio
import json
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException
from langgraph.checkpoint.memory import MemorySaver

from ..agents.graph import get_thread_state, process_message, resume_with_message
from ..core.config import settings
from ..core.tracing import (
    aef_agent_start,
    aef_kafka_consume,
    get_aef_handler,
    session_id_cvar,
)
from .email_service import EmailService

logger = logging.getLogger(__name__)


class MailIncomingConsumer:
    """Consumes KAFKA_MAIL_TOPIC, runs the deal graph, replies via mail_app."""

    def __init__(
        self,
        checkpointer: MemorySaver,
        email_service: EmailService | None = None,
    ) -> None:
        self._checkpointer = checkpointer
        self._email_service = email_service or EmailService()
        self._running = False

        self._consumer = Consumer({
            "bootstrap.servers": settings.adapter_brokers,
            "group.id": settings.kafka_mail_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def close(self) -> None:
        self._running = False
        self._consumer.close()

    def run(self) -> None:
        self._consumer.subscribe([settings.kafka_mail_topic])
        self._running = True
        logger.info(
            f"mail_consumer_started. topic={settings.kafka_mail_topic} "
            f"group={settings.kafka_mail_group_id}"
        )

        while self._running:
            msg = self._consumer.poll(timeout=settings.kafka_poll_timeout_seconds)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"mail_consumer_error. error={msg.error()}")
                raise KafkaException(msg.error())

            self._handle_message(msg)

    def _handle_message(self, msg) -> None:
        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.error(f"mail_parse_error. error={exc} raw={msg.value()!r}")
            self._consumer.commit(message=msg)
            return

        thread_id = payload.get("thread_id") or ""
        message_id = payload.get("message_id") or ""
        # mail_app публикует тело в `body_text` (см. in_memory_db.ingest_message),
        # но исторически тут читалось `body`; принимаем оба ключа.
        message_text = payload.get("body_text") or payload.get("body") or ""

        if not thread_id or not message_text:
            logger.warning(
                f"mail_skip_empty. thread_id={thread_id!r} "
                f"message_id={message_id!r} body_len={len(message_text)}"
            )
            self._consumer.commit(message=msg)
            return

        # thread_id = LangGraph chat_id = AEF session_id (общий ключ корреляции
        # на всём цикле переговоров по треду).
        session_id_cvar.set(thread_id)

        with aef_kafka_consume(
            span_name="consume_mail_incoming",
            headers=dict(msg.headers() or []),
            body=msg.value(),
            topic=settings.kafka_mail_topic,
            kafka_cluster=settings.kafka_cluster_name,
            bootstrap_servers=settings.adapter_brokers.split(","),
            consumer_group=settings.kafka_mail_group_id,
        ):
            with aef_agent_start(input={
                "thread_id": thread_id,
                "message_id": message_id,
                "message_len": len(message_text),
            }) as agent_span:
                agent_span.add_span_attributes(**{
                    "aef.agent_uid": settings.aef_agent_id,
                    "aef.session_id": thread_id,
                    "aef.ttl": settings.operation_ttl_sec,
                    "aef.hops": None,
                    "aef.stop_event": None,
                })

                logger.info(
                    f"mail_message_received. thread_id={thread_id} "
                    f"message_id={message_id}"
                )

                try:
                    result = asyncio.run(self._process_message_async(
                        message_text=message_text,
                        thread_id=thread_id,
                        payload=payload,
                        agent_span=agent_span,
                    ))
                except Exception as exc:
                    logger.exception(
                        f"mail_process_failed. thread_id={thread_id} "
                        f"message_id={message_id} error={exc}"
                    )
                    agent_span.add_span_attributes(**{"aef.stop_event": "process_failed"})
                    # Коммитим, чтобы не зацикливаться на яде; ошибка уже в логах.
                    self._consumer.commit(message=msg)
                    return

                if result.get("sent") is False:
                    logger.error(
                        f"mail_reply_not_queued. thread_id={thread_id} "
                        f"message_id={message_id} destination={result.get('destination')}"
                    )
                    agent_span.add_span_attributes(**{"aef.stop_event": "mail_reply_not_queued"})
                    agent_span.add_output_result(output=result)
                    return

                self._consumer.commit(message=msg)
                agent_span.add_output_result(output=result)

                logger.info(
                    f"mail_message_processed. thread_id={thread_id} "
                    f"message_id={message_id} destination={result.get('destination')}"
                )

    async def _process_message_async(
        self,
        *,
        message_text: str,
        thread_id: str,
        payload: dict,
        agent_span,
    ) -> dict:
        """Run the deal graph and, on success, send the reply via mail_app.

        Возвращает короткий dict для записи в AEF output_result; сам ответ
        пользователю уходит через EmailService (HTTP POST /send_reply).
        """
        callbacks = [get_aef_handler()]
        message_id = payload.get("message_id") or ""

        try:
            existing = await get_thread_state(thread_id, self._checkpointer)
            if (
                message_id
                and existing is not None
                and existing.last_processed_message_id == message_id
                and existing.response_message
            ):
                logger.info(
                    f"mail_duplicate_message_reusing_response. "
                    f"thread_id={thread_id} message_id={message_id}"
                )
                final_state = existing
            elif existing is not None and existing.awaiting_reply:
                coro = resume_with_message(
                    message=message_text,
                    thread_id=thread_id,
                    checkpointer=self._checkpointer,
                    incoming_message_id=message_id or None,
                    callbacks=callbacks,
                )
                final_state = await asyncio.wait_for(coro, timeout=settings.operation_ttl_sec)
            else:
                coro = process_message(
                    message=message_text,
                    checkpointer=self._checkpointer,
                    thread_id=thread_id,
                    incoming_message_id=message_id or None,
                    callbacks=callbacks,
                )
                final_state = await asyncio.wait_for(coro, timeout=settings.operation_ttl_sec)
        except asyncio.TimeoutError:
            logger.error(
                f"mail_operation_ttl_exceeded. thread_id={thread_id} "
                f"ttl_sec={settings.operation_ttl_sec}"
            )
            agent_span.add_span_attributes(**{"aef.stop_event": "ttl_exceeded"})
            return {"destination": "error", "reason": "ttl_exceeded"}

        answer = final_state.response_message
        if not answer:
            logger.warning(f"mail_no_answer. thread_id={thread_id}")
            agent_span.add_span_attributes(**{"aef.stop_event": "no_answer"})
            return {"destination": "error", "reason": "no_answer"}

        destination = "agent"
        manager_email: str | None = None
        if final_state.escalation_requested:
            destination = "escalation"
            manager_email = next(
                (d.assigned_employee for d in final_state.deals if d.assigned_employee),
                settings.default_employee_email,
            )
        elif final_state.phase == "error":
            destination = "error"
            agent_span.add_span_attributes(**{"aef.stop_event": "phase_error"})

        # Сборка reply (логика, ранее жившая в mail_app._enqueue_reply).
        # db_app публикует поле `author_email`, in-memory stub из mail_app —
        # `sender_email`; принимаем оба.
        client_email = (
            payload.get("reply_to_email")
            or payload.get("author_email")
            or payload.get("sender_email")
            or ""
        )
        if not client_email:
            logger.error(f"mail_reply_no_recipient. thread_id={thread_id}")
            return {"destination": "error", "reason": "no_recipient"}

        if destination == "escalation" and manager_email:
            recipient_email = manager_email
            cc = [client_email]
        else:
            recipient_email = client_email
            cc = None

        subject_in = payload.get("subject") or ""
        reply_subject = f"Re: {subject_in}" if subject_in else "Re: (no subject)"
        in_reply_to = payload.get("message_id")

        sent = await self._email_service.send_email(
            to=recipient_email,
            subject=reply_subject,
            body=answer,
            in_reply_to=in_reply_to,
            thread_id=thread_id,
            cc=cc,
            idempotency_key=f"agent-reply:{thread_id}:{message_id}" if message_id else None,
        )

        return {
            "destination": destination,
            "recipient": recipient_email,
            "cc": cc,
            "sent": sent,
        }
