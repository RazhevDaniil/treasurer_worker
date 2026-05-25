import asyncio
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from langgraph.checkpoint.memory import MemorySaver
from pydantic import BaseModel

from .agents.graph import get_thread_state, process_message, resume_with_message
from .core import startup_checkup
from .core.config import settings
from .core.tracing import (
    aef_agent_start,
    aef_input_request,
    bind_x_trace_id,
    current_hops,
    get_aef_handler,
    init_tracing,
    reset_hops,
    safe_span_attributes,
    safe_span_output,
    safe_span_response,
    safe_trace_json,
    session_id_cvar,
    trace_header_dict,
)
from .services.kafka_consumer import ApproveTaskConsumer
from .services.kafka_producer import AgentResultProducer
from .services.mail_consumer import MailIncomingConsumer

logger = logging.getLogger(__name__)

checkpointer = MemorySaver()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise AEF tracing first so synthetic startup probes are also
    captured. Then start the Kafka consumer for approve_app integration,
    run SECURITY §19 readiness probes, and spawn the background re-check
    loop. Probe failures do NOT abort startup — the server stays up
    serving /health=200 while the loop waits for dependencies to recover.
    """
    init_tracing()
    logger.info("agent_app_starting")

    result_producer = AgentResultProducer()
    approve_consumer = ApproveTaskConsumer(result_producer)
    mail_consumer = MailIncomingConsumer(checkpointer=checkpointer)

    t_approve = threading.Thread(
        target=approve_consumer.run, name="approve-kafka-consumer", daemon=True
    )
    t_approve.start()

    t_mail = threading.Thread(
        target=mail_consumer.run, name="mail-kafka-consumer", daemon=True
    )
    t_mail.start()

    ok, failures = await startup_checkup.run_checks(source="startup")
    if ok:
        logger.info("agent_app_ready")
    else:
        logger.warning(f"agent_app_started_not_ready. failures={failures}")

    recheck_task = asyncio.create_task(
        startup_checkup.recheck_loop(), name="readiness-recheck"
    )

    logger.info("agent_app_started")

    yield

    logger.info("agent_app_shutting_down")
    recheck_task.cancel()
    try:
        await recheck_task
    except asyncio.CancelledError:
        pass
    approve_consumer.close()
    mail_consumer.close()
    result_producer.close()
    logger.info("agent_app_stopped")


app = FastAPI(title="Deal Agent API", lifespan=lifespan)


class ChatRequest(BaseModel):
    message: str
    chat_id: str


class DealInfo(BaseModel):
    deal_number: int
    product: str | None = None
    volume: float | None = None
    currency: str | None = None
    term_days: int | None = None
    rate: float | None = None
    inn: str | None = None


class ChatResponse(BaseModel):
    answer: str
    destination: str
    deals: list[DealInfo] = []
    thread_id: str | None = None
    manager_email: str | None = None


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe — always 200 while the process is up."""
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    """Readiness probe — 200 only after startup checks pass.

    503 carries the latest failure list so operators (and `kubectl describe`)
    see which dependency is keeping the pod out of rotation."""
    if startup_checkup.is_ready():
        return {"status": "ready"}
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "failures": startup_checkup.get_failures()},
    )


def _chat_input_body(req: ChatRequest) -> dict:
    """Executable request JSON stored in the operation trace."""
    return {
        "chat_id": req.chat_id,
        "message": req.message,
        "message_len": len(req.message or ""),
    }


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, request: Request, response: Response) -> ChatResponse:
    """Process a user message through the deal agent graph.

    `x-trace-id` is the stable cross-service UID for the whole operation.
    If the caller did not pass a valid UUID v4, the agent creates one and
    returns it in the response header."""
    x_trace_id = bind_x_trace_id(headers=request.headers)
    reset_hops()
    response.headers["x-trace-id"] = x_trace_id
    session_id_cvar.set(req.chat_id)
    logger.info(
        f"chat_request. chat_id={req.chat_id} x_trace_id={x_trace_id} "
        f"message={req.message[:120]!r}"
    )

    input_body = {**_chat_input_body(req), "x_trace_id": x_trace_id}

    with aef_input_request(
        span_name="chat",
        headers=trace_header_dict(dict(request.headers)),
        body=input_body,
        path="/chat",
        method="POST",
    ) as input_req:
        with aef_agent_start(input=input_body) as agent_span:
            safe_span_attributes(agent_span, **{
                "aef.agent_uid": settings.aef_agent_id,
                "aef.agent_name": settings.aef_agent_id,
                "aef.session_id": req.chat_id,
                "aef.ttl": settings.operation_ttl_sec,
                "aef.hops": settings.operation_max_hops,
                "aef.hops_used": current_hops(),
                "aef.stop_event": None,
                "aef.x_trace_id": x_trace_id,
                "aef.operation_uid": x_trace_id,
                "aef.parent_operation_uid": x_trace_id,
                "aef.executable_json": safe_trace_json(input_body),
            })

            chat_response = await _dispatch_chat(req, agent_span)

            safe_span_attributes(agent_span, **{
                "aef.hops_used": current_hops(),
                "aef.result_payload": safe_trace_json(chat_response.model_dump()),
            })
            safe_span_output(agent_span, chat_response.model_dump())

        safe_span_response(
            input_req,
            headers=trace_header_dict(),
            body=chat_response.model_dump(),
            http_code=200,
        )
        return chat_response


async def _dispatch_chat(req: ChatRequest, agent_span) -> ChatResponse:
    """Resolve the right graph entry point, run it under the TTL, and
    surface the §21 `stop_event` on the parent `agent_start` span."""
    callbacks = [get_aef_handler()]

    try:
        existing = await get_thread_state(req.chat_id, checkpointer)
        if existing is not None and existing.awaiting_reply:
            coro = resume_with_message(
                message=req.message,
                thread_id=req.chat_id,
                checkpointer=checkpointer,
                callbacks=callbacks,
            )
        else:
            coro = process_message(
                message=req.message,
                checkpointer=checkpointer,
                thread_id=req.chat_id,
                callbacks=callbacks,
            )
        final_state = await asyncio.wait_for(coro, timeout=settings.operation_ttl_sec)
    except asyncio.TimeoutError:
        logger.error(
            f"operation_ttl_exceeded. chat_id={req.chat_id} "
            f"ttl_sec={settings.operation_ttl_sec}"
        )
        safe_span_attributes(agent_span, **{
            "aef.stop_event": "ttl_exceeded",
            "aef.hops_used": current_hops(),
        })
        return ChatResponse(
            answer=settings.operation_ttl_user_message,
            destination="error",
        )
    except Exception as e:
        logger.error(f"process_failed. chat_id={req.chat_id} error={e}")
        safe_span_attributes(agent_span, **{
            "aef.stop_event": "process_failed",
            "aef.hops_used": current_hops(),
        })
        return ChatResponse(
            answer="Произошла ошибка при обработке сообщения. Попробуйте позже.",
            destination="error",
        )

    answer = final_state.response_message or "Не удалось сформировать ответ."
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
        safe_span_attributes(agent_span, **{
            "aef.stop_event": final_state.stop_event or "phase_error",
            "aef.hops_used": current_hops(),
        })

    logger.info(
        f"chat_response. chat_id={req.chat_id} destination={destination} "
        f"phase={final_state.phase} deals={len(final_state.deals)}"
    )

    deals_info = [
        DealInfo(
            deal_number=d.deal_number,
            product=d.conditions.product,
            volume=d.conditions.volume,
            currency=d.conditions.currency,
            term_days=d.conditions.term_days,
            rate=d.conditions.rate,
            inn=d.conditions.inn,
        )
        for d in final_state.deals
    ]

    return ChatResponse(
        answer=answer,
        destination=destination,
        deals=deals_info,
        thread_id=req.chat_id,
        manager_email=manager_email,
    )
