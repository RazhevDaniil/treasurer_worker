# agent_treasurer_app

LangGraph-агент для автоматической обработки сделок через текстовую переписку. Поддерживает многораундовые переговоры через **interrupt/resume**, распознавание нескольких сделок в одном сообщении и автоматическую эскалацию.

```text
UI / mail_app  ──HTTP──>  agent_treasurer_app  ──HTTP──>  agent_tools_app (расчёт ставок)
                               │
                               └──HTTP──>  mail_app (отправка писем)

approve_app   ──Kafka──>  agent_treasurer_app  ──Kafka──>  approve_app (одобрение сделок)
     AGENT_TASK_TOPIC          AGENT_RESULT_TOPIC
```

---

## Быстрый старт

```bash
cd agent_treasurer_app
pip install -r requirements.txt

# Запустить API-сервис
python main.py
```

Swagger UI: `http://localhost:8081/docs`

---

## Архитектура

### Граф обработки

```
receive → parse_message → process_deals → check_thread → compose_response → send_response → END
```

Обработка ошибок: любая нода до `compose_response` при ошибке маршрутизируется в `compose_response`, чтобы пользователь всегда получил ответ. Если после `compose_response` фаза `phase == "error"` — переход сразу в `END` (минуя `send_response`). После `send_response` граф вызывает **INTERRUPT** (при наличии checkpointer) и ожидает следующего сообщения.

```
┌─────────────────────────────────────────────────────────────────────┐
│                            START                                     │
│                              │                                       │
│                              v                                       │
│                       ┌────────────┐                                 │
│                       │  receive   │  Валидация, started_at          │
│                       └─────┬──────┘                                 │
│                             │                                        │
│                             v                                        │
│                      ┌─────────────┐                                 │
│                      │parse_message│  Разбиение на фрагменты,        │
│                      └─────┬───────┘  DealUpdate по каждому (LLM)    │
│                            │                                         │
│                            v                                         │
│                     ┌──────────────┐                                  │
│                     │process_deals │  Диспетчеризация по интентам     │
│                     └──────┬───────┘                                  │
│                            │                                         │
│                            v                                         │
│                     ┌──────────────┐                                  │
│                     │ check_thread │  Флаги: lock, awaiting, complete │
│                     └──────┬───────┘                                  │
│                            │                                         │
│                            v                                         │
│                   ┌────────────────┐                                  │
│                   │compose_response│  Сборка ответа из deal_results   │
│                   └───────┬────────┘                                  │
│                           │                                          │
│               ┌───────────┴──────────┐                               │
│               │ phase == "error"?    │                                │
│           yes │                      │ no                             │
│               v                      v                                │
│            ┌─────┐         ┌───────────────┐                         │
│            │ END │         │ send_response │  Сохранение, INTERRUPT   │
│            └─────┘         └───────┬───────┘                         │
│                                    │                                  │
│                                    v                                  │
│                                 ┌─────┐                               │
│                                 │ END │                               │
│                                 └─────┘                               │
└─────────────────────────────────────────────────────────────────────┘

Resume flow:
  resume_with_message(new_msg) → receive → parse_message → ...
```

При эскалации отдельное письмо менеджеру **не отправляется**: ответ из `compose_response` уходит ему в `To`, а контрагент ставится в `Cc` — менеджер получает весь тред целиком благодаря `In-Reply-To`/`References`. Маршрутизация выполняется в `mail_app/agent_dispatcher` по полю `manager_email` из `ChatResponse` (см. [mail_app/README.md](../mail_app/README.md)).

### Interrupt/resume цикл

```text
Сообщение 1 (запрос) → обработка → ответ → INTERRUPT (ждём ответа)
                                                 |
Сообщение 2 (торг)   → resume    → обработка → ответ → INTERRUPT
                                                 |
                                            [цикл повторяется]
                                                 |
Сообщение N (согласие) → resume   → одобрение → END
```

Состояние графа сохраняется между итерациями. Это позволяет вести многораундовые переговоры, сохранять полный контекст диалога и возобновлять обработку после сбоев.

---

## Ноды

| Нода | Описание |
|------|----------|
| **receive** | Валидирует `incoming_message`, устанавливает `started_at`, определяет `is_new_thread`. AEF SDK `trace_id` уже выставлен на уровне HTTP-обёртки `/chat` — см. [«Трейс операции»](#трейс-операции-security-18--20). |
| **parse_message** | Разбивает сообщение на фрагменты по сделкам (regex), извлекает `DealUpdate` по каждому фрагменту через LLM. Детектирует «свежий старт» (level 1 — эвристика, level 2 — LLM fallback) |
| **process_deals** | Для каждого `DealUpdate`: проверка обязательных полей → `validate_business_rules` (эскалация / suggestions / подмена) → диспетчеризация по интенту → `get_rate` для расчёта ставки |
| **check_thread** | Устанавливает флаги уровня треда: `thread_locked` (эскалация), `awaiting_reply`, фаза `completed` |
| **compose_response** | Собирает текст ответа из `deal_results`, при мультисделке — секции "Сделка N:" |
| **send_response** | Сохраняет ответ в `message_history`, очищает транзиентные поля, устанавливает `response_sent=True`, вызывает INTERRUPT |

---

## Трейс операции (SECURITY §18 + §20)

Сервис — AI-агент (LLM + LangGraph), трейсинг реализован через **AEF Tracing SDK** (`sber-aef-tracing`) — proto-сообщения уходят в Kafka ФП AEF Controller и визуализируются в UI АС AEF Manager. Полный план миграции и финальная сводка соответствия — [SECURITY_COMPLIANCE_SDK.md](../SECURITY_COMPLIANCE_SDK.md).

| ID | Жизненный цикл | Источник |
| --- | --- | --- |
| `trace_id` / `span_id` / `parent_span_id` | Один проход графа (= обработка одного входящего письма). Иерархия спанов выстраивается через nesting контекстных менеджеров. | AEF SDK генерирует автоматически на каждом `aef_input_request` / `aef_kafka_consume`. |
| `x-trace-id` | Сквозной UUID v4 основной бизнес-операции. Принимается из входящих HTTP/Kafka headers; если отсутствует или не UUID v4 — агент генерирует новый. | [app/core/tracing.py](app/core/tracing.py) хранит UID в contextvar и прокидывает его в HTTP headers / Kafka headers. |
| `session_id` | Стабильный за всю переписку (= `chat_id` для `/chat`, `task_id` для Kafka). | [app/app.py](app/app.py) / [app/services/kafka_consumer.py](app/services/kafka_consumer.py) — `session_id_cvar.set(...)` перед созданием span'ов. |
| `agent-id` / `cluster-id` / `namespace` / `distributive` | Статически на каждый span-batch. | Kafka-headers `AEFKafkaSender(headers={...})` из настроек [app/core/config.py](app/core/config.py). |

**Что эмитится в трейс:**

- **`input_request "chat"`** + вложенный **`agent_start`** — обёртка `/chat` в [app/app.py](app/app.py). В input пишется исполняемый JSON запроса (`chat_id`, `message`, `x_trace_id`), в response — возвращаемый результат. На `agent_start` спане проставлены `aef.agent_uid`, `aef.agent_name`, `aef.operation_uid`, `aef.parent_operation_uid`, `aef.ttl`, `aef.hops`, `aef.hops_used`, `aef.stop_event`, `aef.session_id`, `aef.x_trace_id` (§21).
- **Kafka entrypoints** — `consume_agent_task` для approve-flow и `consume_mail_incoming` для mail-flow обёрнуты в `aef_kafka_consume` + `agent_start` ([app/services/kafka_consumer.py](app/services/kafka_consumer.py), [app/services/mail_consumer.py](app/services/mail_consumer.py)). В трейс попадают входящий JSON, UID операции, TTL/hops/StopEvent и итоговый result payload.
- **LangGraph actions/state changes** — каждый узел графа (`receive`, `parse_message`, `process_deals`, `check_thread`, `compose_response`, `send_response`) обёрнут в `trace_action_span(...)` ([app/agents/graph.py](app/agents/graph.py)). В span сохраняются входное состояние, config, state-delta/result, признаки `aef.is_mutation` и `aef.rollback_possible`.
- **LLM calls** — извлечение условий и разбор ответов обёрнуты в `llm_call` spans вокруг реальных `llm.ainvoke(...)` ([app/agents/nodes/parse_message.py](app/agents/nodes/parse_message.py)); параллельно остаётся автоматический `AEFHandler` callback в `graph.ainvoke(config={"callbacks": [...]})`. В span пишутся prompt/messages, ответ LLM, hop на каждую попытку и `gigaplatform_stop_event` при отказе GP 403.
- **Service/API calls** — `agent_tools_app.get_rate`, `mail_app.send_reply`, `mail_app.fetch_new_emails`, `mail_app.mark_read` имеют `service_call` / `api_call` spans с request/response/result payload, HTTP status, hop-attempt, `is_mutation` и `rollback_possible` ([app/services/deal_service.py](app/services/deal_service.py), [app/services/email_service.py](app/services/email_service.py)).
- **Kafka produce** — `produce_agent_result` пишет request/result payload, `x-trace-id`, hop и признаки необратимой мутации при публикации результата approve-flow ([app/services/kafka_producer.py](app/services/kafka_producer.py)).
- **Safe serialization** — [app/core/tracing.py](app/core/tracing.py) содержит `safe_trace_payload` / `safe_trace_json` / `SafeTraceSpan`: сложные типы приводятся к JSON/строке, payload ограничен `TRACING_MAX_PAYLOAD_SIZE`, ошибки самого tracing-слоя логируются и не прерывают бизнес-flow.

**StopEvent (§21).** При TTL `asyncio.wait_for(timeout=settings.operation_ttl_sec)` на `agent_start` проставляется `aef.stop_event="ttl_exceeded"`; при `phase="error"` от графа — `"phase_error"`; при отказе GigaPlatform `403` с сообщением `The service is temporarily unavailable due to technical reasons.` — `"gigaplatform_stop_event"`. Во всех случаях возвращается контролируемый ответ.

**PreView GigaChat (§26).** `_pick_model()` в [app/core/llm.py](app/core/llm.py) per-call выбирает Main или PreView; `preview_ratio` валидируется как диапазон `0.0..0.05` (до 5% нагрузки). На каждом свежем `GigaChat(...)` подвешен `callbacks=[get_aef_handler()]` — SDK собирает `llm` span с фактической `model`. Выбор дополнительно логируется через stdlib `logging` (`gigachat_installation_picked. installation=... model=...`).

Cross-service propagation выполняется через `x-trace-id`: `/chat` возвращает его в response headers, HTTP-клиенты передают его в `agent_tools_app` / `mail_app`, Kafka producer публикует его в message headers.

---

## Надёжность / Retry (SECURITY §22 + §23)

Внешние LLM/HTTP вызовы проходят через общий инфраструктурный retry-слой с ограниченным количеством попыток и exponential jitter-backoff. Бизнес-сервисы и LangGraph nodes не реализуют собственные циклы повторов: HTTP использует [app/core/http_retry.py](app/core/http_retry.py), LLM — [app/core/llm_retry.py](app/core/llm_retry.py). Kafka producer использует встроенный bounded retry/backoff `confluent-kafka`.

| Слой | Где | Что ретраит | Параметры |
| --- | --- | --- | --- |
| **LLM** | [parse_message.py](app/agents/nodes/parse_message.py) → `llm_ainvoke_with_retry()` | HTTP `500/502/503/504`, `httpx.TimeoutException`, transport (`ConnectError` / `RemoteProtocolError` / `OSError`). `429`, прочие `4xx`, валидационные и бизнес-ошибки не ретраятся. | `llm_max_retries=3`, `llm_retry_base=0.5`, `llm_retry_max=5.0`, `llm_retry_exp_base=2.0`, `llm_retry_jitter=1.0` |
| **HTTP** | [deal_service.py](app/services/deal_service.py), [email_service.py](app/services/email_service.py), [startup_checkup.py](app/core/startup_checkup.py) → `request_with_retry()` | `httpx.RequestError` и HTTP `500/502/503/504`. `408`, `429`, прочие `4xx` не ретраятся и передаются бизнес-обработчику. | `http_max_retries=3`, `http_retry_base=0.5`, `http_retry_max=5.0`, `http_retry_exp_base=2.0`, `http_retry_jitter=1.0` |
| **Kafka producer** | [kafka_producer.py](app/services/kafka_producer.py) | Транспортные сбои confluent-kafka | `enable.idempotence=true`, `acks=all`, `retries=5`, `retry.backoff.ms=200..5000` |

**Типизированные события при деградации GigaChat.** Классификатор `classify_gigachat_error()` ([app/core/llm_retry.py](app/core/llm_retry.py)) отображает любое исключение по `status_code` / `response.status_code` / типу:

| Класс ошибки | Событие | Ретраится? |
| --- | --- | --- |
| HTTP 429 | `gigachat_rate_limited` | нет |
| HTTP 500/502/503/504 | `gigachat_5xx_failed` | да |
| прочие HTTP 5xx | `gigachat_response_error` | нет |
| HTTP 403 + GigaPlatform stop message | `gigaplatform_stop_event` | нет |
| `httpx.TimeoutException` / `asyncio.TimeoutError` | `gigachat_timeout` | да |
| `httpx.ConnectError` / `RemoteProtocolError` / `OSError` | `gigachat_transport_error` | да |
| HTTP 4xx (включая 408) | `gigachat_response_error` | нет |
| прочее | `gigachat_unknown_error` | нет |

На каждой попытке инфраструктурный слой пишет start/success/error, а `before_sleep` фиксирует `attempt`, `next_wait_sec`, `exc_type`, `will_retry=True`. После исчерпания `log_llm_exhausted()` пишет событие с `will_retry=False` и пробрасывает исходное исключение в наружный `except` — он собирает `warnings` и продолжает с пустыми `conditions`. Если все фрагменты упали — узел возвращает `phase="error"`, граф уходит в `compose_response` со шаблонным извинением, `mail_app` получает контролируемый ответ.

В Loki/OpenSearch: `event=gigachat_*` группируется → метрика «доля 429 vs 5xx vs timeout»; `will_retry=true/false` → доля исчерпаний. В UI AEF Manager Traces разбор цепочки ретраев по конкретному запросу — фильтр `session_id=<chat_id>` поднимает все спаны одного `/chat` вызова, включая внутренние повторы LLM/HTTP.

---

## DealUpdate интенты

| Интент | Описание |
|--------|----------|
| `new` | Новая сделка |
| `negotiate` | Клиент не согласен с предложением или хочет изменить условия |
| `approve` | Клиент согласен с текущими условиями |
| `provide_data` | Предоставление недостающих данных (ИНН, срок и т.д.) |
| `escalate` | Клиент явно просит перевести на сотрудника/менеджера |

---

## Валидация бизнес-правил

`DealConditions.validate_business_rules()` возвращает `ValidationResult` с per-currency лимитами. Три типа реакции:

### Лимиты по валютам

| Валюта + тип | Срок | Объём | Вне диапазона |
|---|---|---|---|
| RUB FIX | 1–1096 | 0.5–15 млрд ₽ | эскалация |
| RUB FLOAT | 60–366 | 0.5–15 млрд ₽ | срок ≤59 или объём <0.5B → предложить FIX; срок >366 → эскалация |
| CNY | 1–732 | 30K–500M ¥ | срок/объём вверх → эскалация; объём <30K → рекомендация |
| INR | 1–366 | 1M–<5000M ₹ | срок/объём вверх → эскалация; объём <1M → рекомендация |
| OTHER | — | — | эскалация |

### Типы реакции

| Реакция | Когда | Что происходит |
|---|---|---|
| **Эскалация** | Жёсткое нарушение лимитов, неподдерживаемая валюта | `_escalate_deal()` — тред блокируется, на сделку проставляется `assigned_employee`. Клиент видит причины и email менеджера в ответном письме. На уровне `mail_app` это же письмо адресуется менеджеру (`To`), клиент остаётся в `Cc` — менеджер берёт тред дальше и видит всю переписку через `In-Reply-To`/`References` |
| **Info-only suggestion** | Объём ниже минимума (CNY <30K, INR <1M), невалидный формат ИНН | Текст рекомендации возвращается клиенту сразу, без расчёта ставки. Сделка остаётся в `NEGOTIATING` |
| **Suggestion + подмена** | RUB FLOAT с коротким сроком/малым объёмом | `rate_type` подменяется на FIX, ставка рассчитывается по FIX, в ответ добавляется пояснение |

### Порядок проверок в process_deals

1. **Обязательные поля** (`inn`, `term_days`, `volume`) — если отсутствуют, запрашиваются у клиента
2. **`validate_business_rules`** — per-currency лимиты, формат ИНН → эскалация / suggestion / подмена
3. **`DealService.get_rate`** — расчёт ставки через внешний сервис

### Приписка для FLOAT

При успешном расчёте плавающей ставки к ответу добавляется:
> Ставка считается как КС + спред. Если интересует расчёт от других финансовых показателей — обратитесь к менеджеру.

### Лестница ставок (rate ladder)

При первом запросе ставки `agent_tools_app` возвращает отсортированный по возрастанию список `list[tuple[str, float]]` — до трёх источников для RUB (`calc_rate`, `hist_rate`, `model_rate`), один для CNY/INR (`calc_rate`). Этот список сохраняется в `Deal.rate_ladder`.

**Логика торга:**

| Ситуация | Что происходит |
|----------|----------------|
| Первый запрос / структурные параметры изменились | Запрос нового `rate_ladder`, position=0, предложить первую (минимальную) ставку |
| Клиент недоволен (нет новых условий или просит больше) | `rate_ladder_position += 1`, предложить следующую ставку |
| Клиент просит ставку ≤ текущей позиции в лестнице | Дать запрошенную ставку (выгодна нам), позицию **не продвигать** |
| Клиент указал конкретную ставку > текущей | Продвинуть позицию, предложить `min(запрос, ставка_лестницы)` |
| Ставки исчерпаны (`position >= len(ladder)`) | Автоматическая эскалация |
| CNY/INR — 1 ставка, 1 отказ | Эскалация сразу |

**Структурные параметры** (сброс лестницы при изменении): `product`, `inn`, `term_days`, `volume`, `currency`, `rate_type`, `basis`, `optionality`. Изменение `rate` — предмет торга, лестницу не сбрасывает.

**Определение изменений:** сравнение `update.conditions` с последним `OfferRecord.proposed` (а не с текущими conditions сделки, т.к. `parse_message` мержит conditions до `process_deals`).

`_accept_deal` проверяет: если `rate_ladder` пуст или ставка сделки превышает текущую позицию в лестнице — отправляет на повторные переговоры.

---

## Детектирование свежего старта

Двухуровневая система определения, что контрагент начинает новый диалог (а не продолжает старый):

- **Level 1** — эвристика (regex)
- **Level 2** — LLM fallback (при неуверенности эвристики)

---

## AgentState

```python
class AgentState(BaseModel):
    # === LLM ===
    messages: Annotated[list, add_messages]          # История диалога с LLM
    incoming_message: Optional[str]                   # Текст текущего входящего сообщения

    # === Сделки ===
    deals: list[Deal]                                 # Все сделки в треде
    deal_updates: list[DealUpdate]                    # Действия по сделкам из текущего сообщения
    deal_results: Optional[list[DealResult]]          # Транзиентные результаты обработки

    # === Тред ===
    is_new_thread: bool
    thread_locked: bool                               # Блокировка треда (эскалация)
    message_history: Annotated[list[str], append_to_list]  # Все тексты сообщений

    # === Workflow ===
    phase: Literal["receiving", "parsing", "processing",
                    "responding", "completed", "error"]
    response_message: Optional[str]
    response_sent: bool

    # === Эскалация ===
    escalation_requested: bool
    escalation_reason: Optional[str]
    counterparty_email: Optional[str]                 # Email контрагента (из config.configurable)

    # === Ошибки ===
    last_error: Optional[str]
    warnings: Annotated[list[str], append_to_list]
    error_diagnostics: Optional[str]

    # === Interrupt-флаги ===
    awaiting_reply: bool                              # Граф на паузе, ждём ответа
    requires_human_approval: bool                     # Требуется ручное одобрение
    approval_context: Optional[str]

    # === Метаданные ===
    started_at: datetime
```

---

## HTTP API

### `GET /health` и `GET /ready` (SECURITY §19)

- **`/health`** — liveness. Всегда `200 {"status":"ok"}` пока процесс жив.
- **`/ready`** — readiness. `200 {"status":"ready"}` только после того как все стартовые проверки прошли; иначе `503 {"status":"not_ready", "failures":[{check,error,exc_type}, ...]}`.

Определения проверок и оркестрация — [app/core/startup_checkup.py](app/core/startup_checkup.py). Запуск — внутри `lifespan` ДО приёма трафика, sequential + fail-fast, per-check timeout (см. `readiness_*_timeout_sec` в [app/core/config.py](app/core/config.py)):

| check | что проверяет |
| --- | --- |
| `gigachat` | синтетический промпт (`"Ответь одним словом: OK"`), валидация непустого `content` |
| `mail_app` | `GET {mail_server_api_url}/health/live`, 2xx |
| `agent_tools_app` | 3 синтетические сделки (RUB FIX / RUB FLOAT / CNY FIX) через `DealService.get_rate`, непустая ставочная лестница на каждую |

При провале — лог `startup_check_failed` с `check` / `error` / `exc_type`; сервер не падает (`/health=200`). Фоновая `recheck_loop` каждые `readiness_recheck_interval_sec` (по умолчанию 30s) переподнимает проверки и автоматически выставляет `ready=true` когда зависимость вернётся. Переходы логируются как `readiness_recheck_recovered` / `readiness_recheck_degraded`.

### `POST /chat`

Обработка входящего сообщения.

**Запрос:**

```json
{
  "message": "Добрый день! Прошу рассчитать условия: ИНН 7707083893, срок 90 дней, объём 500 млн.",
  "chat_id": "thread-123"
}
```

**Ответ:**

```json
{
  "answer": "Добрый день! По вашему запросу предлагаем...",
  "destination": "agent",
  "deals": [...],
  "thread_id": "thread-123",
  "manager_email": null
}
```

Поле `destination`:

| Значение | Описание |
|----------|----------|
| `agent` | Ответ сформирован агентом |
| `escalation` | Тред передан сотруднику |
| `error` | Ошибка обработки |

Поле `manager_email` заполняется только при `destination == "escalation"` — берётся из `deals[].assigned_employee` (первый непустой) либо `settings.default_employee_email`. `mail_app` использует его, чтобы отправить тот же `answer` менеджеру в `To`, а контрагента поставить в `Cc`.

---

## Программный API

```python
from agent_treasurer_app.app.agents import (
    process_message,
    resume_with_message,
    get_thread_state,
    get_thread_history,
    create_deal_agent_graph,
    AgentState,
)

# Обработка нового сообщения
state = await process_message(
    message="Прошу рассчитать условия...",
    checkpointer=checkpointer,
    thread_id="thread-123",
    counterparty_id="client-456",
)

# Возобновление с новым сообщением (ответ контрагента)
state = await resume_with_message(
    message="Ставка высоковата, можно 10%?",
    thread_id="thread-123",
    checkpointer=checkpointer,
)

# Текущее состояние треда
state = await get_thread_state("thread-123", checkpointer)
if state and state.awaiting_reply:
    print("Ждём ответа от контрагента")

# История всех состояний (для аналитики)
history = await get_thread_history("thread-123", checkpointer)
```

### Создание графа напрямую

```python
from agent_treasurer_app.app.agents import create_deal_agent_graph, AgentState
from agent_treasurer_app.app.core.checkpointer import get_checkpointer

async with get_checkpointer() as checkpointer:
    graph = create_deal_agent_graph(checkpointer)
    config = {"configurable": {"thread_id": "my-thread"}}

    state = await graph.ainvoke(initial_state, config)
    snapshot = await graph.aget_state(config)
```

---

## Checkpointer

По умолчанию — `MemorySaver` (в памяти процесса, теряется при рестарте). В продакшне — `AsyncPostgresSaver`.

---

## Kafka-интеграция с approve_app

agent_treasurer_app потребляет задания из `AGENT_TASK_TOPIC` и публикует результаты в `AGENT_RESULT_TOPIC`, реализуя тот же контракт, что и approve_app. Без LLM, без графа переговоров, без interrupt/resume — только бизнес-валидация через `approve_service`.

**Контракт (тот же, что у approve_app):**

```json
// AGENT_TASK_TOPIC ← approve_app
{
  "task_id": "uuid",
  "calculation_id": "...",
  "parameters": {
    "policy_rate": "5.25",    // финансы (CalcFundCost)
    "ets": "0.1",
    "inn": "7707083893",      // сделка (внешняя система)
    "term_days": 90,
    "volume": 500000000,
    "currency": "RUB",
    "rate_type": "FIX"
  },
  "created_at": "ISO-8601",
  "ttl_seconds": 90
}

// AGENT_RESULT_TOPIC → approve_app
{
  "task_id": "uuid",
  "calculation_id": "...",
  "decision": "COMPLETED",
  "reason": "Параметры сделки в допустимых пределах. Доступная ставка от 16.00%.",
  "agent_version": "agent_treasurer_app-approve-v1.0.0",
  "processed_at": "ISO-8601"
}
```

**Логика approve_service:**

```
parameters (финансы + сделка)
  → DealConditions (inn, term_days, volume, currency, ...)
  → validate_business_rules()  (per-currency лимиты, формат ИНН)
  → DealService.get_rate()     (проверка доступности ставки)
  → COMPLETED / REJECTED ──(REJECTED)──▶ ApproveNotifier → email казначею
```

**Уведомление менеджера при отказе:** при `decision == "REJECTED"` (включая внутренние сбои `approve()`, маппящиеся в `REJECTED` с reason `Ошибка обработки: ...`) `ApproveNotifier.notify_rejection` отправляет письмо казначею (`settings.default_employee_email`) через `mail_app /api/v1/send_reply`. Содержимое: `task_id`, `calculation_id`, извлекаемые параметры сделки (product, inn, currency, volume с символом валюты, term_days, rate, rate_type, basis, optionality) и причина отказа. Best-effort: сбой отправки логируется как `approve_notify_send_failed`, но публикацию результата в Kafka не блокирует. Идемпотентности на уровне consumer'а нет — повторный TTL-resend задачи со стороны approve_app породит второе письмо.

Consumer запускается в daemon-потоке при старте FastAPI (lifespan); approve + уведомление выполняются в одном `asyncio.run()` на сообщение (один общий event loop). Kafka-параметры:

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `adapter_brokers` | `kafka:9092` | Kafka bootstrap servers |
| `kafka_group_id` | `agent-app-approve-group` | Consumer group ID |
| `kafka_out_topic` | `agent.tasks` | Топик входящих заданий |
| `kafka_in_topic` | `agent.results` | Топик исходящих результатов |
| `kafka_poll_timeout_seconds` | `1.0` | Таймаут poll |

---

## Конфигурация

Управляется через `app/core/config.py` (Pydantic Settings), значения переопределяются переменными окружения.

### LLM

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `is_local` | — | Локальный режим (сертификаты вместо API-ключа) |
| `llm_base_url` | `GIGACHAT_API_URL/v1` | URL LLM-сервиса |
| `llm_model` | `GigaChat-2` | Модель |
| `llm_temperature` | `0.7` | Температура генерации |
| `timeout` | `300` | Таймаут запросов к LLM (сек) |
| `max_tokens` | `10000` | Максимум токенов в ответе |
| `llm_max_retries` | `3` | Кол-во попыток LLM-вызова (SECURITY §22/§23) |
| `llm_retry_base` | `0.5` | Стартовый backoff (сек) для tenacity exponential jitter |
| `llm_retry_max` | `5.0` | Максимальный backoff (сек) для tenacity exponential jitter |
| `llm_retry_exp_base` | `2.0` | Множитель exponential backoff для LLM |
| `llm_retry_jitter` | `1.0` | Максимальный jitter (сек) для LLM retry |
| `profanity_check` | `False` | Проверка на нецензурную лексику |
| `verify_ssl_certs` | `False` | Верификация SSL |

При `is_local=True` используются сертификаты:

| Параметр | Описание |
|----------|----------|
| `GIGACHAT_CRT` | Путь к клиентскому сертификату |
| `GIGACHAT_KEY` | Путь к клиентскому ключу |

### PostgreSQL (checkpointer)

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `postgres_host` | — | Хост |
| `postgres_port` | — | Порт |
| `postgres_user` | — | Пользователь |
| `postgres_password` | — | Пароль |
| `postgres_db` | — | База данных |

### Email-интеграция

| Параметр | Описание |
|----------|----------|
| `agent_email_address` | Email-адрес агента |
| `mail_server_api_url` | URL API почтового сервиса |
| `mail_account_id` | ID аккаунта в mail_app |
| `db_app_url` | URL db_app |

### Агент

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `max_negotiation_iterations` | `1` | Максимум итераций переговоров |
| `default_employee_email` | — | Email сотрудника для эскалации |
| `app_host` | `0.0.0.0` | Хост HTTP API агента |
| `app_port` | `8081` | Порт HTTP API агента |

### Per-currency лимиты

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `rub_fix_min_term` | `1` | RUB FIX: мин. срок (дней) |
| `rub_fix_max_term` | `1096` | RUB FIX: макс. срок (дней) |
| `rub_fix_min_volume` | `500M` | RUB FIX: мин. объём |
| `rub_fix_max_volume` | `15B` | RUB FIX: макс. объём |
| `rub_float_min_term` | `60` | RUB FLOAT: мин. срок (ниже → FIX) |
| `rub_float_max_term` | `366` | RUB FLOAT: макс. срок |
| `rub_float_min_volume` | `500M` | RUB FLOAT: мин. объём (ниже → FIX) |
| `rub_float_max_volume` | `15B` | RUB FLOAT: макс. объём |
| `cny_min_term` | `1` | CNY: мин. срок |
| `cny_max_term` | `732` | CNY: макс. срок |
| `cny_min_volume` | `30K` | CNY: мин. объём (ниже → рекомендация) |
| `cny_max_volume` | `500M` | CNY: макс. объём |
| `inr_min_term` | `1` | INR: мин. срок |
| `inr_max_term` | `366` | INR: макс. срок |
| `inr_min_volume` | `1M` | INR: мин. объём (ниже → рекомендация) |
| `inr_max_volume` | `~5B` | INR: макс. объём |

### Прочее

| Параметр | По умолчанию | Описание |
|----------|-------------|----------|
| `tool_api_url` | `http://localhost:666` | URL сервиса инструментов |
| `redis_url` | — | URL Redis |

---

## Модели данных

Определены в `app/models/schemas.py`:

| Модель | Описание |
|--------|----------|
| `Deal` | Сделка: conditions, status, rate_ladder, rate_ladder_position, offers_history |
| `DealConditions` | Условия сделки (срок, объём, ставка, валюта и т.д.) |
| `DealUpdate` | Действие по сделке (интент + данные) |
| `DealResult` | Результат обработки одной сделки |
| `OfferRecord` | Запись предложения (входящее / исходящее / итерация) |

---

## Структура

```text
agent_treasurer_app/
├── main.py                          # Entry point, FastAPI (импорт из app/)
├── README.md
└── app/
    ├── agents/
    │   ├── __init__.py              # Экспорт: process_message, resume_with_message,
    │   │                            #   get_thread_state, get_thread_history,
    │   │                            #   create_deal_agent_graph, AgentState
    │   ├── graph.py                 # StateGraph, helper-функции
    │   ├── state.py                 # AgentState (Pydantic)
    │   └── nodes/
    │       ├── mailman.py           # mailman_receive_node, mailman_send_response_node
    │       ├── parse_message.py     # parse_message_node
    │       ├── process_deals.py     # process_deals_node
    │       ├── check_thread.py      # check_thread_node
    │       └── compose_response.py  # compose_response_node
    │
    ├── core/
    │   ├── config.py                # Pydantic Settings
    │   ├── llm.py                   # GigaChat LLM
    │   ├── checkpointer.py          # AsyncPostgresSaver
    │   ├── alternative_checkpointers.py
    │   ├── db_session.py            # PG connection settings из TOML (AGENT_DB_SECRETS env)
    │   └── tracing.py               # AEF Tracing SDK bootstrap + re-exports (init_tracing, aef_input_request, aef_agent_start, aef_kafka_produce/consume, aef_custom_span, session_id_cvar)
    │
    ├── models/
    │   ├── schemas.py               # Deal, DealConditions, DealUpdate, DealResult, OfferRecord
    │   └── approve_schemas.py       # AgentTask, AgentResult, AgentDecision (Kafka-контракт)
    │
    ├── services/
    │   ├── deal_service.py          # HTTP-клиент к agent_tools_app
    │   ├── email_service.py         # HTTP-клиент к mail_app
    │   ├── approve_service.py       # Бизнес-валидация без LLM (для approve_app)
    │   ├── approve_notifier.py      # Email казначею при REJECTED (для approve_app)
    │   ├── kafka_consumer.py        # Consumer AGENT_TASK_TOPIC + оркестрация approve/notify
    │   └── kafka_producer.py        # Producer AGENT_RESULT_TOPIC
    │
    └── infrastructure/
        └── ui_API_able.py           # Streamlit UI
```

---

## Пример диалога (лестница ставок)

Сервис вернул: `[("calc_rate", 16.0), ("hist_rate", 17.5), ("model_rate", 19.0)]`

### Итерация 1: запрос условий

```
Контрагент: Добрый день! Прошу рассчитать условия:
            ИНН 7707083893, срок 90 дней, 500 млн руб.
```

-> `process_message()` -> граф -> **INTERRUPT** (rate_ladder_position=0)

```
Агент: Наше предложение по ставке: 16.00%
```

### Итерация 2: отказ → следующая ставка

```
Контрагент: Нет, ставка слишком низкая.
```

-> `resume_with_message()` -> intent=negotiate, position 0→1 -> **INTERRUPT**

```
Агент: Наше предложение по ставке: 17.50%
```

### Итерация 3: клиент просит конкретную ставку

```
Контрагент: Хотим 18%.
```

-> `resume_with_message()` -> intent=negotiate, position 1→2, min(18, 19)=18 -> **INTERRUPT**

```
Агент: Наше предложение по ставке: 18.00%
```

### Итерация 4a: согласие

```
Контрагент: Согласны, оформляем.
```

-> `resume_with_message()` -> intent=approve -> **END**

### Итерация 4b: отказ → лестница исчерпана → эскалация

```
Контрагент: Нет, всё равно мало.
```

-> `resume_with_message()` -> intent=negotiate, position 2→3 (>=len) -> `_escalate_deal()` (`assigned_employee=manager@bank.ru`) -> `check_thread` (`thread_locked=True`, `escalation_requested=True`) -> `compose_response` -> **END**

`/chat` возвращает `destination="escalation"`, `manager_email="manager@bank.ru"`. `mail_app/agent_dispatcher` ставит менеджера в `To`, контрагента — в `Cc`, отправляет одно письмо в существующий тред:

```
From: deals@company.com
To:   manager@bank.ru
Cc:   client@counterparty.ru
Subject: Re: <тема исходного письма>
In-Reply-To / References: <Message-ID входящего>

Добрый день!

Пока не могу предложить условий лучше,
обсудите с менеджером: manager@bank.ru

С уважением,
команда Казначейства
```

Менеджер видит весь тред целиком (за счёт `In-Reply-To`/`References`), отдельного письма с дампом параметров и историей офферов **не отправляется**.
