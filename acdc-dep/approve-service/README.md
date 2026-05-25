# approve-service

Микросервис-оркестратор, обеспечивающий автоматизированное согласование расчётов казначейством.

Сервис слушает входящие события из Kafka, обогащает данные через внешний API (CalcFundCost), передаёт задание AI-агенту для принятия решения, собирает результат и публикует его обратно во внешнюю систему.

## Содержание

- [Архитектура](#архитектура)
- [Стек технологий](#стек-технологий)
- [Быстрый старт](#быстрый-старт)
- [Конфигурация](#конфигурация)
- [Pipeline обработки](#pipeline-обработки)
- [Kafka-топики](#kafka-топики)
- [Статусная модель](#статусная-модель)
- [Обработка ошибок и retry](#обработка-ошибок-и-retry)
- [Дедупликация](#дедупликация)
- [Трейс операции (thread_id / task_id)](#трейс-операции-thread_id--task_id)
- [API-контракты](#api-контракты)
- [Структура проекта](#структура-проекта)
- [Тестирование](#тестирование)

---

## Архитектура

```
┌──────────────┐     ┌──────────────────┐     ┌───────────────┐     ┌──────────────┐
│  Внешняя     │     │  approve-service │     │  AI-агент     │     │  Внешняя     │
│  система     │────>│                  │────>│  (LLM)        │────>│  система     │
│  (Kafka out) │     │  Оркестратор     │<────│               │     │  (Kafka in)  │
└──────────────┘     └────────┬─────────┘     └───────────────┘     └──────────────┘
                              │
                     ┌────────┼─────────┐
                     │        │         │
               ┌─────▼──┐ ┌──▼───┐ ┌───▼────┐
               │CalcFund │ │  db- │ │  DLQ   │
               │Cost API │ │service│ │(Kafka) │
               └─────────┘ └──────┘ └────────┘
```

**Компоненты системы:**

| Компонент | Тип | Описание |
|-----------|-----|----------|
| approve-service | Микросервис (FastAPI) | Основной оркестратор: Kafka consumer/producer, HTTP API, TTL watchdog, бизнес-логика, retry, дедупликация |
| CalcFundCost | Внешний REST API | Возвращает параметры расчёта по id и статусу |
| db-service | Внутренний REST API | CRUD-операции с БД: сохранение параметров, статусов, аудит |
| agent_treasurer_app | Внутренний сервис | Детерминированная бизнес-валидация сделки: лимиты, ставка → COMPLETED/REJECTED |
| Kafka | Брокер сообщений | Транспорт между компонентами |

---

## Стек технологий

| Технология | Назначение |
|------------|-----------|
| Python 3.11+ | Язык разработки |
| FastAPI + uvicorn | HTTP-фреймворк и ASGI-сервер |
| confluent-kafka | Kafka consumer/producer |
| Pydantic v2 | Валидация данных и модели |
| pydantic-settings | Конфигурация из env-переменных |
| httpx | Синхронный HTTP-клиент для REST API |
| structlog | Структурированное логирование |
| pytest | Тестирование |

---

## Быстрый старт

### Предварительные требования

- Python 3.11+
- Запущенные CalcFundCost и db-service (или их моки)

### Установка

```bash
# Клонирование
git clone <repo-url>
cd approve-service

# Установка зависимостей
pip install -r requirements.txt
```

### Запуск сервиса

```bash
# С настройками по умолчанию (FastAPI на порту 8000)
python main.py

# Или через uvicorn напрямую
uvicorn main:app --host 0.0.0.0 --port 8000

# С кастомной конфигурацией через env
ADAPTER_BROKERS=localhost:29092 \
CALC_FUND_COST_URL=http://localhost:8081 \
DB_SERVICE_URL=http://localhost:8082 \
python main.py
```

После запуска доступны:
- **API**: `http://localhost:8000/api/health`, `http://localhost:8000/api/tasks/status?status=SENT_TO_AGENT`
- **Swagger UI**: `http://localhost:8000/docs`

---

## Конфигурация

Все параметры читаются из переменных окружения. Поддерживается `.env` файл.

| Переменная | Описание | Значение по умолчанию |
|------------|----------|-----------------------|
| `ADAPTER_BROKERS` | Адрес Kafka-брокера | `kafka:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID | `approve-service-group` |
| `PALM_CSP_AGENT_OUT_TOPIC` | Входной топик (из внешней системы) | `palm.csp.agent.out` |
| `PALM_CSP_AGENT_IN_TOPIC` | Выходной топик (во внешнюю систему) | `palm.csp.agent.in` |
| `KAFKA_OUT_TOPIC` | Топик заданий агенту | `agent.tasks` |
| `KAFKA_IN_TOPIC` | Топик результатов агента | `agent.results` |
| `DLQ_TOPIC` | Dead Letter Queue | `approve.service.dlq` |
| `CALC_FUND_COST_URL` | URL CalcFundCost API | `http://calcfundcost:8080` |
| `DB_SERVICE_URL` | URL db-service API | `http://db-service:8080` |
| `AGENT_TTL_SECONDS` | Базовый TTL ожидания ответа агента | `90` |
| `ENRICHMENT_RETRY_COUNT` | Максимум попыток retry для CalcFundCost | `3` |
| `ENRICHMENT_RETRY_BACKOFF_MINUTES` | Интервалы backoff (CSV) | `60,120,180` |
| `AGENT_MAX_ATTEMPTS` | Максимум попыток retry для агента | `3` |
| `AGENT_TTL_CHECK_INTERVAL_SECONDS` | Интервал проверки TTL watchdog | `30.0` |
| `SERVER_HOST` | Хост HTTP-сервера | `0.0.0.0` |
| `SERVER_PORT` | Порт HTTP-сервера | `8000` |
| `KAFKA_POLL_TIMEOUT_SECONDS` | Таймаут poll для consumer | `1.0` |

---

## Pipeline обработки

### Этап 1: Приём сообщений (`CspAgentConsumer`)

```
PALM_CSP_AGENT_OUT_TOPIC
  │
  ▼
Десериализация → извлечение calculation_id
  │
  ▼
Дедупликация (GET /approve/tasks/by-calculation/{calculation_id})
  │
  ├─ Дубликат (статус != FAILED) → пропуск
  ├─ FAILED → сброс attempt_count, статус → RECEIVED, reuse того же task_id
  └─ Новая → генерация UUID task_id + POST /approve/tasks
  │
  ▼
Commit offset в Kafka
  │
  ▼
Обогащение (POST CalcFundCost/api/v1/calculations/search)
  │
  ▼
Сохранение snapshot'а (POST /approve/snapshots с task_id + плоскими полями)
  │
  ▼
PATCH /approve/tasks/{task_id}/status → ENRICHED
  │
  ▼
Формирование задания агенту (task_id из БД, parameters, ttl)
  │
  ▼
Публикация в AGENT_TASK_TOPIC + PATCH статуса → SENT_TO_AGENT
```

### Этап 2: Приём результата (`AgentResultConsumer`)

```
AGENT_RESULT_TOPIC
  │
  ▼
Десериализация → валидация AgentResult
  │
  ▼
Lookup задачи по task_id (GET /approve/tasks/{task_id})
  │
  ▼
Проверка: task существует и task_status == SENT_TO_AGENT
  │
  ▼
PATCH /approve/tasks/{task_id}/status:
  task_status   = COMPLETED | REJECTED
  agent_answer  = result.reason       (аудит)
  deal_status   = result.decision
  │
  ▼
Публикация в PALM_CSP_AGENT_IN_TOPIC
  │
  ▼
{ "calculation_id": "...", "status": "approved" | "rejected" }
```

---

## Kafka-топики

| Топик | Направление | Формат сообщения |
|-------|-------------|------------------|
| `PALM_CSP_AGENT_OUT_TOPIC` | Входящий | `{ "calculation_id", "inn"?, "term_days"?, "volume"?, "currency"?, "rate"?, "rate_type"?, "product"?, "basis"?, "optionality"? }` |
| `AGENT_TASK_TOPIC` | Внутренний → agent_treasurer_app | `{ "task_id", "calculation_id", "parameters": {финансы + сделка}, "created_at", "ttl_seconds" }` |
| `AGENT_RESULT_TOPIC` | Внутренний ← agent_treasurer_app | `{ "task_id", "calculation_id", "decision", "reason", "agent_version", "processed_at" }` |
| `PALM_CSP_AGENT_IN_TOPIC` | Исходящий | `{ "calculation_id": "...", "status": "approved\|rejected" }` |
| `APPROVE_SERVICE_DLQ` | Внутренний DLQ | `{ "calculation_id": "..." }` |

Все сообщения используют `calculation_id` как ключ партиционирования.

---

## Статусная модель

### Диаграмма переходов

```
RECEIVED ──────► ENRICHED ──────► SENT_TO_AGENT ──────► COMPLETED
    │                                    │
    │                                    └──────────► REJECTED
    │
    └──► FAILED (при исчерпании retry на любом этапе)
          ▲                ▲
          │                │
     ENRICHED ─────  SENT_TO_AGENT
```

### Описание статусов

| Статус | Описание |
|--------|----------|
| `RECEIVED` | Сообщение принято, дедупликация пройдена |
| `ENRICHED` | Параметры получены из CalcFundCost и сохранены |
| `SENT_TO_AGENT` | Задание отправлено AI-агенту |
| `COMPLETED` | Агент согласовал расчёт → `approved` |
| `REJECTED` | Агент отклонил расчёт → `rejected` |
| `FAILED` | Обработка завершилась неудачей после исчерпания retry |

### Эндпоинт мониторинга

```
GET {approve-service}/api/tasks/status?status={status}&older_than={minutes}
```

Возвращает список задач в указанном статусе, не обновлявшихся дольше N минут. Используется для обнаружения «зависших» задач.

---

## Обработка ошибок и retry

### Retry для CalcFundCost и db-service

| Параметр | Значение |
|----------|----------|
| Максимум попыток | 3 |
| Backoff (нарастающий) | 60 мин → 120 мин → 180 мин |
| При исчерпании | Перемещение в `APPROVE_SERVICE_DLQ` |
| Обработка DLQ | После обработки основной очереди — повторная попытка |
| Финальный неуспех из DLQ | PATCH задачи в `FAILED` с `error_message` |

Счётчик попыток `retry_with_backoff` живёт **только в стеке вызова** — в БД не пишется. Это in-process счётчик, при перезапуске сервиса начинается заново.

### Retry для агента (TTL)

| Параметр | Значение |
|----------|----------|
| TTL попытка 1 | 90 секунд |
| TTL попытка 2 | 180 секунд |
| TTL попытка 3 | 270 секунд |
| При исчерпании | Статус `FAILED` с `error_message` |

В отличие от retry CalcFundCost, счётчик попыток агента (`attempt_count`) **персистится в БД** — TTL watchdog работает асинхронно и должен переживать рестарты. После каждой переотправки `update_task_status` инкрементит `attempt_count`.

### Что считается неуспехом агента

- Агент не вернул ответ в пределах TTL
- Ответ не прошёл валидацию (отсутствуют обязательные поля)
- Агент вернул ошибку

### TTL Watchdog

Фоновый поток (`TtlWatchdog`), который периодически (каждые `AGENT_TTL_CHECK_INTERVAL_SECONDS` секунд) опрашивает задачи в статусе `SENT_TO_AGENT` через `GET /approve/tasks?task_status=SENT_TO_AGENT`:

```
Проверка задач в SENT_TO_AGENT:
  │
  ├── elapsed <= TTL(attempt) → пропустить (агент ещё работает)
  │
  ├── elapsed > TTL(attempt) и attempt < max_attempts
  │   ├─ GET /approve/snapshots/by-task/{task_id} → достаём parameters
  │   └─ переотправить агенту (тот же task_id, attempt+1, увеличенный TTL),
  │      send_task сам обновит attempt_count и статус → SENT_TO_AGENT
  │
  └── elapsed > TTL(attempt) и attempt >= max_attempts
      → PATCH статуса → FAILED + error_message
```

TTL масштабируется линейно: `attempt × AGENT_TTL_SECONDS` (90 → 180 → 270 сек).

---

## Дедупликация

Kafka гарантирует at-least-once delivery, поэтому необходима дедупликация для предотвращения дублей решений во внешней системе.

**Ключ дедупликации:** `calculation_id` (UNIQUE-индекс на стороне db-service).

**Алгоритм:**

```
GET /approve/tasks/by-calculation/{calculation_id}
  │
  ├── 404 → сгенерировать UUID task_id, POST /approve/tasks (RECEIVED, attempt_count=0)
  │
  ├── Задача найдена, статус != FAILED → пропустить (лог: "duplicate, skipping")
  │
  └── Задача найдена, статус == FAILED → reuse существующего task_id, PATCH:
        task_status = RECEIVED, attempt_count = 0, error_message = ""
```

`task_id` стабилен на всю историю задачи — он генерится один раз при первой регистрации и используется во всех последующих обращениях (Kafka payload агенту, lookup ответа, snapshot FK).

---

## Трейс операции (`x-trace-id` / `task_id`)

Сервис — Kafka-оркестратор (не AI-агент), но он сохраняет сквозной UID бизнес-операции. Канонический заголовок: `x-trace-id`, значение — UUID v4. Если входящее сообщение не содержит валидный UID, `approve-service` генерирует новый UUID v4, сохраняет его в `approve_tasks.run_id` / `approve_deal_snapshots.run_id`, прокидывает в HTTP headers к `db-service` и CalcFundCost, а также в Kafka headers к `agent_treasurer` и во внешний result topic.

### Что участвует в корреляции

- **`x-trace-id` / `run_id`** — сквозной UID родительской операции. Не меняется при enrichment, отправке задачи агенту, получении результата, TTL-retry и DLQ-reprocess.
- **`task_id` / `calculation_id`** — бизнес-ключи approve-flow. Стабильный `task_id` генерируется в дедупликации (`deduplication_service.check_and_register`) и хранится в `approve_tasks` + `approve_deal_snapshots.task_id` (UNIQUE FK).
- **`thread_id = task_id`** биндится в `bound_trace` в consumer'ах для локальной structlog-корреляции stdout-логов; `trace_id` биндится тем же helper'ом и попадает в JSON-логи.
- **TTL-retry под одним `x-trace-id`** — `TtlWatchdog` восстанавливает UID из `approve_tasks.run_id`; если историческая строка его не содержит, fallback — валидный UUID v4 `task_id`.

---

## API-контракты

### CalcFundCost API

**Запрос:**
```http
POST {CalcFundCost}/api/v1/calculations/search
Content-Type: application/json

{
  "id": "calculation_id",
  "status": "signOffRequiresFromTreasurer"
}
```

**Ответ (200 OK):**
```json
{
  "id": "calculation_id",
  "status": "signOffRequiresFromTreasurer",
  "parameters": {
    "rate": 5.25,
    "currency": "USD",
    "term_months": 12,
    "counterparty": "...",
    "amount": 1000000
  }
}
```

### db-service API

| Метод | Endpoint | Описание |
|-------|----------|----------|
| `GET` | `/approve/tasks/by-calculation/{calculation_id}` | Lookup задачи по внешнему id (дедупликация) |
| `GET` | `/approve/tasks/{task_id}` | Lookup задачи по внутреннему UUID (валидация ответа агента) |
| `POST` | `/approve/tasks` | Создание задачи. Идемпотентно по `task_id`. |
| `PATCH` | `/approve/tasks/{task_id}/status` | Обновление статуса + agent_answer / deal_status / attempt_count / error_message. |
| `GET` | `/approve/tasks?task_status=...&older_than_minutes=...` | Список зависших задач для TTL watchdog'а |
| `POST` | `/approve/snapshots` | Сохранение snapshot'а с привязкой к `task_id` (FK) |
| `GET` | `/approve/snapshots/by-task/{task_id}` | Достать snapshot для retry в TTL watchdog'е |

### Контракт задания для агента

```json
{
  "task_id": "uuid-v4",
  "calculation_id": "...",
  "parameters": {
    "policy_rate": "5.25",
    "ets": "0.1",
    "cfc_so": "0.2",
    "inn": "7707083893",
    "term_days": 90,
    "volume": 500000000,
    "currency": "RUB",
    "rate_type": "FIX"
  },
  "created_at": "ISO-8601",
  "ttl_seconds": 90
}
```

`task_id` — стабильный на всю задачу, **не меняется между retry**. `parameters` собираются из двух источников: финансовые показатели из CalcFundCost + параметры сделки (`inn`, `term_days`, `volume`, `currency`, ...) из входящего Kafka-сообщения. Оба набора сохраняются в snapshot (см. `SNAPSHOT_FIELDS` в `models/calculation.py`). При retry watchdog подтягивает всё из snapshot'а через `GET /approve/snapshots/by-task/{task_id}`.

### Контракт ответа агента

```json
{
  "task_id": "...",
  "calculation_id": "...",
  "decision": "COMPLETED | REJECTED",
  "reason": "Краткое обоснование решения",
  "agent_version": "v1.0.0",
  "processed_at": "ISO-8601"
}
```

### Выходной контракт (внешняя система)

```json
{
  "calculation_id": "...",
  "status": "approved | rejected"
}
```

---

## Структура проекта

```
approve-service/
├── src/
│   ├── main.py                        # FastAPI app с lifespan, запуск потоков
│   ├── config.py                      # Конфигурация (env vars через pydantic-settings)
│   ├── api/
│   │   └── routes.py                  # HTTP-эндпоинты (мониторинг, health check)
│   ├── consumers/
│   │   ├── csp_agent_consumer.py      # Consumer PALM_CSP_AGENT_OUT_TOPIC
│   │   └── agent_result_consumer.py   # Consumer AGENT_RESULT_TOPIC
│   ├── producers/
│   │   ├── agent_task_producer.py     # Producer AGENT_TASK_TOPIC
│   │   └── csp_result_producer.py     # Producer PALM_CSP_AGENT_IN_TOPIC
│   ├── services/
│   │   ├── enrichment_service.py      # Обогащение через CalcFundCost
│   │   ├── deduplication_service.py   # Дедупликация по calculation_id
│   │   ├── db_client.py              # HTTP-клиент для db-service
│   │   ├── retry_handler.py          # Retry с нарастающим backoff
│   │   ├── dlq_handler.py            # Обработка Dead Letter Queue
│   │   └── ttl_watchdog.py           # Контроль TTL ответа агента
│   ├── models/
│   │   ├── task.py                    # Модель задачи, статусы (Pydantic)
│   │   ├── calculation.py            # Модель расчёта, контракт CalcFundCost
│   │   └── agent_response.py         # Модель задания/ответа агента
│   └── utils/
│       ├── logger.py                  # Настройка structlog
│       └── tracing.py                 # bound_trace (local-only structured logging)
├── tests/
├── requirements.txt
└── approve-service-spec.md            # Формальная спецификация
```

---

## Тестирование

```bash
# Запуск всех тестов
pytest tests/ -v

# Запуск конкретного файла
pytest tests/test_deduplication.py -v

# Запуск конкретного теста
pytest tests/test_deduplication.py::TestCheckAndRegister::test_new_task_created -v
```

### Покрытие тестами

| Модуль | Что тестируется |
|--------|----------------|
| `test_api.py` | Health check, GET /api/tasks/status (фильтрация, валидация параметров) |
| `test_ttl_watchdog.py` | TTL не истёк → не трогаем; истёк → достаём snapshot и переотправляем; нет snapshot'а → шлём пустой dict; исчерпаны попытки → FAILED |
| `test_deduplication.py` | Три сценария дедупликации: новая задача (новый UUID), дубликат, повторная обработка FAILED (reuse task_id) |
| `test_retry_handler.py` | Успех с 1-й попытки, успех со 2-й, исчерпание retry, масштабирование TTL агента |
| `test_enrichment_service.py` | Обогащение: маппинг параметров CalcFundCost → snapshot, привязка к task_id, обновление статуса |
| `test_csp_agent_consumer.py` | Pipeline: happy path, дубликат, retry exhausted → DLQ, невалидный JSON |
| `test_integration.py` | Маппинг decision → status, модель статусов, валидация контракта агента |

---

## Логирование

Используется структурированное логирование (structlog). Каждое событие содержит поля:

| Поле | Описание |
|------|----------|
| `timestamp` | Время события (ISO-8601) |
| `calculation_id` | ID расчёта |
| `task_id` | ID задачи (UUID) |
| `action` | Тип действия (`received`, `enriched`, `sent_to_agent`, `completed`, `rejected`, `failed`, `retry`) |
| `status_from` | Предыдущий статус |
| `status_to` | Новый статус |
| `error` | Сообщение об ошибке (если есть) |
| `attempt` | Номер попытки |
| `thread_id` | Стабильный ID задачи (= `task_id` после dedup). Автоматически из contextvars через `bound_trace`. См. [Трейс операции](#трейс-операции-thread_id--task_id). |

Ответы агента (`reason` и `decision`) сохраняются прямо на задаче через `PATCH /approve/tasks/{task_id}/status`: `agent_answer = result.reason`, `deal_status = result.decision`. Отдельной таблицы `agent_responses` нет — всё, что относится к задаче, лежит в `approve_tasks`.

---

## Ordering и параллельность

- Обработка расчётов через CalcFundCost — **последовательная** (один за другим)
- Consumer group — один инстанс (масштабирование через партиционирование по `calculation_id`)
- Три фоновых daemon-потока работают параллельно:
  1. `CspAgentConsumer` — приём из внешней системы, обогащение, отправка агенту
  2. `AgentResultConsumer` — приём результатов агента, публикация во внешнюю систему
  3. `TtlWatchdog` — контроль TTL, повторные отправки агенту
- FastAPI-сервер обслуживает HTTP API (мониторинг, health check) в основном потоке
- Graceful shutdown через FastAPI lifespan с корректным закрытием всех ресурсов
