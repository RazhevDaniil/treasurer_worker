# mail_app (post-service)

Почтовый шлюз на FastAPI — приём входящих писем по IMAP, публикация их в Kafka для AI-агента, отправка ответов по SMTP.

```text
                    Kafka (KAFKA_MAIL_TOPIC)
IMAP-сервер  ──▶  mail_app  ──────────────────────────▶  agent_treasurer
                     ▲                                          │
SMTP-сервер  ◀──  mail_app  ◀── HTTP POST /api/v1/send_reply ───┘
                     │
                     ▼  HTTP
                  db_app  ──▶  PostgreSQL
```

> Хранение данных вынесено в `db_app`. `mail_app` обращается к нему через HTTP-клиент (`DbClient`). Если `DB_URL` пуст или невалиден — используется `InMemoryDbClient` (in-memory замена для отладки).

---

## Быстрый старт

```bash
cd mail_app
pip install -r requirements.txt

# Положить логин/пароль ТУЗа в postservicecreds.properties (TOML)
# Прописать env-переменные (см. «Конфигурация»)
uvicorn main:app --reload
```

Сервис доступен на `http://localhost:555`.

---

## Архитектура

Три асинхронных воркера работают параллельно внутри одного FastAPI-приложения:

```text
                    ┌──────────────────────────┐
                    │    FastAPI (port 555)    │
                    │    uvicorn, async/await  │
                    └────┬────────┬────────┬───┘
                         │        │        │
                ┌────────▼──┐ ┌───▼──────┐ ┌▼──────────┐
                │ImapWorker │ │  Agent   │ │SmtpWorker │
                │           │ │Dispatcher│ │           │
                │IMAP poll  │ │Kafka     │ │SMTP send  │
                │every 10s  │ │publish   │ │+ retry    │
                └─────┬─────┘ └────┬─────┘ └─────┬─────┘
                      │            │              │
                      │      ┌─────▼──────┐       │
                      │      │  Kafka     │       │
                      │      │  producer  │       │
                      │      └────────────┘       │
                      │                           │
                      └────────────┬──────────────┘
                                   │ HTTP
                            ┌──────▼──────┐
                            │   db_app    │
                            └─────────────┘
```

Все воркеры взаимодействуют с БД через `DbClient` — HTTP-клиент к `db_app`. Блокирующие операции (`imaplib`, `smtplib`, `confluent-kafka`) выполняются в пуле потоков через `asyncio.to_thread()`.

---

## Поток данных

### Входящее письмо

1. **ImapWorker** подключается к IMAP-серверу (`MAIL_IMAP_HOST:993`, SSL) и забирает непрочитанные письма
2. Парсит RFC 822 через `email_parser` (MIME, charset, multipart)
3. Дедуплицирует по `Message-ID`
4. Вызывает `db_client.ingest_message()` — создаёт message + задачу `PROCESS_INCOMING`
5. Помечает письмо как прочитанное на IMAP
6. **AgentDispatcher** забирает задачи `PROCESS_INCOMING` через `db_client.dequeue()`
7. **Публикует payload в Kafka-топик `KAFKA_MAIL_TOPIC`** (идемпотентный producer, `acks=all`, ключ партиционирования — `thread_id` для in-thread порядка)
8. Получает acknowledge от брокера → `db_client.update_task_status(DONE)`. Ответа от агента не ждёт.

> **Маршрутизация ответа** теперь полностью на стороне агента — `agent_treasurer` сам решает, кому шлёт reply (клиенту, или менеджеру с клиентом в Cc при эскалации), и вызывает `POST /api/v1/send_reply` с готовыми полями.

### Исходящее письмо (ответ агента)

1. `agent_treasurer` (или любой другой сервис) вызывает `POST /api/v1/send_reply`
2. mail_app создаёт исходящее сообщение + задачу `SEND_SMTP` через `db_client.save_outgoing()`
3. **SmtpWorker** забирает задачу, строит RFC 822 ответ через `email_builder` (заголовки `In-Reply-To`, `References`, опциональный `Cc`)
4. Отправляет через SMTP (`MAIL_SMTP_HOST:25`, STARTTLS)
5. Обновляет статус задачи (`DONE` / `RETRYING` / `FAILED`)

---

## API

### Health

#### `GET /health/live`

Проба liveness (всегда 200).

```json
{"status": "ok"}
```

#### `GET /health/ready`

Проба readiness.

```json
{"status": "ok"}
```

### Отправка

#### `POST /api/v1/send_reply`

Создать ответное письмо и поставить в очередь на отправку. Основной обратный канал агента; вызывается также вручную внешними сервисами.

**Запрос:**

```json
{
  "recipient_email": "user@example.com",
  "subject": "Re: тема",
  "reply_body": "текст ответа",
  "in_reply_to": "<msg-id>",
  "references": "<msg-id-1> <msg-id-2>",
  "thread_id": "<thread-id>",
  "cc": ["manager-cc@example.com"]
}
```

Поле `cc` опциональное; задаётся, например, при эскалации (`To` — менеджер, `Cc` — клиент).

**Ответ (200):**

```json
{
  "status": "queued",
  "message_id": "<uuid@domain>",
  "task_id": "task-uuid"
}
```

---

## Воркеры

### ImapWorker

**Файл:** `app/workers/imap_worker.py`

- Поллит IMAP каждые `imap_poll_seconds` (по умолчанию 10 с)
- Подключение: IMAP4_SSL, порт 993, `MAIL_IMAP_HOST`
- Забирает непрочитанные письма (`SEARCH UNSEEN`)
- Парсит каждое письмо через `email_parser` (MIME, charset, multipart)
- Дедупликация по `Message-ID`
- После записи в БД — помечает письмо как `\Seen` на IMAP
- При ошибках подключения — логирует и ждёт следующего цикла

### AgentDispatcher

**Файл:** `app/workers/agent_dispatcher.py`

- Забирает батч задач `PROCESS_INCOMING` через `db_client.dequeue()`
- **Публикует payload в `KAFKA_MAIL_TOPIC`** через идемпотентный confluent-kafka producer (`acks=all`, `enable.idempotence=true`)
- Ключ партиционирования — `thread_id` (сохраняет порядок сообщений внутри треда)
- В Kafka-заголовок `idempotency-key` кладёт `task_id` (broker-level dedup)
- При успешной доставке (delivery callback `err=None`) — `db_client.update_task_status(DONE)`
- При `AgentPublishError` (delivery callback с ошибкой / timeout flush) — статус `RETRYING` с экспоненциальным backoff; после `max_attempts` — `FAILED`
- **Ответа агента не ждёт** — reply прилетит асинхронно через `POST /api/v1/send_reply`

### SmtpWorker

**Файл:** `app/workers/smtp_worker.py`

- Забирает батч задач `SEND_SMTP` через `db_client.dequeue()`
- Строит RFC 822 ответное письмо через `email_builder` (включая `Cc`, если задан)
- Отправка через SMTP (`MAIL_SMTP_HOST:25`, STARTTLS)
- Классификация ошибок: временные (4xx, network) → `RETRYING`, постоянные (5xx, permanent SMTP) → `FAILED`
- Максимум `max_attempts` попыток (по умолчанию 20)

---

## Конфигурация

### Env-переменные

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `APP_NAME` | `Mail Gateway Service` | Название приложения |
| `APP_PORT` | `555` | Порт сервиса |
| `LOG_LEVEL` | `INFO` | Уровень логирования |
| `AGENT_URL` | `""` | URL legacy HTTP `/chat` агента (на dev: оставить для тестов; основной канал — Kafka) |
| `AGENT_TIMEOUT_SECONDS` | `30` | Таймаут HTTP-запроса (legacy) |
| `MAIL_IMAP_HOST` | `""` | IMAP-сервер (alias из desc.yaml) |
| `IMAP_PORT` | `993` | Порт IMAP (SSL) |
| `IMAP_ADDRESS` | `AEFContainer-dev@alpha-exchtest.sbrf.ru` | IMAP email-адрес (заполняется из creds) |
| `IMAP_MAILBOX` | `INBOX` | Имя IMAP-ящика |
| `IMAP_POLL_SECONDS` | `10` | Интервал поллинга (сек) |
| `IMAP_USE_IDLE` | `false` | Использовать IMAP IDLE |
| `MAIL_SMTP_HOST` | `""` | SMTP-сервер (alias из desc.yaml) |
| `SMTP_PORT` | `25` | Порт SMTP |
| `SMTP_FROM_ADDRESS` | `AEFContainer-dev@alpha-exchtest.sbrf.ru` | Адрес отправителя (заполняется из creds) |
| `DB_URL` | `""` | URL `db_app`. Пусто → in-memory режим |
| `ADAPTER_BROKERS` | `""` | Бизнесовый Kafka-кластер |
| `KAFKA_MAIL_TOPIC` | `""` | Топик публикации входящих писем |
| `KAFKA_PRODUCER_RETRIES` | `5` | confluent-kafka `retries` |
| `KAFKA_RETRY_BACKOFF_MS` | `200` | confluent-kafka `retry.backoff.ms` |
| `KAFKA_RETRY_BACKOFF_MAX_MS` | `5000` | confluent-kafka `retry.backoff.max.ms` |
| `KAFKA_FLUSH_TIMEOUT_SECONDS` | `10` | Таймаут flush producer'а |
| `MAX_ATTEMPTS` | `20` | Макс. количество retry попыток |
| `BACKOFF_MIN_SECONDS` | `1` | Мин. задержка retry |
| `BACKOFF_MAX_SECONDS` | `120` | Макс. задержка retry |
| `AGENT_BATCH_SIZE` | `20` | Размер батча `AgentDispatcher` |
| `AGENT_POLL_SECONDS` | `5` | Интервал поллинга `AgentDispatcher` |
| `SMTP_BATCH_SIZE` | `20` | Размер батча `SmtpWorker` |
| `SMTP_POLL_SECONDS` | `5` | Интервал поллинга `SmtpWorker` |

### Секреты ТУЗа (postServiceCreds)

Логин и пароль почтового ящика читаются НЕ из env, а из TOML-файла. Путь к файлу — в env-переменной `postServiceCreds` (по умолчанию `/.secrets/postServiceCreds.properties`).

Wrapper в АЭФ монтирует файл по alias `postServiceCreds` (см. `description.yaml` → `secretFiles`).

**Формат файла** (`postservicecreds.properties`):

```toml
[connections.post-service-cred]
username = "robot@example.com"
password = "p@ssw0rd"
```

Логин подставляется и в `IMAP_ADDRESS`, и в `SMTP_FROM_ADDRESS` (один ТУЗ на оба протокола).

> Стартап ждёт появления файла до 5 минут (`get_secret_file_from_env`), затем падает с `FileNotFoundError` — учитывать при локальной разработке и в тестах.

### Алиасы desc.yaml

`description.yaml` объявляет внешние ресурсы, которые wrapper резолвит в env при деплое:

| Alias | Тип | Содержание |
| --- | --- | --- |
| `AGENT_URL` | endpoint | Legacy HTTP /chat (UI/тесты) |
| `MAIL_SMTP_HOST` | endpoint | SMTP-сервер |
| `MAIL_IMAP_HOST` | endpoint | IMAP-сервер |
| `DB_URL` | endpoint | `db_app` |
| `ADAPTER_BROKERS` | endpoint | Брокер бизнесовой Kafka |
| `KAFKA_MAIL_TOPIC` | endpoint | Имя топика на брокере |
| `postServiceCreds` | secretFile | `postservicecreds.properties` (login + password) |

---

## Гарантии

- **Идемпотентность входящих** — дедупликация по `Message-ID` на стороне `db_app` (`PROCESS_INCOMING.task_key`); дополнительно — `Idempotency-Key` в Kafka-заголовке (`task_id`) для broker-side dedup на стороне consumer'а.
- **Атомарность БД** — `ingest_message` и `save_outgoing` создают сообщение и задачу в одной транзакции (на стороне `db_app`).
- **In-thread порядок** — Kafka-publish использует `key=thread_id`, все письма одного треда попадают в одну партицию и обрабатываются consumer'ом строго по порядку.
- **Email Threading (RFC 2822)** — заголовки `In-Reply-To`, `References` сохраняются при приёме и корректно выставляются при ответе для группировки в почтовых клиентах.
- **Эскалация (Cc)** — при `destination=escalation` от агента: `To` = менеджер, `Cc` = клиент (оба остаются в треде).
- **Retry** — экспоненциальный backoff с jitter; разделение временных и постоянных ошибок; максимум 20 попыток (настраивается).
- **Параллельные воркеры** — `db_app` использует `SELECT FOR UPDATE SKIP LOCKED`, несколько экземпляров `mail_app` могут обрабатывать очередь без конфликтов; Kafka-producer идемпотентный (`enable.idempotence=true`).

---

## Структура

```text
mail_app/
├── main.py                     # Entry point (relative + absolute fallback)
├── __init__.py                 # Делает пакетом для тестов (namespaced import)
├── description.yaml            # Wrapper alias'ы (endpoints + secretFiles)
├── requirements.txt
└── app/
    ├── main.py                 # FastAPI, startup/shutdown, workers
    ├── config.py               # Pydantic Settings + загрузка postServiceCreds TOML
    ├── logging_config.py       # stdlib logging setup
    │
    ├── api/
    │   ├── health.py           # GET /health/live, GET /health/ready
    │   └── routes.py           # POST /api/v1/send_reply
    │
    ├── schemas/
    │   └── api.py              # SendReplyRequest (с cc), SendReplyResponse, HealthResponse
    │
    ├── services/
    │   ├── db_client.py        # HTTP-клиент к db_app
    │   ├── in_memory_db.py     # In-memory заглушка (DB_URL пуст)
    │   ├── agent_client.py     # AgentKafkaPublisher (confluent-kafka)
    │   ├── email_builder.py    # Построение RFC 822 ответа (To/Cc/In-Reply-To/References)
    │   ├── email_parser.py     # Парсинг MIME/RFC 822
    │   ├── imap_client.py      # IMAP4_SSL клиент
    │   └── smtp_client.py      # SMTP STARTTLS клиент
    │
    ├── utils/
    │   ├── backoff.py          # Экспоненциальный backoff с jitter
    │   └── time.py             # utc_now()
    │
    └── workers/
        ├── imap_worker.py      # IMAP polling → db_client.ingest_message
        ├── agent_dispatcher.py # PROCESS_INCOMING → KAFKA_MAIL_TOPIC publish
        └── smtp_worker.py      # SEND_SMTP → SMTP send
```

---

## Контракт сообщения в KAFKA_MAIL_TOPIC

**Key:** `thread_id` (UTF-8 bytes) — для in-thread порядка
**Value:** JSON UTF-8 (поле `payload_json` задачи `PROCESS_INCOMING`):

```json
{
  "message_id": "<...@domain>",
  "thread_id": "<...@domain>",
  "author_email": "alice@example.com",
  "sender_name": "Alice",
  "reply_to_email": null,
  "subject": "Депозит 50 млн",
  "body": "...",
  "headers_json": {"In-Reply-To": "...", "x-trace-id": "4d6f8c2a-2d6a-4b5f-9d9c-8a0f7b8f5f12"},
  "x_trace_id": "4d6f8c2a-2d6a-4b5f-9d9c-8a0f7b8f5f12",
  "run_id": "4d6f8c2a-2d6a-4b5f-9d9c-8a0f7b8f5f12"
}
```

**Headers:**

- `idempotency-key`: `task_id` исходящей задачи (broker-level dedup на стороне consumer'а)
- `x-trace-id`: сквозной UUID v4 родительской операции; генерируется при ingest IMAP-письма или принимается из HTTP `/api/v1/send_reply`, сохраняется в `db-service.run_id` и прокидывается дальше в Kafka/HTTP.

Consumer на стороне `agent_treasurer` (`MailIncomingConsumer`) после обработки графа вызывает обратный канал — `POST /api/v1/send_reply` с готовыми `recipient_email`, `subject`, `reply_body`, `cc` и т.д.
