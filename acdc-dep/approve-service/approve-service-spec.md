# approve-service — формальная спецификация

## 1. Назначение

approve-service — микросервис-оркестратор, который обеспечивает автоматизированное согласование расчётов казначейством. Сервис слушает входящие события из Kafka, обогащает данные через внешний API, передаёт задание AI-агенту для принятия решения, собирает результат и публикует его обратно во внешнюю систему.

**Стек**: Python

---

## 2. Компоненты системы

| Компонент | Тип | Описание |
|-----------|-----|----------|
| approve-service | Микросервис (Python) | Основной оркестратор: Kafka consumer/producer, бизнес-логика, retry, дедупликация |
| CalcFundCost | Внешний REST API | Возвращает параметры расчёта по id и статусу |
| db-service | Внутренний REST API | CRUD-операции с БД: сохранение параметров, статусов, аудит |
| Agent (LLM) | AI-агент | Принимает решение по расчёту: согласовано / отказано |
| Kafka | Брокер сообщений | Транспорт между компонентами |

---

## 3. Kafka-топики

| Топик | Направление | Описание |
|-------|-------------|----------|
| `PALM_CSP_AGENT_OUT_TOPIC` | Входящий | ID расчётов в статусе `signOffRequiresFromTreasurer` из внешней системы |
| `AGENT_TASK_TOPIC` | Внутренний | Задания для AI-агента (обогащённые параметры расчёта) |
| `AGENT_RESULT_TOPIC` | Внутренний | Результаты обработки агентом (решение + обоснование) |
| `PALM_CSP_AGENT_IN_TOPIC` | Исходящий | Финальный контракт (id + новый статус) обратно во внешнюю систему |
| `APPROVE_SERVICE_DLQ` | Внутренний | Dead Letter Queue для необработанных сообщений |

---

## 4. Основной pipeline

### 4.1. Приём сообщений (Consumer: PALM_CSP_AGENT_OUT_TOPIC)

**Вход**: Kafka-сообщение с `calculation_id` в статусе `signOffRequiresFromTreasurer`

**Шаги**:

1. Десериализация сообщения, извлечение `calculation_id`
2. **Дедупликация**: `GET {db-service}/api/tasks/{calculation_id}`
   - Если запись существует и статус != `FAILED` → пропустить (лог: "duplicate, skipping")
   - Если запись существует и статус == `FAILED` → разрешить повторную обработку
   - Если записи нет → продолжить
3. **Регистрация задачи**: `POST {db-service}/api/tasks`
   - Тело: `{ calculation_id, internal_status: "RECEIVED", attempt_count: 0, created_at: now() }`
4. Commit offset в Kafka

### 4.2. Обогащение данных (CalcFundCost API)

**Вход**: `calculation_id` в статусе `RECEIVED`

**Шаги**:

1. `POST {CalcFundCost}/api/v1/calculations/search`
   - Тело: `{ id: calculation_id, status: "signOffRequiresFromTreasurer" }`
   - Ответ: JSON с параметрами расчёта (ставки, сроки, валюта, контрагент и т.д.)
2. **Обработка последовательная**: расчёты обрабатываются один за другим (не параллельно) для обращения в CalcFundCost
3. При успехе:
   - Сохранение параметров: `POST {db-service}/api/calculations`
     - Тело: полный набор параметров расчёта
   - Обновление статуса: `PATCH {db-service}/api/tasks/{calculation_id}`
     - Тело: `{ internal_status: "ENRICHED", updated_at: now() }`
4. При ошибке → retry (см. раздел 6.1)

### 4.3. Отправка задания агенту (Producer: AGENT_TASK_TOPIC)

**Вход**: Расчёт в статусе `ENRICHED`

**Шаги**:

1. Формирование задания для агента:
   ```json
   {
     "task_id": "uuid-v4",
     "calculation_id": "...",
     "parameters": { /* параметры из CalcFundCost */ },
     "created_at": "ISO-8601",
     "ttl_seconds": 90
   }
   ```
2. Публикация в `AGENT_TASK_TOPIC`
3. Обновление статуса: `PATCH {db-service}/api/tasks/{calculation_id}`
   - Тело: `{ internal_status: "SENT_TO_AGENT", updated_at: now() }`

### 4.4. Приём результата от агента (Consumer: AGENT_RESULT_TOPIC)

**Вход**: Kafka-сообщение с результатом обработки агентом

**Ожидаемый контракт от агента**:
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

**Шаги**:

1. Десериализация результата
2. Валидация: проверка, что `task_id` существует в db и статус == `SENT_TO_AGENT`
3. Сохранение ответа агента: `POST {db-service}/api/agent-responses`
   - Тело: полный ответ агента (включая reason для аудита)
4. Обновление статуса задачи: `PATCH {db-service}/api/tasks/{calculation_id}`
   - Тело: `{ internal_status: decision, updated_at: now() }`
5. Публикация результата в `PALM_CSP_AGENT_IN_TOPIC`

### 4.5. Отправка результата во внешнюю систему (Producer: PALM_CSP_AGENT_IN_TOPIC)

**Выходной контракт** (согласно существующему формату):
```json
{
  "calculation_id": "...",
  "status": "approved | rejected"
}
```

> Контракт может быть расширен в будущем (timestamp, correlation_id, reason, agent_version).

---

## 5. Статусная модель задачи (agent_task_status)

### 5.1. Таблица в БД

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | UUID | PK |
| `calculation_id` | VARCHAR | ID расчёта (уникальный, для дедупликации) |
| `internal_status` | ENUM | Текущий статус обработки |
| `attempt_count` | INT | Количество попыток (для retry) |
| `error_message` | TEXT | Последнее сообщение об ошибке (nullable) |
| `created_at` | TIMESTAMP | Время создания записи |
| `updated_at` | TIMESTAMP | Время последнего обновления |

### 5.2. Диаграмма переходов статусов

```
RECEIVED → ENRICHED → SENT_TO_AGENT → COMPLETED
                                    → REJECTED
         → FAILED (при исчерпании retry на любом этапе)
```

### 5.3. Описание статусов

| Статус | Описание |
|--------|----------|
| `RECEIVED` | Сообщение принято из `PALM_CSP_AGENT_OUT_TOPIC`, дедупликация пройдена |
| `ENRICHED` | Параметры получены из CalcFundCost и сохранены в db-service |
| `SENT_TO_AGENT` | Задание отправлено в `AGENT_TASK_TOPIC` |
| `COMPLETED` | Агент согласовал расчёт |
| `REJECTED` | Агент отклонил расчёт |
| `FAILED` | Обработка завершилась неудачей после исчерпания retry |

### 5.4. Эндпоинт мониторинга

`GET {approve-service}/api/tasks/status?status={status}&older_than={minutes}`

Возвращает список задач в указанном статусе, которые не обновлялись дольше N минут. Используется для обнаружения "зависших" задач.

---

## 6. Обработка ошибок и retry

### 6.1. Retry-политика для CalcFundCost и db-service

| Параметр | Значение |
|----------|----------|
| Максимум попыток | 3 |
| Backoff (нарастающий) | 60 мин → 120 мин → 180 мин |
| При исчерпании retry | Перемещение сообщения в `APPROVE_SERVICE_DLQ` |
| Обработка DLQ | После обработки всех сообщений из основной очереди — повторная попытка из DLQ |
| При неуспехе из DLQ | `POST {db-service}/api/failed-tasks` → запись в таблицу неуспешных обращений |

### 6.2. Retry-политика для агента (TTL)

| Параметр | Значение |
|----------|----------|
| TTL попытка 1 | 90 секунд |
| TTL попытка 2 | 180 секунд |
| TTL попытка 3 | 270 секунд |
| При исчерпании retry | Перемещение в DLQ, повторная попытка |
| При финальном неуспехе | `PATCH {db-service}/api/tasks/{id}` → статус `FAILED` с error_message |

### 6.3. Что считается неуспехом агента

- Агент не вернул ответ в пределах TTL
- Ответ агента не прошёл валидацию (отсутствуют обязательные поля)
- Агент вернул ошибку (exception в обработке)

---

## 7. Дедупликация

### 7.1. Механизм

- При получении `calculation_id` из `PALM_CSP_AGENT_OUT_TOPIC`
- Запрос в db-service: `GET /api/tasks/{calculation_id}`
- Если задача уже существует в статусе, отличном от `FAILED`, — сообщение пропускается
- Если задача в статусе `FAILED` — разрешена повторная обработка (сброс attempt_count)
- Если задачи нет — создание новой записи

### 7.2. Обоснование

- Kafka гарантирует at-least-once delivery
- Без дедупликации возможны дубли решений во внешней системе
- Ключ дедупликации — `calculation_id`

---

## 8. API-контракты

### 8.1. CalcFundCost API

**Запрос**:
```
POST {CalcFundCost}/api/v1/calculations/search
Content-Type: application/json

{
  "id": "calculation_id",
  "status": "signOffRequiresFromTreasurer"
}
```

**Ответ** (200 OK):
```json
{
  "id": "calculation_id",
  "status": "signOffRequiresFromTreasurer",
  "parameters": {
    "rate": 5.25,
    "currency": "USD",
    "term_months": 12,
    "counterparty": "...",
    "amount": 1000000,
    "...": "другие параметры расчёта"
  }
}
```

### 8.2. db-service API (approve-service использует)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| `GET` | `/api/tasks/{calculation_id}` | Проверка дедупликации |
| `POST` | `/api/tasks` | Создание записи о задаче |
| `PATCH` | `/api/tasks/{calculation_id}` | Обновление статуса задачи |
| `POST` | `/api/calculations` | Сохранение параметров расчёта |
| `POST` | `/api/agent-responses` | Сохранение ответа агента |
| `POST` | `/api/failed-tasks` | Запись неуспешных обращений |

---

## 9. Ordering и параллельность

- Обработка расчётов через CalcFundCost — **последовательная** (один за другим)
- Результаты обогащения собираются в очередь и отправляются агенту в `AGENT_TASK_TOPIC`
- Агент обрабатывает задания из топика в порядке поступления
- Consumer group approve-service — один инстанс (при необходимости масштабирования — партиционирование по `calculation_id`)

---

## 10. Структура проекта (Python)

```
approve-service/
├── src/
│   ├── __init__.py
│   ├── main.py                    # Точка входа, запуск consumers
│   ├── config.py                  # Конфигурация (env vars, Kafka bootstrap, URLs)
│   ├── consumers/
│   │   ├── __init__.py
│   │   ├── csp_agent_consumer.py  # Consumer для PALM_CSP_AGENT_OUT_TOPIC
│   │   └── agent_result_consumer.py # Consumer для AGENT_RESULT_TOPIC
│   ├── producers/
│   │   ├── __init__.py
│   │   ├── agent_task_producer.py # Producer для AGENT_TASK_TOPIC
│   │   └── csp_result_producer.py # Producer для PALM_CSP_AGENT_IN_TOPIC
│   ├── services/
│   │   ├── __init__.py
│   │   ├── enrichment_service.py  # Логика обогащения через CalcFundCost
│   │   ├── deduplication_service.py # Логика дедупликации
│   │   ├── db_client.py           # HTTP-клиент для db-service
│   │   ├── retry_handler.py       # Retry-логика с backoff
│   │   └── dlq_handler.py         # Обработка Dead Letter Queue
│   ├── models/
│   │   ├── __init__.py
│   │   ├── task.py                # Модель задачи (Pydantic)
│   │   ├── calculation.py         # Модель расчёта (Pydantic)
│   │   └── agent_response.py      # Модель ответа агента (Pydantic)
│   └── utils/
│       ├── __init__.py
│       └── logger.py              # Настройка логирования
├── tests/
│   ├── __init__.py
│   ├── test_csp_agent_consumer.py
│   ├── test_enrichment_service.py
│   ├── test_deduplication.py
│   ├── test_retry_handler.py
│   └── test_integration.py
├── requirements.txt
└── README.md
```

---

## 11. Конфигурация (переменные окружения)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `ADAPTER_BROKERS` | Адрес Kafka-брокера | `kafka:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID | `approve-service-group` |
| `PALM_CSP_AGENT_OUT_TOPIC` | Входной топик | `palm.csp.agent.out` |
| `PALM_CSP_AGENT_IN_TOPIC` | Выходной топик | `palm.csp.agent.in` |
| `KAFKA_OUT_TOPIC` | Топик заданий агенту | `agent.tasks` |
| `KAFKA_IN_TOPIC` | Топик результатов агента | `agent.results` |
| `DLQ_TOPIC` | Dead Letter Queue | `approve.service.dlq` |
| `CALC_FUND_COST_URL` | URL CalcFundCost API | `http://calcfundcost:8080` |
| `DB_SERVICE_URL` | URL db-service API | `http://db-service:8080` |
| `AGENT_TTL_SECONDS` | Базовый TTL для агента | `90` |
| `ENRICHMENT_RETRY_COUNT` | Макс. retry для CalcFundCost | `3` |
| `ENRICHMENT_RETRY_BACKOFF_MINUTES` | Backoff-интервалы (CSV) | `60,120,180` |

---

## 12. Логирование и аудит

Каждое действие логируется со следующими полями:

| Поле | Описание |
|------|----------|
| `timestamp` | Время события |
| `calculation_id` | ID расчёта |
| `task_id` | ID задачи (UUID) |
| `action` | Тип действия (received, enriched, sent_to_agent, completed, rejected, failed, retry) |
| `status_from` | Предыдущий статус |
| `status_to` | Новый статус |
| `error` | Сообщение об ошибке (если есть) |
| `attempt` | Номер попытки |

Все ответы агента (включая `reason`) сохраняются через db-service в таблицу `agent_responses` для аудитного следа.
