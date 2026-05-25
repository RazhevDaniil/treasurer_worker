"""Конфигурация approve-service. Все параметры читаются из переменных окружения."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Все поля читаются из переменных окружения. BaseSettings автоматически матчит snake_case → UPPER_CASE (например, calc_fund_cost_url → CALC_FUND_COST_URL)."""

    # Kafka
    adapter_brokers: str = "kafka:9092"
    kafka_group_id: str = "approve-service-group"

    # Топики Kafka
    palm_csp_agent_out_topic: str = "palm.csp.agent.out"
    palm_csp_agent_in_topic: str = "palm.csp.agent.in"
    kafka_out_topic: str = "agent.tasks"
    kafka_in_topic: str = "agent.results"
    dlq_topic: str = "approve.service.dlq"

    # Внешние сервисы
    calc_fund_cost_url: str = "http://calcfundcost:8080"
    db_service_url: str = "http://db-service:8080"

    # Retry-политика для CalcFundCost / db-service (спека §6.1)
    enrichment_retry_count: int = 3
    enrichment_retry_backoff_minutes: str = "60,120,180"

    # TTL для ожидания ответа агента (спека §6.2)
    agent_ttl_seconds: int = 90

    # Retry-политика для агента (спека §6.2): максимум попыток
    agent_max_attempts: int = 3

    # TTL watchdog: интервал проверки зависших задач (секунды)
    agent_ttl_check_interval_seconds: float = 30.0

    # HTTP-сервер
    server_host: str = "0.0.0.0"
    server_port: int = 8000

    # Kafka consumer: таймаут poll
    kafka_poll_timeout_seconds: float = 1.0

    @property
    def backoff_intervals_minutes(self) -> list[int]:
        return [int(x) for x in self.enrichment_retry_backoff_minutes.split(",")]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
