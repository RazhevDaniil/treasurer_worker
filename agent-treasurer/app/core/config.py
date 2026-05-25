"""Configuration settings for the deal agent."""

import logging
import os
import re
from functools import lru_cache
from typing import Annotated, Optional

from pydantic import AliasChoices, Field, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # === LLM Configuration ===
    is_local: bool = Field(default=False, validation_alias="AGENT_IS_LOCAL")

    giga_api_url: str = Field(
        default="https://gigachat-ift.sberdevices.delta.sbrf.ru",
        validation_alias="GIGACHAT_API_URL",
        description="GigaChat host; '/v1' is appended via llm_base_url.",
    )

    @computed_field
    @property
    def llm_base_url(self) -> str:
        return "{}/v1".format(self.giga_api_url)

    main_model: str = Field(
        default="GigaChat-2",
        validation_alias=AliasChoices("MAIN_MODEL", "LLM_MODEL"),
        description="GigaChat model name (Main installation)",
    )
    llm_temperature: float = Field(
        default=0.7,
        description="Default LLM sampling temperature; overridden per call site (e.g. 0 for extraction).",
    )

    # --- SECURITY §26: PreView GigaChat installation (canary) ---
    # Only the model name differs from Main — PreView shares `llm_base_url`.
    preview_model: Optional[str] = Field(default=None)
    preview_ratio: float = Field(
        default=0.0,
        description="Probability in [0.0, 0.05] of routing each LLM call to the PreView model.",
    )

    @field_validator("preview_ratio")
    @classmethod
    def validate_preview_ratio(cls, v: float) -> float:
        if not 0.0 <= v <= 0.05:
            raise ValueError("preview_ratio must be between 0.0 and 0.05 (up to 5% load)")
        return v

    timeout: int=300
    max_tokens: int=10000
    llm_max_retries: int = Field(default=3, description="Max LLM retry attempts (SECURITY §22/§23)")
    llm_retry_base: float = Field(
        default=0.5,
        description="Initial backoff in seconds for exponential-jitter wait between LLM retries (SECURITY §22/§23).",
    )
    llm_retry_max: float = Field(
        default=5.0,
        description="Maximum backoff in seconds for exponential-jitter wait between LLM retries (SECURITY §22/§23).",
    )
    profanity_check: bool=False
    verify_ssl_certs: bool=False

    @property
    def cert_file(self) -> Optional:
        if self.is_local:
            return os.getenv("GIGACHAT_CRT", "/home/jovyan/Kolodyazhny/mail_server/agent_app/core/gigachat/giga.pem")
        else:
            return None

    @property
    def key_file(self) -> Optional:
        if self.is_local:
            return os.getenv("GIGACHAT_KEY", "/home/jovyan/Kolodyazhny/mail_server/agent_app/core/gigachat/giga.key")
        else:
            return None

    # === PostgreSQL Configuration ===
    # Подключение к Postgres настраивается через TOML-файл, путь к которому
    # указан в env-переменной AGENT_DB_SECRETS. См. agent_app/app/core/db_session.py.

    # === Email Configuration (for agent's mailbox) ===
    agent_email_address: str = Field(
        default="deals@company.com",
        description="Agent's email address"
    )
    mail_server_api_url: str = Field(
        default="http://localhost:555",
        description="URL of the mail server API"
    )
    mail_server_api_path: str = Field(
        default="/acdc-dep/db-service-v1000",
        validation_alias="MAIL_SERVER_API_PATH",
        description=(
            "Service path injected into mail_server_api_url when DevOps env omits it "
            "(workaround: ingress addressed as 'host{path}:port'). "
            "Set empty to disable patching."
        ),
    )
    mail_account_id: str = Field(
        default="default",
        description="Mail account ID on the mail server"
    )

    @model_validator(mode="after")
    def _patch_mail_server_api_url(self) -> "Settings":
        # Воркэраунд: девопс прописывает MAIL_SERVER_API_URL как `scheme://host:port`,
        # хотя ingress ожидает `scheme://host{path}:port`. Удалить, когда env починят.
        path = self.mail_server_api_path
        url = self.mail_server_api_url
        if not path or path in url:
            return self
        m = re.fullmatch(r"(https?://[^/:]+):(\d+)/?", url)
        if not m:
            return self
        patched = f"{m.group(1)}{path}:{m.group(2)}"
        logger.warning(
            "mail_server_api_url missing service path; patched %s -> %s "
            "(remove workaround once MAIL_SERVER_API_URL is fixed upstream)",
            url, patched,
        )
        self.mail_server_api_url = patched
        return self
    db_app_url: str = Field(
        default="http://localhost:444",
        description="Base URL of the db_app service"
    )

    # === Agent Settings ===
    max_negotiation_iterations: int = Field(
        default=1,
        description="Maximum negotiation rounds before escalation"
    )
    default_employee_email: str = Field(
        default="manager@company.com",
        description="Default employee for escalation"
    )

    # === Operation TTL (SECURITY §21) ===
    operation_ttl_sec: int = Field(
        default=600,
        description="Global TTL for a single graph invocation (process_message / resume_with_message); on expiry the caller receives a controlled timeout response.",
    )
    operation_ttl_user_message: str = Field(
        default="Извините, ответ агента занимает слишком много времени. Пожалуйста, попробуйте написать ещё раз.",
        description="User-facing message returned when operation_ttl_sec is exceeded.",
    )
    operation_max_hops: int = Field(
        default=20,
        description="Maximum external calls/attempts recorded as hops for one agent operation.",
    )

    # === Per-currency limits: RUB FIX ===
    rub_fix_min_term: int = Field(default=1, description="RUB FIX: мин. срок (дней)")
    rub_fix_max_term: int = Field(default=1096, description="RUB FIX: макс. срок (дней)")
    rub_fix_min_volume: float = Field(default=500_000_000, description="RUB FIX: мин. объём")
    rub_fix_max_volume: float = Field(default=15_000_000_000, description="RUB FIX: макс. объём")

    # === Per-currency limits: RUB FLOAT ===
    rub_float_min_term: int = Field(default=60, description="RUB FLOAT: мин. срок (дней), ниже → предложить FIX")
    rub_float_max_term: int = Field(default=366, description="RUB FLOAT: макс. срок (дней)")
    rub_float_min_volume: float = Field(default=500_000_000, description="RUB FLOAT: мин. объём, ниже → предложить FIX")
    rub_float_max_volume: float = Field(default=15_000_000_000, description="RUB FLOAT: макс. объём")

    # === Per-currency limits: CNY ===
    cny_min_term: int = Field(default=1, description="CNY: мин. срок (дней)")
    cny_max_term: int = Field(default=732, description="CNY: макс. срок (дней)")
    cny_min_volume: float = Field(default=30_000, description="CNY: мин. объём (ниже → рекомендация)")
    cny_max_volume: float = Field(default=500_000_000, description="CNY: макс. объём")

    # === Per-currency limits: INR ===
    inr_min_term: int = Field(default=1, description="INR: мин. срок (дней)")
    inr_max_term: int = Field(default=366, description="INR: макс. срок (дней)")
    inr_min_volume: float = Field(default=1_000_000, description="INR: мин. объём (ниже → рекомендация)")
    inr_max_volume: float = Field(default=4_999_999_999, description="INR: макс. объём")

    # === Agent API Server ===
    app_host: str = Field(
        default="0.0.0.0",
        description="Port for the agent API server"
    )
    
    app_port: int = Field(
        default=8081,
        description="Port for the agent API server"
    )

    # === Tool API Configuration ===
    tool_api_url: str = Field(
        default="http://localhost:666",
        description="URL of the tool API for rate calculations"
    )

    # === Redis Configuration (for alternative checkpointer) ===
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for checkpointer"
    )

    # === Readiness Probe Configuration (SECURITY §19) ===
    readiness_recheck_interval_sec: int = Field(
        default=30,
        description="Interval between background readiness re-checks",
    )
    readiness_gigachat_timeout_sec: float = Field(
        default=30.0,
        description="Per-probe timeout for the GigaChat synthetic ping",
    )
    readiness_mail_timeout_sec: float = Field(
        default=5.0,
        description="Per-probe timeout for the mail_app /health/live request",
    )
    readiness_tools_timeout_sec: float = Field(
        default=15.0,
        description="Per-deal timeout for the agent_tools_app get_rate probe",
    )
    readiness_test_inn: str = Field(
        default="7707083893",
        description="INN used in synthetic deals for the agent_tools_app probe",
    )

    # === Kafka Configuration (for approve_app integration) ===
    adapter_brokers: str = Field(
        default="kafka:9092",
        validation_alias="ADAPTER_BROKERS",
        description="Kafka bootstrap servers",
    )
    kafka_group_id: str = Field(
        default="agent-app-approve-group",
        description="Kafka consumer group ID for agent_app",
    )
    kafka_out_topic: str = Field(
        default="agent.tasks",
        validation_alias="KAFKA_OUT_TOPIC",
        description="Topic for incoming approval tasks from approve_app (CFC → agent).",
    )
    kafka_in_topic: str = Field(
        default="agent.results",
        validation_alias="KAFKA_IN_TOPIC",
        description="Topic for outgoing approval results to approve_app (agent → CFC).",
    )
    kafka_mail_topic: str = Field(
        default="mail.incoming",
        validation_alias="KAFKA_MAIL_TOPIC",
        description="Topic with incoming emails published by mail_app (mail → agent).",
    )
    kafka_mail_group_id: str = Field(
        default="agent-app-mail-group",
        description="Kafka consumer group ID for KAFKA_MAIL_TOPIC.",
    )
    kafka_poll_timeout_seconds: float = Field(
        default=1.0,
        description="Kafka consumer poll timeout",
    )

    # === HTTP retry (SECURITY §22) ===
    http_max_retries: int = Field(
        default=3,
        description="Max attempts (incl. first) for outbound HTTP calls to mail_app / agent_tools_app.",
    )
    http_retry_base: float = Field(
        default=0.5,
        description="Initial backoff in seconds for exponential-jitter wait between HTTP retries.",
    )
    http_retry_max: float = Field(
        default=5.0,
        description="Maximum backoff in seconds for exponential-jitter wait between HTTP retries.",
    )

    # === Kafka producer retry (SECURITY §22) ===
    kafka_producer_retries: int = Field(
        default=5,
        description="confluent-kafka producer 'retries' setting for KAFKA_IN_TOPIC.",
    )
    kafka_retry_backoff_ms: int = Field(
        default=200,
        description="confluent-kafka producer 'retry.backoff.ms' initial backoff.",
    )
    kafka_retry_backoff_max_ms: int = Field(
        default=5000,
        description="confluent-kafka producer 'retry.backoff.max.ms' capped backoff.",
    )

    # === Kafka business cluster identifier (required by AEF kafka spans) ===
    kafka_cluster_name: str = Field(
        default="prototype-kafka-cluster",
        description="Logical name of the business Kafka cluster (AGENT_TASK / AGENT_RESULT). Passed as 'kafka_cluster' to aef_kafka_produce / aef_kafka_consume.",
    )

    # === AEF Tracing SDK (SECURITY §18/§20/§21/§26) ===
    # Identifier fields follow the SDK env-name convention from
    # docs_for_SDK/instructions/prototype_tracing.md (AGENT_ID, CLUSTER_ID,
    # POD_NAMESPACE, DISTRIBUTIVE). AEF_*-prefixed env vars are accepted as
    # a fallback so legacy deployment configs keep working.
    tracing_service_kafka_outbox_topic: str = Field(
        default="",
        validation_alias="TRACING_SERVICE_KAFKA_OUTBOX_TOPIC",
        description="Outbox topic in AEF Controller Kafka for proto trace messages.",
    )
    kafka_hosts: Annotated[list[str], NoDecode] = Field(
        alias="TRACING_SERVICE_KAFKA_BOOTSTRAP_SERVERS"
    )

    @field_validator("kafka_hosts", mode="before")
    @classmethod
    def parse_kafka_hosts(cls, v):
        if isinstance(v, str):
            return [h.strip() for h in v.strip("[] ").split(",") if h.strip()]
        return v

    aef_kafka_security_protocol: str = Field(
        default="PLAINTEXT",
        validation_alias="AEF_KAFKA_SECURITY_PROTOCOL",
        description="security_protocol for the AEF Controller Kafka producer.",
    )
    aef_kafka_max_request_size: int = Field(
        default=10_485_760,
        validation_alias="AEF_KAFKA_MAX_REQUEST_SIZE",
        description="max_request_size (bytes) for the AEF Controller Kafka producer. 10 MB by default.",
    )
    tracing_max_payload_size: int = Field(
        default=10_000,
        validation_alias="TRACING_MAX_PAYLOAD_SIZE",
        description="Maximum serialized request/response payload size stored in trace attributes.",
    )
    aef_agent_id: str = Field(
        default="prototype-treasurer",
        validation_alias=AliasChoices("AGENT_ID", "AEF_AGENT_ID"),
        description="agent-id Kafka header — КЭ модуля агента или prototype-маска (см. docs_for_SDK/instructions/prototype_tracing.md).",
    )
    aef_cluster_id: str = Field(
        default="prototype-cluster-id1",
        validation_alias=AliasChoices("CLUSTER_ID", "AEF_CLUSTER_ID"),
        description="cluster-id Kafka header — наименование кластера Kubernetes или prototype-маска.",
    )
    aef_namespace: str = Field(
        default="prototype-namespace1",
        validation_alias=AliasChoices("POD_NAMESPACE", "AEF_NAMESPACE"),
        description="namespace Kafka header — наименование NameSpace или prototype-маска.",
    )
    aef_distributive: str = Field(
        default="prototype-distributive1",
        validation_alias=AliasChoices("DISTRIBUTIVE", "AEF_DISTRIBUTIVE"),
        description="distributive Kafka header — ссылка на дистрибутив агента или prototype-маска.",
    )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience export
settings = get_settings()
