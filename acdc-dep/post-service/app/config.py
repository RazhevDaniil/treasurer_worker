import os
import logging
import time
from pathlib import Path

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, TomlConfigSettingsSource

_LOGGER = logging.getLogger(__name__)

def get_secret_file_from_env() -> str:
    file_path = os.getenv("postServiceCreds", ".secrets/postServiceCreds.properties")

    secret_path = Path(file_path)

    if not secret_path.is_absolute():
        secret_path = Path("/") / secret_path

    _LOGGER.info("Попытка чтения секрета из: %s", secret_path)

    # Ждем появления файла с задержкой
    max_wait_seconds = 300  # Максимальное время ожидания (5 минут)
    wait_interval = 1  # Интервал проверки в секундах
    elapsed_seconds = 0

    while not secret_path.exists():
        if elapsed_seconds >= max_wait_seconds:
            raise FileNotFoundError(f"Файл секрета не найден после ожидания: {secret_path}")
        _LOGGER.info("Файл секрета не найден, ожидание... (%d/%d сек)", elapsed_seconds, max_wait_seconds)
        time.sleep(wait_interval)
        elapsed_seconds += wait_interval

    # Если путь существует, но это не файл (например, директория)
    if not secret_path.is_file():
        raise RuntimeError(f"Путь секрета не является файлом: {secret_path}")

    _LOGGER.info("Файл секрета найден: %s", secret_path)
    return str(secret_path)


class PostServiceCred(BaseModel):
    username: str
    password: SecretStr


class Connections(BaseModel):
    model_config = {"populate_by_name": True}

    post_service_cred: PostServiceCred = Field(alias="post-service-cred")


class SecretConfig(BaseSettings):
    connections: Connections

    @classmethod
    def settings_customise_sources(cls, settings_cls, **kwargs):
        return (
            TomlConfigSettingsSource(
                settings_cls,
                toml_file=get_secret_file_from_env(),
            ),
        )


class Settings(BaseSettings):
    # App
    app_name: str = "Mail Gateway Service"
    app_port: int = 555
    log_level: str = "INFO"

    # Agent
    agent_url: str = ""
    agent_timeout_seconds: int = 30
    agent_batch_size: int = 20
    agent_poll_seconds: int = 5

    # Kafka (outbound: mail → agent)
    kafka_mail_topic: str = ""
    adapter_brokers: str = ""
    kafka_producer_retries: int = 5
    kafka_retry_backoff_ms: int = 200
    kafka_retry_backoff_max_ms: int = 5000
    kafka_flush_timeout_seconds: float = 10.0

    # SMTP
    smtp_host: str = Field(default="", validation_alias="MAIL_SMTP_HOST")
    smtp_port: int = 25
    smtp_from_address: str = "AEFContainer-dev@alpha-exchtest.sbrf.ru"
    smtp_batch_size: int = 20
    smtp_poll_seconds: int = 5

    # IMAP
    imap_host: str = Field(default="", validation_alias="MAIL_IMAP_HOST")
    imap_port: int = 993
    imap_address: str = "AEFContainer-dev@alpha-exchtest.sbrf.ru"
    imap_mailbox: str = "INBOX"
    imap_poll_seconds: int = 10
    imap_use_idle: bool = False

    # Auth
    username: str = ""
    password: SecretStr | None = None

    # Retry policies
    max_attempts: int = 20
    backoff_min_seconds: int = 1
    backoff_max_seconds: int = 120

    # DB service
    db_app_url: str = Field(default="", validation_alias="DB_URL")

    @classmethod
    def load(cls) -> "Settings":
        secret_cfg = SecretConfig()

        username = secret_cfg.connections.post_service_cred.username
        password = secret_cfg.connections.post_service_cred.password

        return cls(
            username=username,
            password=password,
            # imap_address=username,
            # smtp_from_address=username,
        )

    @property
    def password_value(self) -> str:
        return self.password.get_secret_value() if self.password else ""

settings = Settings.load()
_LOGGER.info(
    "Settings loaded: app_name=%s imap_host=%s smtp_host=%s username=%s",
    settings.app_name,
    settings.imap_host,
    settings.smtp_host,
    settings.username,
)
