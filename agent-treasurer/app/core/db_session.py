"""PostgreSQL connection settings for agent_app, loaded from a TOML file.

Mirrors the pattern used in db_app/app/db/session.py: the path to a TOML
file is taken from the AGENT_DB_SECRETS env var, and the file is parsed
through pydantic-settings' TomlConfigSettingsSource. The resolved URL is
used by the LangGraph PostgreSQL checkpointer and by raw asyncpg calls
for schema bootstrap.
"""

import logging
import os

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, TomlConfigSettingsSource

log = logging.getLogger("agent_app.db_session")

config_file: str | None = os.environ.get("AGENT_DB_SECRETS")


class Database(BaseModel):
    """Connection settings for a single database."""

    dialect: str | None = Field(default="")
    username: str | None = Field(default="")
    password: SecretStr | None = Field(default="")
    host: str | None = Field(default="")
    port: str | None = Field(default="")
    database: str | None = Field(default="")
    query: str | None = Field(default="")
    url: str | None = Field(default="")


class Connections(BaseModel):
    """All database connections."""

    model_config = {"populate_by_name": True}

    db1: Database = Field(alias="deal-agent-db")


class ConnectionsConfig(BaseSettings):
    """Database settings loaded from a TOML file pointed to by AGENT_DB_SECRETS."""

    connections: Connections

    @classmethod
    def settings_customise_sources(cls, settings_cls, **kwargs):
        return (TomlConfigSettingsSource(settings_cls, toml_file=config_file),)


def _normalize_dialect(dialect: str | None) -> str:
    """Strip SQLAlchemy driver suffix (e.g. ``postgresql+asyncpg`` → ``postgresql``).

    LangGraph's AsyncPostgresSaver (psycopg) and raw asyncpg both expect a
    plain ``postgresql://`` URL — no ``+driver`` part. Stripping lets the
    same TOML serve both db_app (SQLAlchemy form) and agent_app.
    """
    if not dialect:
        return ""
    return dialect.split("+", 1)[0]


def _build_url(db: Database) -> str:
    """Build a driver-less ``postgresql://...`` URL for psycopg/asyncpg."""
    if db.url:
        scheme, sep, rest = db.url.partition("://")
        return f"{_normalize_dialect(scheme)}{sep}{rest}" if sep else db.url
    query = "" if db.query is None else f"?{db.query}" if db.query else ""
    password = db.password.get_secret_value() if db.password else ""
    dialect = _normalize_dialect(db.dialect) or "postgresql"
    return f"{dialect}://{db.username}:{password}@{db.host}:{db.port}/{db.database}{query}"


cfg = ConnectionsConfig()
db_info: Database = cfg.connections.db1
db_url: str = _build_url(db_info)

log.info(
    f"db_url_resolved. host={db_info.host} port={db_info.port} database={db_info.database}"
)