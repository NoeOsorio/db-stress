from __future__ import annotations

from typing import Optional
from urllib.parse import quote

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Postgres ---
    # Porter's RDS datastore add-on injects DB_URL / DB_HOST / DB_PORT / DB_USER /
    # DB_PASS / DB_NAME. We also accept the legacy POSTGRES_URL / PG_* names so
    # local development setups keep working.
    POSTGRES_URL: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DB_URL", "POSTGRES_URL"),
    )
    PG_HOST: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DB_HOST", "PG_HOST"),
    )
    PG_PORT: int = Field(
        default=5432,
        validation_alias=AliasChoices("DB_PORT", "PG_PORT"),
    )
    PG_USER: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DB_USER", "PG_USER"),
    )
    PG_PASSWORD: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DB_PASS", "DB_PASSWORD", "PG_PASSWORD"),
    )
    PG_DATABASE: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DB_NAME", "PG_DATABASE"),
    )
    PG_SSLMODE: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DB_SSLMODE", "PG_SSLMODE"),
    )

    # --- Redis ---
    # Porter's ElastiCache datastore add-on injects REDIS_URL / REDIS_HOST /
    # REDIS_PORT / REDIS_PASS. We accept REDIS_PASSWORD as an alias for
    # compatibility with existing setups.
    REDIS_URL: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("REDIS_URL"),
    )
    REDIS_HOST: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("REDIS_HOST"),
    )
    REDIS_PORT: int = Field(
        default=6379,
        validation_alias=AliasChoices("REDIS_PORT"),
    )
    REDIS_PASSWORD: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("REDIS_PASS", "REDIS_PASSWORD"),
    )
    REDIS_DB: int = 0
    REDIS_TLS: bool = False

    # --- Safety caps ---
    MAX_JOB_DURATION_SEC: int = 600
    MAX_WORKERS_PER_JOB: int = 50
    MAX_CONNECTIONS_PER_JOB: int = 200
    MAX_CONCURRENT_JOBS: int = 5
    ALLOW_DISK_WORKLOADS: bool = True

    # Namespace used for any objects this app creates in the target DB.
    # Always prefixed so cleanup is unambiguous.
    OBJECT_PREFIX: str = "stresstest_"

    def postgres_dsn(self) -> Optional[str]:
        if self.POSTGRES_URL:
            return self.POSTGRES_URL
        if not (self.PG_HOST and self.PG_USER and self.PG_DATABASE):
            return None
        pw = f":{quote(self.PG_PASSWORD)}" if self.PG_PASSWORD else ""
        dsn = (
            f"postgresql://{quote(self.PG_USER)}{pw}"
            f"@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DATABASE}"
        )
        if self.PG_SSLMODE:
            dsn += f"?sslmode={self.PG_SSLMODE}"
        return dsn

    def redis_url(self) -> Optional[str]:
        if self.REDIS_URL:
            return self.REDIS_URL
        if not self.REDIS_HOST:
            return None
        scheme = "rediss" if self.REDIS_TLS else "redis"
        auth = f":{quote(self.REDIS_PASSWORD)}@" if self.REDIS_PASSWORD else ""
        return f"{scheme}://{auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


settings = Settings()
