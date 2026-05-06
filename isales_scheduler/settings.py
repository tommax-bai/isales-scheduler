"""Process-level settings loaded from environment variables.

Spec: design.md § Migration Plan (env table).
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ISALES_", case_sensitive=False)

    database_url: str = Field(alias="ISALES_DATABASE_URL")
    redis_url: str = Field(alias="ISALES_REDIS_URL")
    telephony_api_base: str = Field(
        default="http://127.0.0.1:8001",
        alias="ISALES_TELEPHONY_API_BASE",
    )
    scheduler_tick_interval: int = Field(default=60, alias="ISALES_SCHEDULER_TICK_INTERVAL")
    scheduler_batch_size: int = Field(default=50, alias="ISALES_SCHEDULER_BATCH_SIZE")
    scheduler_history_n: int = Field(default=3, alias="ISALES_SCHEDULER_HISTORY_N")
    max_concurrency: int = Field(default=8, alias="ISALES_MAX_CONCURRENCY")
    select_timeout_seconds: float = Field(default=1.0, alias="ISALES_SELECT_TIMEOUT_SECONDS")


def load_settings() -> Settings:
    return Settings()
