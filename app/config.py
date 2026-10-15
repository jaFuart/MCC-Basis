from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://mcc:mcc@localhost:5432/mcc"
    rabbitmq_url: str = "amqp://mcc:mcc@localhost:5672/"
    api_key: str = Field(default="local-development-key-change-me", min_length=16)
    gateway_delay_min: float = Field(default=2, ge=0)
    gateway_delay_max: float = Field(default=5, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    webhook_timeout: float = Field(default=10, gt=0)
    retry_base_delay: float = Field(default=1, gt=0)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)

    @model_validator(mode="after")
    def check_delay_range(self) -> "Settings":
        if self.gateway_delay_max < self.gateway_delay_min:
            raise ValueError("GATEWAY_DELAY_MAX must be >= GATEWAY_DELAY_MIN")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
