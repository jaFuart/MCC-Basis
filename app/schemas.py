from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, JsonValue, field_validator


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: Annotated[AnyHttpUrl, Field(max_length=2048)]

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value


class PaymentDetails(PaymentAccepted):
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue]
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
    processing_attempts: int
    webhook_delivered_at: datetime | None
    dead_lettered_at: datetime | None
    last_error: str | None
