"""One consumer: gateway emulation, stored result, webhook, retry and DLQ."""

import asyncio
import base64
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import (
    DEAD_LETTER_EVENT,
    MAX_ATTEMPTS,
    PAYMENT_EVENT,
    broker,
    declare_topology,
    payment_exchange,
    payment_queue,
    publish_event,
    publish_infrastructure_retry,
)
from app.config import get_settings
from app.db import SessionFactory, engine
from app.models import Outbox, Payment
from app.outbox import run_outbox

logger = logging.getLogger(__name__)


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=MAX_ATTEMPTS, strict=True)


class UnknownPaymentError(Exception):
    pass


async def emulate_gateway(payment: Payment) -> str:
    settings = get_settings()
    await asyncio.sleep(random.uniform(settings.gateway_delay_min, settings.gateway_delay_max))
    return "succeeded" if random.random() < settings.gateway_success_rate else "failed"


def webhook_payload(payment: Payment) -> dict[str, Any]:
    def iso(value: datetime) -> str:
        # SQLite tests omit time zone information; production PostgreSQL uses
        # TIMESTAMPTZ. Both serialize as UTC for this contract.
        return value.replace(tzinfo=UTC).isoformat() if value.tzinfo is None else value.isoformat()

    return {
        "event": "payment.processed",
        "payment_id": str(payment.id),
        "amount": str(payment.amount),
        "currency": payment.currency,
        "description": payment.description,
        "metadata": payment.payment_metadata,
        "status": payment.status,
        "created_at": iso(payment.created_at),
        "processed_at": iso(payment.processed_at),
    }


async def send_webhook(payment: Payment) -> None:
    async with httpx.AsyncClient(
        timeout=get_settings().webhook_timeout,
        follow_redirects=False,
        trust_env=False,
    ) as client:
        response = await client.post(
            payment.webhook_url,
            json=webhook_payload(payment),
            headers={
                "Idempotency-Key": str(payment.id),
                "X-Webhook-ID": str(payment.id),
                "X-Webhook-Event": "payment.processed",
            },
        )
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"Webhook returned HTTP {response.status_code}")


def _finished(payment: Payment, event: PaymentEvent) -> bool:
    return (
        payment.webhook_delivered_at is not None
        or payment.dead_lettered_at is not None
        or event.attempt < payment.processing_attempts
        # A failed attempt already has a durable retry or DLQ record. A
        # duplicate must not execute that attempt again or bypass its delay.
        or (event.attempt == payment.processing_attempts and payment.last_error is not None)
    )


async def _record_failure(
    session: AsyncSession, payment: Payment, event: PaymentEvent, exc: Exception
) -> None:
    now = datetime.now(UTC)
    payment.last_error = f"{type(exc).__name__}: {exc}"[:2000]
    payload: dict[str, Any] = {"payment_id": str(payment.id), "attempt": event.attempt + 1}
    if event.attempt < MAX_ATTEMPTS:
        event_type = PAYMENT_EVENT
        delay = get_settings().retry_base_delay * 2 ** (event.attempt - 1)
        available_at = now + timedelta(seconds=delay)
    else:
        event_type = DEAD_LETTER_EVENT
        available_at = now
        payment.dead_lettered_at = now
        # Exhausted gateway infrastructure errors are a failed payment. An
        # already stored gateway result stays intact if only delivery failed.
        if payment.status == "pending":
            payment.status = "failed"
            payment.processed_at = now
        payload = {
            "payment_id": str(payment.id),
            "attempt": event.attempt,
            "error": payment.last_error,
            "failed_at": now.isoformat(),
        }
    session.add(
        Outbox(
            id=uuid4(),
            payment_id=payment.id,
            event_type=event_type,
            payload=payload,
            available_at=available_at,
            created_at=now,
            attempts=0,
        )
    )


async def process_payment(
    event: PaymentEvent,
    *,
    session_factory: async_sessionmaker[AsyncSession] = SessionFactory,
    gateway: Callable[[Payment], Awaitable[str]] = emulate_gateway,
    webhook: Callable[[Payment], Awaitable[None]] = send_webhook,
) -> None:
    # Store the gateway outcome in its own committed transaction before HTTP.
    # Redelivery after a webhook failure or worker restart never charges again.
    async with session_factory() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise UnknownPaymentError(str(event.payment_id))
        if _finished(payment, event):
            return
        payment.processing_attempts = event.attempt
        payment.last_error = None
        if payment.status == "pending":
            try:
                payment.status = await gateway(payment)
                if payment.status not in {"succeeded", "failed"}:
                    raise ValueError("Gateway returned an unsupported status")
                payment.processed_at = datetime.now(UTC)
            except Exception as exc:
                # Keep pending on retriable gateway exceptions, unlike an
                # ordinary declined payment which is terminal and notified.
                payment.status = "pending"
                await _record_failure(session, payment, event, exc)
                return

    async with session_factory() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise UnknownPaymentError(str(event.payment_id))
        if _finished(payment, event):
            return
        try:
            await webhook(payment)
        except Exception as exc:
            await _record_failure(session, payment, event, exc)
            logger.warning(
                "Payment %s attempt %s failed: %s", payment.id, event.attempt, payment.last_error
            )
        else:
            payment.webhook_delivered_at = datetime.now(UTC)
            payment.last_error = None
            logger.info("Payment %s completed with status %s", payment.id, payment.status)


def _safe_payload(body: Any) -> Any:
    if isinstance(body, bytes):
        return {"raw_body_base64": base64.b64encode(body).decode("ascii")}
    return body


async def _publish_poison(body: Any, message: RabbitMessage, error: str) -> None:
    await publish_event(
        DEAD_LETTER_EVENT,
        {
            "original": _safe_payload(body),
            "error": error[:2000],
            "failed_at": datetime.now(UTC).isoformat(),
        },
        message_id=message.message_id or str(uuid4()),
    )


async def handle_message(body: Any, message: RabbitMessage) -> None:
    """Ack only after the result or a durable replacement has been recorded."""
    try:
        event = PaymentEvent.model_validate(body)
    except ValidationError as exc:
        try:
            await _publish_poison(body, message, f"Malformed payment event: {exc}")
        except Exception:
            logger.exception("Unable to publish malformed event to DLQ")
            await message.nack(requeue=True)
        else:
            await message.ack()
        return

    try:
        await process_payment(event)
    except UnknownPaymentError:
        try:
            await _publish_poison(body, message, f"Payment {event.payment_id} does not exist")
        except Exception:
            logger.exception("Unable to publish unknown payment to DLQ")
            await message.nack(requeue=True)
        else:
            await message.ack()
        return
    except Exception as exc:
        # PostgreSQL failure cannot be recorded in its own outbox. Use durable
        # Rabbit TTL queues instead of an in-memory counter or infinite nack.
        headers = dict(message.headers or {})
        try:
            infrastructure_attempt = int(headers.get("x-infrastructure-attempt", 1))
        except (TypeError, ValueError):
            infrastructure_attempt = 1
        error = f"{type(exc).__name__}: {exc}"[:2000]
        try:
            if infrastructure_attempt >= MAX_ATTEMPTS:
                await _publish_poison(body, message, f"Infrastructure attempts exhausted: {error}")
            else:
                headers["x-infrastructure-attempt"] = infrastructure_attempt + 1
                headers["x-last-error"] = error
                await publish_infrastructure_retry(
                    body,
                    attempt=infrastructure_attempt + 1,
                    message_id=message.message_id or str(uuid4()),
                    headers=headers,
                )
        except Exception:
            logger.exception("Infrastructure retry publication failed; retaining original message")
            await message.nack(requeue=True)
        else:
            await message.ack()
        return
    await message.ack()


async def safe_decoder(
    message: RabbitMessage, original_decoder: Callable[..., Awaitable[Any]]
) -> Any:
    # FastStream decodes before invoking a handler. Preserve malformed bytes
    # so they reach explicit validation/DLQ handling instead of raising there.
    try:
        return await original_decoder(message)
    except Exception:
        return message.body


@broker.subscriber(
    payment_queue,
    payment_exchange,
    ack_policy=AckPolicy.MANUAL,
    decoder=safe_decoder,
)
async def consume_payment(message: RabbitMessage) -> None:
    # Validate inside the acknowledged handler so malformed payloads cannot
    # fail dependency parsing before our poison-message handling runs.
    try:
        body = await message.decode()
    except Exception as exc:
        try:
            await _publish_poison(message.body, message, f"Cannot decode event: {exc}")
        except Exception:
            logger.exception("Unable to publish undecodable event to DLQ")
            await message.nack(requeue=True)
        else:
            await message.ack()
        return
    await handle_message(body, message)


app = FastStream(broker)
_stop = asyncio.Event()
_outbox_task: asyncio.Task[None] | None = None


@app.on_startup
async def initialize() -> None:
    await broker.connect()
    await declare_topology()


@app.after_startup
async def start_outbox() -> None:
    global _outbox_task
    _stop.clear()
    _outbox_task = asyncio.create_task(run_outbox(_stop), name="outbox-relay")


@app.on_shutdown
async def stop_outbox() -> None:
    _stop.set()
    if _outbox_task is not None:
        await _outbox_task


@app.after_shutdown
async def close_database() -> None:
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(app.run())
