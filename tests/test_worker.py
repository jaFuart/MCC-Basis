import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from faststream.rabbit import TestRabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app import worker
from app.broker import DEAD_LETTER_EVENT, PAYMENT_EVENT, broker, payment_exchange
from app.models import Base, Outbox, Payment
from app.worker import PaymentEvent, process_payment


@pytest.fixture
async def worker_database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def create_payment(factory) -> Payment:
    payment = Payment(
        id=uuid4(),
        amount=Decimal("125.50"),
        currency="RUB",
        description="test payment",
        payment_metadata={"order_id": "A-42"},
        status="pending",
        idempotency_key=str(uuid4()),
        request_fingerprint="f" * 64,
        webhook_url="http://webhook.test/result",
        created_at=datetime.now(UTC),
        processing_attempts=0,
    )
    async with factory() as session, session.begin():
        session.add(payment)
    return payment


async def read_payment(factory, payment_id):
    async with factory() as session:
        return await session.get(Payment, payment_id)


@pytest.mark.parametrize("gateway_status", ["succeeded", "failed"])
async def test_gateway_result_webhook_and_duplicate(worker_database, gateway_status):
    payment = await create_payment(worker_database)
    gateway = AsyncMock(return_value=gateway_status)
    webhook = AsyncMock()
    event = PaymentEvent(payment_id=payment.id)
    for _ in range(2):
        await process_payment(
            event, session_factory=worker_database, gateway=gateway, webhook=webhook
        )
    stored = await read_payment(worker_database, payment.id)
    assert stored.status == gateway_status
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert stored.processing_attempts == 1
    gateway.assert_awaited_once()
    webhook.assert_awaited_once()


async def test_webhook_retries_three_times_with_exponential_outbox_delay(worker_database):
    payment = await create_payment(worker_database)
    gateway = AsyncMock(return_value="succeeded")
    webhook = AsyncMock(side_effect=RuntimeError("webhook unavailable"))
    for attempt in range(1, 4):
        event = PaymentEvent(payment_id=payment.id, attempt=attempt)
        await process_payment(
            event, session_factory=worker_database, gateway=gateway, webhook=webhook
        )
        # Duplicate publication of the failed attempt must not bypass delay.
        await process_payment(
            event, session_factory=worker_database, gateway=gateway, webhook=webhook
        )
    stored = await read_payment(worker_database, payment.id)
    assert stored.status == "succeeded"
    assert stored.processing_attempts == 3
    assert stored.dead_lettered_at is not None
    assert stored.webhook_delivered_at is None
    assert "webhook unavailable" in stored.last_error
    gateway.assert_awaited_once()
    assert webhook.await_count == 3
    async with worker_database() as session:
        events = list((await session.scalars(select(Outbox).order_by(Outbox.created_at))).all())
    assert [event.event_type for event in events] == [
        PAYMENT_EVENT,
        PAYMENT_EVENT,
        DEAD_LETTER_EVENT,
    ]
    assert [event.payload["attempt"] for event in events] == [2, 3, 3]
    assert (events[0].available_at - events[0].created_at).total_seconds() == 1
    assert (events[1].available_at - events[1].created_at).total_seconds() == 2
    assert events[2].payload["error"] == stored.last_error


async def test_retry_recovery_never_calls_gateway_again(worker_database):
    payment = await create_payment(worker_database)
    gateway = AsyncMock(return_value="succeeded")
    webhook = AsyncMock(side_effect=[RuntimeError("temporary failure"), None])
    for attempt in (1, 2):
        await process_payment(
            PaymentEvent(payment_id=payment.id, attempt=attempt),
            session_factory=worker_database,
            gateway=gateway,
            webhook=webhook,
        )
    stored = await read_payment(worker_database, payment.id)
    assert stored.webhook_delivered_at is not None
    assert stored.last_error is None
    assert stored.dead_lettered_at is None
    assert stored.processing_attempts == 2
    gateway.assert_awaited_once()


async def test_restart_between_gateway_commit_and_webhook_resumes_stored_result(worker_database):
    payment = await create_payment(worker_database)
    gateway = AsyncMock(return_value="succeeded")
    event = PaymentEvent(payment_id=payment.id)

    async def stopped_webhook(_):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await process_payment(
            event, session_factory=worker_database, gateway=gateway, webhook=stopped_webhook
        )
    before_restart = await read_payment(worker_database, payment.id)
    assert before_restart.status == "succeeded"
    assert before_restart.processed_at is not None
    assert before_restart.webhook_delivered_at is None
    webhook = AsyncMock()
    await process_payment(event, session_factory=worker_database, gateway=gateway, webhook=webhook)
    after_restart = await read_payment(worker_database, payment.id)
    assert after_restart.processed_at == before_restart.processed_at
    assert after_restart.webhook_delivered_at is not None
    gateway.assert_awaited_once()


async def test_gateway_exception_is_retried_and_exhausted_to_dlq(worker_database):
    payment = await create_payment(worker_database)
    gateway = AsyncMock(side_effect=RuntimeError("gateway unavailable"))
    webhook = AsyncMock()
    for attempt in range(1, 4):
        await process_payment(
            PaymentEvent(payment_id=payment.id, attempt=attempt),
            session_factory=worker_database,
            gateway=gateway,
            webhook=webhook,
        )
    stored = await read_payment(worker_database, payment.id)
    assert stored.status == "failed"
    assert stored.processing_attempts == 3
    assert stored.dead_lettered_at is not None
    assert stored.processed_at is not None
    assert gateway.await_count == 3
    webhook.assert_not_awaited()


def message(headers=None):
    return SimpleNamespace(
        headers=headers or {}, message_id=str(uuid4()), ack=AsyncMock(), nack=AsyncMock()
    )


async def test_malformed_event_is_confirmed_to_dlq_before_ack(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(worker, "publish_event", publish)
    msg = message()
    await worker.handle_message({"payment_id": "not-a-uuid"}, msg)
    assert publish.await_args.args[0] == DEAD_LETTER_EVENT
    assert "Malformed" in publish.await_args.args[1]["error"]
    msg.ack.assert_awaited_once()
    msg.nack.assert_not_awaited()


async def test_unknown_payment_is_dead_lettered(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(worker, "publish_event", publish)
    monkeypatch.setattr(
        worker, "process_payment", AsyncMock(side_effect=worker.UnknownPaymentError)
    )
    msg = message()
    await worker.handle_message({"payment_id": str(uuid4()), "attempt": 1}, msg)
    assert "does not exist" in publish.await_args.args[1]["error"]
    msg.ack.assert_awaited_once()


@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_database_failures_use_broker_backed_bounded_retries(monkeypatch, attempt):
    retry = AsyncMock()
    dead_letter = AsyncMock()
    monkeypatch.setattr(
        worker, "process_payment", AsyncMock(side_effect=RuntimeError("db offline"))
    )
    monkeypatch.setattr(worker, "publish_infrastructure_retry", retry)
    monkeypatch.setattr(worker, "publish_event", dead_letter)
    msg = message({"x-infrastructure-attempt": attempt})
    body = {"payment_id": str(uuid4()), "attempt": 1}
    await worker.handle_message(body, msg)
    if attempt < 3:
        assert retry.await_args.kwargs["attempt"] == attempt + 1
        assert retry.await_args.kwargs["headers"]["x-infrastructure-attempt"] == attempt + 1
        dead_letter.assert_not_awaited()
    else:
        assert dead_letter.await_args.args[0] == DEAD_LETTER_EVENT
        retry.assert_not_awaited()
    msg.ack.assert_awaited_once()
    msg.nack.assert_not_awaited()


async def test_cannot_confirm_retry_keeps_original_message(monkeypatch):
    monkeypatch.setattr(
        worker, "process_payment", AsyncMock(side_effect=RuntimeError("db offline"))
    )
    monkeypatch.setattr(
        worker,
        "publish_infrastructure_retry",
        AsyncMock(side_effect=RuntimeError("broker offline")),
    )
    msg = message()
    await worker.handle_message({"payment_id": str(uuid4())}, msg)
    msg.ack.assert_not_awaited()
    msg.nack.assert_awaited_once_with(requeue=True)


async def test_malformed_json_reaches_manual_dlq_handler(monkeypatch):
    publish = AsyncMock()
    monkeypatch.setattr(worker, "publish_event", publish)
    async with TestRabbitBroker(broker):
        await broker.publish(
            b"{broken-json",
            exchange=payment_exchange,
            routing_key=PAYMENT_EVENT,
            content_type="application/json",
        )
    assert publish.await_args.args[0] == DEAD_LETTER_EVENT
    assert publish.await_args.args[1]["original"]["raw_body_base64"]


async def test_webhook_http_contract(worker_database, monkeypatch):
    payment = await create_payment(worker_database)
    payment.status = "succeeded"
    payment.processed_at = datetime.now(UTC)
    requests = []

    async def receive(request):
        requests.append(request)
        return httpx.Response(204)

    transport = httpx.MockTransport(receive)
    client_type = httpx.AsyncClient

    def mock_client(**kwargs):
        return client_type(**kwargs, transport=transport)

    monkeypatch.setattr(worker.httpx, "AsyncClient", mock_client)
    await worker.send_webhook(payment)
    assert requests[0].method == "POST"
    assert requests[0].headers["Idempotency-Key"] == str(payment.id)
    assert requests[0].headers["X-Webhook-ID"] == str(payment.id)
    assert b'"amount":"125.50"' in requests[0].content
    assert b'"status":"succeeded"' in requests[0].content


async def test_webhook_redirect_is_an_error(worker_database, monkeypatch):
    payment = await create_payment(worker_database)
    payment.status = "succeeded"
    payment.processed_at = datetime.now(UTC)
    transport = httpx.MockTransport(lambda _: httpx.Response(302, headers={"Location": "/other"}))
    client_type = httpx.AsyncClient
    monkeypatch.setattr(
        worker.httpx, "AsyncClient", lambda **kwargs: client_type(**kwargs, transport=transport)
    )
    with pytest.raises(RuntimeError, match="HTTP 302"):
        await worker.send_webhook(payment)
