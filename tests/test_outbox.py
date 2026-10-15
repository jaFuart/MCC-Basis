from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.broker import PAYMENT_EVENT
from app.models import Base, Outbox, Payment
from app.outbox import publish_batch


@pytest.fixture
async def outbox_database(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def seed(factory, *, delay_seconds=0):
    now = datetime.now(UTC)
    payment = Payment(
        id=uuid4(),
        amount=Decimal("1.00"),
        currency="RUB",
        description="outbox test",
        payment_metadata={},
        status="pending",
        idempotency_key=str(uuid4()),
        request_fingerprint="f" * 64,
        webhook_url="http://webhook.test/result",
        created_at=now,
        processing_attempts=0,
    )
    event = Outbox(
        id=uuid4(),
        payment_id=payment.id,
        event_type=PAYMENT_EVENT,
        payload={"payment_id": str(payment.id), "attempt": 1},
        created_at=now,
        available_at=now + timedelta(seconds=delay_seconds),
        attempts=0,
    )
    async with factory() as session, session.begin():
        session.add(payment)
        session.add(event)
    return now, event


async def read_event(factory, event_id):
    async with factory() as session:
        return await session.get(Outbox, event_id)


async def test_only_confirmed_events_are_marked_published(outbox_database):
    now, event = await seed(outbox_database)
    publisher = AsyncMock()
    assert await publish_batch(outbox_database, publisher, now=now) == 1
    stored = await read_event(outbox_database, event.id)
    assert stored.published_at is not None
    assert stored.attempts == 1
    publisher.assert_awaited_once_with(PAYMENT_EVENT, event.payload, message_id=str(event.id))
    assert await publish_batch(outbox_database, publisher, now=now) == 0
    assert publisher.await_count == 1


async def test_failure_stays_unpublished_and_retries_after_delay(outbox_database):
    now, event = await seed(outbox_database)
    publisher = AsyncMock(side_effect=RuntimeError("not confirmed"))
    assert await publish_batch(outbox_database, publisher, now=now) == 0
    stored = await read_event(outbox_database, event.id)
    assert stored.published_at is None
    assert stored.attempts == 1
    assert "not confirmed" in stored.last_error
    assert (stored.available_at - now.replace(tzinfo=None)).total_seconds() == 1
    assert await publish_batch(outbox_database, publisher, now=now + timedelta(seconds=0.5)) == 0
    assert publisher.await_count == 1
    publisher.side_effect = None
    assert await publish_batch(outbox_database, publisher, now=now + timedelta(seconds=1)) == 1
    stored = await read_event(outbox_database, event.id)
    assert stored.attempts == 2
    assert stored.last_error is None
    assert stored.published_at is not None


async def test_scheduled_payment_retry_is_not_published_early(outbox_database):
    now, event = await seed(outbox_database, delay_seconds=10)
    publisher = AsyncMock()
    assert await publish_batch(outbox_database, publisher, now=now) == 0
    publisher.assert_not_awaited()
    assert await publish_batch(outbox_database, publisher, now=now + timedelta(seconds=10)) == 1
    assert (await read_event(outbox_database, event.id)).published_at is not None


async def test_one_bad_publication_does_not_abandon_other_records(outbox_database):
    now, failed = await seed(outbox_database)
    _, successful = await seed(outbox_database)

    async def publish(event_type, payload, *, message_id):
        if message_id == str(failed.id):
            raise RuntimeError("temporarily unroutable")

    assert await publish_batch(outbox_database, publish, now=now + timedelta(seconds=1)) == 1
    assert (await read_event(outbox_database, failed.id)).published_at is None
    assert (await read_event(outbox_database, successful.id)).published_at is not None


async def test_outbox_and_payment_rollback_together(outbox_database):
    now = datetime.now(UTC)
    payment_id = uuid4()
    with pytest.raises(RuntimeError):
        async with outbox_database() as session, session.begin():
            session.add(
                Payment(
                    id=payment_id,
                    amount=Decimal("1.00"),
                    currency="RUB",
                    description="rollback",
                    payment_metadata={},
                    status="pending",
                    idempotency_key=str(uuid4()),
                    request_fingerprint="f" * 64,
                    webhook_url="http://webhook.test/result",
                    created_at=now,
                    processing_attempts=0,
                )
            )
            session.add(
                Outbox(
                    id=uuid4(),
                    payment_id=payment_id,
                    event_type=PAYMENT_EVENT,
                    payload={"payment_id": str(payment_id), "attempt": 1},
                    created_at=now,
                    available_at=now,
                    attempts=0,
                )
            )
            await session.flush()
            raise RuntimeError("simulate failed request transaction")
    async with outbox_database() as session:
        assert await session.get(Payment, payment_id) is None
        assert (await session.scalars(select(Outbox))).all() == []
