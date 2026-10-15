import asyncio
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings
from app.db import get_session
from app.main import app
from app.models import Base, Outbox, Payment


@pytest.fixture
async def api(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/payments.sqlite")

    @event.listens_for(engine.sync_engine, "connect")
    def foreign_keys(connection, record):
        connection.execute("PRAGMA foreign_keys=ON")

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async def sessions():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = sessions
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": get_settings().api_key},
    ) as client:
        yield client, factory
    app.dependency_overrides.clear()
    await engine.dispose()


def payment_body(**changes):
    return {
        "amount": "120.50",
        "currency": "RUB",
        "description": "Order 123",
        "metadata": {"order_id": 123},
        "webhook_url": "http://client.test/webhook",
        **changes,
    }


@pytest.mark.parametrize(
    "path", ["/health", "/docs", "/openapi.json", "/api/v1/payments/" + str(uuid4())]
)
async def test_all_endpoints_require_api_key(api, path):
    client, _ = api
    response = await client.get(path, headers={"X-API-Key": "wrong"})
    assert response.status_code == 401
    client.headers.pop("X-API-Key")
    assert (await client.get(path)).status_code == 401
    assert (await client.post("/api/v1/payments", json=payment_body())).status_code == 401


async def test_atomic_create_and_read(api):
    client, factory = api
    response = await client.post(
        "/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": "first"}
    )
    assert response.status_code == 202
    accepted = response.json()
    assert accepted["status"] == "pending"
    details = (await client.get(f"/api/v1/payments/{accepted['payment_id']}")).json()
    assert details["amount"] == "120.50"
    assert details["metadata"] == {"order_id": 123}
    assert details["processed_at"] is None
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        outbox = await session.scalar(select(Outbox))
        assert str(outbox.payment_id) == accepted["payment_id"]
        assert outbox.payload == {"payment_id": accepted["payment_id"], "attempt": 1}
        assert outbox.published_at is None


async def test_concurrent_duplicates_and_body_conflict(api):
    client, factory = api

    async def create(body):
        return await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": "same"})

    responses = await asyncio.gather(*(create(payment_body()) for _ in range(8)))
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert (await create(payment_body(amount="121.00"))).status_code == 409
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_amount_is_canonical_for_idempotency(api):
    client, _ = api
    responses = []
    for amount in ["10", "10.0", "10.00"]:
        response = await client.post(
            "/api/v1/payments",
            json=payment_body(amount=amount),
            headers={"Idempotency-Key": "decimal"},
        )
        assert response.status_code == 202
        responses.append(response.json())
    assert responses[0] == responses[1] == responses[2]


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "NaN"},
        {"amount": "10000000000000000"},
        {"currency": "GBP"},
        {"webhook_url": "file:///tmp/hook"},
        {"unexpected": "field"},
    ],
)
async def test_validation_does_not_write(api, changes):
    client, factory = api
    response = await client.post(
        "/api/v1/payments", json=payment_body(**changes), headers={"Idempotency-Key": "invalid"}
    )
    assert response.status_code == 422
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_missing_idempotency_key_and_unknown_payment(api):
    client, _ = api
    assert (await client.post("/api/v1/payments", json=payment_body())).status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422
