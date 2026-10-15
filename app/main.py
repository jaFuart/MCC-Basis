import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Security
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import APIKeyHeader
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import engine, get_session
from app.models import Outbox, Payment, utcnow
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(key: Annotated[str | None, Security(api_key_header)]):
    if key is None or not hmac.compare_digest(key.encode(), get_settings().api_key.encode()):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="MCC Basis",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    dependencies=[Depends(require_api_key)],
    lifespan=lifespan,
)

Session = Annotated[AsyncSession, Depends(get_session)]


@app.get("/health", include_in_schema=False)
async def health(session: Session):
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/openapi.json", include_in_schema=False)
async def openapi():
    return app.openapi()


@app.get("/docs", include_in_schema=False)
async def docs():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="MCC Basis — API")


def accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
):
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key must not be blank")
    payload = body.model_dump(mode="json")
    fingerprint = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    payment_id = uuid4()
    now = utcnow()
    payment = Payment(
        id=payment_id,
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        payment_metadata=body.metadata,
        status="pending",
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
        webhook_url=str(body.webhook_url),
        created_at=now,
    )
    try:
        session.add(payment)
        await session.flush()
        session.add(
            Outbox(
                payment_id=payment_id,
                event_type="payments.new",
                payload={"payment_id": str(payment_id), "attempt": 1},
                created_at=now,
                available_at=now,
            )
        )
        await session.commit()
    except IntegrityError:
        # The database unique constraint also serializes concurrent duplicate POSTs.
        await session.rollback()
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            raise
        if existing.request_fingerprint != fingerprint:
            raise HTTPException(
                status_code=409, detail="Idempotency-Key was already used for a different request"
            ) from None
        return accepted(existing)
    return accepted(payment)


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
async def get_payment(payment_id: UUID, session: Session):
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetails(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        processing_attempts=payment.processing_attempts,
        webhook_delivered_at=payment.webhook_delivered_at,
        dead_lettered_at=payment.dead_lettered_at,
        last_error=payment.last_error,
    )
