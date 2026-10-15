"""Transactional outbox relay with at-least-once delivery."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import publish_event
from app.config import get_settings
from app.db import SessionFactory
from app.models import Outbox

logger = logging.getLogger(__name__)
Publisher = Callable[..., Awaitable[None]]


async def publish_batch(
    session_factory: async_sessionmaker[AsyncSession] = SessionFactory,
    publisher: Publisher = publish_event,
    *,
    now: datetime | None = None,
) -> int:
    """Keep row locks through publish and commit; crash duplicates are safe."""
    settings = get_settings()
    now = now or datetime.now(UTC)
    published = 0
    async with session_factory() as session, session.begin():
        events = (
            await session.scalars(
                select(Outbox)
                .where(Outbox.published_at.is_(None), Outbox.available_at <= now)
                .order_by(Outbox.available_at, Outbox.created_at, Outbox.id)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for event in events:
            event.attempts += 1
            try:
                await publisher(
                    event.event_type,
                    event.payload,
                    message_id=str(event.id),
                )
            except Exception as exc:
                event.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                # Publication itself retries indefinitely: abandoning an
                # outbox record would violate the delivery guarantee.
                delay = min(60.0, settings.retry_base_delay * 2 ** min(event.attempts - 1, 6))
                event.available_at = now + timedelta(seconds=delay)
                logger.warning("Outbox publication failed for %s: %s", event.id, event.last_error)
            else:
                event.published_at = datetime.now(UTC)
                event.last_error = None
                published += 1
    return published


async def run_outbox(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await publish_batch()
        except Exception:
            logger.exception("Outbox relay failed; unpublished records remain in PostgreSQL")
        try:
            await asyncio.wait_for(stop.wait(), timeout=get_settings().outbox_poll_interval)
        except TimeoutError:
            pass
