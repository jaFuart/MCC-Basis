"""Durable RabbitMQ topology and confirmed publication."""

from typing import Any

from faststream.rabbit import Channel, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import get_settings

MAX_ATTEMPTS = 3
PAYMENT_EVENT = "payments.new"
DEAD_LETTER_EVENT = "payments.dlq"

payment_exchange = RabbitExchange("payments", durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", durable=True)
payment_queue = RabbitQueue(
    PAYMENT_EVENT,
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-single-active-consumer": True,
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": DEAD_LETTER_EVENT,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": MAX_ATTEMPTS,
    },
)
dead_letter_queue = RabbitQueue(DEAD_LETTER_EVENT, queue_type=QueueType.QUORUM, durable=True)


def infrastructure_retry_queue(attempt: int) -> RabbitQueue:
    """Broker storage remains available when PostgreSQL cannot store a retry."""
    delay_ms = max(1, round(get_settings().retry_base_delay * 2 ** (attempt - 2) * 1000))
    return RabbitQueue(
        f"payments.infrastructure-retry.{attempt}",
        queue_type=QueueType.QUORUM,
        durable=True,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": payment_exchange.name,
            "x-dead-letter-routing-key": PAYMENT_EVENT,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


broker = RabbitBroker(
    get_settings().rabbitmq_url,
    default_channel=Channel(
        prefetch_count=1,
        publisher_confirms=True,
        on_return_raises=True,
    ),
    graceful_timeout=30,
)


async def declare_topology() -> None:
    """Declare destinations before any consumer or outbox publisher starts."""
    main_exchange = await broker.declare_exchange(payment_exchange)
    dlx = await broker.declare_exchange(dead_letter_exchange)
    main_queue = await broker.declare_queue(payment_queue)
    dlq = await broker.declare_queue(dead_letter_queue)
    await main_queue.bind(main_exchange, routing_key=PAYMENT_EVENT)
    await dlq.bind(dlx, routing_key=DEAD_LETTER_EVENT)
    for attempt in range(2, MAX_ATTEMPTS + 1):
        await broker.declare_queue(infrastructure_retry_queue(attempt))


async def publish_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    message_id: str,
    headers: dict[str, Any] | None = None,
) -> None:
    if event_type == PAYMENT_EVENT:
        exchange = payment_exchange
    elif event_type == DEAD_LETTER_EVENT:
        exchange = dead_letter_exchange
    else:
        raise ValueError(f"Unknown outbox event type: {event_type}")
    # on_return_raises + mandatory detects missing bindings; publisher confirms
    # ensure the outbox is marked published only after RabbitMQ accepts it.
    await broker.publish(
        payload,
        exchange=exchange,
        routing_key=event_type,
        message_id=message_id,
        headers=headers,
        persist=True,
        mandatory=True,
        timeout=10,
    )


async def publish_infrastructure_retry(
    payload: Any,
    *,
    attempt: int,
    message_id: str,
    headers: dict[str, Any],
) -> None:
    await broker.publish(
        payload,
        queue=infrastructure_retry_queue(attempt),
        message_id=message_id,
        headers=headers,
        persist=True,
        mandatory=True,
        timeout=10,
    )
