"""Exercise a running stack with a real webhook, PostgreSQL and RabbitMQ.

Run inside the API container: python scripts/smoke.py.
Only records with a fresh run UUID are created. Existing payments are untouched.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


class WebhookFixture:
    def __init__(self, host: str, port: int) -> None:
        self.calls: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.lock = threading.Lock()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                mode, _, token = self.path.strip("/").partition("/")
                with fixture.lock:
                    fixture.calls[token].append(
                        {"body": body, "headers": dict(self.headers), "time": time.monotonic()}
                    )
                    count = len(fixture.calls[token])
                status = 500 if mode == "fail" or (mode == "retry" and count < 3) else 204
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer((host, port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> WebhookFixture:
        self.thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def received(self, token: str) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.calls[token])


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


async def request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    status: int,
    **kwargs: Any,
) -> httpx.Response:
    response = await client.request(method, path, **kwargs)
    check(
        response.status_code == status,
        f"{method} {path}: expected {status}, got {response.status_code}: {response.text}",
    )
    return response


async def ready(client: httpx.AsyncClient, wait_seconds: float) -> None:
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        try:
            response = await client.get("/health")
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.5)
    raise AssertionError("API health check did not become ready")


async def payment_done(
    client: httpx.AsyncClient,
    payment_id: str,
    wait_seconds: float,
    *,
    dead_lettered: bool = False,
) -> dict[str, Any]:
    deadline = time.monotonic() + wait_seconds
    current: dict[str, Any] = {}
    while time.monotonic() < deadline:
        current = (await request(client, "GET", f"/api/v1/payments/{payment_id}", 200)).json()
        terminal = current["status"] in {"succeeded", "failed"}
        finished = (
            current.get("dead_lettered_at")
            if dead_lettered
            else current.get("webhook_delivered_at")
        )
        if terminal and finished:
            return current
        await asyncio.sleep(0.2)
    raise AssertionError(f"Payment {payment_id} did not finish: {current}")


async def duplicate_event(payment_id: str) -> None:
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"], timeout=5)
    try:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange("payments")
        await exchange.publish(
            aio_pika.Message(
                body=json.dumps({"payment_id": payment_id, "attempt": 1}).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="payments.new",
            mandatory=True,
        )
    finally:
        await connection.close()


async def dlq_present(payment_id: str) -> None:
    """Inspect the queue without acknowledging or removing anyone's messages."""
    connection = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"], timeout=5)
    messages = []
    found = False
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue("payments.dlq", durable=True, passive=True)
        count = queue.declaration_result.message_count
        for _ in range(count):
            message = await queue.get(fail=False)
            if message is None:
                break
            messages.append(message)
            body = json.loads(message.body)
            if body.get("payment_id") == payment_id:
                check(body.get("attempt") == 3, "Dead letter must identify the final attempt")
                found = True
        check(found, f"No dead-letter message for {payment_id}")
    finally:
        for message in messages:
            await message.nack(requeue=True)
        await connection.close()


async def wait_for_dlq(payment_id: str, wait_seconds: float) -> None:
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            await dlq_present(payment_id)
            return
        except (AssertionError, OSError, TimeoutError, aio_pika.exceptions.AMQPException):
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.2)


async def verify_checkpoint(client: httpx.AsyncClient, filename: str, wait_seconds: float) -> None:
    checkpoint = json.loads(await asyncio.to_thread(Path(filename).read_text))
    for saved in checkpoint["payments"]:
        current = (
            await request(client, "GET", f"/api/v1/payments/{saved['payment_id']}", 200)
        ).json()
        for field in (
            "payment_id",
            "status",
            "amount",
            "currency",
            "created_at",
            "processed_at",
            "processing_attempts",
            "webhook_delivered_at",
            "dead_lettered_at",
        ):
            check(current.get(field) == saved.get(field), f"Restart changed {field}: {current}")
    accepted = await request(
        client,
        "POST",
        "/api/v1/payments",
        202,
        headers={"Idempotency-Key": checkpoint["idempotency_key"]},
        json=checkpoint["payload"],
    )
    response = accepted.json()
    check(
        response["payment_id"] == checkpoint["accepted"]["payment_id"],
        "Payment ID did not survive restart",
    )
    check(
        response["created_at"] == checkpoint["accepted"]["created_at"],
        "Creation time did not survive restart",
    )
    check(
        response["status"] == checkpoint["payments"][0]["status"],
        "Repeated POST lost current status",
    )
    await wait_for_dlq(checkpoint["dead_letter_id"], wait_seconds)
    print("PASS: payments, idempotency and dead letter survived restart", flush=True)


async def outbox_count(payment_id: str, *, published: bool) -> int:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    predicate = "IS NOT NULL" if published else "IS NULL"
    try:
        async with engine.connect() as connection:
            count = await connection.scalar(
                text(
                    "SELECT COUNT(*) FROM outbox "
                    f"WHERE payment_id = :payment_id AND published_at {predicate}"
                ),
                {"payment_id": UUID(payment_id)},
            )
            return int(count)
    finally:
        await engine.dispose()


async def seed_pending(client: httpx.AsyncClient, args: argparse.Namespace) -> None:
    run_id = str(uuid4())
    payload = {
        "amount": "49.99",
        "currency": "USD",
        "metadata": {"run_id": run_id, "scenario": "outbox-consumer-outage"},
        "webhook_url": f"{args.webhook_base.rstrip('/')}/ok/{run_id}",
    }
    accepted = (
        await request(
            client,
            "POST",
            "/api/v1/payments",
            202,
            headers={"Idempotency-Key": f"smoke-pending-{run_id}"},
            json=payload,
        )
    ).json()
    details = (
        await request(client, "GET", f"/api/v1/payments/{accepted['payment_id']}", 200)
    ).json()
    check(details["status"] == "pending", "Stop the consumer before seeding a pending payment")
    check(details["processing_attempts"] == 0, "Consumer processed the outage test too early")
    check(
        await outbox_count(accepted["payment_id"], published=False) == 1,
        "Accepted payment has no durable unpublished outbox record",
    )
    await asyncio.to_thread(
        Path(args.checkpoint).write_text,
        json.dumps({"run_id": run_id, "payload": payload, "accepted": accepted}, indent=2) + "\n",
    )
    print(
        f"PASS: payment {accepted['payment_id']} and unpublished outbox persisted during outage",
        flush=True,
    )


async def resume_pending(
    client: httpx.AsyncClient, args: argparse.Namespace, fixture: WebhookFixture
) -> None:
    checkpoint = json.loads(await asyncio.to_thread(Path(args.checkpoint).read_text))
    payment_id = checkpoint["accepted"]["payment_id"]
    print("READY: webhook fixture listening; start the consumer in another terminal", flush=True)
    delivered = await payment_done(client, payment_id, args.timeout)
    calls = fixture.received(checkpoint["run_id"])
    check(len(calls) == 1, f"Expected one delivery after outage, got {len(calls)}")
    check(
        delivered["processing_attempts"] == 1, "Outage payment needed an unexpected delivery retry"
    )
    check(
        delivered["created_at"] == checkpoint["accepted"]["created_at"],
        "Outage replaced the payment",
    )
    check(calls[0]["body"]["metadata"] == checkpoint["payload"]["metadata"], "Outage lost metadata")
    check(
        await outbox_count(payment_id, published=True) == 1,
        "Initial outbox event was not marked published",
    )
    check(
        await outbox_count(payment_id, published=False) == 0,
        "Unpublished event remains after recovery",
    )
    print("PASS: persisted outbox delivered the payment after consumer recovery", flush=True)


async def smoke(
    client: httpx.AsyncClient, args: argparse.Namespace, fixture: WebhookFixture
) -> None:
    run_id = str(uuid4())
    unknown_id = str(uuid4())
    for path in ("/health", "/docs", "/openapi.json", f"/api/v1/payments/{unknown_id}"):
        await request(client, "GET", path, 401, headers={"X-API-Key": ""})
        await request(client, "GET", path, 401, headers={"X-API-Key": "incorrect-key"})
    await request(client, "POST", "/api/v1/payments", 401, headers={"X-API-Key": ""}, json={})
    await request(client, "GET", "/openapi.json", 200)
    await request(client, "GET", f"/api/v1/payments/{unknown_id}", 404)
    await request(client, "GET", "/api/v1/payments/not-a-uuid", 422)
    print("PASS: authentication, OpenAPI and payment lookup", flush=True)

    payload = {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Integration smoke test",
        "metadata": {"run_id": run_id, "invoice_id": "sample-123"},
        "webhook_url": f"{args.webhook_base.rstrip('/')}/retry/{run_id}",
    }
    await request(client, "POST", "/api/v1/payments", 422, json=payload)
    for field, value in (
        ("amount", "0"),
        ("amount", "-1.00"),
        ("amount", "1.001"),
        ("amount", "10000000000000000.00"),
        ("currency", "GBP"),
        ("webhook_url", "ftp://example.com/hook"),
    ):
        await request(
            client,
            "POST",
            "/api/v1/payments",
            422,
            headers={"Idempotency-Key": str(uuid4())},
            json={**payload, field: value},
        )
    print(
        "PASS: required idempotency key, decimal precision, currency and URL validation", flush=True
    )

    idempotency_key = f"smoke-{run_id}"
    responses = await asyncio.gather(
        *(
            request(
                client,
                "POST",
                "/api/v1/payments",
                202,
                headers={"Idempotency-Key": idempotency_key},
                json=payload,
            )
            for _ in range(8)
        )
    )
    accepted = responses[0].json()
    check(
        set(accepted) == {"payment_id", "status", "created_at"},
        f"Unexpected accepted response: {accepted}",
    )
    check(accepted["status"] == "pending", f"Unexpected initial status: {accepted}")
    check(
        all(
            response.json()["payment_id"] == accepted["payment_id"]
            and response.json()["created_at"] == accepted["created_at"]
            and response.json()["status"] in {"pending", "succeeded", "failed"}
            for response in responses
        ),
        "Concurrent duplicates created different payments",
    )
    await request(
        client,
        "POST",
        "/api/v1/payments",
        409,
        headers={"Idempotency-Key": idempotency_key},
        json={**payload, "amount": "124.45"},
    )
    print("PASS: concurrent idempotency and conflicting payload", flush=True)

    delivered = await payment_done(client, accepted["payment_id"], args.timeout)
    calls = fixture.received(run_id)
    check(len(calls) == 3, f"Expected two retries and delivery, got {len(calls)} webhook calls")
    check(delivered["processing_attempts"] == 3, f"Unexpected delivered attempt count: {delivered}")
    check(not delivered.get("dead_lettered_at"), "Delivered webhook marked as dead letter")
    for call in calls:
        body = call["body"]
        headers = {key.lower(): value for key, value in call["headers"].items()}
        check(body["payment_id"] == accepted["payment_id"], "Wrong webhook payment ID")
        check(body["status"] == delivered["status"], "Retry changed gateway result")
        check(
            body["amount"] == "123.45" and body["metadata"] == payload["metadata"],
            "Webhook lost payment data",
        )
        check(
            headers.get("idempotency-key") == accepted["payment_id"],
            "Missing stable webhook deduplication key",
        )
        check(headers.get("x-webhook-id") == accepted["payment_id"], "Missing stable webhook ID")
    first_gap = calls[1]["time"] - calls[0]["time"]
    second_gap = calls[2]["time"] - calls[1]["time"]
    retry_base = float(os.environ.get("RETRY_BASE_DELAY", "1"))
    check(
        first_gap >= retry_base * 0.8 and second_gap >= retry_base * 1.8,
        "Webhook retries did not use exponential delay",
    )
    print("PASS: webhook retries preserve result and use exponential delay", flush=True)

    await duplicate_event(accepted["payment_id"])
    await asyncio.sleep(2)
    current = (
        await request(client, "GET", f"/api/v1/payments/{accepted['payment_id']}", 200)
    ).json()
    check(len(fixture.received(run_id)) == 3, "Completed duplicate caused another webhook")
    check(
        current["processed_at"] == delivered["processed_at"],
        "Duplicate reprocessed the payment gateway",
    )
    check(current["processing_attempts"] == 3, "Duplicate changed webhook attempts")
    repeated = await request(
        client,
        "POST",
        "/api/v1/payments",
        202,
        headers={"Idempotency-Key": idempotency_key},
        json=payload,
    )
    check(
        repeated.json() == {**accepted, "status": delivered["status"]},
        "Repeated POST did not return the original payment with its current status",
    )
    print("PASS: duplicate broker delivery and repeated POST after completion", flush=True)

    dead_token = str(uuid4())
    dead_payload = {**payload, "webhook_url": f"{args.webhook_base.rstrip('/')}/fail/{dead_token}"}
    dead_accepted = (
        await request(
            client,
            "POST",
            "/api/v1/payments",
            202,
            headers={"Idempotency-Key": f"smoke-{dead_token}"},
            json=dead_payload,
        )
    ).json()
    dead = await payment_done(client, dead_accepted["payment_id"], args.timeout, dead_lettered=True)
    check(
        len(fixture.received(dead_token)) == 3,
        "Permanent webhook failure did not stop after three attempts",
    )
    check(dead["processing_attempts"] == 3, "Dead-letter attempt count is wrong")
    check(not dead.get("webhook_delivered_at"), "Failed webhook marked delivered")
    check(dead.get("last_error"), "Dead letter does not explain its failure")
    # The outbox publish can happen just after the durable dead-letter flag is committed.
    await wait_for_dlq(dead_accepted["payment_id"], args.timeout)
    print("PASS: permanent webhook failure reaches durable DLQ after three attempts", flush=True)

    checkpoint = {
        "run_id": run_id,
        "payments": [delivered, dead],
        "idempotency_key": idempotency_key,
        "payload": payload,
        "accepted": accepted,
        "dead_letter_id": dead_accepted["payment_id"],
    }
    await asyncio.to_thread(
        Path(args.checkpoint).write_text,
        json.dumps(checkpoint, ensure_ascii=False, indent=2) + "\n",
    )
    print(f"PASS: all smoke checks; restart checkpoint saved to {args.checkpoint}", flush=True)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--webhook-base", default="http://api:8090")
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=8090)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--checkpoint")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--verify-only", action="store_true")
    modes.add_argument("--seed-pending", action="store_true")
    modes.add_argument("--resume-pending", action="store_true")
    args = parser.parse_args()
    if args.checkpoint is None:
        args.checkpoint = (
            "/tmp/mcc-basis-pending.json"
            if args.seed_pending or args.resume_pending
            else "/tmp/mcc-basis-smoke.json"
        )
    api_key = os.environ["API_KEY"]
    async with httpx.AsyncClient(
        base_url=args.api_base,
        headers={"X-API-Key": api_key},
        timeout=10,
        trust_env=False,
    ) as client:
        await ready(client, args.timeout)
        if args.verify_only:
            await verify_checkpoint(client, args.checkpoint, args.timeout)
        elif args.seed_pending:
            await seed_pending(client, args)
        else:
            with WebhookFixture(args.listen_host, args.listen_port) as fixture:
                if args.resume_pending:
                    await resume_pending(client, args, fixture)
                else:
                    await smoke(client, args, fixture)


if __name__ == "__main__":
    asyncio.run(main())
