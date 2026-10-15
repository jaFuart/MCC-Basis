"""Create payments and transactional outbox.

Revision ID: 0001
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("processing_attempts", sa.Integer(), nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.Column("dead_lettered_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.CheckConstraint("amount > 0", name="ck_payments_positive_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
        sa.CheckConstraint(
            "processing_attempts >= 0 AND processing_attempts <= 3", name="ck_payments_attempts"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.ForeignKeyConstraint(["payment_id"], ["payments.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_outbox_payment_id", "outbox", ["payment_id"])
    op.create_index("ix_outbox_pending", "outbox", ["published_at", "available_at", "created_at"])


def downgrade():
    op.drop_table("outbox")
    op.drop_table("payments")
