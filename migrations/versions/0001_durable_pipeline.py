"""Create durable graph, scoring, inbox, outbox, and replay state.

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("tx_id", sa.String(64), primary_key=True),
        sa.Column("time_step", sa.Integer(), nullable=False),
        sa.Column("features", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("feature_schema_version", sa.String(64), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("graph_watermark", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_transactions_time_step", "transactions", ["time_step"])

    op.create_table(
        "transaction_edges",
        sa.Column(
            "source_tx_id",
            sa.String(64),
            sa.ForeignKey("transactions.tx_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "destination_tx_id",
            sa.String(64),
            sa.ForeignKey("transactions.tx_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("source_tx_id", "destination_tx_id"),
    )
    op.create_index("ix_transaction_edges_source", "transaction_edges", ["source_tx_id"])
    op.create_index(
        "ix_transaction_edges_destination",
        "transaction_edges",
        ["destination_tx_id"],
    )

    op.create_table(
        "risk_scores",
        sa.Column(
            "tx_id",
            sa.String(64),
            sa.ForeignKey("transactions.tx_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("model_version", sa.String(128), nullable=False),
        sa.Column("model_checksum", sa.String(80), nullable=False),
        sa.Column("model_deployed_at", sa.DateTime(timezone=True)),
        sa.Column("feature_schema_version", sa.String(64), nullable=False),
        sa.Column("illicit_probability", sa.Float(), nullable=False),
        sa.Column("queue_delay_ms", sa.Float(), nullable=False),
        sa.Column("inference_latency_ms", sa.Float(), nullable=False),
        sa.Column("end_to_end_latency_ms", sa.Float(), nullable=False),
        sa.Column("graph_watermark", sa.Integer(), nullable=False),
        sa.Column("source_event_id", sa.String(36), nullable=False, unique=True),
        sa.Column("scored_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "illicit_probability >= 0 AND illicit_probability <= 1",
            name="ck_risk_probability_range",
        ),
        sa.PrimaryKeyConstraint("tx_id", "model_version"),
    )
    op.create_index("ix_risk_scores_scored_at", "risk_scores", ["scored_at"])

    op.create_table(
        "consumer_inbox",
        sa.Column("event_id", sa.String(36), nullable=False),
        sa.Column("consumer_name", sa.String(96), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("event_id", "consumer_name"),
    )

    op.create_table(
        "outbox_events",
        sa.Column("outbox_id", sa.String(64), primary_key=True),
        sa.Column("aggregate_id", sa.String(128), nullable=False),
        sa.Column("topic", sa.String(249), nullable=False),
        sa.Column("event_key", sa.String(512), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("schema_version", sa.String(16), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text()),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox_events",
        ["topic", "published_at", "created_at"],
    )

    op.create_table(
        "stream_checkpoints",
        sa.Column("stream_name", sa.String(64), primary_key=True),
        sa.Column("last_completed_time_step", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("source_offset", sa.BigInteger(), nullable=False, server_default="-1"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "replay_control",
        sa.Column("stream_name", sa.String(64), primary_key=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="running"),
        sa.Column("events_per_second", sa.Float(), nullable=False, server_default="20"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "status IN ('running', 'paused', 'completed')",
            name="ck_replay_control_status",
        ),
        sa.CheckConstraint("events_per_second > 0", name="ck_replay_positive_rate"),
    )


def downgrade() -> None:
    op.drop_table("replay_control")
    op.drop_table("stream_checkpoints")
    op.drop_index("ix_outbox_unpublished", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("consumer_inbox")
    op.drop_index("ix_risk_scores_scored_at", table_name="risk_scores")
    op.drop_table("risk_scores")
    op.drop_index("ix_transaction_edges_destination", table_name="transaction_edges")
    op.drop_index("ix_transaction_edges_source", table_name="transaction_edges")
    op.drop_table("transaction_edges")
    op.drop_index("ix_transactions_time_step", table_name="transactions")
    op.drop_table("transactions")
