from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TransactionNode(Base):
    __tablename__ = "transactions"

    tx_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    time_step: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    features: Mapped[list[float]] = mapped_column(JSONB, nullable=False)
    feature_schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    graph_watermark: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class TransactionEdge(Base):
    __tablename__ = "transaction_edges"
    __table_args__ = (
        PrimaryKeyConstraint("source_tx_id", "destination_tx_id"),
        Index("ix_transaction_edges_source", "source_tx_id"),
        Index("ix_transaction_edges_destination", "destination_tx_id"),
    )

    source_tx_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("transactions.tx_id", ondelete="CASCADE"), nullable=False
    )
    destination_tx_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("transactions.tx_id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RiskScore(Base):
    __tablename__ = "risk_scores"
    __table_args__ = (
        PrimaryKeyConstraint("tx_id", "model_version"),
        Index("ix_risk_scores_scored_at", "scored_at"),
    )

    tx_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("transactions.tx_id", ondelete="CASCADE"), nullable=False
    )
    model_version: Mapped[str] = mapped_column(String(128), nullable=False)
    model_checksum: Mapped[str] = mapped_column(String(80), nullable=False)
    model_deployed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    feature_schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    illicit_probability: Mapped[float] = mapped_column(Float, nullable=False)
    queue_delay_ms: Mapped[float] = mapped_column(Float, nullable=False)
    inference_latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    end_to_end_latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    graph_watermark: Mapped[int] = mapped_column(Integer, nullable=False)
    source_event_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    scored_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ConsumerInbox(Base):
    __tablename__ = "consumer_inbox"
    __table_args__ = (PrimaryKeyConstraint("event_id", "consumer_name"),)

    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    consumer_name: Mapped[str] = mapped_column(String(96), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_outbox_unpublished", "topic", "published_at", "created_at"),)

    outbox_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    topic: Mapped[str] = mapped_column(String(249), nullable=False)
    event_key: Mapped[str] = mapped_column(String(512), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)


class StreamCheckpoint(Base):
    __tablename__ = "stream_checkpoints"

    stream_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_completed_time_step: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="0",
    )
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="-1")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ReplayControl(Base):
    __tablename__ = "replay_control"

    stream_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="running")
    events_per_second: Mapped[float] = mapped_column(Float, nullable=False, server_default="20")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
