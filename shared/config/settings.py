from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from socket import gethostname

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="RISK_",
        extra="ignore",
        case_sensitive=False,
    )

    service_name: str = "risk-service"
    instance_id: str = Field(default_factory=gethostname, min_length=1, max_length=128)
    environment: str = "local"
    log_level: str = "INFO"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    kafka_request_timeout_ms: int = Field(default=30_000, ge=1_000, le=120_000)
    kafka_poll_timeout_ms: int = Field(default=100, ge=25, le=30_000)
    kafka_max_poll_records: int = Field(default=64, ge=1, le=1_000)
    kafka_max_message_bytes: int = Field(
        default=64 * 1024 * 1024,
        ge=1 * 1024 * 1024,
        le=64 * 1024 * 1024,
    )
    graph_batch_topic: str = "risk.graph-batches.v1"
    scoring_request_topic: str = "risk.scoring-requests.v1"
    scoring_result_topic: str = "risk.scoring-results.v1"
    scoring_dlq_topic: str = "risk.scoring-dlq.v1"
    graph_batch_partitions: int = Field(default=1, ge=1, le=1)
    scoring_request_partitions: int = Field(default=6, ge=1, le=128)
    scoring_result_partitions: int = Field(default=6, ge=1, le=128)
    scoring_dlq_partitions: int = Field(default=3, ge=1, le=32)
    graph_materializer_group: str = "risk-graph-materializer-v1"
    inference_worker_group: str = "risk-inference-workers-v1"
    result_projector_group: str = "risk-result-projector-v1"

    postgres_dsn: str = "postgresql+asyncpg://risk:risk@localhost:5432/risk"
    database_pool_size: int = Field(default=10, ge=1, le=100)
    database_max_overflow: int = Field(default=10, ge=0, le=100)
    query_timeout_ms: int = Field(default=1_500, ge=100, le=30_000)

    redis_url: str = "redis://localhost:6379/0"
    redis_stream_key: str = "risk:scoring-results:v1"
    redis_stream_maxlen: int = Field(default=10_000, ge=100, le=1_000_000)
    redis_replay_limit: int = Field(default=500, ge=1, le=10_000)
    redis_subgraph_ttl_seconds: int = Field(default=30, ge=1, le=3_600)
    redis_socket_timeout_seconds: float = Field(default=2.0, ge=0.1, le=30)

    model_path: Path = Path("models/gat_resnet_elliptic.pt")
    scaler_path: Path = Path("models/scaler.pkl")
    model_version: str = "gat-resnet-elliptic-v1"
    model_deployment_timestamp: datetime | None = None
    feature_schema_version: str = "elliptic-165-v1"
    elliptic_data_dir: Path = Path(".")
    stream_name: str = "elliptic-replay"
    replay_events_per_second: float = Field(default=20.0, ge=0.1, le=10_000)

    max_in_flight_inference: int = Field(default=8, ge=1, le=1_024)
    inference_batch_size: int = Field(default=1, ge=1, le=1)
    inference_batch_wait_ms: int = Field(default=0, ge=0, le=0)
    inference_timeout_seconds: float = Field(default=10.0, ge=0.1, le=300)
    retry_max_attempts: int = Field(default=4, ge=1, le=20)
    retry_base_delay_ms: int = Field(default=100, ge=10, le=60_000)
    retry_max_delay_ms: int = Field(default=5_000, ge=100, le=300_000)
    outbox_batch_size: int = Field(default=100, ge=1, le=1_000)
    outbox_poll_interval_ms: int = Field(default=25, ge=5, le=5_000)

    graph_max_hops: int = Field(default=2, ge=1, le=4)
    graph_max_nodes: int = Field(default=100, ge=1, le=5_000)
    graph_max_edges: int = Field(default=500, ge=1, le=20_000)
    presentation_threshold: float = Field(default=0.9, ge=0.0, le=1.0)

    websocket_client_queue_size: int = Field(default=256, ge=8, le=10_000)
    websocket_heartbeat_seconds: float = Field(default=15.0, ge=1, le=120)
    websocket_send_timeout_seconds: float = Field(default=5.0, ge=0.1, le=60)
    websocket_max_message_bytes: int = Field(default=4_096, ge=256, le=65_536)
    websocket_max_clients: int = Field(default=2_000, ge=1, le=100_000)

    cors_origins: list[str] = ["http://localhost:3000"]
    metrics_port: int = Field(default=9_100, ge=1_024, le=65_535)
    otel_exporter_endpoint: str | None = None
    shutdown_timeout_seconds: float = Field(default=30.0, ge=1, le=300)

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("log_level must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return normalized

    @field_validator("kafka_security_protocol")
    @classmethod
    def validate_security_protocol(cls, value: str) -> str:
        allowed = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
        if value not in allowed:
            raise ValueError(f"kafka_security_protocol must be one of {sorted(allowed)}")
        return value

    @model_validator(mode="after")
    def validate_kafka_credentials(self) -> Settings:
        if self.kafka_security_protocol.startswith("SASL"):
            missing = [
                name
                for name, value in (
                    ("kafka_sasl_mechanism", self.kafka_sasl_mechanism),
                    ("kafka_sasl_username", self.kafka_sasl_username),
                    ("kafka_sasl_password", self.kafka_sasl_password),
                )
                if not value
            ]
            if missing:
                raise ValueError("SASL Kafka configuration is missing: " + ", ".join(missing))
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
