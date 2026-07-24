from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

from gnn_inference import GNNInference


class ModelRuntime:
    """Worker-only adapter around the unchanged trained model implementation."""

    def __init__(
        self,
        *,
        model_path: Path,
        scaler_path: Path,
        model_version: str,
        feature_schema_version: str,
        deployment_timestamp: datetime | None = None,
    ):
        self.model_path = model_path
        self.model_version = model_version
        self.feature_schema_version = feature_schema_version
        self.deployment_timestamp = deployment_timestamp or datetime.fromtimestamp(
            model_path.stat().st_mtime,
            tz=UTC,
        )
        self.model_checksum = f"sha256:{self._sha256(model_path)}"
        self.inference = GNNInference(
            model_path=str(model_path),
            scaler_path=str(scaler_path),
        )

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def predict(self, subgraph: dict) -> float:
        # The original in-memory GraphBuffer treated each source edge as an
        # undirected neighborhood relation and emitted both directions to the
        # trained GNN. PostgreSQL stores each source edge once, so reconstruct
        # that exact inference representation at the worker boundary.
        model_subgraph = {
            **subgraph,
            "edges": [
                directed
                for source, destination in subgraph.get("edges", [])
                for directed in ((source, destination), (destination, source))
            ],
        }
        return self.inference.predict(model_subgraph)
