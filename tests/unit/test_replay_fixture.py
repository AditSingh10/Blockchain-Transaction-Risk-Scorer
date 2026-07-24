from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.generate_fixture import (
    EDGE_OFFSETS,
    NODES_PER_TIME_STEP,
    fixture_edges,
    generate,
)
from services.replay_producer.main import EllipticDataset, pacing_delay_seconds
from shared.model.runtime import ModelRuntime


def test_graph_batch_delay_uses_transaction_rate() -> None:
    assert pacing_delay_seconds(3, 20) == pytest.approx(0.15)
    assert pacing_delay_seconds(10, 5) == pytest.approx(2)


def test_fixture_exercises_real_model_risk_bands(tmp_path: Path) -> None:
    node_count = NODES_PER_TIME_STEP * 2
    dataset_path = generate(tmp_path, node_count=node_count)
    features = pd.read_csv(dataset_path / "elliptic_txs_features.csv", header=None)
    assert len(features) == node_count
    assert features.shape[1] == 167
    first_step_edges = fixture_edges(NODES_PER_TIME_STEP)
    assert len(first_step_edges) >= NODES_PER_TIME_STEP * len(EDGE_OFFSETS) - 25

    dataset = EllipticDataset(tmp_path)
    runtime = ModelRuntime(
        model_path=Path("models/gat_resnet_elliptic.pt"),
        scaler_path=Path("models/scaler.pkl"),
        model_version="fixture-test",
        feature_schema_version="elliptic-165-v1",
    )
    probabilities: list[float] = []
    for time_step in dataset.time_steps[:2]:
        nodes, edges = dataset.batch(time_step)
        subgraph = {
            "nodes": [
                {
                    "txId": node.tx_id,
                    "features": node.features,
                    "time_step": node.time_step,
                }
                for node in nodes
            ],
            "edges": [(edge.source_tx_id, edge.destination_tx_id) for edge in edges],
        }
        probabilities.extend(
            runtime.predict({**subgraph, "center": node.tx_id}) for node in nodes[:24]
        )

    assert min(probabilities) < 0.1
    assert max(probabilities) > 0.9
    assert len({round(probability, 2) for probability in probabilities}) >= 8
