#!/usr/bin/env python3
"""Generate a deterministic Elliptic-shaped dataset for CI and local development."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import joblib
import numpy as np

FIXTURE_TIME_STEPS = 100
NODES_PER_TIME_STEP = 96
FIXTURE_NODE_COUNT = FIXTURE_TIME_STEPS * NODES_PER_TIME_STEP
EDGE_OFFSETS = (1, 2, 3, 5, 8)

CALIBRATED_GROUP_INDICES = (80, 209, 313, 239)
FEATURE_PATTERN_SIZE = 3
CALIBRATED_GROUP_INTERVAL = 24
FEATURE_SEED = 20260723


def fixture_nodes(node_count: int) -> tuple[tuple[int, int], ...]:
    return tuple((1001 + index, 1 + index // NODES_PER_TIME_STEP) for index in range(node_count))


def fixture_edges(node_count: int) -> tuple[tuple[int, int], ...]:
    edges: list[tuple[int, int]] = []
    for step_start in range(0, node_count, NODES_PER_TIME_STEP):
        step_size = min(NODES_PER_TIME_STEP, node_count - step_start)
        for local_index in range(step_size):
            for offset in EDGE_OFFSETS:
                if local_index >= offset:
                    edges.append(
                        (
                            1001 + step_start + local_index,
                            1001 + step_start + local_index - offset,
                        )
                    )
    return tuple(edges)


def feature_rows(nodes: tuple[tuple[int, int], ...], scaler_path: Path) -> dict[int, list[float]]:
    scaler = joblib.load(scaler_path)
    generator = np.random.default_rng(FEATURE_SEED)
    standardized_rows = generator.normal(0, 1, (len(nodes), 165))
    calibration_generator = np.random.default_rng(FEATURE_SEED)
    calibration_pool = calibration_generator.normal(
        0,
        1,
        (max(CALIBRATED_GROUP_INDICES) + 1, FEATURE_PATTERN_SIZE, 165),
    )
    # Sparse, deterministic 2.5-sigma feature groups keep the dense graph from
    # averaging every model output into the clear band. These are inputs only;
    # the unchanged checkpoint still computes every displayed probability.
    for group_number, start in enumerate(range(0, len(nodes), CALIBRATED_GROUP_INTERVAL)):
        end = min(start + FEATURE_PATTERN_SIZE, len(nodes))
        pattern = calibration_pool[
            CALIBRATED_GROUP_INDICES[group_number % len(CALIBRATED_GROUP_INDICES)]
        ]
        standardized_rows[start:end] = pattern[: end - start] * 2.5
    raw_rows = scaler.inverse_transform(standardized_rows)
    return {
        tx_id: [round(float(value), 8) for value in row]
        for (tx_id, _), row in zip(nodes, raw_rows, strict=True)
    }


def generate(
    output: Path,
    scaler_path: Path = Path("models/scaler.pkl"),
    node_count: int = FIXTURE_NODE_COUNT,
) -> Path:
    dataset = output / "elliptic_bitcoin_dataset"
    dataset.mkdir(parents=True, exist_ok=True)
    nodes = fixture_nodes(node_count)
    edges = fixture_edges(node_count)
    rows = feature_rows(nodes, scaler_path)
    with (dataset / "elliptic_txs_features.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        for tx_id, time_step in nodes:
            writer.writerow([tx_id, time_step, *rows[tx_id]])
    with (dataset / "elliptic_txs_edgelist.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["txId1", "txId2"])
        writer.writerows(edges)
    with (dataset / "elliptic_txs_classes.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["txId", "class"])
        writer.writerows((tx_id, "unknown") for tx_id, _ in nodes)
    return dataset


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scaler-path", type=Path, default=Path("models/scaler.pkl"))
    args = parser.parse_args()
    print(generate(args.output, args.scaler_path))


if __name__ == "__main__":
    main()
