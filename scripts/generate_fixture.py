#!/usr/bin/env python3
"""Generate a small deterministic Elliptic-shaped dataset for CI and local smoke tests."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

NODES = (
    (1001, 1),
    (1002, 1),
    (1003, 1),
    (1004, 2),
    (1005, 2),
    (1006, 2),
    (1007, 3),
    (1008, 3),
    (1009, 3),
    (1010, 4),
    (1011, 4),
    (1012, 4),
)
EDGES = (
    (1001, 1002),
    (1002, 1003),
    (1002, 1004),
    (1003, 1005),
    (1004, 1005),
    (1004, 1006),
    (1005, 1007),
    (1006, 1008),
    (1007, 1008),
    (1007, 1009),
    (1008, 1010),
    (1009, 1011),
    (1010, 1012),
)


def features(tx_id: int, time_step: int) -> list[float]:
    return [
        round((((tx_id * (index + 3)) + time_step * 17) % 211 - 105) / 53.0, 6)
        for index in range(165)
    ]


def generate(output: Path) -> Path:
    dataset = output / "elliptic_bitcoin_dataset"
    dataset.mkdir(parents=True, exist_ok=True)
    with (dataset / "elliptic_txs_features.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        for tx_id, time_step in NODES:
            writer.writerow([tx_id, time_step, *features(tx_id, time_step)])
    with (dataset / "elliptic_txs_edgelist.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["txId1", "txId2"])
        writer.writerows(EDGES)
    with (dataset / "elliptic_txs_classes.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["txId", "class"])
        writer.writerows((tx_id, "unknown") for tx_id, _ in NODES)
    return dataset


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(generate(args.output))


if __name__ == "__main__":
    main()
