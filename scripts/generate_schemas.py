#!/usr/bin/env python3
"""Generate committed JSON Schemas from the authoritative Pydantic contracts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from shared.contracts.events import EVENT_MODELS


def generate(output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    index: dict[str, str] = {}
    for model in EVENT_MODELS:
        event_type = model.model_fields["event_type"].default
        filename = f"{event_type}.schema.json"
        (output / filename).write_text(
            json.dumps(model.model_json_schema(), indent=2, sort_keys=True) + "\n"
        )
        index[str(event_type)] = filename
    (output / "index.json").write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("shared/contracts/schemas"),
    )
    args = parser.parse_args()
    generate(args.output)
