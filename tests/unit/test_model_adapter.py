from __future__ import annotations

from shared.model.runtime import ModelRuntime


class RecordingInference:
    def __init__(self) -> None:
        self.subgraph = None

    def predict(self, subgraph: dict) -> float:
        self.subgraph = subgraph
        return 0.42


def test_worker_reconstructs_original_bidirectional_model_edges() -> None:
    runtime = ModelRuntime.__new__(ModelRuntime)
    runtime.inference = RecordingInference()
    result = runtime.predict(
        {
            "center": "a",
            "nodes": [{"txId": "a"}, {"txId": "b"}],
            "edges": [("a", "b")],
        }
    )
    assert result == 0.42
    assert runtime.inference.subgraph["edges"] == [("a", "b"), ("b", "a")]
