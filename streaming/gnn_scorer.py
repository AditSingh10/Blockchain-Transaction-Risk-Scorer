from gnn_inference import GNNInference


class GNNScorer:
    """Wraps GNNInference to match the scorer interface expected by StreamingService.

    StreamingService calls: scorer.score(tx_id, subgraph) -> float
    """

    def __init__(
        self,
        model_path: str = "models/gat_resnet_elliptic.pt",
        scaler_path: str = "models/scaler.pkl",
    ) -> None:
        self._inference = GNNInference(model_path=model_path, scaler_path=scaler_path)
        # Expose model threshold so the API layer can access it without a separate import
        self.threshold: float = self._inference.threshold

    def score(self, tx_id, subgraph: dict) -> float:
        """Return illicit probability for tx_id's local subgraph.

        tx_id is accepted for API compatibility with StreamingService but is not used
        directly — the center node is identified via subgraph['center'].
        """
        return self._inference.predict(subgraph)
