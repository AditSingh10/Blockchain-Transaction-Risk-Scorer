import joblib
import numpy as np
import torch
import torch.nn.functional as F
from torch_geometric.nn import GATConv


class GATResNet(torch.nn.Module):
    """Three-layer GAT-ResNet with residual and optional input skip connection.

    Architecture must match the training notebook exactly so load_state_dict succeeds.
    """

    def __init__(
        self,
        in_channels: int,
        hidden_channels: int = 100,
        num_classes: int = 2,
        heads1: int = 8,
        heads2: int = 8,
        heads3: int = 1,
        dropout: float = 0.5,
        use_skip: bool = True,
    ) -> None:
        super().__init__()

        self.dropout = dropout
        self.use_skip = use_skip

        self.gat1 = GATConv(
            in_channels=in_channels,
            out_channels=hidden_channels,
            heads=heads1,
            concat=True,
            dropout=dropout,
        )
        dim1 = hidden_channels * heads1

        self.gat2 = GATConv(
            in_channels=dim1,
            out_channels=hidden_channels,
            heads=heads2,
            concat=True,
            dropout=dropout,
        )
        dim2 = hidden_channels * heads2

        self.gat3 = GATConv(
            in_channels=dim2,
            out_channels=num_classes,
            heads=heads3,
            concat=False,
            dropout=dropout,
        )

        self.res1 = torch.nn.Linear(dim1, dim2, bias=False)
        self.res2 = torch.nn.Linear(dim2, num_classes, bias=False)

        if self.use_skip:
            self.skip_proj = torch.nn.Linear(in_channels, num_classes, bias=False)
        else:
            self.skip_proj = None

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        x_in = x

        h1 = self.gat1(x, edge_index)
        h1 = F.elu(h1)
        h1 = F.dropout(h1, p=self.dropout, training=self.training)

        h2 = self.gat2(h1, edge_index)
        h2 = h2 + self.res1(h1)
        h2 = F.elu(h2)
        h2 = F.dropout(h2, p=self.dropout, training=self.training)

        logits = self.gat3(h2, edge_index)
        logits = logits + self.res2(h2)

        if self.skip_proj is not None:
            logits = logits + self.skip_proj(x_in)

        return logits


class GNNInference:
    """Loads a saved GATResNet checkpoint and runs per-subgraph inference.

    Usage:
        engine = GNNInference()
        prob = engine.predict(subgraph_dict)  # returns float in [0, 1]
    """

    def __init__(
        self,
        model_path: str = "models/gat_resnet_elliptic.pt",
        scaler_path: str = "models/scaler.pkl",
    ) -> None:
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        ckpt = torch.load(model_path, map_location=self.device, weights_only=False)
        self.threshold: float = float(ckpt["threshold"])

        self.model = GATResNet(**ckpt["model_config"])
        self.model.load_state_dict(ckpt["model_state_dict"])
        self.model.eval()
        self.model.to(self.device)

        try:
            self.scaler = joblib.load(scaler_path)
        except FileNotFoundError:
            raise RuntimeError(
                f"Scaler not found at '{scaler_path}'. "
                "Run 'python prepare_model.py' first to fit and save the scaler."
            )

    def predict(self, subgraph: dict) -> float:
        """Return illicit probability for the center node of the subgraph.

        Returns 0.0 for degenerate inputs (empty subgraph, center not found).

        Args:
            subgraph: dict with keys 'nodes', 'edges', 'center' as returned by
                      GraphBuffer.get_subgraph().
        """
        nodes = subgraph.get("nodes", [])
        edges = subgraph.get("edges", [])
        center_tx_id = subgraph.get("center")

        if not nodes:
            return 0.0

        # GraphBuffer uses a set for visited nodes so ordering is non-deterministic.
        # Build explicit index map and look up center by id.
        id2idx: dict = {n["txId"]: i for i, n in enumerate(nodes)}

        if center_tx_id not in id2idx:
            return 0.0

        center_idx = id2idx[center_tx_id]

        # Feature matrix: shape (N, 165)
        X = np.array([n["features"] for n in nodes], dtype=np.float32)
        X_scaled = self.scaler.transform(X)

        # Edge index: bidirectional, skip edges whose endpoints aren't in this subgraph
        src_list: list[int] = []
        dst_list: list[int] = []
        for src_id, dst_id in edges:
            i = id2idx.get(src_id)
            j = id2idx.get(dst_id)
            if i is None or j is None:
                continue
            src_list.append(i)
            dst_list.append(j)

        if src_list:
            edge_index = torch.tensor(
                [src_list, dst_list], dtype=torch.long, device=self.device
            )
        else:
            edge_index = torch.zeros((2, 0), dtype=torch.long, device=self.device)

        x = torch.tensor(X_scaled, dtype=torch.float, device=self.device)

        with torch.no_grad():
            logits = self.model(x, edge_index)
            probs = torch.softmax(logits, dim=1)

        return float(probs[center_idx, 1].item())
