

https://github.com/user-attachments/assets/343f2382-38b1-4bc3-a350-620430f42f7e





# Crypto Fraud Detection

Real-time Bitcoin transaction fraud detection using a Graph Attention Network (GAT-ResNet) trained on the Elliptic dataset. A streaming pipeline replays the dataset transaction-by-transaction, runs GNN inference on each transaction's 2-hop subgraph, and pushes scored results to a React dashboard over WebSocket.

<img width="1511" height="766" alt="Screenshot 2026-05-15 at 12 32 53 PM" src="https://github.com/user-attachments/assets/075e558f-5cc9-4616-884e-63a60ee6b91e" />



---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11+ |
| Node.js | 18+ |
| Elliptic dataset | [kaggle.com/ellipticco/elliptic-data-set](https://www.kaggle.com/datasets/ellipticco/elliptic-data-set) |

The dataset directory must contain:
```
elliptic_bitcoin_dataset/
  elliptic_txs_features.csv
  elliptic_txs_edgelist.csv
  elliptic_txs_classes.csv
```

---

## Setup

**1. Install Python dependencies** (run from project root):
```bash
pip install -r requirements.txt
```

**2. Fit and save the feature scaler** (one-time, skip if `models/scaler.pkl` already exists):
```bash
ELLIPTIC_DATA_DIR=/path/to/data python prepare_model.py
```
This fits a `StandardScaler` on training timesteps 1–34 and saves it to `models/scaler.pkl`. The GNN checkpoint (`models/gat_resnet_elliptic.pt`) is already committed.

---

## Running

**Terminal 1 — backend:**
```bash
ELLIPTIC_DATA_DIR=/path/to/data uvicorn api.main:app --reload
```
API available at `http://localhost:8000`. Startup loads the model and dataset (~5 seconds).

**Terminal 2 — frontend:**
```bash
cd frontend
npm install        # first time only
npm start
```
Dashboard available at `http://localhost:3000`.

---

## Dashboard pages

| Page | What it shows |
|---|---|
| **Live Monitor** | Streaming transaction table with real-time GNN risk scores. Toggle between table and force-graph view. Adjust flagging threshold and stream speed with sliders. Click any row to inspect its 2-hop subgraph. |
| **Alerts** | All transactions flagged above the threshold, tiered by severity (Critical / High / Medium / Low). |
| **Entity Explorer** | Search any transaction ID to see its risk score, neighbors, and interactive 2-hop subgraph pulled from the backend. |
| **Model Performance** | Precision-Recall curve, threshold sensitivity chart, and confusion matrix from the offline test evaluation (timesteps 42–49). |
| **System Metrics** | Live inference latency and throughput charts, plus running totals for processed and flagged transactions. |

---

## Model

**Architecture:** GATResNet — 3-layer Graph Attention Network with residual connections and an input skip connection.

**Dataset:** Elliptic Bitcoin Dataset — 203,769 nodes, 234,355 edges, 166 features per node, 49 time steps.

**Training split:** train 1–34 · val 35–41 · test 42–49 (temporal, no leakage)

**Test performance at threshold 0.90:**

| Metric | Value |
|---|---|
| AUC-PR (illicit class) | 0.874 |
| MCC | 0.609 |
| Illicit Precision | 68% |
| Illicit Recall | 60% |
| Illicit F1 | 0.639 |
| Weighted F1 | 0.942 |

---

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check — confirms model is loaded |
| `GET /entity/{tx_id}` | Risk score + neighbor list for a transaction |
| `GET /subgraph/{tx_id}` | 2-hop subgraph with per-node risk scores |
| `WS /ws` | Streaming scored transactions (20 tx/s default) |

The WebSocket accepts control messages:
```json
{ "type": "set_threshold", "value": 0.85 }
{ "type": "set_speed",     "interval": 0.1 }
```
