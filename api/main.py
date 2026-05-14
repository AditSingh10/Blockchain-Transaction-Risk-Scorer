import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Add project root to path so streaming.* and gnn_inference resolve correctly
# when uvicorn is invoked from any directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming.graph_buffer import GraphBuffer
from streaming.gnn_scorer import GNNScorer
from streaming.stream_simulator import StreamSimulator
from streaming.streaming_service import StreamingService

# ---------------------------------------------------------------------------
# Shared state — populated during lifespan startup
# ---------------------------------------------------------------------------

_state: dict = {
    "service": None,
    "scorer": None,
    "graph_buffer": None,
    "ready": False,
    "error": None,
    "tx_scores_cache": {},  # str(int(tx_id)) -> float risk score
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    data_dir = os.environ.get("ELLIPTIC_DATA_DIR")
    if not data_dir:
        _state["error"] = "ELLIPTIC_DATA_DIR environment variable is not set"
    else:
        try:
            dataset_dir = os.path.join(data_dir, "elliptic_bitcoin_dataset")
            nodes_df = pd.read_csv(
                os.path.join(dataset_dir, "elliptic_txs_features.csv"), header=None
            )
            edges_df = pd.read_csv(
                os.path.join(dataset_dir, "elliptic_txs_edgelist.csv")
            )
            nodes_df = nodes_df.rename(columns={0: "txId", 1: "time_step"})

            graph_buffer = GraphBuffer()
            scorer = GNNScorer()
            stream = StreamSimulator(nodes_df, edges_df)
            service = StreamingService(
                stream=stream, graph_buffer=graph_buffer, scorer=scorer
            )

            _state.update(
                {
                    "service": service,
                    "scorer": scorer,
                    "graph_buffer": graph_buffer,
                    "ready": True,
                }
            )
            print("Model and streaming service ready.")
        except Exception as exc:
            _state["error"] = str(exc)
            print(f"Startup error: {exc}", file=sys.stderr)

    yield
    # No cleanup needed — everything is in-memory


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(title="Crypto Fraud Detection API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Seconds to sleep between sending individual transactions over the WebSocket.
# Default 0.05 s ≈ 20 tx/s. Override with TX_INTERVAL_SECONDS env var.
_TX_INTERVAL = float(os.environ.get("TX_INTERVAL_SECONDS", "0.05"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_buffer_key(graph_buffer: GraphBuffer, tx_id: str):
    """Return the actual numpy.int64 key in graph_buffer.nodes matching tx_id.

    GraphBuffer stores keys as numpy.int64 (from pandas), so a plain Python
    int lookup via dict[] would fail. We iterate and compare via int().
    Returns None if not found.
    """
    try:
        target = int(tx_id)
    except (ValueError, TypeError):
        return None
    for k in graph_buffer.nodes:
        if int(k) == target:
            return k
    return None


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "model_loaded": _state["ready"],
        "error": _state.get("error"),
    }


@app.get("/subgraph/{tx_id}")
async def get_subgraph(tx_id: str):
    """Return the 2-hop subgraph around tx_id with cached risk scores.

    Nodes that haven't been scored yet show risk_score=0.0 — they appear in
    the graph because they're neighbors of a scored node.
    """
    if not _state["ready"]:
        raise HTTPException(
            status_code=503, detail=_state.get("error", "Service not ready")
        )

    graph_buffer: GraphBuffer = _state["graph_buffer"]
    cache: dict = _state["tx_scores_cache"]

    actual_key = _find_buffer_key(graph_buffer, tx_id)
    if actual_key is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Transaction {tx_id} not found in graph buffer. "
                "Wait for it to be streamed."
            ),
        )

    subgraph = graph_buffer.get_subgraph(actual_key)

    nodes = [
        {
            "id": str(int(n["txId"])),
            "risk_score": cache.get(str(int(n["txId"])), 0.0),
        }
        for n in subgraph["nodes"]
    ]
    edges = [
        {"source": str(int(src)), "target": str(int(dst))}
        for src, dst in subgraph["edges"]
    ]

    return {
        "nodes": nodes,
        "edges": edges,
        "center": str(int(subgraph["center"])),
    }


@app.get("/entity/{tx_id}")
async def get_entity(tx_id: str):
    """Return risk score and neighbors for a transaction.

    If the transaction was already streamed and scored, the cached score is
    returned immediately. Otherwise inference is run on-demand.
    """
    if not _state["ready"]:
        raise HTTPException(
            status_code=503, detail=_state.get("error", "Service not ready")
        )

    graph_buffer: GraphBuffer = _state["graph_buffer"]
    scorer: GNNScorer = _state["scorer"]
    cache: dict = _state["tx_scores_cache"]

    actual_key = _find_buffer_key(graph_buffer, tx_id)
    if actual_key is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Transaction {tx_id} not yet in graph buffer. "
                "Wait for it to be streamed."
            ),
        )

    cache_key = str(int(actual_key))
    cached = cache_key in cache

    if cached:
        risk = cache[cache_key]
    else:
        subgraph = graph_buffer.get_subgraph(actual_key)
        risk = await asyncio.to_thread(scorer._inference.predict, subgraph)
        cache[cache_key] = risk

    neighbors = [str(int(n)) for n in graph_buffer.adj_list.get(actual_key, set())]

    return {
        "tx_id": cache_key,
        "risk_score": round(risk, 6),
        "flagged": risk >= scorer.threshold,
        "neighbors": neighbors,
        "cached": cached,
    }


# ---------------------------------------------------------------------------
# WebSocket streaming endpoint
# ---------------------------------------------------------------------------


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    if not _state["ready"]:
        await websocket.send_text(
            json.dumps({"type": "error", "message": _state.get("error", "Service not ready")})
        )
        await websocket.close()
        return

    service: StreamingService = _state["service"]
    graph_buffer: GraphBuffer = _state["graph_buffer"]
    scorer: GNNScorer = _state["scorer"]
    cache: dict = _state["tx_scores_cache"]

    # Per-connection mutable config — asyncio is single-threaded so no locks needed.
    conn_state = {
        "interval": _TX_INTERVAL,
        "threshold": float(scorer.threshold),
    }

    async def config_listener():
        """Receive client config messages and update conn_state in-place."""
        while True:
            try:
                raw = await websocket.receive_text()
                msg = json.loads(raw)
                if msg.get("type") == "set_speed":
                    conn_state["interval"] = float(msg["interval"])
                elif msg.get("type") == "set_threshold":
                    conn_state["threshold"] = float(msg["value"])
            except Exception:
                # WebSocket closed or parse error — stop listener silently.
                break

    listener_task = asyncio.create_task(config_listener())

    try:
        while True:
            t_start = time.perf_counter()
            try:
                results = await asyncio.to_thread(service.process_next_event)
            except StopIteration:
                await websocket.send_text(json.dumps({"type": "stream_ended"}))
                break

            batch_ms = (time.perf_counter() - t_start) * 1000
            per_tx_ms = batch_ms / max(len(results), 1)

            for result in results:
                tx_id = result["txId"]
                risk = float(result["risk_score"])

                # Cache the score so /subgraph and /entity can serve it without
                # re-running inference.
                cache_key = str(int(tx_id))
                cache[cache_key] = risk

                node_data = graph_buffer.nodes.get(tx_id, {})
                features = node_data.get("features", [])
                amount = abs(float(features[2])) * 10 if len(features) > 2 else 0.0

                neighbors = [
                    str(int(n)) for n in graph_buffer.adj_list.get(tx_id, set())
                ]

                # Use per-connection threshold so the UI slider takes effect
                # immediately without restarting the stream.
                threshold = conn_state["threshold"]

                payload = {
                    "type": "transaction",
                    "data": {
                        "tx_id": cache_key,
                        "timestamp": int(time.time() * 1000),
                        "amount": round(amount, 6),
                        "illicit_probability": round(risk, 6),
                        "threshold": threshold,
                        "flagged": risk >= threshold,
                        "inference_latency_ms": round(per_tx_ms, 2),
                        "neighbors": neighbors,
                    },
                }

                await websocket.send_text(json.dumps(payload))
                await asyncio.sleep(conn_state["interval"])

    except WebSocketDisconnect:
        pass  # client disconnected cleanly — no action needed
    except Exception as exc:
        try:
            await websocket.send_text(
                json.dumps({"type": "error", "message": "Internal server error"})
            )
        except Exception:
            pass
        print(f"WebSocket error: {exc}", file=sys.stderr)
    finally:
        listener_task.cancel()
