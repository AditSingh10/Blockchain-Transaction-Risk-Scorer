import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import {
  DEMO_ALERTS,
  DEMO_DISCONNECTED,
  DEMO_GRAPH_EDGES,
  DEMO_GRAPH_NODES,
  DEMO_LATENCY,
  DEMO_MODE,
  DEMO_THROUGHPUT,
  DEMO_TRANSACTIONS,
} from '../demo/fixtures';
import {
  GraphEdge,
  GraphNode,
  LatencyPoint,
  ThroughputPoint,
  Transaction,
} from '../types';

interface WebSocketContextValue {
  connected: boolean;
  demoMode: boolean;
  streamStatus: 'running' | 'paused' | 'completed' | 'unknown';
  transactions: Transaction[];
  isPaused: boolean;
  setIsPaused: (v: boolean) => void;
  avgLatency: number;
  threshold: number;
  setThreshold: (v: number) => void;
  alerts: Transaction[];
  clearAlerts: () => void;
  graphNodes: GraphNode[];
  graphEdges: GraphEdge[];
  latencyHistory: LatencyPoint[];
  throughputHistory: ThroughputPoint[];
  totalCount: number;
  pendingCount: number;
  streamSpeed: number;
  setStreamSpeed: (v: number) => void;
}

const defaultValue: WebSocketContextValue = {
  connected: false,
  demoMode: false,
  streamStatus: 'unknown',
  transactions: [],
  isPaused: false,
  setIsPaused: () => {},
  avgLatency: 0,
  threshold: 0.9,
  setThreshold: () => {},
  alerts: [],
  clearAlerts: () => {},
  graphNodes: [],
  graphEdges: [],
  latencyHistory: [],
  throughputHistory: [],
  totalCount: 0,
  pendingCount: 0,
  streamSpeed: 0.05,
  setStreamSpeed: () => {},
};

const WebSocketContext = createContext<WebSocketContextValue>(defaultValue);
const MAX_VISIBLE_TRANSACTIONS = 10_000;
const MAX_PENDING_TRANSACTIONS = 10_000;
const MAX_GRAPH_NODES = 10_000;
const MAX_GRAPH_EDGES = 50_000;

export const WebSocketProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [connected, setConnected] = useState(DEMO_MODE && !DEMO_DISCONNECTED);
  const [streamStatus, setStreamStatus] = useState<
    'running' | 'paused' | 'completed' | 'unknown'
  >(DEMO_MODE && !DEMO_DISCONNECTED ? 'running' : 'unknown');
  const [transactions, setTransactions] = useState<Transaction[]>(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_TRANSACTIONS.slice(0, 58) : []
  );
  const [isPaused, setIsPaused] = useState(false);
  const [latencies, setLatencies] = useState<number[]>(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_LATENCY.map(point => point.latency) : []
  );
  const [threshold, setThresholdState] = useState(0.9);
  const [alerts, setAlerts] = useState<Transaction[]>(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_ALERTS : []
  );
  const [graphNodes, setGraphNodes] = useState<GraphNode[]>(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_GRAPH_NODES : []
  );
  const [graphEdges, setGraphEdges] = useState<GraphEdge[]>(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_GRAPH_EDGES : []
  );
  const [latencyHistory, setLatencyHistory] = useState<LatencyPoint[]>(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_LATENCY : []
  );
  const [throughputHistory, setThroughputHistory] = useState<ThroughputPoint[]>(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_THROUGHPUT : []
  );
  const [totalCount, setTotalCount] = useState(
    DEMO_MODE && !DEMO_DISCONNECTED ? DEMO_TRANSACTIONS.length : 0
  );
  const [pendingCount, setPendingCount] = useState(0);
  const [streamSpeed, setStreamSpeedState] = useState(0.05);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptRef = useRef(0);
  // A cursor is scoped to this mounted session. On a full page load the
  // gateway replays its bounded recent window so the analyst workspace can
  // rebuild; reconnects within the session resume after the latest received
  // Redis Stream ID.
  const lastEventIdRef = useRef<string | null>(null);
  const recentEventIdsRef = useRef<string[]>([]);
  const recentEventIdSetRef = useRef(new Set<string>());
  const pendingTransactionsRef = useRef<Transaction[]>([]);
  const pendingHeadRef = useRef(0);
  const transactionIdsRef = useRef(
    new Set(DEMO_MODE && !DEMO_DISCONNECTED
      ? DEMO_TRANSACTIONS.slice(0, 58).map(transaction => transaction.tx_id)
      : [])
  );
  const alertIdsRef = useRef(
    new Set(DEMO_MODE && !DEMO_DISCONNECTED
      ? DEMO_ALERTS.map(transaction => transaction.tx_id)
      : [])
  );
  const graphNodeIdsRef = useRef(
    new Set(DEMO_MODE && !DEMO_DISCONNECTED
      ? DEMO_GRAPH_NODES.map(node => node.id)
      : [])
  );
  const graphEdgeKeysRef = useRef(
    new Set(DEMO_MODE && !DEMO_DISCONNECTED
      ? DEMO_GRAPH_EDGES.map(edge => [edge.source, edge.target].sort().join('\u0000'))
      : [])
  );
  const isPausedRef = useRef(isPaused);
  const thresholdRef = useRef(threshold);
  const streamSpeedRef = useRef(streamSpeed);
  const totalCountRef = useRef(totalCount);
  const demoIndexRef = useRef(58);

  useEffect(() => { isPausedRef.current = isPaused; }, [isPaused]);
  useEffect(() => { thresholdRef.current = threshold; }, [threshold]);
  useEffect(() => { streamSpeedRef.current = streamSpeed; }, [streamSpeed]);

  const setThreshold = useCallback((value: number) => {
    setThresholdState(value);
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'set_threshold', value }));
    }
  }, []);

  const clearAlerts = useCallback(() => {
    alertIdsRef.current.clear();
    setAlerts([]);
  }, []);

  const setPaused = useCallback((value: boolean) => {
    setIsPaused(value);
    setStreamStatus(value ? 'paused' : 'running');
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({
        type: value ? 'pause_replay' : 'resume_replay',
      }));
    }
  }, []);

  const setStreamSpeed = useCallback((value: number) => {
    setStreamSpeedState(value);
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'set_speed', interval: value }));
    }
  }, []);

  const ingestTransaction = useCallback((incoming: Transaction) => {
    const tx = {
      ...incoming,
      timestamp: incoming.timestamp || Date.now(),
      threshold: thresholdRef.current,
      flagged: incoming.illicit_probability >= thresholdRef.current,
    };
    totalCountRef.current += 1;
    setLatencies(previous => [...previous.slice(-49), tx.inference_latency_ms]);
    setTransactions(previous => {
      if (transactionIdsRef.current.has(tx.tx_id)) {
        return [
          tx,
          ...previous.filter(transaction => transaction.tx_id !== tx.tx_id),
        ];
      }
      transactionIdsRef.current.add(tx.tx_id);
      const next = [tx, ...previous];
      if (next.length > MAX_VISIBLE_TRANSACTIONS) {
        for (const removed of next.slice(MAX_VISIBLE_TRANSACTIONS)) {
          transactionIdsRef.current.delete(removed.tx_id);
        }
        return next.slice(0, MAX_VISIBLE_TRANSACTIONS);
      }
      return next;
    });
    if (tx.flagged) {
      setAlerts(previous => {
        if (alertIdsRef.current.has(tx.tx_id)) {
          return [
            tx,
            ...previous.filter(transaction => transaction.tx_id !== tx.tx_id),
          ];
        }
        alertIdsRef.current.add(tx.tx_id);
        const next = [tx, ...previous];
        if (next.length > MAX_VISIBLE_TRANSACTIONS) {
          for (const removed of next.slice(MAX_VISIBLE_TRANSACTIONS)) {
            alertIdsRef.current.delete(removed.tx_id);
          }
          return next.slice(0, MAX_VISIBLE_TRANSACTIONS);
        }
        return next;
      });
    }
    setGraphNodes(previous => {
      const node = {
        id: tx.tx_id,
        illicit_probability: tx.illicit_probability,
        flagged: tx.flagged,
      };
      if (graphNodeIdsRef.current.has(tx.tx_id)) {
        return [node, ...previous.filter(existing => existing.id !== tx.tx_id)];
      }
      graphNodeIdsRef.current.add(tx.tx_id);
      const next = [node, ...previous];
      if (next.length > MAX_GRAPH_NODES) {
        for (const removed of next.slice(MAX_GRAPH_NODES)) {
          graphNodeIdsRef.current.delete(removed.id);
        }
        return next.slice(0, MAX_GRAPH_NODES);
      }
      return next;
    });
    setGraphEdges(previous => {
      const added: GraphEdge[] = [];
      for (const neighbor of tx.neighbors ?? []) {
        const key = [tx.tx_id, neighbor].sort().join('\u0000');
        if (graphEdgeKeysRef.current.has(key)) continue;
        graphEdgeKeysRef.current.add(key);
        added.push({ source: tx.tx_id, target: neighbor });
      }
      if (added.length === 0) return previous;
      const next = [...added, ...previous];
      if (next.length > MAX_GRAPH_EDGES) {
        for (const removed of next.slice(MAX_GRAPH_EDGES)) {
          graphEdgeKeysRef.current.delete(
            [removed.source, removed.target].sort().join('\u0000')
          );
        }
        return next.slice(0, MAX_GRAPH_EDGES);
      }
      return next;
    });
    setLatencyHistory(previous => [
      ...previous,
      { time: tx.timestamp, latency: tx.inference_latency_ms },
    ].slice(-60));
    setThroughputHistory(previous => {
      const second = Math.floor(tx.timestamp / 1000);
      const last = previous[previous.length - 1];
      if (last && Math.floor(last.time / 1000) === second) {
        const next = [...previous];
        next[next.length - 1] = { ...last, tps: last.tps + 1 };
        return next;
      }
      return [...previous, { time: tx.timestamp, tps: 1 }].slice(-60);
    });
    setTotalCount(totalCountRef.current);
  }, []);

  const enqueueTransaction = useCallback((incoming: Transaction) => {
    const pending = pendingTransactionsRef.current;
    const pendingLength = pending.length - pendingHeadRef.current;
    if (pendingLength >= MAX_PENDING_TRANSACTIONS) {
      pendingHeadRef.current += pendingLength - MAX_PENDING_TRANSACTIONS + 1;
    }
    if (pendingHeadRef.current > 1024 && pendingHeadRef.current * 2 > pending.length) {
      pending.splice(0, pendingHeadRef.current);
      pendingHeadRef.current = 0;
    }
    pending.push(incoming);
    setPendingCount(pending.length - pendingHeadRef.current);
  }, []);

  useEffect(() => {
    if (!DEMO_MODE || DEMO_DISCONNECTED) return;
    setConnected(true);
    const intervalMs = Math.max(streamSpeed * 1000, 10);
    const timer = window.setInterval(() => {
      if (isPausedRef.current) return;
      const index = demoIndexRef.current % DEMO_TRANSACTIONS.length;
      ingestTransaction({ ...DEMO_TRANSACTIONS[index], timestamp: Date.now() });
      demoIndexRef.current += 1;
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [ingestTransaction, streamSpeed]);

  useEffect(() => {
    if (DEMO_MODE) return;
    const intervalMs = Math.max(streamSpeed * 1000, 10);
    const timer = window.setInterval(() => {
      if (isPausedRef.current) return;
      const pending = pendingTransactionsRef.current;
      const next = pending[pendingHeadRef.current];
      if (!next) return;
      pendingHeadRef.current += 1;
      if (pendingHeadRef.current > 1024 && pendingHeadRef.current * 2 > pending.length) {
        pending.splice(0, pendingHeadRef.current);
        pendingHeadRef.current = 0;
      }
      setPendingCount(pending.length - pendingHeadRef.current);
      ingestTransaction(next);
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [ingestTransaction, streamSpeed]);

  useEffect(() => {
    if (DEMO_MODE) return;
    let stopped = false;

    const rememberEvent = (eventId: string) => {
      if (recentEventIdSetRef.current.has(eventId)) return false;
      recentEventIdSetRef.current.add(eventId);
      recentEventIdsRef.current.push(eventId);
      if (recentEventIdsRef.current.length > 1000) {
        const expired = recentEventIdsRef.current.shift();
        if (expired) recentEventIdSetRef.current.delete(expired);
      }
      return true;
    };

    const connect = () => {
      const configuredUrl = process.env.REACT_APP_WS_URL;
      const fallbackProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const fallbackUrl = `${fallbackProtocol}//${window.location.hostname}:8000/api/v1/ws`;
      const url = new URL(configuredUrl || fallbackUrl);
      if (lastEventIdRef.current) {
        url.searchParams.set('last_event_id', lastEventIdRef.current);
      }

      const ws = new WebSocket(url.toString());
      wsRef.current = ws;
      ws.onopen = () => {
        reconnectAttemptRef.current = 0;
        setConnected(true);
        ws.send(JSON.stringify({ type: 'set_threshold', value: thresholdRef.current }));
        ws.send(JSON.stringify({ type: 'set_speed', interval: streamSpeedRef.current }));
        if (isPausedRef.current) ws.send(JSON.stringify({ type: 'pause_replay' }));
      };
      ws.onclose = () => {
        setConnected(false);
        if (stopped) return;
        const delay = Math.min(1000 * (2 ** reconnectAttemptRef.current), 10_000);
        reconnectAttemptRef.current += 1;
        reconnectTimerRef.current = window.setTimeout(connect, delay);
      };
      ws.onerror = () => {
        setConnected(false);
        ws.close();
      };
      ws.onmessage = (event: MessageEvent) => {
        let message: unknown;
        try {
          message = JSON.parse(event.data);
        } catch {
          return;
        }
        if (typeof message !== 'object' || message === null || !('type' in message)) return;
        const typed = message as {
          type: string;
          data?: Transaction;
          event_id?: string;
          stream_id?: string;
          last_event_id?: string | null;
          replay_status?: 'running' | 'paused' | 'completed';
        };
        if (typed.replay_status) setStreamStatus(typed.replay_status);
        const cursor = typed.stream_id || typed.last_event_id;
        if (cursor) {
          lastEventIdRef.current = cursor;
        }
        if (typed.type !== 'transaction' || !typed.data) return;
        if (typed.event_id && !rememberEvent(typed.event_id)) return;
        enqueueTransaction(typed.data);
      };
    };

    connect();
    return () => {
      stopped = true;
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      wsRef.current?.close();
    };
  }, [enqueueTransaction]);

  const avgLatency = latencies.length
    ? latencies.reduce((sum, latency) => sum + latency, 0) / latencies.length
    : 0;

  const value = useMemo(
    () => ({
      connected,
      demoMode: DEMO_MODE,
      streamStatus,
      transactions,
      isPaused,
      setIsPaused: setPaused,
      avgLatency,
      threshold,
      setThreshold,
      alerts,
      clearAlerts,
      graphNodes,
      graphEdges,
      latencyHistory,
      throughputHistory,
      totalCount,
      pendingCount,
      streamSpeed,
      setStreamSpeed,
    }),
    [
      connected,
      streamStatus,
      transactions,
      isPaused,
      setPaused,
      avgLatency,
      threshold,
      setThreshold,
      alerts,
      clearAlerts,
      graphNodes,
      graphEdges,
      latencyHistory,
      throughputHistory,
      totalCount,
      pendingCount,
      streamSpeed,
      setStreamSpeed,
    ]
  );

  return <WebSocketContext.Provider value={value}>{children}</WebSocketContext.Provider>;
};

export const useWebSocketContext = (): WebSocketContextValue => useContext(WebSocketContext);
