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
  GraphEdge,
  GraphNode,
  LatencyPoint,
  ThroughputPoint,
  Transaction,
} from '../types';

interface WebSocketContextValue {
  connected: boolean;
  transactions: Transaction[];
  isPaused: boolean;
  setIsPaused: (v: boolean) => void;
  avgLatency: number;
  // Threshold — controls flagging display + synced to backend
  threshold: number;
  setThreshold: (v: number) => void;
  // Alerts — all effectively-flagged transactions, newest-first, cap 1000
  alerts: Transaction[];
  clearAlerts: () => void;
  // Live graph data
  graphNodes: GraphNode[];
  graphEdges: GraphEdge[];
  // Time-series history for SystemMetrics
  latencyHistory: LatencyPoint[];
  throughputHistory: ThroughputPoint[];
  // Aggregate counters
  totalCount: number;
  // Stream speed control
  streamSpeed: number;
  setStreamSpeed: (v: number) => void;
}

const defaultValue: WebSocketContextValue = {
  connected: false,
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
  streamSpeed: 0.05,
  setStreamSpeed: () => {},
};

const WebSocketContext = createContext<WebSocketContextValue>(defaultValue);

export const WebSocketProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [connected, setConnected] = useState(false);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [isPaused, setIsPaused] = useState(false);
  const [latencies, setLatencies] = useState<number[]>([]);

  const [threshold, setThresholdState] = useState(0.9);
  const [alerts, setAlerts] = useState<Transaction[]>([]);
  const [graphNodes, setGraphNodes] = useState<GraphNode[]>([]);
  const [graphEdges, setGraphEdges] = useState<GraphEdge[]>([]);
  const [latencyHistory, setLatencyHistory] = useState<LatencyPoint[]>([]);
  const [throughputHistory, setThroughputHistory] = useState<ThroughputPoint[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [streamSpeed, setStreamSpeedState] = useState(0.05);

  const wsRef = useRef<WebSocket | null>(null);
  // Refs for stale-closure-safe access inside onmessage (created once on mount)
  const isPausedRef = useRef(isPaused);
  const thresholdRef = useRef(threshold);
  const totalCountRef = useRef(0);

  useEffect(() => { isPausedRef.current = isPaused; }, [isPaused]);
  useEffect(() => { thresholdRef.current = threshold; }, [threshold]);

  const setThreshold = useCallback((v: number) => {
    setThresholdState(v);
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'set_threshold', value: v }));
    }
  }, []);

  const clearAlerts = useCallback(() => setAlerts([]), []);

  const setStreamSpeed = useCallback((v: number) => {
    setStreamSpeedState(v);
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'set_speed', interval: v }));
    }
  }, []);

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws');
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onerror = () => setConnected(false);

    ws.onmessage = (event: MessageEvent) => {
      let msg: any;
      try { msg = JSON.parse(event.data); } catch { return; }
      if (msg.type !== 'transaction') return;
      if (isPausedRef.current) return;

      const tx: Transaction = msg.data;
      const effectiveFlagged = tx.illicit_probability >= thresholdRef.current;

      // Running total (ref — no re-render per tx)
      totalCountRef.current += 1;

      // Rolling latency average
      setLatencies(prev => [...prev.slice(-49), tx.inference_latency_ms]);

      // Main transaction list (newest-first, cap 500)
      setTransactions(prev => [tx, ...prev].slice(0, 500));

      // Alerts — all effectively-flagged, cap 1000
      if (effectiveFlagged) {
        setAlerts(prev => [tx, ...prev].slice(0, 1000));
      }

      // Live graph — rolling 150 nodes + 600 edges
      setGraphNodes(prev => [
        { id: tx.tx_id, illicit_probability: tx.illicit_probability, flagged: effectiveFlagged },
        ...prev,
      ].slice(0, 150));
      setGraphEdges(prev => [
        ...(tx.neighbors ?? []).map(nId => ({ source: tx.tx_id, target: nId })),
        ...prev,
      ].slice(0, 600));

      // Latency history for SystemMetrics chart
      setLatencyHistory(prev => [
        ...prev,
        { time: tx.timestamp, latency: tx.inference_latency_ms },
      ].slice(-60));

      // Throughput history — bucket by second
      setThroughputHistory(prev => {
        const nowSec = Math.floor(tx.timestamp / 1000);
        const last = prev[prev.length - 1];
        if (last && Math.floor(last.time / 1000) === nowSec) {
          const updated = [...prev];
          updated[updated.length - 1] = { time: last.time, tps: last.tps + 1 };
          return updated;
        }
        return [...prev, { time: tx.timestamp, tps: 1 }].slice(-60);
      });

      // Snapshot totalCount in state (cheap — batched with the setTransactions render)
      setTotalCount(totalCountRef.current);
    };

    return () => { ws.close(); };
  }, []); // intentionally runs once on mount

  const avgLatency = latencies.length
    ? latencies.reduce((a, b) => a + b, 0) / latencies.length
    : 0;

  const value = useMemo(
    () => ({
      connected, transactions, isPaused, setIsPaused, avgLatency,
      threshold, setThreshold,
      alerts, clearAlerts,
      graphNodes, graphEdges,
      latencyHistory, throughputHistory,
      totalCount,
      streamSpeed, setStreamSpeed,
    }),
    [
      connected, transactions, isPaused, avgLatency,
      threshold, setThreshold,
      alerts, clearAlerts,
      graphNodes, graphEdges,
      latencyHistory, throughputHistory,
      totalCount,
      streamSpeed, setStreamSpeed,
    ]
  );

  return (
    <WebSocketContext.Provider value={value}>
      {children}
    </WebSocketContext.Provider>
  );
};

export const useWebSocketContext = (): WebSocketContextValue => useContext(WebSocketContext);
