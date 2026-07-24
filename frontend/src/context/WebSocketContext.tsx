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
  streamSpeed: number;
  setStreamSpeed: (v: number) => void;
}

const defaultValue: WebSocketContextValue = {
  connected: false,
  demoMode: false,
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
  const [connected, setConnected] = useState(DEMO_MODE && !DEMO_DISCONNECTED);
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
  const [streamSpeed, setStreamSpeedState] = useState(0.05);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptRef = useRef(0);
  const lastEventIdRef = useRef(window.localStorage.getItem('risk-monitor-last-event-id'));
  const recentEventIdsRef = useRef<string[]>([]);
  const recentEventIdSetRef = useRef(new Set<string>());
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

  const clearAlerts = useCallback(() => setAlerts([]), []);

  const setPaused = useCallback((value: boolean) => {
    setIsPaused(value);
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
    setTransactions(previous => [tx, ...previous].slice(0, 500));
    if (tx.flagged) setAlerts(previous => [tx, ...previous].slice(0, 1000));
    setGraphNodes(previous => [
      { id: tx.tx_id, illicit_probability: tx.illicit_probability, flagged: tx.flagged },
      ...previous.filter(node => node.id !== tx.tx_id),
    ].slice(0, 150));
    setGraphEdges(previous => [
      ...(tx.neighbors ?? []).map(source => ({ source: tx.tx_id, target: source })),
      ...previous,
    ].slice(0, 600));
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

  useEffect(() => {
    if (!DEMO_MODE || DEMO_DISCONNECTED) return;
    setConnected(true);
    const intervalMs = Math.max(streamSpeed * 1000, 120);
    const timer = window.setInterval(() => {
      if (isPausedRef.current) return;
      const index = demoIndexRef.current % DEMO_TRANSACTIONS.length;
      ingestTransaction(DEMO_TRANSACTIONS[index]);
      demoIndexRef.current += 1;
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
        };
        const cursor = typed.stream_id || typed.last_event_id;
        if (cursor) {
          lastEventIdRef.current = cursor;
          window.localStorage.setItem('risk-monitor-last-event-id', cursor);
        }
        if (typed.type !== 'transaction' || !typed.data || isPausedRef.current) return;
        if (typed.event_id && !rememberEvent(typed.event_id)) return;
        ingestTransaction(typed.data);
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
  }, [ingestTransaction]);

  const avgLatency = latencies.length
    ? latencies.reduce((sum, latency) => sum + latency, 0) / latencies.length
    : 0;

  const value = useMemo(
    () => ({
      connected,
      demoMode: DEMO_MODE,
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
      streamSpeed,
      setStreamSpeed,
    }),
    [
      connected,
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
      streamSpeed,
      setStreamSpeed,
    ]
  );

  return <WebSocketContext.Provider value={value}>{children}</WebSocketContext.Provider>;
};

export const useWebSocketContext = (): WebSocketContextValue => useContext(WebSocketContext);
