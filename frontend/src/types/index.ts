export interface Transaction {
  tx_id: string;
  timestamp: number;
  amount: number;
  illicit_probability: number;
  threshold: number;
  flagged: boolean;
  inference_latency_ms: number;
  neighbors?: string[];
  queue_delay_ms?: number;
  end_to_end_latency_ms?: number;
  scoring_end_to_end_latency_ms?: number;
  persistence_latency_ms?: number;
  redis_publication_latency_ms?: number;
  redis_to_websocket_latency_ms?: number;
  ingest_to_redis_ms?: number;
  delivery_mode?: 'live' | 'replay';
  model_version?: string;
  model_deployed_at?: string;
  model_checksum?: string;
  feature_schema_version?: string;
  graph_watermark?: number;
}

export type NavPage = 'monitor' | 'alerts' | 'entity' | 'performance' | 'metrics';
export type ReviewState = 'Unreviewed' | 'Reviewing' | 'Reviewed';

export interface GraphNode {
  id: string;
  illicit_probability: number;
  flagged: boolean;
}

export interface GraphEdge {
  source: string;
  target: string;
}

export interface SubgraphData {
  nodes: Array<{ id: string; risk_score: number }>;
  edges: Array<{ source: string; target: string }>;
  center: string;
}

export interface LatencyPoint {
  time: number;
  latency: number;
}

export interface ThroughputPoint {
  time: number;
  tps: number;
}

export interface EntityData {
  tx_id: string;
  risk_score: number;
  flagged: boolean;
  neighbors: string[];
  cached: boolean;
}
