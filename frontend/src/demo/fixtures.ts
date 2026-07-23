import {
  EntityData,
  GraphEdge,
  GraphNode,
  LatencyPoint,
  SubgraphData,
  ThroughputPoint,
  Transaction,
} from '../types';

export const DEMO_MODE = process.env.REACT_APP_DEMO_MODE === 'true';
export const DEMO_DISCONNECTED = process.env.REACT_APP_DEMO_STATE === 'disconnected';

const baseTime = Date.UTC(2026, 6, 23, 18, 14, 0);
const ids = Array.from({ length: 96 }, (_, index) => String(298745100 + index * 7919));

const riskFor = (index: number) => {
  if (index % 19 === 0) return 0.982 - (index % 3) * 0.004;
  if (index % 13 === 0) return 0.936 + (index % 4) * 0.008;
  if (index % 8 === 0) return 0.68 + (index % 5) * 0.041;
  return 0.06 + ((index * 17) % 38) / 100;
};

const chronological: Transaction[] = ids.map((tx_id, index) => {
  const probability = Math.min(riskFor(index), 0.997);
  const neighborIndexes = [index - 1, index - 3, index - 8].filter(i => i >= 0);
  return {
    tx_id,
    timestamp: baseTime + index * 780,
    amount: 0.0247 + ((index * 187) % 9200) / 1000,
    illicit_probability: probability,
    threshold: 0.9,
    flagged: probability >= 0.9,
    inference_latency_ms: 11.8 + ((index * 23) % 96) / 10,
    neighbors: neighborIndexes.map(i => ids[i]),
  };
});

export const DEMO_TRANSACTIONS = [...chronological].reverse();
export const DEMO_ALERTS = DEMO_TRANSACTIONS.filter(tx => tx.illicit_probability >= 0.9);
export const DEMO_GRAPH_NODES: GraphNode[] = DEMO_TRANSACTIONS.slice(0, 68).map(tx => ({
  id: tx.tx_id,
  illicit_probability: tx.illicit_probability,
  flagged: tx.flagged,
}));
const graphNodeIds = new Set(DEMO_GRAPH_NODES.map(node => node.id));
export const DEMO_GRAPH_EDGES: GraphEdge[] = DEMO_TRANSACTIONS.slice(0, 68).flatMap(tx =>
  (tx.neighbors ?? [])
    .filter(neighbor => graphNodeIds.has(neighbor))
    .map(neighbor => ({ source: tx.tx_id, target: neighbor }))
);
export const DEMO_LATENCY: LatencyPoint[] = chronological.slice(-60).map(tx => ({
  time: tx.timestamp,
  latency: tx.inference_latency_ms,
}));
export const DEMO_THROUGHPUT: ThroughputPoint[] = Array.from({ length: 45 }, (_, index) => ({
  time: baseTime + (51 + index) * 1000,
  tps: 14 + ((index * 7) % 11),
}));

const transactionById = new Map(DEMO_TRANSACTIONS.map(tx => [tx.tx_id, tx]));

export const getDemoEntity = async (txId: string): Promise<EntityData> => {
  await new Promise(resolve => window.setTimeout(resolve, 260));
  const tx = transactionById.get(txId);
  if (!tx) throw new Error(`Transaction ${txId} is not available in the demo stream.`);
  return {
    tx_id: tx.tx_id,
    risk_score: tx.illicit_probability,
    flagged: tx.flagged,
    neighbors: tx.neighbors ?? [],
    cached: true,
  };
};

export const getDemoSubgraph = async (txId: string): Promise<SubgraphData> => {
  await new Promise(resolve => window.setTimeout(resolve, 180));
  const center = transactionById.get(txId);
  if (!center) throw new Error(`Transaction ${txId} is not available in the demo graph.`);

  const firstHop = center.neighbors ?? [];
  const secondHop = firstHop.flatMap(id => transactionById.get(id)?.neighbors ?? []);
  const nodeIds = Array.from(new Set([txId, ...firstHop, ...secondHop])).slice(0, 28);
  const nodeSet = new Set(nodeIds);
  const nodes = nodeIds.map(id => ({
    id,
    risk_score: transactionById.get(id)?.illicit_probability ?? 0,
  }));
  const edges = nodeIds.flatMap(id =>
    (transactionById.get(id)?.neighbors ?? [])
      .filter(neighbor => nodeSet.has(neighbor))
      .map(neighbor => ({ source: id, target: neighbor }))
  );
  return { center: txId, nodes, edges };
};
