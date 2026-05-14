import React, { useEffect, useMemo, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { SubgraphData } from '../../types';

interface Props {
  txId: string;
  height?: number;
}

export const SubgraphViewer: React.FC<Props> = ({ txId, height = 300 }) => {
  const [data, setData] = useState<SubgraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(400);

  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver(entries => {
      const w = entries[0]?.contentRect.width;
      if (w) setContainerWidth(w);
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setData(null);

    fetch(`http://localhost:8000/subgraph/${txId}`)
      .then(res => {
        if (!res.ok) {
          if (res.status === 404) throw new Error('Transaction not yet in graph buffer — wait for it to be streamed.');
          throw new Error(`Server error: ${res.status}`);
        }
        return res.json();
      })
      .then((d: SubgraphData) => setData(d))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [txId]);

  // ForceGraph2D mutates node objects — always clone
  const graphData = useMemo(() => {
    if (!data) return { nodes: [], links: [] };
    return {
      nodes: data.nodes.map(n => ({ ...n })),
      links: data.edges,
    };
  }, [data]);

  const nodeColor = (n: any) => {
    if (n.id === txId) return '#f97316';            // orange-500 — center node
    if (n.risk_score >= 0.9) return '#ef4444';      // red-500
    if (n.risk_score >= 0.5) return '#f59e0b';      // amber-500
    return '#22c55e';                                // green-500
  };

  const nodeVal = (n: any) => n.id === txId ? 8 : 4;

  const nodeLabel = (n: any) =>
    `${n.id}${n.id === txId ? ' (center)' : ''}\nRisk: ${(n.risk_score * 100).toFixed(1)}%`;

  if (loading) {
    return (
      <div className="flex items-center justify-center bg-slate-50 border border-slate-200 rounded" style={{ height }}>
        <span className="text-sm text-slate-400">Loading subgraph...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center bg-slate-50 border border-slate-200 rounded px-4 text-center" style={{ height }}>
        <span className="text-xs text-slate-400">{error}</span>
      </div>
    );
  }

  return (
    <div ref={containerRef} className="rounded overflow-hidden border border-slate-200">
      <ForceGraph2D
        graphData={graphData}
        nodeId="id"
        nodeVal={nodeVal}
        nodeColor={nodeColor}
        nodeLabel={nodeLabel}
        linkColor={() => '#cbd5e1'}
        linkWidth={1}
        backgroundColor="#f8fafc"
        width={containerWidth}
        height={height}
        cooldownTicks={100}
        d3AlphaDecay={0.03}
        d3VelocityDecay={0.4}
      />
    </div>
  );
};
