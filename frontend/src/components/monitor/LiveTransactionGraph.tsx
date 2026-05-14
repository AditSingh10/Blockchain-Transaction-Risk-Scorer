import React, { useEffect, useMemo, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { GraphEdge, GraphNode } from '../../types';

interface Props {
  nodes: GraphNode[];
  edges: GraphEdge[];
  threshold: number;
}

export const LiveTransactionGraph: React.FC<Props> = ({ nodes, edges, threshold }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(800);

  // Measure container width dynamically
  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver(entries => {
      const entry = entries[0];
      if (entry) setContainerWidth(entry.contentRect.width);
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  // Only include edges where both endpoints are in the current rolling node window
  const nodeSet = useMemo(() => new Set(nodes.map(n => n.id)), [nodes]);
  const validEdges = useMemo(
    () => edges.filter(e => nodeSet.has(e.source as string) && nodeSet.has(e.target as string)),
    [edges, nodeSet]
  );

  // ForceGraph2D mutates node objects in-place (adds x, y, vx, vy).
  // Clone to prevent corrupting the context state arrays.
  const graphData = useMemo(() => ({
    nodes: nodes.map(n => ({ ...n })),
    links: validEdges,
  }), [nodes, validEdges]);

  const nodeColor = (n: any) => {
    const prob: number = (n as GraphNode).illicit_probability;
    if (prob >= threshold) return '#ef4444';  // red-500 — flagged
    if (prob >= 0.5) return '#f59e0b';        // amber-500 — suspicious
    return '#22c55e';                          // green-500 — clear
  };

  const nodeLabel = (n: any) => {
    const node = n as GraphNode;
    return `${node.id}\nRisk: ${(node.illicit_probability * 100).toFixed(1)}%`;
  };

  return (
    <div ref={containerRef} className="flex-1 w-full relative bg-slate-50">
      {nodes.length === 0 ? (
        <div className="absolute inset-0 flex items-center justify-center text-slate-400 text-sm">
          Waiting for stream data...
        </div>
      ) : (
        <>
          {/* Legend */}
          <div className="absolute top-3 right-3 z-10 bg-white border border-slate-200 rounded shadow-sm px-3 py-2 text-xs space-y-1">
            <div className="font-medium text-slate-600 mb-1">Risk Level</div>
            <div className="flex items-center space-x-2">
              <span className="inline-block w-3 h-3 rounded-full bg-red-500" />
              <span className="text-slate-500">Flagged (≥ threshold)</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="inline-block w-3 h-3 rounded-full bg-amber-400" />
              <span className="text-slate-500">Suspicious (≥ 50%)</span>
            </div>
            <div className="flex items-center space-x-2">
              <span className="inline-block w-3 h-3 rounded-full bg-green-500" />
              <span className="text-slate-500">Clear</span>
            </div>
            <div className="text-slate-400 pt-1 border-t border-slate-100">{nodes.length} nodes</div>
          </div>

          <ForceGraph2D
            graphData={graphData}
            nodeId="id"
            nodeVal={4}
            nodeColor={nodeColor}
            nodeLabel={nodeLabel}
            linkColor={() => '#94a3b8'}
            linkWidth={0.5}
            backgroundColor="#f8fafc"
            width={containerWidth}
            height={500}
            cooldownTicks={50}
            d3AlphaDecay={0.02}
            d3VelocityDecay={0.3}
          />
        </>
      )}
    </div>
  );
};
