import React, { useEffect, useMemo, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { GraphEdge, GraphNode, Transaction } from '../../types';
import { formatPercent, truncateId } from '../../utils/format';
import { IconButton } from '../ui/Workbench';

interface Props {
  nodes: GraphNode[];
  edges: GraphEdge[];
  transactions: Transaction[];
  threshold: number;
  selectedId?: string | null;
  onSelect: (transaction: Transaction) => void;
}

export const LiveTransactionGraph: React.FC<Props> = ({
  nodes,
  edges,
  transactions,
  threshold,
  selectedId,
  onSelect,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<any>(null);
  const [size, setSize] = useState({ width: 800, height: 520 });
  const [hasInteracted, setHasInteracted] = useState(false);

  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver(entries => {
      const rect = entries[0]?.contentRect;
      if (rect) setSize({ width: rect.width, height: Math.max(rect.height, 360) });
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  const transactionMap = useMemo(
    () => new Map(transactions.map(transaction => [transaction.tx_id, transaction])),
    [transactions]
  );
  const nodeSet = useMemo(() => new Set(nodes.map(node => node.id)), [nodes]);
  const validEdges = useMemo(
    () => edges.filter(edge =>
      nodeSet.has(String((edge.source as any)?.id ?? edge.source))
      && nodeSet.has(String((edge.target as any)?.id ?? edge.target))
    ),
    [edges, nodeSet]
  );
  const graphData = useMemo(() => ({
    nodes: nodes.map(node => ({ ...node })),
    links: validEdges.map(edge => ({ ...edge })),
  }), [nodes, validEdges]);

  const nodeColor = (node: any) => {
    if (node.id === selectedId) return '#8ec7ff';
    if (node.illicit_probability >= 0.95) return '#ef5b64';
    if (node.illicit_probability >= threshold) return '#d96962';
    if (node.illicit_probability >= 0.5) return '#d89b3c';
    return '#4f6b7f';
  };

  return (
    <div ref={containerRef} className="live-graph-canvas">
      <div className="graph-legend" aria-label="Graph risk legend">
        <span><i className="legend-dot selected" />Selected</span>
        <span><i className="legend-dot critical" />High risk</span>
        <span><i className="legend-dot elevated" />Elevated</span>
        <span><i className="legend-dot clear" />Below threshold</span>
        <code>{nodes.length} nodes / {validEdges.length} links</code>
      </div>
      <div className="graph-controls">
        <IconButton icon="zoomIn" label="Zoom in" onClick={() => graphRef.current?.zoom(1.35, 250)} />
        <IconButton icon="zoomOut" label="Zoom out" onClick={() => graphRef.current?.zoom(0.75, 250)} />
        <IconButton icon="fit" label="Fit graph" onClick={() => graphRef.current?.zoomToFit(350, 48)} />
      </div>
      {!hasInteracted && nodes.length > 0 && (
        <div className="graph-hint">Select a node to synchronize the inspector · scroll to zoom</div>
      )}
      <ForceGraph2D
        ref={graphRef}
        graphData={graphData}
        nodeId="id"
        nodeVal={(node: any) => node.id === selectedId ? 8 : node.illicit_probability >= threshold ? 6 : 4}
        nodeColor={nodeColor}
        nodeLabel={(node: any) => `${truncateId(node.id, 12, 8)}\nRisk ${formatPercent(node.illicit_probability, 1)}`}
        onNodeClick={(node: any) => {
          setHasInteracted(true);
          const transaction = transactionMap.get(node.id);
          if (transaction) onSelect(transaction);
        }}
        onZoom={() => setHasInteracted(true)}
        linkColor={(link: any) =>
          link.source?.id === selectedId || link.target?.id === selectedId ? '#6aa7dc' : '#34434f'
        }
        linkWidth={(link: any) =>
          link.source?.id === selectedId || link.target?.id === selectedId ? 1.8 : 0.65
        }
        backgroundColor="rgba(0,0,0,0)"
        width={size.width}
        height={size.height}
        cooldownTicks={70}
        d3AlphaDecay={0.025}
        d3VelocityDecay={0.34}
      />
    </div>
  );
};
