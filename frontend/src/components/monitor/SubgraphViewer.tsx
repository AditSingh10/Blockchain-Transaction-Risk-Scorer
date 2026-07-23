import React, { useEffect, useMemo, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { DEMO_MODE, getDemoSubgraph } from '../../demo/fixtures';
import { SubgraphData } from '../../types';
import { formatPercent, truncateId } from '../../utils/format';
import { IconButton, LoadingState } from '../ui/Workbench';

interface Props {
  txId: string;
  height?: number;
  compact?: boolean;
  selectedId?: string | null;
  onNodeSelect?: (nodeId: string) => void;
}

export const SubgraphViewer: React.FC<Props> = ({
  txId,
  height = 300,
  compact = false,
  selectedId,
  onNodeSelect,
}) => {
  const [data, setData] = useState<SubgraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<any>(null);
  const [containerWidth, setContainerWidth] = useState(400);

  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver(entries => {
      const width = entries[0]?.contentRect.width;
      if (width) setContainerWidth(width);
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    setData(null);

    const request = DEMO_MODE
      ? getDemoSubgraph(txId)
      : fetch(`http://localhost:8000/subgraph/${encodeURIComponent(txId)}`).then(async response => {
          if (!response.ok) {
            const body = await response.json().catch(() => ({}));
            throw new Error(
              body.detail
              ?? (response.status === 404
                ? 'Transaction is not yet available in the graph buffer.'
                : `Graph service returned ${response.status}.`)
            );
          }
          return response.json() as Promise<SubgraphData>;
        });

    request
      .then(result => { if (active) setData(result); })
      .catch((reason: Error) => { if (active) setError(reason.message); })
      .finally(() => { if (active) setLoading(false); });

    return () => { active = false; };
  }, [txId]);

  const graphData = useMemo(() => {
    if (!data) return { nodes: [], links: [] };
    return {
      nodes: data.nodes.map(node => ({ ...node })),
      links: data.edges.map(edge => ({ ...edge })),
    };
  }, [data]);

  const nodeColor = (node: any) => {
    if (node.id === selectedId) return '#8ec7ff';
    if (node.id === txId) return '#4c9ee8';
    if (node.risk_score >= 0.9) return '#ef5b64';
    if (node.risk_score >= 0.5) return '#d89b3c';
    return '#577288';
  };

  if (loading) {
    return <div className="graph-state" style={{ height }}><LoadingState label="Loading neighborhood" /></div>;
  }

  if (error) {
    return (
      <div className="graph-state graph-error" style={{ height }}>
        <strong>Neighborhood unavailable</strong>
        <span>{error}</span>
      </div>
    );
  }

  return (
    <div ref={containerRef} className={`subgraph-canvas${compact ? ' is-compact' : ''}`}>
      {!compact && (
        <div className="graph-controls">
          <IconButton icon="zoomIn" label="Zoom in" onClick={() => graphRef.current?.zoom(1.35, 250)} />
          <IconButton icon="zoomOut" label="Zoom out" onClick={() => graphRef.current?.zoom(0.75, 250)} />
          <IconButton icon="fit" label="Fit graph" onClick={() => graphRef.current?.zoomToFit(350, 40)} />
        </div>
      )}
      <ForceGraph2D
        ref={graphRef}
        graphData={graphData}
        nodeId="id"
        nodeVal={(node: any) => node.id === txId ? 7 : node.id === selectedId ? 6 : 4}
        nodeColor={nodeColor}
        nodeLabel={(node: any) =>
          `${truncateId(node.id, 12, 8)}${node.id === txId ? ' · query center' : ''}\nRisk ${formatPercent(node.risk_score, 1)}`
        }
        onNodeClick={(node: any) => onNodeSelect?.(node.id)}
        linkColor={() => '#3e4d5a'}
        linkWidth={(link: any) =>
          link.source?.id === selectedId || link.target?.id === selectedId ? 1.8 : 0.8
        }
        backgroundColor="rgba(0,0,0,0)"
        width={containerWidth}
        height={height}
        cooldownTicks={80}
        d3AlphaDecay={0.03}
        d3VelocityDecay={0.4}
      />
    </div>
  );
};
