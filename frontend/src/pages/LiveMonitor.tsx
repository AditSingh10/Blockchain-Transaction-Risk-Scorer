import React, { useEffect, useMemo, useState } from 'react';
import { LiveTransactionGraph } from '../components/monitor/LiveTransactionGraph';
import { TransactionInspector } from '../components/monitor/TransactionDrawer';
import { Icon } from '../components/ui/Icon';
import {
  EmptyState,
  Panel,
  PanelHeader,
  RiskBadge,
  StatusIndicator,
  Toolbar,
} from '../components/ui/Workbench';
import { useWebSocketContext } from '../context/WebSocketContext';
import { Transaction } from '../types';
import {
  formatAmount,
  formatPercent,
  formatTime,
  truncateId,
} from '../utils/format';

type ViewMode = 'table' | 'graph';
type SortKey = 'time' | 'risk' | 'amount' | 'latency';

const speedLabel = (interval: number) =>
  `${interval <= 0.01 ? 100 : Math.round(1 / interval)} tx/s`;

export const LiveMonitor: React.FC = () => {
  const {
    connected,
    transactions,
    isPaused,
    setIsPaused,
    threshold,
    setThreshold,
    streamSpeed,
    setStreamSpeed,
    graphNodes,
    graphEdges,
  } = useWebSocketContext();
  const [selectedTx, setSelectedTx] = useState<Transaction | null>(null);
  const [flaggedOnly, setFlaggedOnly] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>('graph');
  const [sortKey, setSortKey] = useState<SortKey>('time');
  const [sortDescending, setSortDescending] = useState(true);

  useEffect(() => {
    if (!selectedTx && transactions.length > 0) setSelectedTx(transactions[0]);
  }, [selectedTx, transactions]);

  const filtered = useMemo(() => {
    const next = transactions.filter(transaction =>
      !flaggedOnly || transaction.illicit_probability >= threshold
    );
    return [...next].sort((a, b) => {
      const values: Record<SortKey, [number, number]> = {
        time: [a.timestamp, b.timestamp],
        risk: [a.illicit_probability, b.illicit_probability],
        amount: [a.amount, b.amount],
        latency: [a.inference_latency_ms, b.inference_latency_ms],
      };
      const [left, right] = values[sortKey];
      return (left - right) * (sortDescending ? -1 : 1);
    });
  }, [flaggedOnly, sortDescending, sortKey, threshold, transactions]);

  const setSort = (key: SortKey) => {
    if (key === sortKey) setSortDescending(value => !value);
    else {
      setSortKey(key);
      setSortDescending(true);
    }
  };

  const highRiskCount = transactions.filter(tx => tx.illicit_probability >= threshold).length;
  const density = transactions.slice(0, 42).reverse();

  return (
    <div className="page page-live-monitor">
      <header className="workbench-heading">
        <div>
          <div className="heading-line">
            <h1>Live Monitor</h1>
            <StatusIndicator
              state={connected ? (isPaused ? 'warning' : 'healthy') : 'critical'}
              label={connected ? (isPaused ? 'Stream paused' : 'Live inference') : 'Backend disconnected'}
              compact
            />
          </div>
          <p>Score incoming Bitcoin transactions and pivot into graph context.</p>
        </div>

        <Toolbar className="monitor-toolbar">
          <label className="control-group range-control">
            <span>Threshold <code>{formatPercent(threshold, 0)}</code></span>
            <input
              type="range"
              min="0.5"
              max="0.99"
              step="0.01"
              value={threshold}
              onChange={event => setThreshold(Number(event.target.value))}
            />
          </label>
          <label className="control-group range-control speed-control">
            <span>Rate <code>{speedLabel(streamSpeed)}</code></span>
            <input
              type="range"
              min="0.01"
              max="0.5"
              step="0.01"
              value={streamSpeed}
              onChange={event => setStreamSpeed(Number(event.target.value))}
            />
          </label>
          <label className="check-control">
            <input
              type="checkbox"
              checked={flaggedOnly}
              onChange={event => setFlaggedOnly(event.target.checked)}
            />
            <span>Flagged only</span>
          </label>
          <div className="segmented-control" aria-label="Workspace view">
            <button
              className={viewMode === 'table' ? 'is-active' : ''}
              onClick={() => setViewMode('table')}
              aria-pressed={viewMode === 'table'}
            >
              <Icon name="table" size={14} />Table
            </button>
            <button
              className={viewMode === 'graph' ? 'is-active' : ''}
              onClick={() => setViewMode('graph')}
              aria-pressed={viewMode === 'graph'}
            >
              <Icon name="graph" size={14} />Graph
            </button>
          </div>
          <button
            className={`button stream-control${isPaused ? ' is-paused' : ''}`}
            onClick={() => setIsPaused(!isPaused)}
          >
            <Icon name={isPaused ? 'play' : 'pause'} size={14} />
            {isPaused ? 'Resume' : 'Pause'}
          </button>
        </Toolbar>
      </header>

      <div className={`monitor-workspace${selectedTx ? ' has-inspector' : ''}`}>
        <Panel className="stream-rail">
          <PanelHeader
            title="Incoming stream"
            meta={`${transactions.length} buffered`}
          />
          <div className="stream-rail-summary">
            <span><strong>{highRiskCount}</strong> above threshold</span>
            <span><strong>{transactions.length - highRiskCount}</strong> cleared</span>
          </div>
          <div className="stream-list">
            {transactions.slice(0, 40).map(transaction => {
              const selected = transaction.tx_id === selectedTx?.tx_id;
              return (
                <button
                  key={`${transaction.tx_id}-${transaction.timestamp}`}
                  className={`stream-item${selected ? ' is-selected' : ''}`}
                  onClick={() => setSelectedTx(transaction)}
                >
                  <span className="stream-time">{formatTime(transaction.timestamp)}</span>
                  <span className="stream-id mono">{truncateId(transaction.tx_id, 7, 4)}</span>
                  <span className={`stream-risk${transaction.illicit_probability >= threshold ? ' is-high' : ''}`}>
                    {formatPercent(transaction.illicit_probability, 1)}
                  </span>
                </button>
              );
            })}
            {transactions.length === 0 && (
              <EmptyState
                title={connected ? 'Waiting for transactions' : 'Stream unavailable'}
                detail={connected ? 'The first scored event will appear here.' : 'Start the API or use opt-in demo mode.'}
              />
            )}
          </div>
        </Panel>

        <Panel className="analysis-surface">
          <PanelHeader
            title={viewMode === 'graph' ? 'Transaction graph' : 'Scored transactions'}
            meta={viewMode === 'graph'
              ? `${graphNodes.length} visible nodes`
              : `${filtered.length} matching rows`}
            actions={<span className="surface-context mono">BTC / ELLIPTIC / LIVE</span>}
          />

          {viewMode === 'graph' ? (
            graphNodes.length > 0 ? (
              <LiveTransactionGraph
                nodes={graphNodes}
                edges={graphEdges}
                transactions={transactions}
                threshold={threshold}
                selectedId={selectedTx?.tx_id}
                onSelect={setSelectedTx}
              />
            ) : (
              <EmptyState
                icon="graph"
                title={connected ? 'Building transaction graph' : 'Graph feed disconnected'}
                detail={connected ? 'Nodes appear as transactions are scored.' : 'No graph data is available from the backend.'}
              />
            )
          ) : (
            <div className="data-table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th><button onClick={() => setSort('time')}>Observed <SortMark active={sortKey === 'time'} desc={sortDescending} /></button></th>
                    <th>Transaction ID</th>
                    <th className="numeric"><button onClick={() => setSort('amount')}>Amount <SortMark active={sortKey === 'amount'} desc={sortDescending} /></button></th>
                    <th className="numeric"><button onClick={() => setSort('risk')}>Risk <SortMark active={sortKey === 'risk'} desc={sortDescending} /></button></th>
                    <th>Status</th>
                    <th className="numeric"><button onClick={() => setSort('latency')}>Latency <SortMark active={sortKey === 'latency'} desc={sortDescending} /></button></th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map(transaction => (
                    <tr
                      key={`${transaction.tx_id}-${transaction.timestamp}`}
                      className={transaction.tx_id === selectedTx?.tx_id ? 'is-selected' : ''}
                      onClick={() => setSelectedTx(transaction)}
                      onKeyDown={event => {
                        if (event.key === 'Enter' || event.key === ' ') setSelectedTx(transaction);
                      }}
                      tabIndex={0}
                    >
                      <td className="mono muted">{formatTime(transaction.timestamp)}</td>
                      <td className="mono" title={transaction.tx_id}>{truncateId(transaction.tx_id, 12, 7)}</td>
                      <td className="mono numeric">{formatAmount(transaction.amount)}</td>
                      <td className="mono numeric risk-cell">{formatPercent(transaction.illicit_probability, 2)}</td>
                      <td><RiskBadge probability={transaction.illicit_probability} threshold={threshold} /></td>
                      <td className="mono numeric muted">{transaction.inference_latency_ms.toFixed(2)} ms</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {filtered.length === 0 && (
                <EmptyState
                  icon="table"
                  title="No matching transactions"
                  detail={transactions.length ? 'Adjust the risk filter to expand the review set.' : 'Waiting for scored stream data.'}
                />
              )}
            </div>
          )}

          <div className="event-timeline">
            <div className="timeline-label">
              <Icon name="timeline" size={14} />
              <span>Event density</span>
              <code>{density.length ? `${formatTime(density[0].timestamp, false)}–${formatTime(density.at(-1)!.timestamp, false)}` : 'No range'}</code>
            </div>
            <div className="timeline-bars">
              {density.map(transaction => (
                <button
                  key={`${transaction.tx_id}-timeline`}
                  title={`${formatTime(transaction.timestamp)} · ${formatPercent(transaction.illicit_probability, 1)} risk`}
                  className={`${transaction.illicit_probability >= threshold ? 'is-high' : ''}${transaction.tx_id === selectedTx?.tx_id ? ' is-selected' : ''}`}
                  style={{ height: `${Math.max(16, transaction.illicit_probability * 100)}%` }}
                  onClick={() => setSelectedTx(transaction)}
                />
              ))}
            </div>
          </div>
        </Panel>

        <TransactionInspector
          transaction={selectedTx}
          threshold={threshold}
          onClose={() => setSelectedTx(null)}
        />
      </div>
    </div>
  );
};

const SortMark: React.FC<{ active: boolean; desc: boolean }> = ({ active, desc }) => (
  <span className={`sort-mark${active ? ' is-active' : ''}`}>{active ? (desc ? '↓' : '↑') : '↕'}</span>
);
