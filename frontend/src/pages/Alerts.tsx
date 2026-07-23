import React, { useEffect, useMemo, useState } from 'react';
import { TransactionInspector } from '../components/monitor/TransactionDrawer';
import { Icon } from '../components/ui/Icon';
import {
  EmptyState,
  MetricReadout,
  Panel,
  PanelHeader,
  RiskBadge,
} from '../components/ui/Workbench';
import { useWebSocketContext } from '../context/WebSocketContext';
import { ReviewState, Transaction } from '../types';
import {
  formatAmount,
  formatPercent,
  formatTime,
  truncateId,
} from '../utils/format';

type SeverityFilter = 'all' | 'critical' | 'high';
type AlertSortKey = 'time' | 'risk' | 'amount' | 'latency';

export const Alerts: React.FC = () => {
  const { alerts, clearAlerts, threshold } = useWebSocketContext();
  const [selectedTx, setSelectedTx] = useState<Transaction | null>(null);
  const [severity, setSeverity] = useState<SeverityFilter>('all');
  const [reviewStates, setReviewStates] = useState<Record<string, ReviewState>>({});
  const [sortKey, setSortKey] = useState<AlertSortKey>('time');
  const [sortDescending, setSortDescending] = useState(true);

  useEffect(() => {
    if (!selectedTx && alerts.length) setSelectedTx(alerts[0]);
  }, [alerts, selectedTx]);

  const critical = alerts.filter(tx => tx.illicit_probability >= 0.95);
  const high = alerts.filter(tx =>
    tx.illicit_probability >= threshold && tx.illicit_probability < 0.95
  );
  const filtered = useMemo(() => {
    const matching = alerts.filter(tx =>
      severity === 'all'
      || (severity === 'critical' && tx.illicit_probability >= 0.95)
      || (severity === 'high' && tx.illicit_probability >= threshold && tx.illicit_probability < 0.95)
    );
    return [...matching].sort((a, b) => {
      const values: Record<AlertSortKey, [number, number]> = {
        time: [a.timestamp, b.timestamp],
        risk: [a.illicit_probability, b.illicit_probability],
        amount: [a.amount, b.amount],
        latency: [a.inference_latency_ms, b.inference_latency_ms],
      };
      const [left, right] = values[sortKey];
      return (left - right) * (sortDescending ? -1 : 1);
    });
  }, [alerts, severity, sortDescending, sortKey, threshold]);

  const updateReview = (txId: string, state: ReviewState) => {
    setReviewStates(previous => ({ ...previous, [txId]: state }));
  };

  const setSort = (key: AlertSortKey) => {
    if (key === sortKey) setSortDescending(value => !value);
    else {
      setSortKey(key);
      setSortDescending(true);
    }
  };

  return (
    <div className="page">
      <header className="workbench-heading">
        <div>
          <h1>Alert Queue</h1>
          <p>Prioritize transactions whose risk score crossed the active operating threshold.</p>
        </div>
        <div className="heading-actions">
          <span className="local-state-note">Review state is local to this browser</span>
          <button className="button button-subtle" onClick={clearAlerts} disabled={!alerts.length}>
            Clear local queue
          </button>
        </div>
      </header>

      <div className="metric-rail">
        <MetricReadout label="Queue total" value={alerts.length} detail="Flagged events" />
        <MetricReadout label="Critical" value={critical.length} detail="Risk ≥ 95%" tone="critical" />
        <MetricReadout label="High" value={high.length} detail={`${formatPercent(threshold, 0)}–94.99%`} tone="warning" />
        <MetricReadout
          label="Newest alert"
          value={alerts[0] ? formatTime(alerts[0].timestamp) : '—'}
          detail={alerts[0] ? truncateId(alerts[0].tx_id, 8, 4) : 'No queued alert'}
        />
      </div>

      <div className="review-workspace">
        <Panel className="queue-panel">
          <PanelHeader
            title="Review queue"
            meta={`${filtered.length} visible`}
            actions={
              <div className="segmented-control">
                {(['all', 'critical', 'high'] as SeverityFilter[]).map(option => (
                  <button
                    key={option}
                    className={severity === option ? 'is-active' : ''}
                    onClick={() => setSeverity(option)}
                  >
                    {option === 'all' ? 'All' : option[0].toUpperCase() + option.slice(1)}
                  </button>
                ))}
              </div>
            }
          />
          <div className="data-table-wrap">
            <table className="data-table alert-table">
              <thead>
                <tr>
                  <th><button onClick={() => setSort('time')}>Observed <SortMark active={sortKey === 'time'} desc={sortDescending} /></button></th>
                  <th>Transaction ID</th>
                  <th className="numeric"><button onClick={() => setSort('risk')}>Risk <SortMark active={sortKey === 'risk'} desc={sortDescending} /></button></th>
                  <th>Severity</th>
                  <th className="numeric"><button onClick={() => setSort('amount')}>Amount <SortMark active={sortKey === 'amount'} desc={sortDescending} /></button></th>
                  <th className="numeric"><button onClick={() => setSort('latency')}>Latency <SortMark active={sortKey === 'latency'} desc={sortDescending} /></button></th>
                  <th>Queue reason</th>
                  <th>Review</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((tx, index) => {
                  const key = `${tx.tx_id}-${index}`;
                  const reviewState = reviewStates[tx.tx_id] ?? 'Unreviewed';
                  return (
                    <tr
                      key={key}
                      className={selectedTx?.tx_id === tx.tx_id ? 'is-selected' : ''}
                      onClick={() => setSelectedTx(tx)}
                      tabIndex={0}
                      onKeyDown={event => {
                        if (event.key === 'Enter') setSelectedTx(tx);
                      }}
                    >
                      <td className="mono muted">{formatTime(tx.timestamp)}</td>
                      <td className="mono" title={tx.tx_id}>{truncateId(tx.tx_id, 10, 6)}</td>
                      <td className="mono numeric risk-cell">{formatPercent(tx.illicit_probability, 2)}</td>
                      <td><RiskBadge probability={tx.illicit_probability} threshold={threshold} /></td>
                      <td className="mono numeric">{formatAmount(tx.amount)}</td>
                      <td className="mono numeric muted">{tx.inference_latency_ms.toFixed(2)} ms</td>
                      <td className="queue-reason">
                        <Icon name="alert" size={13} />
                        Score ≥ {formatPercent(tx.threshold, 0)}
                      </td>
                      <td onClick={event => event.stopPropagation()}>
                        <select
                          className={`review-select state-${reviewState.toLowerCase()}`}
                          value={reviewState}
                          onChange={event => updateReview(tx.tx_id, event.target.value as ReviewState)}
                          aria-label={`Review state for transaction ${tx.tx_id}`}
                        >
                          <option>Unreviewed</option>
                          <option>Reviewing</option>
                          <option>Reviewed</option>
                        </select>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {filtered.length === 0 && (
              <EmptyState
                icon="alert"
                title={alerts.length ? 'No alerts in this severity' : 'Alert queue is clear'}
                detail={alerts.length
                  ? 'Choose another severity segment to continue review.'
                  : 'Transactions appear here when their score crosses the active threshold.'}
              />
            )}
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
