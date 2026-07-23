import React from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Icon } from '../components/ui/Icon';
import {
  EmptyState,
  MetricReadout,
  Panel,
  PanelHeader,
  StatusIndicator,
} from '../components/ui/Workbench';
import { useWebSocketContext } from '../context/WebSocketContext';
import { formatPercent, formatTime } from '../utils/format';

const tick = { fontSize: 10, fill: '#788896', fontFamily: 'ui-monospace, monospace' };
const grid = { strokeDasharray: '2 3', stroke: '#27323c' };
const tooltip = {
  fontSize: 12,
  color: '#d8e0e7',
  background: '#18212a',
  border: '1px solid #34414d',
  borderRadius: 2,
};

export const SystemMetrics: React.FC = () => {
  const {
    connected,
    demoMode,
    latencyHistory,
    throughputHistory,
    alerts,
    totalCount,
    avgLatency,
    isPaused,
  } = useWebSocketContext();
  const currentTps = throughputHistory.at(-1)?.tps ?? 0;
  const flaggedRatio = totalCount > 0 ? alerts.length / totalCount : 0;
  const degraded = connected && avgLatency > 100;

  return (
    <div className="page">
      <header className="workbench-heading">
        <div>
          <div className="heading-line">
            <h1>System Metrics</h1>
            <StatusIndicator
              state={!connected ? 'critical' : degraded ? 'warning' : 'healthy'}
              label={!connected ? 'Pipeline disconnected' : degraded ? 'Latency degraded' : isPaused ? 'Stream paused' : 'Pipeline healthy'}
              compact
            />
          </div>
          <p>Operational telemetry for stream ingestion, graph assembly, and model inference.</p>
        </div>
        {demoMode && <span className="environment-flag">Deterministic demo telemetry</span>}
      </header>

      <div className="pipeline-strip">
        {[
          ['Stream ingest', connected],
          ['Graph buffer', connected],
          ['GAT inference', connected && !degraded],
          ['WebSocket delivery', connected],
        ].map(([label, healthy], index) => (
          <React.Fragment key={String(label)}>
            {index > 0 && <span className="pipeline-link" />}
            <div className={`pipeline-stage${healthy ? ' is-healthy' : ' is-offline'}`}>
              <span>{index + 1}</span>
              <strong>{label}</strong>
              <small>{healthy ? 'Available' : 'Unavailable'}</small>
            </div>
          </React.Fragment>
        ))}
      </div>

      <div className="metric-rail">
        <MetricReadout label="Connection" value={connected ? 'Online' : 'Offline'} detail={isPaused ? 'Stream paused' : 'WebSocket state'} tone={connected ? 'healthy' : 'critical'} />
        <MetricReadout label="Current rate" value={`${currentTps} tx/s`} detail="Latest one-second bucket" />
        <MetricReadout label="Average latency" value={avgLatency > 0 ? `${avgLatency.toFixed(1)} ms` : '—'} detail="Rolling 50 events" tone={degraded ? 'warning' : 'default'} />
        <MetricReadout label="Flagged rate" value={formatPercent(flaggedRatio, 2)} detail="Current client session" tone={flaggedRatio > 0.1 ? 'warning' : 'default'} />
        <MetricReadout label="Processed" value={totalCount.toLocaleString()} detail="Current client session" />
      </div>

      <div className="operations-grid">
        <Panel className="chart-panel operations-chart">
          <PanelHeader title="Inference latency" meta="Last 60 events · milliseconds" />
          <div className="chart-body">
            {latencyHistory.length > 1 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={latencyHistory} margin={{ top: 18, right: 22, bottom: 8, left: 0 }}>
                  <CartesianGrid {...grid} />
                  <XAxis dataKey="time" tickFormatter={value => formatTime(value, false)} tick={tick} interval="preserveStartEnd" />
                  <YAxis tick={tick} unit="ms" width={42} />
                  <Tooltip
                    labelFormatter={value => formatTime(Number(value))}
                    formatter={(value: any) => [`${Number(value).toFixed(2)} ms`, 'Latency']}
                    contentStyle={tooltip}
                  />
                  <Line
                    type="monotone"
                    dataKey="latency"
                    stroke="#5aa9ed"
                    strokeWidth={1.7}
                    dot={false}
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <EmptyState
                icon="metrics"
                title={connected ? 'Waiting for latency samples' : 'Latency feed unavailable'}
                detail={connected ? 'The chart begins after two scored events.' : 'Reconnect the backend to resume telemetry.'}
              />
            )}
          </div>
        </Panel>

        <Panel className="chart-panel operations-chart">
          <PanelHeader title="Scoring throughput" meta="Last 60 seconds · transactions/second" />
          <div className="chart-body">
            {throughputHistory.length > 1 ? (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={throughputHistory} margin={{ top: 18, right: 22, bottom: 8, left: 0 }}>
                  <CartesianGrid {...grid} />
                  <XAxis dataKey="time" tickFormatter={value => formatTime(value, false)} tick={tick} interval="preserveStartEnd" />
                  <YAxis tick={tick} width={32} />
                  <Tooltip
                    labelFormatter={value => formatTime(Number(value))}
                    formatter={(value: any) => [`${value} tx/s`, 'Throughput']}
                    contentStyle={tooltip}
                  />
                  <Area
                    type="stepAfter"
                    dataKey="tps"
                    stroke="#5aa9ed"
                    fill="#18344a"
                    strokeWidth={1.5}
                    dot={false}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <EmptyState
                icon="activity"
                title={connected ? 'Waiting for throughput samples' : 'Throughput feed unavailable'}
                detail={connected ? 'The chart begins after two one-second buckets.' : 'Reconnect the backend to resume telemetry.'}
              />
            )}
          </div>
        </Panel>
      </div>

      {!connected && (
        <div className="operational-message" role="status">
          <Icon name="alert" size={16} />
          <div>
            <strong>Scoring pipeline is disconnected</strong>
            <span>The frontend is not receiving `/ws` events. Verify the API is running at localhost:8000.</span>
          </div>
        </div>
      )}
    </div>
  );
};
