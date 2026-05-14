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
import { useWebSocketContext } from '../context/WebSocketContext';

const TICK_STYLE = { fontSize: 10, fill: '#94a3b8' };
const GRID_STYLE = { strokeDasharray: '3 3', stroke: '#e2e8f0' };
const TOOLTIP_STYLE = { fontSize: 12, borderColor: '#e2e8f0' };

const formatTime = (t: number) => new Date(t).toLocaleTimeString();

export const SystemMetrics: React.FC = () => {
  const { latencyHistory, throughputHistory, alerts, totalCount, avgLatency } = useWebSocketContext();

  const currentTps = throughputHistory.at(-1)?.tps ?? 0;
  const flaggedRatio = totalCount > 0 ? ((alerts.length / totalCount) * 100).toFixed(2) : '0.00';

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900 tracking-tight">System Metrics</h1>
        <p className="text-sm text-slate-500 mt-1">Live inference pipeline health and throughput</p>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-white p-5 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">Throughput</div>
          <div className="text-3xl font-semibold text-slate-900">
            {currentTps} <span className="text-base font-normal text-slate-500">tx/s</span>
          </div>
        </div>
        <div className="bg-white p-5 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">Avg Latency</div>
          <div className="text-3xl font-semibold text-slate-900">
            {avgLatency > 0 ? avgLatency.toFixed(1) : '—'} <span className="text-base font-normal text-slate-500">ms</span>
          </div>
        </div>
        <div className="bg-white p-5 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">Flagged Rate</div>
          <div className="text-3xl font-semibold text-slate-900">
            {flaggedRatio}<span className="text-base font-normal text-slate-500">%</span>
          </div>
        </div>
        <div className="bg-white p-5 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-2">Total Processed</div>
          <div className="text-3xl font-semibold text-slate-900">{totalCount.toLocaleString()}</div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-6">
        {/* Latency history */}
        <div className="bg-white p-6 rounded border border-slate-200 shadow-sm h-72 flex flex-col">
          <h3 className="text-sm font-semibold text-slate-900 mb-1">Inference Latency</h3>
          <p className="text-xs text-slate-400 mb-4">Per-transaction latency over time (last 60 events)</p>
          <ResponsiveContainer width="100%" height="100%">
            {latencyHistory.length > 1 ? (
              <LineChart data={latencyHistory} margin={{ top: 4, right: 16, bottom: 4, left: 4 }}>
                <CartesianGrid {...GRID_STYLE} />
                <XAxis
                  dataKey="time"
                  tickFormatter={formatTime}
                  tick={TICK_STYLE}
                  interval="preserveStartEnd"
                />
                <YAxis tick={TICK_STYLE} unit="ms" />
                <Tooltip
                  labelFormatter={t => formatTime(t as number)}
                  formatter={(v: any) => [`${(v as number).toFixed(1)}ms`, 'Latency']}
                  contentStyle={TOOLTIP_STYLE}
                />
                <Line
                  type="monotone" dataKey="latency"
                  stroke="#3b82f6" strokeWidth={1.5} dot={false}
                  isAnimationActive={false}
                />
              </LineChart>
            ) : (
              <div className="flex items-center justify-center h-full text-slate-400 text-sm">
                Waiting for stream data...
              </div>
            )}
          </ResponsiveContainer>
        </div>

        {/* Throughput over time */}
        <div className="bg-white p-6 rounded border border-slate-200 shadow-sm h-72 flex flex-col">
          <h3 className="text-sm font-semibold text-slate-900 mb-1">Throughput Over Time</h3>
          <p className="text-xs text-slate-400 mb-4">Transactions scored per second (last 60 seconds)</p>
          <ResponsiveContainer width="100%" height="100%">
            {throughputHistory.length > 1 ? (
              <AreaChart data={throughputHistory} margin={{ top: 4, right: 16, bottom: 4, left: 4 }}>
                <CartesianGrid {...GRID_STYLE} />
                <XAxis
                  dataKey="time"
                  tickFormatter={formatTime}
                  tick={TICK_STYLE}
                  interval="preserveStartEnd"
                />
                <YAxis tick={TICK_STYLE} />
                <Tooltip
                  labelFormatter={t => formatTime(t as number)}
                  formatter={(v: any) => [`${v} tx/s`, 'Throughput']}
                  contentStyle={TOOLTIP_STYLE}
                />
                <Area
                  type="monotone" dataKey="tps"
                  stroke="#6366f1" fill="#e0e7ff" strokeWidth={1.5}
                  dot={false} isAnimationActive={false}
                  name="TX/s"
                />
              </AreaChart>
            ) : (
              <div className="flex items-center justify-center h-full text-slate-400 text-sm">
                Waiting for stream data...
              </div>
            )}
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};
