import React, { useState } from 'react';
import { Transaction } from '../types';
import { TransactionDrawer } from '../components/monitor/TransactionDrawer';
import { useWebSocketContext } from '../context/WebSocketContext';

function getSeverity(prob: number): { label: string; classes: string } {
  if (prob >= 0.95) return { label: 'Critical', classes: 'bg-red-100 text-red-800 border-red-300' };
  if (prob >= 0.90) return { label: 'High',     classes: 'bg-red-50 text-red-700 border-red-200' };
  if (prob >= 0.75) return { label: 'Medium',   classes: 'bg-amber-50 text-amber-700 border-amber-200' };
  return                   { label: 'Low',      classes: 'bg-yellow-50 text-yellow-700 border-yellow-200' };
}

export const Alerts: React.FC = () => {
  const { alerts, clearAlerts } = useWebSocketContext();
  const [selectedTx, setSelectedTx] = useState<Transaction | null>(null);

  const criticalCount = alerts.filter(tx => tx.illicit_probability >= 0.95).length;
  const highCount     = alerts.filter(tx => tx.illicit_probability >= 0.90 && tx.illicit_probability < 0.95).length;
  const lastAlert     = alerts[0];

  return (
    <div className="max-w-7xl mx-auto flex flex-col h-full">
      {/* Header */}
      <div className="flex justify-between items-end mb-6 shrink-0">
        <div>
          <div className="flex items-center space-x-3">
            <h1 className="text-xl font-semibold text-slate-900 tracking-tight">Active Alerts</h1>
            {alerts.length > 0 && (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                {alerts.length}
              </span>
            )}
          </div>
          <p className="text-sm text-slate-500 mt-1">Flagged transactions requiring analyst review</p>
        </div>
        {alerts.length > 0 && (
          <button
            onClick={clearAlerts}
            className="text-sm font-medium px-4 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded transition-colors"
          >
            Clear All
          </button>
        )}
      </div>

      {/* Stats strip */}
      <div className="grid grid-cols-4 gap-4 mb-6 shrink-0">
        <div className="bg-white p-4 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Total Flagged</div>
          <div className="text-2xl font-semibold text-slate-900">{alerts.length}</div>
        </div>
        <div className="bg-white p-4 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Critical (≥95%)</div>
          <div className="text-2xl font-semibold text-red-700">{criticalCount}</div>
        </div>
        <div className="bg-white p-4 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">High (≥90%)</div>
          <div className="text-2xl font-semibold text-red-600">{highCount}</div>
        </div>
        <div className="bg-white p-4 rounded border border-slate-200 shadow-sm">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Last Alert</div>
          <div className="text-sm font-medium text-slate-900 mt-1">
            {lastAlert ? new Date(lastAlert.timestamp).toLocaleTimeString() : '—'}
          </div>
        </div>
      </div>

      {/* Alerts table */}
      <div className="bg-white border border-slate-200 rounded flex-1 overflow-hidden flex flex-col shadow-sm">
        <div className="overflow-auto flex-1">
          <table className="min-w-full divide-y divide-slate-200 text-sm text-left">
            <thead className="bg-slate-50 sticky top-0 z-10 shadow-sm">
              <tr>
                <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs">Time</th>
                <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs">Tx Hash</th>
                <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-right">Risk Score</th>
                <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-center">Severity</th>
                <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-right">Amount</th>
                <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-right">Latency</th>
                <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs" />
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 bg-white">
              {alerts.map((tx, i) => {
                const sev = getSeverity(tx.illicit_probability);
                return (
                  <tr key={`${tx.tx_id}-${i}`} className="hover:bg-slate-50 transition-colors">
                    <td className="px-6 py-3 text-slate-500 whitespace-nowrap">
                      {new Date(tx.timestamp).toLocaleTimeString()}
                    </td>
                    <td className="px-6 py-3 font-mono text-slate-600 truncate max-w-[180px]">
                      {tx.tx_id}
                    </td>
                    <td className="px-6 py-3 text-right">
                      <div className="flex items-center justify-end space-x-2">
                        <div className="w-16 bg-red-100 rounded-sm h-1.5 overflow-hidden">
                          <div className="bg-red-500 h-full" style={{ width: `${tx.illicit_probability * 100}%` }} />
                        </div>
                        <span className="text-red-700 font-semibold w-12 text-right">
                          {(tx.illicit_probability * 100).toFixed(1)}%
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-3 text-center">
                      <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${sev.classes}`}>
                        {sev.label}
                      </span>
                    </td>
                    <td className="px-6 py-3 text-right text-slate-900 font-medium">
                      {tx.amount.toFixed(4)}
                    </td>
                    <td className="px-6 py-3 text-right text-slate-400 font-mono text-xs">
                      {tx.inference_latency_ms}ms
                    </td>
                    <td className="px-6 py-3 text-right">
                      <button
                        onClick={() => setSelectedTx(tx)}
                        className="text-xs text-slate-500 hover:text-slate-900 underline underline-offset-2"
                      >
                        View
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {alerts.length === 0 && (
            <div className="p-12 text-center text-slate-500 text-sm">
              No flagged transactions yet. Alerts appear here when the risk score exceeds the threshold.
            </div>
          )}
        </div>
      </div>

      <TransactionDrawer transaction={selectedTx} onClose={() => setSelectedTx(null)} />
    </div>
  );
};
