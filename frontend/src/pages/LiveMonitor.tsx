import React, { useState } from 'react';
import { Transaction } from '../types';
import { TransactionDrawer } from '../components/monitor/TransactionDrawer';
import { LiveTransactionGraph } from '../components/monitor/LiveTransactionGraph';
import { useWebSocketContext } from '../context/WebSocketContext';

type ViewMode = 'table' | 'graph';

export const LiveMonitor: React.FC = () => {
  const {
    transactions, isPaused, setIsPaused,
    threshold, setThreshold,
    streamSpeed, setStreamSpeed,
    graphNodes, graphEdges,
  } = useWebSocketContext();

  const [selectedTx, setSelectedTx] = useState<Transaction | null>(null);
  const [flaggedOnly, setFlaggedOnly] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>('table');

  const filtered = transactions.filter(tx => {
    const effectiveFlagged = tx.illicit_probability >= threshold;
    if (flaggedOnly && !effectiveFlagged) return false;
    return true;
  });

  const isFiltering = flaggedOnly;

  return (
    <div className="max-w-7xl mx-auto flex flex-col h-full">
      {/* Header */}
      <div className="flex justify-between items-end mb-6 shrink-0">
        <div>
          <h1 className="text-xl font-semibold text-slate-900 tracking-tight">Live Monitoring Console</h1>
          <p className="text-sm text-slate-500 mt-1">Real-time blockchain transaction inference stream</p>
        </div>

        <div className="flex items-center space-x-3 bg-white p-2.5 rounded border border-slate-200 shadow-sm">
          {/* Threshold slider */}
          <div className="flex items-center space-x-3 border-r border-slate-200 pr-3">
            <label className="text-sm font-medium text-slate-700 flex items-center">
              <span className="mr-3 w-32 whitespace-nowrap">Threshold: {(threshold * 100).toFixed(0)}%</span>
              <input
                type="range" min="0" max="1" step="0.01"
                value={threshold}
                onChange={e => setThreshold(parseFloat(e.target.value))}
                className="w-24 accent-slate-600"
              />
            </label>
          </div>

          {/* Speed slider */}
          <div className="flex items-center space-x-3 border-r border-slate-200 pr-3">
            <label className="text-sm font-medium text-slate-700 flex items-center">
              <span className="mr-3 w-24 whitespace-nowrap">
                {streamSpeed <= 0.01 ? '100' : Math.round(1 / streamSpeed)} tx/s
              </span>
              <input
                type="range" min="0.01" max="2" step="0.01"
                value={streamSpeed}
                onChange={e => setStreamSpeed(parseFloat(e.target.value))}
                className="w-20 accent-slate-600"
              />
            </label>
          </div>

          {/* Flagged only checkbox */}
          <div className="flex items-center space-x-2 border-r border-slate-200 pr-3">
            <label className="text-sm text-slate-700 flex items-center cursor-pointer">
              <input
                type="checkbox" checked={flaggedOnly}
                onChange={e => setFlaggedOnly(e.target.checked)}
                className="mr-2 rounded border-slate-300 text-slate-600 focus:ring-slate-500 cursor-pointer"
              />
              Flagged Only
            </label>
          </div>

          {/* View toggle */}
          <div className="flex items-center border border-slate-200 rounded overflow-hidden text-sm mr-1">
            <button
              onClick={() => setViewMode('table')}
              className={`px-3 py-1.5 transition-colors ${viewMode === 'table' ? 'bg-slate-800 text-white' : 'bg-white text-slate-600 hover:bg-slate-50'}`}
            >
              Table
            </button>
            <button
              onClick={() => setViewMode('graph')}
              className={`px-3 py-1.5 transition-colors ${viewMode === 'graph' ? 'bg-slate-800 text-white' : 'bg-white text-slate-600 hover:bg-slate-50'}`}
            >
              Graph
            </button>
          </div>

          {/* Pause/Resume */}
          <button
            onClick={() => setIsPaused(!isPaused)}
            className="text-sm font-medium px-4 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-800 rounded transition-colors"
          >
            {isPaused ? '▶ Resume' : '⏸ Pause'}
          </button>
        </div>
      </div>

      {/* Main panel */}
      <div className="bg-white border border-slate-200 rounded flex-1 overflow-hidden flex flex-col shadow-sm">
        {viewMode === 'table' ? (
          <div className="overflow-auto flex-1">
            <table className="min-w-full divide-y divide-slate-200 text-sm text-left">
              <thead className="bg-slate-50 sticky top-0 z-10 shadow-sm">
                <tr>
                  <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs">Time</th>
                  <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs">Tx Hash</th>
                  <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-right">Amount</th>
                  <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-right">Probability</th>
                  <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-center">Status</th>
                  <th className="px-6 py-3.5 font-semibold text-slate-600 uppercase tracking-wider text-xs text-right">Latency</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 bg-white">
                {filtered.map((tx) => {
                  const effectiveFlagged = tx.illicit_probability >= threshold;
                  return (
                    <tr
                      key={tx.tx_id}
                      onClick={() => setSelectedTx(tx)}
                      className="hover:bg-slate-50 cursor-pointer transition-colors"
                    >
                      <td className="px-6 py-3 text-slate-500 whitespace-nowrap">
                        {new Date(tx.timestamp).toLocaleTimeString()}
                      </td>
                      <td className="px-6 py-3 font-mono text-slate-600 truncate max-w-[200px]">
                        {tx.tx_id}
                      </td>
                      <td className="px-6 py-3 text-right text-slate-900 font-medium">
                        {tx.amount.toFixed(4)}
                      </td>
                      <td className="px-6 py-3 text-right">
                        <span className={effectiveFlagged ? 'text-red-700 font-semibold' : 'text-slate-600'}>
                          {(tx.illicit_probability * 100).toFixed(2)}%
                        </span>
                      </td>
                      <td className="px-6 py-3 text-center">
                        {effectiveFlagged ? (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-50 text-red-700 border border-red-200">
                            Flagged
                          </span>
                        ) : (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-50 text-slate-600 border border-slate-200">
                            Cleared
                          </span>
                        )}
                      </td>
                      <td className="px-6 py-3 text-right text-slate-400 font-mono text-xs">
                        {tx.inference_latency_ms}ms
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {filtered.length === 0 && (
              <div className="p-12 text-center text-slate-500 text-sm">
                {transactions.length === 0 && !isFiltering
                  ? 'Waiting for stream...'
                  : 'No transactions matching current filters.'}
              </div>
            )}
          </div>
        ) : (
          <LiveTransactionGraph
            nodes={graphNodes}
            edges={graphEdges}
            threshold={threshold}
          />
        )}
      </div>

      <TransactionDrawer transaction={selectedTx} onClose={() => setSelectedTx(null)} />
    </div>
  );
};
