import React from 'react';
import { useWebSocketContext } from '../../context/WebSocketContext';

export const TopBar: React.FC = () => {
  const { connected, avgLatency, threshold } = useWebSocketContext();

  return (
    <header className="h-14 bg-white border-b border-slate-200 flex items-center justify-between px-6 shrink-0">
      <div className="flex items-center space-x-6 text-sm">
        <div className="flex items-center space-x-2">
          <span className="text-slate-500">Model:</span>
          <span className="font-medium text-slate-900 bg-slate-100 px-2 py-0.5 rounded">GAT-ResNet v1.2.0</span>
        </div>
        <div className="flex items-center space-x-2">
          <span className="text-slate-500">Threshold:</span>
          <span className="font-medium text-slate-900">{(threshold * 100).toFixed(0)}%</span>
        </div>
        <div className="flex items-center space-x-2">
          <span className="text-slate-500">Avg Latency:</span>
          <span className="font-medium text-slate-900">
            {avgLatency > 0 ? `${avgLatency.toFixed(1)}ms` : '—'}
          </span>
        </div>
      </div>
      <div className="flex items-center space-x-3">
        <div className="flex items-center space-x-2">
          <span className="relative flex h-2.5 w-2.5">
            {connected && (
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
            )}
            <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${connected ? 'bg-emerald-500' : 'bg-slate-400'}`} />
          </span>
          <span className={`text-sm font-medium ${connected ? 'text-emerald-700' : 'text-slate-500'}`}>
            {connected ? 'Live Stream Active' : 'Disconnected'}
          </span>
        </div>
      </div>
    </header>
  );
};
