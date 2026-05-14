import React, { useState } from 'react';
import { SubgraphViewer } from '../components/monitor/SubgraphViewer';

interface EntityData {
  tx_id: string;
  risk_score: number;
  flagged: boolean;
  neighbors: string[];
  cached: boolean;
}

export const EntityExplorer: React.FC = () => {
  const [searchInput, setSearchInput] = useState('');
  const [queryId, setQueryId] = useState<string | null>(null);
  const [entityData, setEntityData] = useState<EntityData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const doSearch = async (id: string) => {
    const trimmed = id.trim();
    if (!trimmed) return;
    setQueryId(trimmed);
    setSearchInput(trimmed);
    setLoading(true);
    setError(null);
    setEntityData(null);

    try {
      const res = await fetch(`http://localhost:8000/entity/${encodeURIComponent(trimmed)}`);
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `Server error ${res.status}`);
      }
      const data: EntityData = await res.json();
      setEntityData(data);
    } catch (e: any) {
      setError(e.message ?? 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    doSearch(searchInput);
  };

  const riskColor = (score: number) =>
    score >= 0.9 ? 'text-red-700' : score >= 0.5 ? 'text-amber-600' : 'text-emerald-700';

  const riskBg = (score: number) =>
    score >= 0.9 ? 'bg-red-50 border-red-200' : score >= 0.5 ? 'bg-amber-50 border-amber-200' : 'bg-emerald-50 border-emerald-200';

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-900 tracking-tight">Entity Explorer</h1>
        <p className="text-sm text-slate-500 mt-1">
          Search for a transaction ID to inspect its risk score, neighbors, and 2-hop subgraph.
          Only transactions that have been streamed are available.
        </p>
      </div>

      {/* Search bar */}
      <form onSubmit={handleSubmit} className="flex space-x-3">
        <input
          type="text"
          value={searchInput}
          onChange={e => setSearchInput(e.target.value)}
          placeholder="Enter transaction ID (e.g. 123456789)"
          className="flex-1 border border-slate-300 rounded px-4 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-slate-400 focus:border-slate-400"
        />
        <button
          type="submit"
          disabled={loading || !searchInput.trim()}
          className="px-6 py-2 bg-slate-800 text-white text-sm font-medium rounded hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {loading ? 'Searching...' : 'Search'}
        </button>
      </form>

      {/* Error state */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Results */}
      {entityData && queryId && (
        <div className="space-y-6">
          {/* Risk score card */}
          <div className={`rounded border p-6 ${riskBg(entityData.risk_score)}`}>
            <div className="flex items-start justify-between">
              <div>
                <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Transaction</div>
                <div className="font-mono text-slate-800 text-sm break-all">{entityData.tx_id}</div>
              </div>
              <div className="text-right ml-6">
                <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">Illicit Probability</div>
                <div className={`text-4xl font-bold tracking-tight ${riskColor(entityData.risk_score)}`}>
                  {(entityData.risk_score * 100).toFixed(1)}%
                </div>
                <div className="mt-1">
                  {entityData.flagged ? (
                    <span className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-red-100 text-red-700 border border-red-300">
                      Flagged for Review
                    </span>
                  ) : (
                    <span className="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-slate-100 text-slate-600 border border-slate-300">
                      Cleared
                    </span>
                  )}
                </div>
                {entityData.cached && (
                  <div className="text-xs text-slate-400 mt-1">cached score</div>
                )}
              </div>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-6">
            {/* Neighbors */}
            <div className="bg-white border border-slate-200 rounded shadow-sm">
              <div className="px-4 py-3 border-b border-slate-200 flex items-center justify-between">
                <div className="text-sm font-semibold text-slate-900">Neighbors</div>
                <span className="text-xs text-slate-400">{entityData.neighbors.length} connected</span>
              </div>
              <div className="p-4 max-h-48 overflow-y-auto space-y-1">
                {entityData.neighbors.length === 0 ? (
                  <p className="text-sm text-slate-400">No connections found.</p>
                ) : (
                  entityData.neighbors.map(nId => (
                    <button
                      key={nId}
                      onClick={() => doSearch(nId)}
                      className="w-full text-left px-3 py-1.5 rounded text-xs font-mono text-slate-600 hover:bg-slate-50 hover:text-slate-900 transition-colors border border-transparent hover:border-slate-200"
                    >
                      {nId}
                    </button>
                  ))
                )}
              </div>
            </div>

            {/* Quick stats */}
            <div className="bg-white border border-slate-200 rounded shadow-sm">
              <div className="px-4 py-3 border-b border-slate-200">
                <div className="text-sm font-semibold text-slate-900">Risk Summary</div>
              </div>
              <div className="p-4 space-y-4">
                <div>
                  <div className="flex justify-between text-xs text-slate-500 mb-1">
                    <span>Illicit probability</span>
                    <span>{(entityData.risk_score * 100).toFixed(2)}%</span>
                  </div>
                  <div className="w-full bg-slate-100 h-2 rounded-sm overflow-hidden">
                    <div
                      className={`h-full ${entityData.risk_score >= 0.9 ? 'bg-red-500' : entityData.risk_score >= 0.5 ? 'bg-amber-400' : 'bg-emerald-500'}`}
                      style={{ width: `${entityData.risk_score * 100}%` }}
                    />
                  </div>
                </div>
                <div className="text-xs text-slate-500 space-y-1">
                  <div className="flex justify-between">
                    <span>Degree (direct neighbors)</span>
                    <span className="font-medium text-slate-700">{entityData.neighbors.length}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Score cached</span>
                    <span className="font-medium text-slate-700">{entityData.cached ? 'Yes' : 'No (just computed)'}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Subgraph visualization */}
          <div className="bg-white border border-slate-200 rounded shadow-sm">
            <div className="px-4 py-3 border-b border-slate-200 flex items-center justify-between">
              <div className="text-sm font-semibold text-slate-900">2-Hop Subgraph</div>
              <div className="text-xs text-slate-400">Orange = center node · Red = high risk · Green = clear</div>
            </div>
            <div className="p-4">
              <SubgraphViewer txId={queryId} height={420} />
            </div>
          </div>
        </div>
      )}

      {/* Empty state */}
      {!entityData && !loading && !error && (
        <div className="bg-white border border-slate-200 rounded shadow-sm p-16 text-center">
          <div className="text-slate-400 text-sm">
            Enter a transaction ID above to explore its risk profile and neighborhood graph.
          </div>
          <div className="text-slate-300 text-xs mt-2">
            Tip: copy a tx hash from the Live Monitor table.
          </div>
        </div>
      )}
    </div>
  );
};
