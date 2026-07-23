import React, { FormEvent, useCallback, useEffect, useState } from 'react';
import { SubgraphViewer } from '../components/monitor/SubgraphViewer';
import { Icon } from '../components/ui/Icon';
import {
  EmptyState,
  InspectorSection,
  LoadingState,
  Panel,
  PanelHeader,
  RiskBadge,
} from '../components/ui/Workbench';
import {
  DEMO_MODE,
  DEMO_TRANSACTIONS,
  getDemoEntity,
} from '../demo/fixtures';
import { useNavigation } from '../context/NavigationContext';
import { EntityData } from '../types';
import { formatPercent, truncateId } from '../utils/format';

export const EntityExplorer: React.FC = () => {
  const { entityQuery } = useNavigation();
  const [searchInput, setSearchInput] = useState('');
  const [queryId, setQueryId] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [entityData, setEntityData] = useState<EntityData | null>(null);
  const [history, setHistory] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const doSearch = useCallback(async (rawId: string) => {
    const id = rawId.trim();
    if (!id) return;
    setQueryId(id);
    setSelectedNodeId(id);
    setSearchInput(id);
    setLoading(true);
    setError(null);
    setEntityData(null);
    setHistory(previous => [...previous.filter(item => item !== id), id].slice(-6));

    try {
      const data = DEMO_MODE
        ? await getDemoEntity(id)
        : await fetch(`http://localhost:8000/entity/${encodeURIComponent(id)}`).then(async response => {
            if (!response.ok) {
              const body = await response.json().catch(() => ({}));
              throw new Error(body.detail ?? `Entity service returned ${response.status}.`);
            }
            return response.json() as Promise<EntityData>;
          });
      setEntityData(data);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Entity lookup failed.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (entityQuery) doSearch(entityQuery);
  }, [doSearch, entityQuery]);

  useEffect(() => {
    if (DEMO_MODE && !entityQuery && !queryId) doSearch(DEMO_TRANSACTIONS[0].tx_id);
  }, [doSearch, entityQuery, queryId]);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    doSearch(searchInput);
  };

  const selectedIsCenter = selectedNodeId === queryId;

  return (
    <div className="page entity-page">
      <header className="workbench-heading">
        <div>
          <h1>Entity Explorer</h1>
          <p>Trace a transaction through its available two-hop graph neighborhood.</p>
        </div>
      </header>

      <form className="query-toolbar" onSubmit={submit}>
        <Icon name="search" size={15} />
        <input
          value={searchInput}
          onChange={event => setSearchInput(event.target.value)}
          placeholder="Enter a streamed transaction ID"
          aria-label="Transaction ID"
        />
        <button className="button button-primary" disabled={!searchInput.trim() || loading}>
          {loading ? 'Querying…' : 'Run query'}
        </button>
        <span className="query-scope">Scope: streamed graph buffer</span>
      </form>

      {history.length > 0 && (
        <nav className="query-history" aria-label="Investigation history">
          <span>History</span>
          {history.map((id, index) => (
            <React.Fragment key={id}>
              {index > 0 && <Icon name="arrow" size={12} />}
              <button
                className={id === queryId ? 'is-current' : ''}
                onClick={() => doSearch(id)}
                title={id}
              >
                {truncateId(id, 8, 5)}
              </button>
            </React.Fragment>
          ))}
        </nav>
      )}

      {loading && (
        <Panel className="entity-state-panel">
          <LoadingState label={`Resolving transaction ${truncateId(searchInput, 10, 6)}`} />
        </Panel>
      )}

      {error && !loading && (
        <Panel className="entity-state-panel error-state">
          <Icon name="alert" size={20} />
          <div>
            <strong>Transaction unavailable</strong>
            <p>{error}</p>
          </div>
          <button className="button" onClick={() => doSearch(searchInput)}>Retry</button>
        </Panel>
      )}

      {entityData && queryId && !loading && (
        <div className="entity-workspace">
          <Panel className="neighbor-panel">
            <PanelHeader
              title="Direct neighbors"
              meta={`${entityData.neighbors.length} connected`}
            />
            <div className="neighbor-list">
              <button
                className={selectedNodeId === queryId ? 'is-selected center-node' : 'center-node'}
                onClick={() => setSelectedNodeId(queryId)}
              >
                <Icon name="target" size={14} />
                <span className="mono">{truncateId(queryId, 10, 5)}</span>
                <small>Query center</small>
              </button>
              {entityData.neighbors.map(neighbor => (
                <button
                  key={neighbor}
                  className={selectedNodeId === neighbor ? 'is-selected' : ''}
                  onClick={() => setSelectedNodeId(neighbor)}
                  onDoubleClick={() => doSearch(neighbor)}
                >
                  <Icon name="entity" size={14} />
                  <span className="mono">{truncateId(neighbor, 10, 5)}</span>
                  <small>Direct neighbor</small>
                </button>
              ))}
              {entityData.neighbors.length === 0 && (
                <EmptyState
                  icon="entity"
                  title="No direct neighbors"
                  detail="The queried transaction is isolated in the current graph buffer."
                />
              )}
            </div>
          </Panel>

          <Panel className="entity-graph-panel">
            <PanelHeader
              title="Neighborhood graph"
              meta="Two-hop context"
              actions={
                <div className="inline-legend">
                  <span><i className="legend-dot selected" />Selected</span>
                  <span><i className="legend-dot center" />Query</span>
                  <span><i className="legend-dot critical" />High risk</span>
                  <span><i className="legend-dot elevated" />Elevated</span>
                </div>
              }
            />
            <SubgraphViewer
              txId={queryId}
              selectedId={selectedNodeId}
              onNodeSelect={setSelectedNodeId}
              height={560}
            />
          </Panel>

          <aside className="entity-inspector">
            <PanelHeader title="Node inspector" meta={selectedIsCenter ? 'Query center' : 'Neighbor'} />
            <div className="entity-inspector-body">
              <div className="inspector-risk-header">
                <div>
                  <span className="eyebrow">Selected node</span>
                  <strong className="node-id mono">{truncateId(selectedNodeId ?? queryId, 14, 8)}</strong>
                </div>
                {selectedIsCenter && (
                  <RiskBadge probability={entityData.risk_score} />
                )}
              </div>

              <InspectorSection title="Graph context">
                <dl className="inspector-list">
                  <div><dt>Relationship</dt><dd>{selectedIsCenter ? 'Query center' : 'Direct or 2-hop neighbor'}</dd></div>
                  <div><dt>Center ID</dt><dd className="mono">{truncateId(queryId, 10, 6)}</dd></div>
                  {selectedIsCenter && (
                    <>
                      <div><dt>Direct neighbors</dt><dd className="mono">{entityData.neighbors.length}</dd></div>
                      <div><dt>Score source</dt><dd>{entityData.cached ? 'Cached inference' : 'Computed now'}</dd></div>
                    </>
                  )}
                </dl>
              </InspectorSection>

              {selectedIsCenter ? (
                <InspectorSection title="Risk summary">
                  <div className="entity-risk-score">
                    <strong>{formatPercent(entityData.risk_score, 2)}</strong>
                    <span>{entityData.flagged ? 'Above model threshold' : 'Below model threshold'}</span>
                  </div>
                </InspectorSection>
              ) : (
                <div className="inspector-callout">
                  Neighbor metadata is resolved only when it becomes the query center.
                </div>
              )}
            </div>
            {!selectedIsCenter && selectedNodeId && (
              <div className="inspector-actions">
                <button className="button button-primary" onClick={() => doSearch(selectedNodeId)}>
                  Refocus investigation
                </button>
              </div>
            )}
          </aside>
        </div>
      )}

      {!queryId && !loading && !error && (
        <Panel className="entity-state-panel">
          <EmptyState
            icon="search"
            title="Begin with a transaction ID"
            detail="Use global search or enter an ID from the live stream to open its neighborhood."
          />
        </Panel>
      )}
    </div>
  );
};
