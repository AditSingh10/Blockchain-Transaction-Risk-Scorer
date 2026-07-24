import React, { FormEvent, useState } from 'react';
import { useNavigation } from '../../context/NavigationContext';
import { useWebSocketContext } from '../../context/WebSocketContext';
import { formatPercent } from '../../utils/format';
import { Icon } from '../ui/Icon';
import { StatusIndicator } from '../ui/Workbench';

export const TopBar: React.FC = () => {
  const { navigateToEntity } = useNavigation();
  const {
    connected,
    demoMode,
    streamStatus,
    pendingCount,
    avgLatency,
    threshold,
    isPaused,
    setIsPaused,
  } = useWebSocketContext();
  const [query, setQuery] = useState('');
  const replayComplete = streamStatus === 'completed' && pendingCount === 0;

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (!query.trim()) return;
    navigateToEntity(query);
    setQuery('');
  };

  return (
    <header className="topbar">
      <form className="global-search" onSubmit={submit}>
        <Icon name="search" size={15} />
        <input
          value={query}
          onChange={event => setQuery(event.target.value)}
          placeholder="Search transaction ID"
          aria-label="Search transaction ID"
        />
        <kbd>Enter</kbd>
      </form>

      <div className="topbar-telemetry">
        {demoMode && <span className="environment-flag">Demo data</span>}
        <StatusIndicator
          state={connected ? 'healthy' : 'critical'}
          label={connected ? 'Connected' : 'Disconnected'}
          compact
        />
        <div className="telemetry-item">
          <span>Model</span>
          <code>GAT-RN 1.2</code>
        </div>
        <div className="telemetry-item">
          <span>Threshold</span>
          <code>{formatPercent(threshold, 0)}</code>
        </div>
        <div className="telemetry-item latency-item">
          <span>Latency</span>
          <code>{avgLatency > 0 ? `${avgLatency.toFixed(1)} ms` : '—'}</code>
        </div>
        <button
          className={`stream-state${isPaused ? ' is-paused' : ''}`}
          onClick={() => setIsPaused(!isPaused)}
          disabled={replayComplete}
          type="button"
        >
          <Icon name={isPaused ? 'play' : 'pause'} size={13} />
          {replayComplete ? 'Complete' : isPaused ? 'Resume' : pendingCount > 0 ? 'Replaying' : 'Streaming'}
        </button>
      </div>
    </header>
  );
};
