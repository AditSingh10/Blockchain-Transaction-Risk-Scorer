import React from 'react';
import { useNavigation } from '../../context/NavigationContext';
import { useWebSocketContext } from '../../context/WebSocketContext';
import { NavPage } from '../../types';
import { Icon, IconName } from '../ui/Icon';
import { StatusIndicator } from '../ui/Workbench';

const NAV_ITEMS: Array<{ label: string; page: NavPage; icon: IconName }> = [
  { label: 'Live Monitor', page: 'monitor', icon: 'activity' },
  { label: 'Alerts', page: 'alerts', icon: 'alert' },
  { label: 'Entity Explorer', page: 'entity', icon: 'graph' },
  { label: 'Model Performance', page: 'performance', icon: 'model' },
  { label: 'System Metrics', page: 'metrics', icon: 'metrics' },
];

export const Sidebar: React.FC = () => {
  const { currentPage, navigate } = useNavigation();
  const { alerts, connected, demoMode } = useWebSocketContext();

  return (
    <aside className="sidebar">
      <div className="product-lockup">
        <span className="product-mark"><Icon name="target" size={18} /></span>
        <div className="product-copy">
          <strong>Risk Monitor</strong>
          <span>Blockchain intelligence</span>
        </div>
      </div>

      <nav className="primary-nav" aria-label="Primary navigation">
        <span className="nav-section-label">Workspace</span>
        {NAV_ITEMS.map(({ label, page, icon }) => (
          <button
            key={page}
            className={`nav-item${currentPage === page ? ' is-active' : ''}`}
            onClick={() => navigate(page)}
            aria-current={currentPage === page ? 'page' : undefined}
            title={label}
          >
            <Icon name={icon} />
            <span>{label}</span>
            {page === 'alerts' && alerts.length > 0 && (
              <span className="nav-count">{alerts.length > 99 ? '99+' : alerts.length}</span>
            )}
          </button>
        ))}
      </nav>

      <div className="sidebar-system">
        <div className="sidebar-system-row">
          <Icon name="model" size={14} />
          <span>GAT-ResNet</span>
          <code>v1.2.0</code>
        </div>
        <div className="sidebar-system-row">
          <Icon name="database" size={14} />
          <span>Elliptic</span>
          <code>165F</code>
        </div>
        <StatusIndicator
          state={connected ? 'healthy' : 'critical'}
          label={connected ? (demoMode ? 'Demo stream' : 'Pipeline online') : 'Pipeline offline'}
          compact
        />
      </div>
    </aside>
  );
};
