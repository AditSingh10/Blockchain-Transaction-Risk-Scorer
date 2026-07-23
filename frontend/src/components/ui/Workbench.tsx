import React from 'react';
import { Icon, IconName } from './Icon';

export const Panel: React.FC<React.HTMLAttributes<HTMLDivElement>> = ({
  className = '',
  ...props
}) => <section className={`panel ${className}`} {...props} />;

export const PanelHeader: React.FC<{
  title: string;
  meta?: React.ReactNode;
  actions?: React.ReactNode;
}> = ({ title, meta, actions }) => (
  <header className="panel-header">
    <div className="panel-title">
      <span>{title}</span>
      {meta && <span className="panel-meta">{meta}</span>}
    </div>
    {actions && <div className="panel-actions">{actions}</div>}
  </header>
);

export const Toolbar: React.FC<React.HTMLAttributes<HTMLDivElement>> = ({
  className = '',
  ...props
}) => <div className={`toolbar ${className}`} {...props} />;

export const StatusIndicator: React.FC<{
  state: 'healthy' | 'warning' | 'critical' | 'neutral';
  label: string;
  compact?: boolean;
}> = ({ state, label, compact }) => (
  <span className={`status-indicator status-${state}${compact ? ' is-compact' : ''}`}>
    <span className="status-dot" />
    <span>{label}</span>
  </span>
);

export const RiskBadge: React.FC<{ probability: number; threshold?: number }> = ({
  probability,
  threshold = 0.9,
}) => {
  const level =
    probability >= 0.95 ? 'critical'
    : probability >= threshold ? 'high'
    : probability >= 0.5 ? 'elevated'
    : 'clear';
  const label =
    level === 'critical' ? 'Critical'
    : level === 'high' ? 'High'
    : level === 'elevated' ? 'Elevated'
    : 'Clear';
  return (
    <span className={`risk-badge risk-${level}`}>
      <span className="risk-mark" />
      {label}
    </span>
  );
};

export const MetricReadout: React.FC<{
  label: string;
  value: React.ReactNode;
  detail?: React.ReactNode;
  tone?: 'default' | 'critical' | 'warning' | 'healthy';
}> = ({ label, value, detail, tone = 'default' }) => (
  <div className={`metric-readout metric-${tone}`}>
    <span className="metric-label">{label}</span>
    <strong className="metric-value">{value}</strong>
    {detail && <span className="metric-detail">{detail}</span>}
  </div>
);

export const EmptyState: React.FC<{
  icon?: IconName;
  title: string;
  detail: string;
}> = ({ icon = 'activity', title, detail }) => (
  <div className="empty-state">
    <Icon name={icon} size={20} />
    <strong>{title}</strong>
    <span>{detail}</span>
  </div>
);

export const LoadingState: React.FC<{ label?: string }> = ({ label = 'Loading data' }) => (
  <div className="loading-state" role="status">
    <span className="loading-bar" />
    <span>{label}</span>
  </div>
);

export const IconButton: React.FC<
  React.ButtonHTMLAttributes<HTMLButtonElement> & { icon: IconName; label: string }
> = ({ icon, label, className = '', ...props }) => (
  <button className={`icon-button ${className}`} title={label} aria-label={label} {...props}>
    <Icon name={icon} />
  </button>
);

export const InspectorSection: React.FC<{
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}> = ({ title, children, defaultOpen = true }) => (
  <details className="inspector-section" open={defaultOpen}>
    <summary>{title}</summary>
    <div className="inspector-section-body">{children}</div>
  </details>
);
