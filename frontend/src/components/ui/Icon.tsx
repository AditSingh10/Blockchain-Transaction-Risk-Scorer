import React from 'react';

export type IconName =
  | 'activity'
  | 'alert'
  | 'arrow'
  | 'chevron'
  | 'close'
  | 'database'
  | 'entity'
  | 'fit'
  | 'graph'
  | 'menu'
  | 'metrics'
  | 'model'
  | 'pause'
  | 'play'
  | 'search'
  | 'table'
  | 'target'
  | 'timeline'
  | 'zoomIn'
  | 'zoomOut';

const paths: Record<IconName, React.ReactNode> = {
  activity: <path d="M3 12h3l2-7 4 14 3-10 2 3h4" />,
  alert: <><path d="M12 3 2.8 19h18.4L12 3Z" /><path d="M12 9v4m0 3h.01" /></>,
  arrow: <path d="m9 18 6-6-6-6" />,
  chevron: <path d="m8 10 4 4 4-4" />,
  close: <path d="m6 6 12 12M18 6 6 18" />,
  database: <><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" /></>,
  entity: <><circle cx="12" cy="8" r="3" /><path d="M5 20c.8-4 3.1-6 7-6s6.2 2 7 6" /></>,
  fit: <path d="M8 3H3v5m13-5h5v5M8 21H3v-5m13 5h5v-5" />,
  graph: <><circle cx="5" cy="12" r="2" /><circle cx="17" cy="6" r="2" /><circle cx="19" cy="18" r="2" /><path d="m7 11 8-4m-8 6 10 4M17 8l2 8" /></>,
  menu: <path d="M4 7h16M4 12h16M4 17h16" />,
  metrics: <path d="M4 19V9m5 10V5m5 14v-7m5 7V3" />,
  model: <><path d="M4 7.5 12 3l8 4.5-8 4.5-8-4.5Z" /><path d="m4 12 8 4.5 8-4.5M4 16.5 12 21l8-4.5" /></>,
  pause: <path d="M9 5v14m6-14v14" />,
  play: <path d="m8 5 11 7-11 7V5Z" />,
  search: <><circle cx="11" cy="11" r="7" /><path d="m20 20-4-4" /></>,
  table: <><rect x="3" y="4" width="18" height="16" rx="1" /><path d="M3 9h18M9 4v16" /></>,
  target: <><circle cx="12" cy="12" r="8" /><circle cx="12" cy="12" r="3" /><path d="M12 2v3m0 14v3M2 12h3m14 0h3" /></>,
  timeline: <><path d="M3 17h18M5 14V9m4 5V5m4 9v-3m4 3V7m4 7v-2" /></>,
  zoomIn: <><circle cx="10" cy="10" r="6" /><path d="m15 15 5 5M10 7v6M7 10h6" /></>,
  zoomOut: <><circle cx="10" cy="10" r="6" /><path d="m15 15 5 5M7 10h6" /></>,
};

export const Icon: React.FC<{ name: IconName; size?: number; className?: string }> = ({
  name,
  size = 16,
  className,
}) => (
  <svg
    aria-hidden="true"
    className={className}
    fill="none"
    height={size}
    viewBox="0 0 24 24"
    width={size}
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    strokeWidth="1.7"
  >
    {paths[name]}
  </svg>
);
