import React from 'react';
import { useNavigation } from '../../context/NavigationContext';
import { NavPage } from '../../types';

const NAV_ITEMS: Array<{ label: string; page: NavPage }> = [
  { label: 'Live Monitor',      page: 'monitor' },
  { label: 'Alerts',            page: 'alerts' },
  { label: 'Entity Explorer',   page: 'entity' },
  { label: 'Model Performance', page: 'performance' },
  { label: 'System Metrics',    page: 'metrics' },
];

export const Sidebar: React.FC = () => {
  const { currentPage, navigate } = useNavigation();

  return (
    <div className="w-64 bg-slate-900 text-slate-300 flex flex-col border-r border-slate-800 shrink-0">
      <div className="h-14 flex items-center px-4 font-semibold text-white border-b border-slate-800 tracking-tight">
        Risk Monitor
      </div>
      <nav className="flex-1 py-4">
        <ul className="space-y-1">
          {NAV_ITEMS.map(({ label, page }) => (
            <li key={page}>
              <button
                onClick={() => navigate(page)}
                className={`w-full text-left block px-4 py-2 text-sm transition-colors ${
                  currentPage === page
                    ? 'bg-slate-800 text-white font-medium'
                    : 'hover:bg-slate-800 hover:text-white'
                }`}
              >
                {label}
              </button>
            </li>
          ))}
        </ul>
      </nav>
    </div>
  );
};
