import React from 'react';
import { Sidebar } from './Sidebar';
import { TopBar } from './TopBar';

export const AppLayout: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div className="app-shell">
    <Sidebar />
    <div className="app-column">
      <TopBar />
      <main className="app-main">{children}</main>
    </div>
  </div>
);
