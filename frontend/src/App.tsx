import React from 'react';
import { AppLayout } from './components/layout/AppLayout';
import { LiveMonitor } from './pages/LiveMonitor';
import { Alerts } from './pages/Alerts';
import { EntityExplorer } from './pages/EntityExplorer';
import { ModelPerformance } from './pages/ModelPerformance';
import { SystemMetrics } from './pages/SystemMetrics';
import { WebSocketProvider } from './context/WebSocketContext';
import { NavigationProvider, useNavigation } from './context/NavigationContext';
import { NavPage } from './types';

const PAGE_MAP: Record<NavPage, React.FC> = {
  monitor: LiveMonitor,
  alerts: Alerts,
  entity: EntityExplorer,
  performance: ModelPerformance,
  metrics: SystemMetrics,
};

const AppInner: React.FC = () => {
  const { currentPage } = useNavigation();
  const Content = PAGE_MAP[currentPage];
  return (
    <AppLayout>
      <Content />
    </AppLayout>
  );
};

const App: React.FC = () => (
  <NavigationProvider>
    <WebSocketProvider>
      <AppInner />
    </WebSocketProvider>
  </NavigationProvider>
);

export default App;
