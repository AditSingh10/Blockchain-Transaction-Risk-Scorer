import React, { createContext, useCallback, useContext, useState } from 'react';
import { NavPage } from '../types';

interface NavigationContextValue {
  currentPage: NavPage;
  navigate: (page: NavPage) => void;
  entityQuery: string;
  navigateToEntity: (txId: string) => void;
}

const NavigationContext = createContext<NavigationContextValue>({
  currentPage: 'monitor',
  navigate: () => {},
  entityQuery: '',
  navigateToEntity: () => {},
});

export const NavigationProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [currentPage, setCurrentPage] = useState<NavPage>('monitor');
  const [entityQuery, setEntityQuery] = useState('');
  const navigate = useCallback((page: NavPage) => setCurrentPage(page), []);
  const navigateToEntity = useCallback((txId: string) => {
    setEntityQuery(txId.trim());
    setCurrentPage('entity');
  }, []);

  return (
    <NavigationContext.Provider value={{ currentPage, navigate, entityQuery, navigateToEntity }}>
      {children}
    </NavigationContext.Provider>
  );
};

export const useNavigation = (): NavigationContextValue => useContext(NavigationContext);
