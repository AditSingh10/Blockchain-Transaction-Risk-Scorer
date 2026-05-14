import React, { createContext, useCallback, useContext, useState } from 'react';
import { NavPage } from '../types';

interface NavigationContextValue {
  currentPage: NavPage;
  navigate: (page: NavPage) => void;
}

const NavigationContext = createContext<NavigationContextValue>({
  currentPage: 'monitor',
  navigate: () => {},
});

export const NavigationProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [currentPage, setCurrentPage] = useState<NavPage>('monitor');
  const navigate = useCallback((page: NavPage) => setCurrentPage(page), []);

  return (
    <NavigationContext.Provider value={{ currentPage, navigate }}>
      {children}
    </NavigationContext.Provider>
  );
};

export const useNavigation = (): NavigationContextValue => useContext(NavigationContext);
