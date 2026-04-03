import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      /**
       * Data classification caching strategy:
       *
       * semi-static (systems, metrics, pipeline): staleTime = 15s
       * static (configs, metadata):               staleTime = 5min
       * real-time (events, alerts):               staleTime = 0 (always fresh)
       *
       * Individual useQuery calls override these defaults as needed.
       */
      staleTime:              15_000,
      gcTime:                 5  * 60 * 1_000,
      refetchOnWindowFocus:   false,
      refetchOnReconnect:     true,
      retry:                  1,
    },
  },
});

interface QueryProviderProps {
  readonly children: ReactNode;
}

export default function QueryProvider({ children }: QueryProviderProps) {
  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  );
}
