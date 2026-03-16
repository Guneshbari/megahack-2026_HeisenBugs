import { useState, useEffect, useCallback, createContext, useContext, useRef, type ReactNode } from 'react';
import type { Severity, TelemetryEvent, SystemInfo, Alert, MetricPoint } from '../types/telemetry';
import { fetchEvents, fetchSystems, fetchAlerts, fetchMetrics } from '../lib/api';

// ── Types ───────────────────────────────────────────────
export type TimeRange = '5m' | '15m' | '1h' | '6h' | '24h';
export type AutoRefresh = 'off' | '5s' | '10s' | '30s' | '1m';

const TIME_RANGE_MS: Record<TimeRange, number> = {
  '5m': 5 * 60_000,
  '15m': 15 * 60_000,
  '1h': 60 * 60_000,
  '6h': 6 * 60 * 60_000,
  '24h': 24 * 60 * 60_000,
};

const REFRESH_MS: Record<AutoRefresh, number | null> = {
  off: null,
  '5s': 5_000,
  '10s': 10_000,
  '30s': 30_000,
  '1m': 60_000,
};

export const TIME_RANGE_LABELS: Record<TimeRange, string> = {
  '5m': 'Last 5 min',
  '15m': 'Last 15 min',
  '1h': 'Last 1 hour',
  '6h': 'Last 6 hours',
  '24h': 'Last 24 hours',
};

export const REFRESH_LABELS: Record<AutoRefresh, string> = {
  off: 'Off',
  '5s': '5s',
  '10s': '10s',
  '30s': '30s',
  '1m': '1m',
};

interface DashboardState {
  // Time range
  timeRange: TimeRange;
  setTimeRange: (r: TimeRange) => void;
  autoRefresh: AutoRefresh;
  setAutoRefresh: (r: AutoRefresh) => void;

  // Global filters
  selectedSystems: string[];
  setSelectedSystems: (s: string[]) => void;
  selectedSeverities: Severity[];
  setSelectedSeverities: (s: Severity[]) => void;
  selectedFaultTypes: string[];
  setSelectedFaultTypes: (f: string[]) => void;
  searchQuery: string;
  setSearchQuery: (q: string) => void;

  // Live data
  allEvents: TelemetryEvent[];
  systems: SystemInfo[];
  alerts: Alert[];
  metrics: MetricPoint[];
  isLoading: boolean;
  apiError: string | null;

  // Computed
  filteredEvents: TelemetryEvent[];
  filteredAlerts: Alert[];
  filteredSystems: SystemInfo[];
  refreshTick: number;
  clearFilters: () => void;
  hasActiveFilters: boolean;
}

const DashboardContext = createContext<DashboardState | null>(null);

// ── Provider ─────────────────────────────────────────────
interface DashboardProviderProps {
  readonly children: ReactNode;
}

export function DashboardProvider({ children }: DashboardProviderProps) {
  const [timeRange, setTimeRange] = useState<TimeRange>('24h');
  const [autoRefresh, setAutoRefresh] = useState<AutoRefresh>('10s');
  const [selectedSystems, setSelectedSystems] = useState<string[]>([]);
  const [selectedSeverities, setSelectedSeverities] = useState<Severity[]>([]);
  const [selectedFaultTypes, setSelectedFaultTypes] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [refreshTick, setRefreshTick] = useState(0);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Live data state
  const [allEvents, setAllEvents] = useState<TelemetryEvent[]>([]);
  const [systems, setSystems] = useState<SystemInfo[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [metrics, setMetrics] = useState<MetricPoint[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [apiError, setApiError] = useState<string | null>(null);

  // Fetch all data from the API
  const loadData = useCallback(async () => {
    try {
      const [eventsData, systemsData, alertsData, metricsData] = await Promise.all([
        fetchEvents(),
        fetchSystems(),
        fetchAlerts(),
        fetchMetrics(),
      ]);
      setAllEvents(eventsData);
      setSystems(systemsData);
      setAlerts(alertsData);
      setMetrics(metricsData);
      setApiError(null);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'API connection failed';
      setApiError(msg);
      console.warn('SentinelCore API error:', msg);
    } finally {
      setIsLoading(false);
    }
  }, []);

  // Initial data load
  useEffect(() => {
    loadData();
  }, [loadData]);

  // Auto-refresh timer — re-fetch data from API
  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    const ms = REFRESH_MS[autoRefresh];
    if (ms) {
      intervalRef.current = setInterval(() => {
        setRefreshTick((t) => t + 1);
        loadData();
      }, ms);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [autoRefresh, loadData]);

  const clearFilters = useCallback(() => {
    setSelectedSystems([]);
    setSelectedSeverities([]);
    setSelectedFaultTypes([]);
    setSearchQuery('');
  }, []);

  const hasActiveFilters =
    selectedSystems.length > 0 ||
    selectedSeverities.length > 0 ||
    selectedFaultTypes.length > 0 ||
    searchQuery.length > 0;

  // Filter events based on all global state — use real time
  const filteredEvents = allEvents.filter((e) => {
    // Time range filter — use real current time
    const now = Date.now();
    const eventTime = new Date(e.event_time).getTime();
    if (eventTime < now - TIME_RANGE_MS[timeRange]) return false;

    // System filter
    if (selectedSystems.length > 0 && !selectedSystems.includes(e.hostname)) return false;

    // Severity filter
    if (selectedSeverities.length > 0 && !selectedSeverities.includes(e.severity)) return false;

    // Fault type filter
    if (selectedFaultTypes.length > 0 && !selectedFaultTypes.includes(e.fault_type)) return false;

    // Search
    if (searchQuery) {
      const term = searchQuery.toLowerCase();
      const matches =
        e.fault_description.toLowerCase().includes(term) ||
        e.hostname.toLowerCase().includes(term) ||
        e.system_id.toLowerCase().includes(term) ||
        e.fault_type.toLowerCase().includes(term) ||
        e.provider_name.toLowerCase().includes(term) ||
        String(e.event_id).includes(term);
      if (!matches) return false;
    }

    return true;
  });

  // Filter alerts based on global state
  const filteredAlerts = alerts.filter((a) => {
    const now = Date.now();
    const alertTime = new Date(a.triggered_at).getTime();
    if (alertTime < now - TIME_RANGE_MS[timeRange]) return false;
    if (selectedSystems.length > 0 && !selectedSystems.includes(a.hostname)) return false;
    if (selectedSeverities.length > 0 && !selectedSeverities.includes(a.severity)) return false;
    if (searchQuery) {
      const term = searchQuery.toLowerCase();
      if (!a.rule.toLowerCase().includes(term) && 
          !a.title.toLowerCase().includes(term) && 
          !a.hostname.toLowerCase().includes(term)) return false;
    }
    return true;
  });

  // Filter systems based on global state (no time filtering for status, just metadata filtering)
  const filteredSystems = systems.filter((s) => {
    if (selectedSystems.length > 0 && !selectedSystems.includes(s.hostname)) return false;
    if (searchQuery) {
      const term = searchQuery.toLowerCase();
      if (!s.hostname.toLowerCase().includes(term) && 
          !s.system_id.toLowerCase().includes(term)) return false;
    }
    return true;
  });

  return (
    <DashboardContext.Provider
      value={{
        timeRange,
        setTimeRange,
        autoRefresh,
        setAutoRefresh,
        selectedSystems,
        setSelectedSystems,
        selectedSeverities,
        setSelectedSeverities,
        selectedFaultTypes,
        setSelectedFaultTypes,
        searchQuery,
        setSearchQuery,
        allEvents,
        systems,
        alerts,
        metrics,
        isLoading,
        apiError,
        filteredEvents,
        filteredAlerts,
        filteredSystems,
        refreshTick,
        clearFilters,
        hasActiveFilters,
      }}
    >
      {children}
    </DashboardContext.Provider>
  );
}

// ── Hook ─────────────────────────────────────────────────
export function useDashboard(): DashboardState {
  const ctx = useContext(DashboardContext);
  if (!ctx) throw new Error('useDashboard must be used within DashboardProvider');
  return ctx;
}
