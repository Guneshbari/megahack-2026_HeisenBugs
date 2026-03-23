import {
  useState,
  useEffect,
  useCallback,
  useMemo,
  createContext,
  useContext,
  useRef,
  type ReactNode,
} from 'react';
import type {
  Severity,
  TelemetryEvent,
  SystemInfo,
  Alert,
  MetricPoint,
  SeverityCount,
  FaultTypeCount,
  SystemFailureCount,
} from '../types/telemetry';
import {
  fetchAlerts,
  fetchDashboardMetrics,
  fetchEvents,
  fetchFaultDistribution,
  fetchMetrics,
  fetchPipelineHealth,
  fetchSeverityDistribution,
  fetchSystemFailures,
  fetchSystems,
  RECENT_EVENTS_LIMIT,
  type DashboardMetrics,
  type PipelineHealthData,
} from '../lib/api';

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

const TIME_RANGE_WINDOW_MINUTES: Record<TimeRange, number> = {
  '5m': 5,
  '15m': 15,
  '1h': 60,
  '6h': 360,
  '24h': 1440,
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

type SystemHealthLevel = 'healthy' | 'warning' | 'error' | 'critical';

interface SystemEventSummary {
  eventCount: number;
  criticalCount: number;
  errorCount: number;
  warningCount: number;
  healthScore: number;
  healthLevel: SystemHealthLevel;
  latestEvent: TelemetryEvent | null;
}

function scoreToHealthLevel(score: number): SystemHealthLevel {
  if (score >= 10) return 'critical';
  if (score >= 5) return 'error';
  if (score >= 2) return 'warning';
  return 'healthy';
}

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
  dashboardMetrics: DashboardMetrics;
  severityDistribution: SeverityCount[];
  faultDistribution: FaultTypeCount[];
  systemFailures: SystemFailureCount[];
  pipelineHealth: PipelineHealthData | null;
  pipelineHealthError: string | null;
  isLoading: boolean;
  apiError: string | null;
  recentEventsLimit: number;
  canUseAggregateViews: boolean;

  // Computed
  filteredEvents: TelemetryEvent[];
  filteredEventsBySystemId: Record<string, TelemetryEvent[]>;
  filteredSystemEventSummaries: Record<string, SystemEventSummary>;
  topSystemsByEventVolume: { name: string; events: number }[];
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
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('');
  const [refreshTick, setRefreshTick] = useState(0);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const loadInFlightRef = useRef(false);
  const isPageVisibleRef = useRef(true);

  // Live data state
  const [allEvents, setAllEvents] = useState<TelemetryEvent[]>([]);
  const [systems, setSystems] = useState<SystemInfo[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [metrics, setMetrics] = useState<MetricPoint[]>([]);
  const [dashboardMetrics, setDashboardMetrics] = useState<DashboardMetrics>({
    total_events: 0,
    critical_events: 0,
    warning_events: 0,
  });
  const [severityDistribution, setSeverityDistribution] = useState<SeverityCount[]>([]);
  const [faultDistribution, setFaultDistribution] = useState<FaultTypeCount[]>([]);
  const [systemFailures, setSystemFailures] = useState<SystemFailureCount[]>([]);
  const [pipelineHealth, setPipelineHealth] = useState<PipelineHealthData | null>(null);
  const [pipelineHealthError, setPipelineHealthError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [apiError, setApiError] = useState<string | null>(null);
  const aggregateWindowMinutes = TIME_RANGE_WINDOW_MINUTES[timeRange];

  // Fetch all data from the API
  const loadData = useCallback(async () => {
    if (loadInFlightRef.current) {
      return;
    }

    loadInFlightRef.current = true;
    try {
      const [
        eventsResult,
        systemsResult,
        alertsResult,
        metricsResult,
        dashboardMetricsResult,
        severityDistributionResult,
        faultDistributionResult,
        systemFailuresResult,
        pipelineHealthResult,
      ] = await Promise.allSettled([
        fetchEvents({
          limit: RECENT_EVENTS_LIMIT,
          search: debouncedSearchQuery || undefined,
          system_id: selectedSystems.length === 1 ? selectedSystems[0] : undefined,
          severity: selectedSeverities.length === 1 ? selectedSeverities[0] : undefined,
          fault_type: selectedFaultTypes.length === 1 ? selectedFaultTypes[0] : undefined,
        }),
        fetchSystems(),
        fetchAlerts(),
        fetchMetrics(),
        fetchDashboardMetrics(aggregateWindowMinutes),
        fetchSeverityDistribution(aggregateWindowMinutes),
        fetchFaultDistribution(aggregateWindowMinutes),
        fetchSystemFailures(6, aggregateWindowMinutes),
        fetchPipelineHealth(),
      ]);

      const failures: string[] = [];
      const taskResults = [
        { name: 'events', result: eventsResult },
        { name: 'systems', result: systemsResult },
        { name: 'alerts', result: alertsResult },
        { name: 'metrics', result: metricsResult },
        { name: 'dashboard-metrics', result: dashboardMetricsResult },
        { name: 'severity-distribution', result: severityDistributionResult },
        { name: 'fault-distribution', result: faultDistributionResult },
        { name: 'system-failures', result: systemFailuresResult },
        { name: 'pipeline-health', result: pipelineHealthResult },
      ];

      taskResults.forEach(({ name, result }) => {
        if (result.status === 'rejected') {
          const reason = result.reason instanceof Error ? result.reason.message : 'Unknown error';
          failures.push(`${name}: ${reason}`);
        }
      });

      if (eventsResult.status === 'fulfilled') setAllEvents(eventsResult.value);
      if (systemsResult.status === 'fulfilled') setSystems(systemsResult.value);
      if (alertsResult.status === 'fulfilled') setAlerts(alertsResult.value);
      if (metricsResult.status === 'fulfilled') setMetrics(metricsResult.value);
      if (dashboardMetricsResult.status === 'fulfilled') setDashboardMetrics(dashboardMetricsResult.value);
      if (severityDistributionResult.status === 'fulfilled') setSeverityDistribution(severityDistributionResult.value);
      if (faultDistributionResult.status === 'fulfilled') setFaultDistribution(faultDistributionResult.value);
      if (systemFailuresResult.status === 'fulfilled') setSystemFailures(systemFailuresResult.value);
      if (pipelineHealthResult.status === 'fulfilled') {
        setPipelineHealth(pipelineHealthResult.value);
        setPipelineHealthError(null);
      } else {
        const reason = pipelineHealthResult.reason instanceof Error
          ? pipelineHealthResult.reason.message
          : 'Pipeline API unavailable';
        setPipelineHealthError(reason);
      }

      const errorMessage = failures.length > 0 ? failures.join(' | ') : null;
      setApiError(errorMessage);
      if (errorMessage) {
        console.warn('SentinelCore API error:', errorMessage);
      }
    } finally {
      loadInFlightRef.current = false;
      setIsLoading(false);
    }
  }, [aggregateWindowMinutes, debouncedSearchQuery, selectedSystems, selectedSeverities, selectedFaultTypes]);

  // Debounce search query
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearchQuery(searchQuery), 500);
    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Initial data load
  useEffect(() => {
    void loadData();
  }, [loadData]);

  useEffect(() => {
    if (typeof document === 'undefined') {
      return undefined;
    }

    isPageVisibleRef.current = !document.hidden;
    const handleVisibilityChange = () => {
      isPageVisibleRef.current = !document.hidden;
      if (isPageVisibleRef.current) {
        setRefreshTick((tick) => tick + 1);
        void loadData();
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, [loadData]);

  // Auto-refresh timer — re-fetch data from API
  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    const ms = REFRESH_MS[autoRefresh];
    if (ms) {
      intervalRef.current = setInterval(() => {
        if (!isPageVisibleRef.current) {
          return;
        }
        setRefreshTick((t) => t + 1);
        void loadData();
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
  const canUseAggregateViews = !hasActiveFilters;

  // Filter events based on all global state — use real time
  const filteredEvents = useMemo(() => allEvents.filter((e) => {
    const now = Date.now();
    const eventTime = new Date(e.event_time).getTime();
    if (eventTime < now - TIME_RANGE_MS[timeRange]) return false;

    if (selectedSystems.length > 0 && !selectedSystems.includes(e.hostname)) return false;
    if (selectedSeverities.length > 0 && !selectedSeverities.includes(e.severity)) return false;
    if (selectedFaultTypes.length > 0 && !selectedFaultTypes.includes(e.fault_type)) return false;

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
  }), [
    allEvents,
    timeRange,
    selectedSystems,
    selectedSeverities,
    selectedFaultTypes,
    searchQuery,
  ]);

  const {
    filteredEventsBySystemId,
    filteredSystemEventSummaries,
    topSystemsByEventVolume,
  } = useMemo(() => {
    const eventsBySystemId: Record<string, TelemetryEvent[]> = {};
    const summariesBySystemId: Record<string, SystemEventSummary> = {};
    const eventsByHostname: Record<string, number> = {};

    filteredEvents.forEach((event) => {
      if (!eventsBySystemId[event.system_id]) {
        eventsBySystemId[event.system_id] = [];
      }
      eventsBySystemId[event.system_id].push(event);

      const existingSummary = summariesBySystemId[event.system_id] ?? {
        eventCount: 0,
        criticalCount: 0,
        errorCount: 0,
        warningCount: 0,
        healthScore: 0,
        healthLevel: 'healthy' as SystemHealthLevel,
        latestEvent: null,
      };

      existingSummary.eventCount += 1;
      if (
        !existingSummary.latestEvent ||
        new Date(event.event_time).getTime() > new Date(existingSummary.latestEvent.event_time).getTime()
      ) {
        existingSummary.latestEvent = event;
      }

      if (event.severity === 'CRITICAL') existingSummary.criticalCount += 1;
      if (event.severity === 'ERROR') existingSummary.errorCount += 1;
      if (event.severity === 'WARNING') existingSummary.warningCount += 1;

      summariesBySystemId[event.system_id] = existingSummary;
      eventsByHostname[event.hostname] = (eventsByHostname[event.hostname] || 0) + 1;
    });

    Object.entries(eventsBySystemId).forEach(([systemId, events]) => {
      events.sort((left, right) => new Date(left.event_time).getTime() - new Date(right.event_time).getTime());
      const summary = summariesBySystemId[systemId];
      const healthScore = summary.criticalCount * 5 + summary.errorCount * 2 + summary.warningCount;
      summary.healthScore = healthScore;
      summary.healthLevel = scoreToHealthLevel(healthScore);
    });

    const topSystemEvents = Object.entries(eventsByHostname)
      .map(([name, events]) => ({ name, events }))
      .sort((left, right) => right.events - left.events)
      .slice(0, 6);

    return {
      filteredEventsBySystemId: eventsBySystemId,
      filteredSystemEventSummaries: summariesBySystemId,
      topSystemsByEventVolume: topSystemEvents,
    };
  }, [filteredEvents]);

  // Filter alerts based on global state
  const filteredAlerts = useMemo(() => {
    const _alerts = alerts.filter((a) => {
      // Unacknowledged (active) alerts should NEVER be hidden by time range
      const now = Date.now();
      const alertTime = new Date(a.triggered_at).getTime();
      if (a.acknowledged && alertTime < now - TIME_RANGE_MS[timeRange]) return false;

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

    // Limit unacknowledged alerts to the most recent 50 to preserve UI performance
    let unacknowledgedCount = 0;
    return _alerts.filter(a => {
      if (!a.acknowledged) {
        unacknowledgedCount++;
        return unacknowledgedCount <= 50;
      }
      return true;
    });
  }, [alerts, timeRange, selectedSystems, selectedSeverities, searchQuery]);

  // Filter systems based on global state (no time filtering for status, just metadata filtering)
  const filteredSystems = useMemo(() => systems.filter((s) => {
    if (selectedSystems.length > 0 && !selectedSystems.includes(s.hostname)) return false;
    if (searchQuery) {
      const term = searchQuery.toLowerCase();
      if (!s.hostname.toLowerCase().includes(term) && 
          !s.system_id.toLowerCase().includes(term)) return false;
    }
    return true;
  }), [systems, selectedSystems, searchQuery]);

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
        dashboardMetrics,
        severityDistribution,
        faultDistribution,
        systemFailures,
        pipelineHealth,
        pipelineHealthError,
        isLoading,
        apiError,
        recentEventsLimit: RECENT_EVENTS_LIMIT,
        canUseAggregateViews,
        filteredEvents,
        filteredEventsBySystemId,
        filteredSystemEventSummaries,
        topSystemsByEventVolume,
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
