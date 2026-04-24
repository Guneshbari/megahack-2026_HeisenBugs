import { create } from 'zustand';
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
  fetchMLPredictions,
  fetchFeatureSnapshots,
  RECENT_EVENTS_LIMIT,
  type DashboardMetrics,
  type PipelineHealthData,
} from '../lib/api';
import { useSignalStore } from './signalStore';
import { useIncidentStore } from './incidentStore';
import { useForecastStore } from './forecastStore';
import { useAdaptiveAlertStore } from './adaptiveAlertStore';
import { useFeedbackStore } from './feedbackStore';
import {
  type TimeRange,
  type AutoRefresh,
  type SystemEventSummary,
  TIME_RANGE_WINDOW_MINUTES,
  deriveFilteredEvents,
  deriveSystemSummaries,
  deriveFilteredAlerts,
  deriveFilteredSystems,
} from '../lib/dashboardDerived';

interface DashboardState {
  // Config
  timeRange: TimeRange;
  autoRefresh: AutoRefresh;

  // Global filters
  selectedSystems: string[];
  selectedSeverities: Severity[];
  selectedFaultTypes: string[];
  searchQuery: string;

  // Live Data
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

  // Status
  isLoading: boolean;
  apiError: string | null;
  refreshTick: number;
  recentEventsLimit: number;
  
  // Derived state (pre-computed on updates to avoid full selector re-evaluations)
  filteredEvents: TelemetryEvent[];
  filteredEventsBySystemId: Record<string, TelemetryEvent[]>;
  filteredSystemEventSummaries: Record<string, SystemEventSummary>;
  topSystemsByEventVolume: { name: string; events: number }[];
  filteredAlerts: Alert[];
  filteredSystems: SystemInfo[];

  // Actions
  setTimeRange: (r: TimeRange) => void;
  setAutoRefresh: (r: AutoRefresh) => void;
  setSelectedSystems: (s: string[]) => void;
  setSelectedSeverities: (s: Severity[]) => void;
  setSelectedFaultTypes: (f: string[]) => void;
  setSearchQuery: (q: string) => void;
  clearFilters: () => void;
  loadData: () => Promise<void>;
  tickRefresh: () => void;
}

function deriveMetricsFromEvents(events: TelemetryEvent[], windowMinutes: number): MetricPoint[] {
  if (events.length === 0) return [];

  const now = Date.now();
  const windowMs = windowMinutes * 60_000;
  const recentEvents = events
    .map((event) => ({
      ...event,
      ts: new Date(event.event_time || event.ingested_at || 0).getTime(),
    }))
    .filter((event) => Number.isFinite(event.ts) && now - event.ts <= windowMs);

  if (recentEvents.length === 0) return [];

  const minTs = Math.min(...recentEvents.map((event) => event.ts));
  const maxTs = Math.max(...recentEvents.map((event) => event.ts));
  const spanMs = Math.max(maxTs - minTs, 1);

  let bucketSizeMs = 60 * 60_000;
  if (spanMs <= 15 * 60_000) {
    bucketSizeMs = 30_000;
  } else if (spanMs <= 60 * 60_000) {
    bucketSizeMs = 60_000;
  } else if (spanMs <= 6 * 60 * 60_000) {
    bucketSizeMs = 5 * 60_000;
  } else if (spanMs <= 24 * 60 * 60_000) {
    bucketSizeMs = 15 * 60_000;
  }

  const buckets = new Map<number, TelemetryEvent[]>();

  for (const event of recentEvents) {
    const bucket = Math.floor(event.ts / bucketSizeMs) * bucketSizeMs;
    const bucketEvents = buckets.get(bucket) ?? [];
    bucketEvents.push(event);
    buckets.set(bucket, bucketEvents);
  }

  return [...buckets.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([bucketTs, bucketEvents]) => {
      const countSeverity = (severity: Severity) => bucketEvents.filter((event) => event.severity === severity).length;
      const avg = (selector: (event: TelemetryEvent) => number) =>
        bucketEvents.reduce((sum, event) => sum + (selector(event) || 0), 0) / (bucketEvents.length || 1);

      return {
        timestamp: new Date(bucketTs).toISOString(),
        event_count: bucketEvents.length,
        critical_count: countSeverity('CRITICAL'),
        error_count: countSeverity('ERROR'),
        warning_count: countSeverity('WARNING'),
        info_count: countSeverity('INFO'),
        avg_cpu: avg((event) => event.cpu_usage_percent),
        avg_memory: avg((event) => event.memory_usage_percent),
        avg_disk_free: avg((event) => event.disk_free_percent),
      };
    });
}

export const useDashboardStore = create<DashboardState>((set, get) => ({
  timeRange: '24h',
  autoRefresh: '30s',

  selectedSystems: [],
  selectedSeverities: [],
  selectedFaultTypes: [],
  searchQuery: '',

  allEvents: [],
  systems: [],
  alerts: [],
  metrics: [],
  dashboardMetrics: {
    total_events: 0,
    critical_events: 0,
    warning_events: 0,
  },
  severityDistribution: [],
  faultDistribution: [],
  systemFailures: [],
  pipelineHealth: null,
  pipelineHealthError: null,

  isLoading: true,
  apiError: null,
  refreshTick: 0,
  recentEventsLimit: RECENT_EVENTS_LIMIT,

  filteredEvents: [],
  filteredEventsBySystemId: {},
  filteredSystemEventSummaries: {},
  topSystemsByEventVolume: [],
  filteredAlerts: [],
  filteredSystems: [],

  setTimeRange: (r) => {
    set({ timeRange: r });
    get().loadData(); // Need to reload metrics scoped to time range
  },
  setAutoRefresh: (r) => set({ autoRefresh: r }),

  // Filters recalculate local state instantly without API reload for events/alerts
  setSelectedSystems: (s) => set((state) => applyFilters({ ...state, selectedSystems: s })),
  setSelectedSeverities: (s) => set((state) => applyFilters({ ...state, selectedSeverities: s })),
  setSelectedFaultTypes: (f) => set((state) => applyFilters({ ...state, selectedFaultTypes: f })),
  setSearchQuery: (q) => set((state) => applyFilters({ ...state, searchQuery: q })),

  clearFilters: () => set((state) => applyFilters({
    ...state,
    selectedSystems: [],
    selectedSeverities: [],
    selectedFaultTypes: [],
    searchQuery: '',
  })),

  tickRefresh: () => {
    set((s) => ({ refreshTick: s.refreshTick + 1 }));
    get().loadData();
  },

  loadData: async () => {
    const s = get();
    set({ isLoading: true });

    try {
      const windowMin = TIME_RANGE_WINDOW_MINUTES[s.timeRange];

      const [
        eventsResult, systemsResult, alertsResult, metricsResult, dbMetricsResult,
        sevDistResult, faultDistResult, sysFailResult, pipeResult, mlResult, featureResult
      ] = await Promise.allSettled([
        fetchEvents({
          limit: RECENT_EVENTS_LIMIT,
          search: s.searchQuery || undefined,
          system_id: s.selectedSystems.length === 1 ? s.selectedSystems[0] : undefined,
          severity: s.selectedSeverities.length === 1 ? s.selectedSeverities[0] : undefined,
          fault_type: s.selectedFaultTypes.length === 1 ? s.selectedFaultTypes[0] : undefined,
        }),
        fetchSystems(),
        fetchAlerts(),
        fetchMetrics(undefined, undefined, windowMin),
        fetchDashboardMetrics(windowMin),
        fetchSeverityDistribution(windowMin),
        fetchFaultDistribution(windowMin),
        fetchSystemFailures(6, windowMin),
        fetchPipelineHealth(),
        fetchMLPredictions(),
        fetchFeatureSnapshots(undefined, 500),
      ]);

      const partialUpdate: Partial<DashboardState> = {};

      if (eventsResult.status === 'fulfilled') partialUpdate.allEvents = eventsResult.value;
      if (systemsResult.status === 'fulfilled') partialUpdate.systems = systemsResult.value;
      if (alertsResult.status === 'fulfilled') partialUpdate.alerts = alertsResult.value;
      if (metricsResult.status === 'fulfilled') partialUpdate.metrics = metricsResult.value;
      if (dbMetricsResult.status === 'fulfilled') partialUpdate.dashboardMetrics = dbMetricsResult.value;
      if (sevDistResult.status === 'fulfilled') partialUpdate.severityDistribution = sevDistResult.value;
      if (faultDistResult.status === 'fulfilled') partialUpdate.faultDistribution = faultDistResult.value;
      if (sysFailResult.status === 'fulfilled') partialUpdate.systemFailures = sysFailResult.value;
      
      if (pipeResult.status === 'fulfilled') {
        partialUpdate.pipelineHealth = pipeResult.value;
        partialUpdate.pipelineHealthError = null;
      } else {
        partialUpdate.pipelineHealthError = pipeResult.reason instanceof Error ? pipeResult.reason.message : 'Unavailable';
      }

      // Sync inner stores — preserve stale data when fetch fails
      const mlPreds = mlResult.status === 'fulfilled' ? mlResult.value : null;
      const snaps   = featureResult.status === 'fulfilled' ? featureResult.value : null;
      const evts    = partialUpdate.allEvents ?? s.allEvents;

      if (!partialUpdate.metrics || partialUpdate.metrics.length < 6) {
        partialUpdate.metrics = deriveMetricsFromEvents(evts, windowMin);
      }
      
      useSignalStore.getState().setEvents(evts);
      if (mlPreds !== null) useSignalStore.getState().setMLPredictions(mlPreds);
      if (snaps   !== null) useSignalStore.getState().setFeatureSnapshots(snaps);

      // Resolve: fresh data if available, else current store state
      const resolvedMlPreds = mlPreds ?? useSignalStore.getState().mlPredictions;
      const resolvedSnaps   = snaps   ?? useSignalStore.getState().featureSnapshots;

      // Orchestrate order!
      useForecastStore.getState().ingest(resolvedMlPreds, resolvedSnaps);
      const signals = useSignalStore.getState().signals;
      const systemsList = partialUpdate.systems ?? s.systems;
      const avgCpu = systemsList.length > 0 ? systemsList.reduce((sum, sys) => sum + sys.cpu_usage_percent, 0) / systemsList.length : 0;
      
      useIncidentStore.getState().deriveAll(signals, resolvedMlPreds, resolvedSnaps, avgCpu);

      const alertsData = partialUpdate.alerts ?? s.alerts;
      const avgResolutionMsByRule = useFeedbackStore.getState().avgResolutionMsByRule;
      useAdaptiveAlertStore.getState().process(alertsData, avgResolutionMsByRule);

      // Apply derived computing synchronously
      const nextState = { ...s, ...partialUpdate };
      set(applyFilters(nextState));

    } finally {
      set({ isLoading: false });
    }
  },
}));

// Apply filters over entire static state
function applyFilters(state: DashboardState): DashboardState {
  const filteredEvents = deriveFilteredEvents(
    state.allEvents, state.timeRange, state.selectedSystems, state.selectedSeverities, state.selectedFaultTypes, state.searchQuery
  );

  const { filteredEventsBySystemId, filteredSystemEventSummaries, topSystemsByEventVolume } = deriveSystemSummaries(filteredEvents);

  const filteredAlerts = deriveFilteredAlerts(
    state.alerts, state.timeRange, state.selectedSystems, state.selectedSeverities, state.searchQuery
  );

  const filteredSystems = deriveFilteredSystems(
    state.systems, state.selectedSystems, state.searchQuery
  );

  return {
    ...state,
    filteredEvents,
    filteredEventsBySystemId,
    filteredSystemEventSummaries,
    topSystemsByEventVolume,
    filteredAlerts,
    filteredSystems,
  };
}
