/**
 * SentinelCore — API Client
 * Centralized fetch layer for the FastAPI backend.
 * Provides typed methods that match the existing telemetry interfaces.
 */

import type {
  TelemetryEvent,
  SystemInfo,
  Alert,
  MetricPoint,
  SeverityCount,
  FaultTypeCount,
  SystemFailureCount,
} from '../types/telemetry';

const configuredApiBase = import.meta.env.VITE_SENTINEL_API_BASE_URL?.trim();
const API_BASE = (configuredApiBase || 'http://localhost:8080').replace(/\/+$/, '');
const configuredApiBearerToken = import.meta.env.VITE_SENTINEL_API_BEARER_TOKEN?.trim() || null;
export const RECENT_EVENTS_LIMIT = Number.parseInt(
  import.meta.env.VITE_SENTINEL_RECENT_EVENTS_LIMIT ?? '1000',
  10,
);
export const hasConfiguredApiBearerToken = Boolean(configuredApiBearerToken);

let apiSessionAuthenticated = false;

if (!configuredApiBase) {
  console.warn('SentinelCore frontend is using the fallback API base URL. Set VITE_SENTINEL_API_BASE_URL for production deployments.');
}

export function syncApiSessionAuth(isAuthenticated: boolean): void {
  apiSessionAuthenticated = isAuthenticated;
}

function buildHeaders(): HeadersInit | undefined {
  if (!apiSessionAuthenticated || !configuredApiBearerToken) {
    return undefined;
  }

  return {
    Authorization: `Bearer ${configuredApiBearerToken}`,
  };
}

function sanitizeTelemetryEvent(event: TelemetryEvent): TelemetryEvent {
  const safeEvent = { ...event };
  delete safeEvent.raw_xml;
  return safeEvent;
}

function buildEndpoint(
  endpoint: string,
  query?: Record<string, string | number | undefined>,
): string {
  const url = new URL(`${API_BASE}${endpoint}`);
  Object.entries(query ?? {}).forEach(([key, value]) => {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

async function fetchJSON<T>(
  endpoint: string,
  query?: Record<string, string | number | undefined>,
): Promise<T> {
  const res = await fetch(buildEndpoint(endpoint, query), {
    headers: buildHeaders(),
  });
  if (!res.ok) {
    if (res.status === 401 || res.status === 403) {
      throw new Error('API authorization failed. Configure VITE_SENTINEL_API_BEARER_TOKEN to match the backend bearer token.');
    }
    throw new Error(`API error ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

// ── Core data fetchers ──────────────────────────────────

interface FetchEventsOptions {
  limit?: number;
  includeRawXml?: boolean;
}

export async function fetchEvents(
  limitOrOptions: number | FetchEventsOptions = RECENT_EVENTS_LIMIT,
): Promise<TelemetryEvent[]> {
  const options = typeof limitOrOptions === 'number'
    ? { limit: limitOrOptions, includeRawXml: false }
    : {
        limit: limitOrOptions.limit ?? RECENT_EVENTS_LIMIT,
        includeRawXml: limitOrOptions.includeRawXml ?? false,
      };

  const events = await fetchJSON<TelemetryEvent[]>('/events', {
    limit: options.limit,
    include_raw_xml: options.includeRawXml ? 1 : 0,
  });

  return options.includeRawXml ? events : events.map(sanitizeTelemetryEvent);
}

export async function fetchSystems(): Promise<SystemInfo[]> {
  return fetchJSON<SystemInfo[]>('/systems');
}

export async function fetchAlerts(): Promise<Alert[]> {
  return fetchJSON<Alert[]>('/alerts');
}

export async function fetchMetrics(): Promise<MetricPoint[]> {
  return fetchJSON<MetricPoint[]>('/metrics');
}

// ── Aggregation endpoints ───────────────────────────────

export interface DashboardMetrics {
  total_events: number;
  critical_events: number;
  warning_events: number;
}

export async function fetchDashboardMetrics(windowMinutes?: number): Promise<DashboardMetrics> {
  return fetchJSON<DashboardMetrics>('/dashboard-metrics', {
    window_minutes: windowMinutes,
  });
}

export async function fetchFaultDistribution(windowMinutes?: number): Promise<FaultTypeCount[]> {
  return fetchJSON<FaultTypeCount[]>('/fault-distribution', {
    window_minutes: windowMinutes,
  });
}

export async function fetchSeverityDistribution(windowMinutes?: number): Promise<SeverityCount[]> {
  return fetchJSON<SeverityCount[]>('/severity-distribution', {
    window_minutes: windowMinutes,
  });
}

export async function fetchSystemFailures(
  limit = 6,
  windowMinutes?: number,
): Promise<SystemFailureCount[]> {
  return fetchJSON<SystemFailureCount[]>('/system-failures', {
    limit,
    window_minutes: windowMinutes,
  });
}

export interface SystemMetrics {
  avg_cpu: number;
  avg_memory: number;
  avg_disk: number;
}

export async function fetchSystemMetrics(): Promise<SystemMetrics> {
  return fetchJSON<SystemMetrics>('/system-metrics');
}

// ── Pipeline health ────────────────────────────────────

export interface PipelineHealthData {
  events_per_sec: number;
  eps_change_pct: number;
  avg_latency_ms: number;
  kafka_lag: number;
  lag_status: string;
  db_write_rate: number;
  trend_eps: { time: string; value: number }[];
  trend_latency: { time: string; value: number }[];
}

export async function fetchPipelineHealth(): Promise<PipelineHealthData> {
  return fetchJSON<PipelineHealthData>('/pipeline-health');
}

// ── Health check ────────────────────────────────────────

export async function checkAPIHealth(): Promise<boolean> {
  try {
    const res = await fetch(buildEndpoint('/health'), {
      headers: buildHeaders(),
    });
    return res.ok;
  } catch {
    return false;
  }
}
