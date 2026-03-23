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
import { auth } from './firebase';

const configuredApiBase = import.meta.env.VITE_SENTINEL_API_BASE_URL?.trim();
const API_BASE = (configuredApiBase || 'http://localhost:8080').replace(/\/+$/, '');
export const RECENT_EVENTS_LIMIT = Number.parseInt(
  import.meta.env.VITE_SENTINEL_RECENT_EVENTS_LIMIT ?? '1000',
  10,
);

let apiSessionAuthenticated = false;

if (!configuredApiBase) {
  console.warn('SentinelCore frontend is using the fallback API base URL. Set VITE_SENTINEL_API_BASE_URL for production deployments.');
}

export function syncApiSessionAuth(isAuthenticated: boolean): void {
  apiSessionAuthenticated = isAuthenticated;
}

async function buildHeaders(): Promise<HeadersInit> {
  if (!apiSessionAuthenticated || !auth.currentUser) {
    throw new Error('Not authenticated. Please log in to access this resource.');
  }

  try {
    // force=true refreshes the token if it is close to expiry (< 5 min)
    const token = await auth.currentUser.getIdToken(true);
    return {
      Authorization: `Bearer ${token}`,
    };
  } catch (error) {
    console.error('Failed to get Firebase token for API request:', error);
    throw new Error('Authentication token unavailable. Please refresh your session.');
  }
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
  const headers = await buildHeaders();
  const res = await fetch(buildEndpoint(endpoint, query), { headers });
  if (!res.ok) {
    if (res.status === 401 || res.status === 403) {
      throw new Error('API authorization failed. You must log in to access this data.');
    }
    throw new Error(`API error ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

// ── Core data fetchers ──────────────────────────────────

interface FetchEventsOptions {
  limit?: number;
  includeRawXml?: boolean;
  system_id?: string;
  severity?: string;
  fault_type?: string;
  search?: string;
}

export async function fetchEvents(
  limitOrOptions: number | FetchEventsOptions = RECENT_EVENTS_LIMIT,
): Promise<TelemetryEvent[]> {
  const options = typeof limitOrOptions === 'number'
    ? { limit: limitOrOptions, includeRawXml: false }
    : {
        limit: limitOrOptions.limit ?? RECENT_EVENTS_LIMIT,
        includeRawXml: limitOrOptions.includeRawXml ?? false,
        system_id: limitOrOptions.system_id,
        severity: limitOrOptions.severity,
        fault_type: limitOrOptions.fault_type,
        search: limitOrOptions.search,
      };

  const query: Record<string, string | number | undefined> = {
    limit: options.limit,
    include_raw_xml: options.includeRawXml ? 1 : 0,
  };

  if (options.system_id) query.system_id = options.system_id;
  if (options.severity) query.severity = options.severity;
  if (options.fault_type) query.fault_type = options.fault_type;
  if (options.search) query.search = options.search;

  const events = await fetchJSON<TelemetryEvent[]>('/events', query);

  return options.includeRawXml ? events : events.map(sanitizeTelemetryEvent);
}

export async function fetchSystems(): Promise<SystemInfo[]> {
  return fetchJSON<SystemInfo[]>('/systems');
}

export async function fetchAlerts(): Promise<Alert[]> {
  return fetchJSON<Alert[]>('/alerts');
}

export async function fetchRecentAlerts(): Promise<Alert[]> {
  return fetchJSON<Alert[]>('/alerts/recent');
}

export async function fetchMetrics(startTime?: string, endTime?: string): Promise<MetricPoint[]> {
  const query: Record<string, string> = {};
  if (startTime) query.start_time = startTime;
  if (endTime) query.end_time = endTime;
  return fetchJSON<MetricPoint[]>('/metrics', query);
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
    const headers = await buildHeaders();
    const res = await fetch(buildEndpoint('/health'), { headers });
    return res.ok;
  } catch {
    return false;
  }
}

// ── Interactive/Mutation endpoints ───────────────────────

export async function registerSystem(hostname: string, ipAddress: string, agentKey: string): Promise<{ success: boolean; system_id?: string }> {
  const headers = await buildHeaders();
  const res = await fetch(buildEndpoint('/systems/register'), {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ hostname, ip_address: ipAddress, agent_key: agentKey }),
  });
  return res.json();
}

export async function executeSystemCommand(systemId: string, command: string): Promise<{ success: boolean; output: string }> {
  const headers = await buildHeaders();
  const res = await fetch(buildEndpoint('/systems/command'), {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ system_id: systemId, command }),
  });
  return res.json();
}

export async function createAlertRule(ruleName: string, condition: string, severity: string, threshold: number): Promise<{ success: boolean }> {
  const headers = await buildHeaders();
  const res = await fetch(buildEndpoint('/alerts/rules'), {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ rule_name: ruleName, condition, severity, threshold }),
  });
  return res.json();
}

/**
 * Download a JSON report via authenticated fetch (avoids exposing the URL without a token).
 */
export async function downloadReport(): Promise<void> {
  const headers = await buildHeaders();
  const res = await fetch(buildEndpoint('/report/generate'), { headers });
  if (!res.ok) throw new Error(`Report generation failed: ${res.status}`);
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'sentinelcore_report.json';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
