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
  MLPrediction,
  FeatureSnapshot,
  MLAnomaly,
  MLCluster,
} from '../types/telemetry';
import { auth } from './firebase';
import * as mockApi from './mockApi';

const mockModeEnv = import.meta.env.VITE_SENTINEL_USE_MOCK_DATA?.trim().toLowerCase();

export const USE_MOCK_DATA = mockModeEnv === '1'
  || mockModeEnv === 'true'
  || mockModeEnv === 'yes'
  || mockModeEnv === 'on';
export const DASHBOARD_DATA_MODE = USE_MOCK_DATA ? 'mock' : 'live';

const configuredApiBase = import.meta.env.VITE_SENTINEL_API_BASE_URL?.trim();
const API_BASE = (configuredApiBase || 'http://localhost:8000').replace(/\/+$/, '');
export const RECENT_EVENTS_LIMIT = Number.parseInt(
  import.meta.env.VITE_SENTINEL_RECENT_EVENTS_LIMIT ?? '1000',
  10,
);

let apiSessionAuthenticated = false;

if (!configuredApiBase) {
  console.warn('SentinelCore frontend is using the fallback API base URL. Set VITE_SENTINEL_API_BASE_URL for production deployments.');
}

if (USE_MOCK_DATA) {
  console.warn('SentinelCore frontend is running in mock data mode because VITE_SENTINEL_USE_MOCK_DATA is enabled.');
}

export function syncApiSessionAuth(isAuthenticated: boolean): void {
  apiSessionAuthenticated = isAuthenticated;
}

export function isApiSessionAuthenticated(): boolean {
  return apiSessionAuthenticated;
}

export function getTransportStatusLabel(isConnected: boolean): 'LIVE' | 'MOCK' | 'OFFLINE' {
  if (USE_MOCK_DATA) {
    return 'MOCK';
  }
  return isConnected ? 'LIVE' : 'OFFLINE';
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
  const safeEvent = {
    ...event,
    hostname: event.hostname || event.system_id,
    event_time: event.event_time || event.ingested_at || new Date().toISOString(),
    fault_description: event.fault_description || event.parsed_message || event.event_message || event.fault_type,
  };
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
  if (USE_MOCK_DATA) {
    if (endpoint === '/events') return mockApi.fetchEvents() as unknown as T;
    if (endpoint === '/systems') return mockApi.fetchSystems() as unknown as T;
    if (endpoint === '/alerts') return mockApi.fetchAlerts() as unknown as T;
    if (endpoint === '/alerts/recent') return mockApi.fetchRecentAlerts() as unknown as T;
    if (endpoint === '/metrics') return mockApi.fetchMetrics(query?.start_time as string, query?.end_time as string, query?.window_minutes as number) as unknown as T;
    if (endpoint === '/dashboard-metrics') return mockApi.fetchDashboardMetrics(query?.window_minutes as number) as unknown as T;
    if (endpoint === '/fault-distribution') return mockApi.fetchFaultDistribution(query?.window_minutes as number) as unknown as T;
    if (endpoint === '/severity-distribution') return mockApi.fetchSeverityDistribution(query?.window_minutes as number) as unknown as T;
    if (endpoint === '/system-failures') return mockApi.fetchSystemFailures(query?.limit as number, query?.window_minutes as number) as unknown as T;
    if (endpoint === '/system-metrics') return mockApi.fetchSystemMetrics() as unknown as T;
    if (endpoint === '/pipeline-health') return mockApi.fetchPipelineHealth() as unknown as T;
    if (endpoint === '/ml/predictions') return mockApi.fetchMLPredictions(query?.limit as number) as unknown as T;
    if (endpoint === '/feature-snapshots') return mockApi.fetchFeatureSnapshots(query?.system_id as string, query?.limit as number) as unknown as T;
  }

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

async function fetchMutationJSON<T>(input: RequestInfo | URL, init: RequestInit): Promise<T> {
  const res = await fetch(input, init);
  const data = await res.json().catch(() => ({}));
  if (!res.ok || (typeof data === 'object' && data !== null && 'success' in data && data.success === false)) {
    const message = typeof data === 'object' && data !== null && 'error' in data
      ? String(data.error)
      : `API error ${res.status}: ${res.statusText}`;
    throw new Error(message);
  }
  return data as T;
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

export async function fetchMetrics(
  startTime?: string,
  endTime?: string,
  windowMinutes?: number,
): Promise<MetricPoint[]> {
  const query: Record<string, string | number | undefined> = {};
  if (startTime) query.start_time = startTime;
  if (endTime) query.end_time = endTime;
  if (windowMinutes && !startTime && !endTime) query.window_minutes = windowMinutes;
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

// ── ML / Feature intelligence endpoints ───────────────────────────

export async function fetchMLPredictions(limit = 100): Promise<MLPrediction[]> {
  return fetchJSON<MLPrediction[]>('/ml/predictions', { limit });
}

export async function fetchFeatureSnapshots(systemId?: string, limit = 100): Promise<FeatureSnapshot[]> {
  return fetchJSON<FeatureSnapshot[]>('/feature-snapshots', {
    system_id: systemId,
    limit,
  });
}

/**
 * Fetch latest anomaly predictions per system from /ml/anomalies (v2-isof).
 * @param limit         Max systems to return (default 50).
 * @param onlyAnomalies When true, only returns systems flagged as anomalous.
 */
export async function fetchMLAnomalies(limit = 50, onlyAnomalies = false): Promise<MLAnomaly[]> {
  if (USE_MOCK_DATA) return mockApi.fetchMLAnomalies(limit, onlyAnomalies);
  return fetchJSON<MLAnomaly[]>('/ml/anomalies', {
    limit,
    only_anomalies: onlyAnomalies ? 'true' : 'false',
  });
}

/**
 * Fetch latest KMeans cluster assignments per system from /ml/clusters.
 * Only returns systems where cluster_id IS NOT NULL (i.e., scored by sklearn).
 */
export async function fetchMLClusters(limit = 50): Promise<MLCluster[]> {
  if (USE_MOCK_DATA) return mockApi.fetchMLClusters(limit);
  return fetchJSON<MLCluster[]>('/ml/clusters', { limit });
}

// Re-export types for consumers
export type { MLPrediction, FeatureSnapshot, MLAnomaly, MLCluster };

// ── Health check ────────────────────────────────────────

export async function checkAPIHealth(): Promise<boolean> {
  if (USE_MOCK_DATA) return true;
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
  if (USE_MOCK_DATA) return { success: true, system_id: 'mock-sys-' + Math.floor(Math.random() * 1000) };
  const headers = await buildHeaders();
  return fetchMutationJSON<{ success: boolean; system_id?: string }>(buildEndpoint('/systems/register'), {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ hostname, ip_address: ipAddress, agent_key: agentKey }),
  });
}

export async function executeSystemCommand(systemId: string, command: string): Promise<{ success: boolean; output: string }> {
  if (USE_MOCK_DATA) return { success: true, output: `[MOCK] Command '${command}' queued for node ${systemId}.` };
  const headers = await buildHeaders();
  return fetchMutationJSON<{ success: boolean; output: string }>(buildEndpoint('/systems/command'), {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ system_id: systemId, command }),
  });
}

export async function alertAction(
  action: 'acknowledge' | 'escalate',
  alertId: string,
): Promise<{ success: boolean }> {
  if (USE_MOCK_DATA) return { success: true };
  const headers = await buildHeaders();
  return fetchMutationJSON<{ success: boolean }>(buildEndpoint(`/alerts/${action}`), {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ alert_id: alertId }),
  });
}

export async function createAlertRule(
  ruleName: string,
  condition: string,
  severity: string,
  threshold: number,
  cooldownMinutes = 30,
  escalationTarget?: string,
): Promise<{ success: boolean }> {
  if (USE_MOCK_DATA) return { success: true };
  const headers = await buildHeaders();
  return fetchMutationJSON<{ success: boolean }>(buildEndpoint('/alerts/rules'), {
    method: 'POST',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      rule_name: ruleName,
      condition,
      severity,
      threshold,
      cooldown_minutes: cooldownMinutes,
      escalation_target: escalationTarget?.trim() || undefined,
    }),
  });
}

/**
 * Download a JSON report via authenticated fetch (avoids exposing the URL without a token).
 */
export async function downloadReport(): Promise<void> {
  if (USE_MOCK_DATA) {
    alert("Mock report downloaded!");
    return;
  }
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
