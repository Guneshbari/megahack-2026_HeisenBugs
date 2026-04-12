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

export const RECENT_EVENTS_LIMIT = 1000;

const MOCK_SYSTEMS: SystemInfo[] = [
  {
    system_id: 'db-prod-01',
    hostname: 'db-prod-01.internal',
    status: 'online',
    cpu_usage_percent: 45,
    memory_usage_percent: 60,
    disk_free_percent: 30, // 30% free
    os_version: 'Windows Server 2022',
    last_seen: new Date().toISOString(),
    last_updated_at: new Date().toISOString(),
    ip_address: '10.0.1.15',
    total_events: 1542,
  },
  {
    system_id: 'app-prod-01',
    hostname: 'app-prod-01.internal',
    status: 'degraded',
    cpu_usage_percent: 92,
    memory_usage_percent: 85,
    disk_free_percent: 15,
    os_version: 'Ubuntu 22.04 LTS',
    last_seen: new Date().toISOString(),
    last_updated_at: new Date().toISOString(),
    ip_address: '10.0.1.25',
    total_events: 345,
  },
  {
    system_id: 'cache-prod-02',
    hostname: 'cache-prod-02.internal',
    status: 'offline',
    cpu_usage_percent: 0,
    memory_usage_percent: 0,
    disk_free_percent: 0,
    os_version: 'Debian 12',
    last_seen: new Date(Date.now() - 3600000).toISOString(),
    last_updated_at: new Date(Date.now() - 3600000).toISOString(),
    ip_address: '10.0.1.30',
    total_events: 89,
  },
  {
    system_id: 'web-prod-01',
    hostname: 'web-prod-01.internal',
    status: 'online',
    cpu_usage_percent: 25,
    memory_usage_percent: 40,
    disk_free_percent: 80,
    os_version: 'Ubuntu 22.04 LTS',
    last_seen: new Date().toISOString(),
    last_updated_at: new Date().toISOString(),
    ip_address: '10.0.1.40',
    total_events: 120,
  },
  {
    system_id: 'api-prod-01',
    hostname: 'api-prod-01.internal',
    status: 'online',
    cpu_usage_percent: 55,
    memory_usage_percent: 70,
    disk_free_percent: 45,
    os_version: 'Ubuntu 22.04 LTS',
    last_seen: new Date().toISOString(),
    last_updated_at: new Date().toISOString(),
    ip_address: '10.0.1.45',
    total_events: 550,
  }
];

const FAULT_DETAILS: Record<string, { subtype: string; parsed: string }[]> = {
  'Auth Failure':  [
    { subtype: 'brute_force',      parsed: 'Multiple failed login attempts from 192.168.1.45' },
    { subtype: 'credential_reuse', parsed: 'Account locked after 5 failures: svc_backup' },
  ],
  'High CPU':      [
    { subtype: 'runaway_process',  parsed: 'Process sentinel_worker.exe consuming 94% CPU' },
    { subtype: 'resource_leak',    parsed: 'Memory + CPU exhaustion detected on pid 8821' },
  ],
  'Disk Space':    [
    { subtype: 'log_accumulation', parsed: 'C:\\Logs partition at 97% capacity' },
    { subtype: 'data_ingress',     parsed: 'DB data volume growing 2GB/hr, low on space' },
  ],
  'Service Crash': [
    { subtype: 'oom_kill',         parsed: 'Process api_gateway killed by OOM killer (oom_score=987)' },
    { subtype: 'unhandled_exc',    parsed: 'Unhandled exception in thread pool: NullPointerException' },
  ],
  'Network Drop':  [
    { subtype: 'nic_flap',         parsed: 'NIC eth0 link down/up cycle detected (MTU 1500)' },
    { subtype: 'upstream_loss',    parsed: 'BGP peer 203.0.113.1 unreachable, packet loss 100%' },
  ],
};

const FAULT_TYPES = ['Auth Failure', 'High CPU', 'Disk Space', 'Service Crash', 'Network Drop'];
const SEVERITIES  = ['INFO', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'] as const;

const MOCK_EVENTS: TelemetryEvent[] = Array.from({ length: 150 }).map((_, i) => {
  const ft      = FAULT_TYPES[i % FAULT_TYPES.length];
  const detail  = FAULT_DETAILS[ft][(i >> 1) % 2];
  const sys     = MOCK_SYSTEMS[i % MOCK_SYSTEMS.length];
  return {
    system_id:            sys.system_id,
    hostname:             sys.hostname,
    event_record_id:      1000 + i,
    provider_name:        i % 3 === 0 ? 'Microsoft-Windows-Security-Auditing' : 'System',
    event_id:             4624 + (i % 10),
    severity:             SEVERITIES[i % 5],
    fault_type:           ft,
    fault_subtype:        detail.subtype,
    parsed_message:       detail.parsed,
    fault_description:    `Event ${i}: ${detail.parsed}`,
    confidence_score:     0.4 + (i % 7) * 0.08,          // 0.40–0.88
    cpu_usage_percent:    sys.cpu_usage_percent + (Math.random() * 10 - 5),
    memory_usage_percent: sys.memory_usage_percent + (Math.random() * 10 - 5),
    disk_free_percent:    sys.disk_free_percent + (Math.random() * 5 - 2.5),
    event_time:           new Date(Date.now() - i * 60_000).toISOString(),
    ingested_at:          new Date(Date.now() - i * 60_000).toISOString(),
    diagnostic_context:   { mock: true, i },
  };
});

const MOCK_ALERTS: Alert[] = [
  {
    alert_id: 'ALT-001',
    system_id: 'app-prod-01',
    hostname: 'app-prod-01.internal',
    severity: 'CRITICAL',
    rule: 'High CPU Detection',
    title: 'CRITICAL: High CPU on app-prod-01',
    description: 'CPU usage has exceeded 90% for over 5 minutes.',
    triggered_at: new Date(Date.now() - 300000).toISOString(),
    acknowledged: false,
  },
  {
    alert_id: 'ALT-002',
    system_id: 'db-prod-01',
    hostname: 'db-prod-01.internal',
    severity: 'ERROR',
    rule: 'Disk Space Low',
    title: 'ERROR: Disk Space on db-prod-01',
    description: 'Available disk space is below 30% threshold.',
    triggered_at: new Date(Date.now() - 3600000).toISOString(),
    acknowledged: true,
    acknowledged_at: new Date(Date.now() - 1800000).toISOString(),
    acknowledged_by: 'admin',
  },
  {
    alert_id: 'ALT-003',
    system_id: 'cache-prod-02',
    hostname: 'cache-prod-02.internal',
    severity: 'CRITICAL',
    rule: 'Node Offline',
    title: 'CRITICAL: Node Offline cache-prod-02',
    description: 'System heartbeat flatlined.',
    triggered_at: new Date(Date.now() - 7200000).toISOString(),
    acknowledged: false,
    escalated: true,
  }
];

export async function fetchEvents(limitOrOptions?: unknown): Promise<TelemetryEvent[]> {
  void limitOrOptions;
  return Promise.resolve(MOCK_EVENTS);
}

export async function fetchSystems(): Promise<SystemInfo[]> {
  return Promise.resolve(MOCK_SYSTEMS);
}

export async function fetchAlerts(): Promise<Alert[]> {
  return Promise.resolve(MOCK_ALERTS);
}

export async function fetchRecentAlerts(): Promise<Alert[]> {
  return Promise.resolve(MOCK_ALERTS);
}

export async function fetchMetrics(startTime?: string, endTime?: string, windowMinutes?: number): Promise<MetricPoint[]> {
  void startTime;
  void endTime;
  void windowMinutes;
  const points: MetricPoint[] = [];
  const now = Date.now();
  for (let i = 24; i >= 0; i--) {
    points.push({
      timestamp: new Date(now - i * 3600000).toISOString(),
      event_count: Math.floor(Math.random() * 500) + 100,
      critical_count: Math.floor(Math.random() * 10),
      error_count: Math.floor(Math.random() * 20),
      warning_count: Math.floor(Math.random() * 30),
      info_count: Math.floor(Math.random() * 400),
      avg_cpu: 30 + Math.random() * 40,
      avg_memory: 40 + Math.random() * 40,
      avg_disk_free: 20 + Math.random() * 60,
    });
  }
  return Promise.resolve(points);
}

export interface DashboardMetrics {
  total_events: number;
  critical_events: number;
  warning_events: number;
}

export async function fetchDashboardMetrics(windowMinutes?: number): Promise<DashboardMetrics> {
  void windowMinutes;
  return Promise.resolve({
    total_events: MOCK_EVENTS.length,
    critical_events: MOCK_EVENTS.filter(e => e.severity === 'CRITICAL').length,
    warning_events: MOCK_EVENTS.filter(e => e.severity === 'WARNING').length,
  });
}

export async function fetchFaultDistribution(windowMinutes?: number): Promise<FaultTypeCount[]> {
  void windowMinutes;
  return Promise.resolve([
    { fault_type: 'Auth Failure', count: 45 },
    { fault_type: 'High CPU', count: 32 },
    { fault_type: 'Disk Space', count: 28 },
    { fault_type: 'Service Crash', count: 15 },
    { fault_type: 'Network Drop', count: 12 },
  ]);
}

export async function fetchSeverityDistribution(windowMinutes?: number): Promise<SeverityCount[]> {
  void windowMinutes;
  return Promise.resolve([
    { severity: 'INFO', count: 85 },
    { severity: 'WARNING', count: 42 },
    { severity: 'ERROR', count: 28 },
    { severity: 'CRITICAL', count: 12 },
  ]);
}

export async function fetchSystemFailures(limit = 6, windowMinutes?: number): Promise<SystemFailureCount[]> {
  void limit;
  void windowMinutes;
  return Promise.resolve([
    { hostname: 'app-prod-01.internal', system_id: 'app-prod-01', failure_count: 55 },
    { hostname: 'db-prod-01.internal', system_id: 'db-prod-01', failure_count: 23 },
    { hostname: 'cache-prod-02.internal', system_id: 'cache-prod-02', failure_count: 14 },
    { hostname: 'web-prod-01.internal', system_id: 'web-prod-01', failure_count: 8 },
  ]);
}

export async function fetchSystemMetrics(): Promise<{ avg_cpu: number; avg_memory: number; avg_disk: number }> {
  return Promise.resolve({
    avg_cpu: 45.2,
    avg_memory: 62.5,
    avg_disk: 35.8,
  });
}

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
  const trend_eps = [];
  const trend_latency = [];
  const now = Date.now();
  for(let i=20; i>=0; i--) {
    trend_eps.push({ time: new Date(now - i*60000).toISOString(), value: 200 + Math.random() * 50 });
    trend_latency.push({ time: new Date(now - i*60000).toISOString(), value: 50 + Math.random() * 20 });
  }

  return Promise.resolve({
    events_per_sec: 235.4,
    eps_change_pct: 12,
    avg_latency_ms: 104,
    kafka_lag: 0,
    lag_status: 'Optimal',
    db_write_rate: 230.1,
    trend_eps,
    trend_latency,
  });
}

// ── ML Predictions ──────────────────────────────────────

const MOCK_ML_PREDICTIONS: MLPrediction[] = [
  {
    system_id:          'app-prod-01',
    prediction_time:    new Date(Date.now() - 120_000).toISOString(),
    anomaly_score:      0.87,
    failure_probability: 0.79,
    predicted_fault:    'Service Crash',
    model_version:      'v1.4.2',
  },
  {
    system_id:          'db-prod-01',
    prediction_time:    new Date(Date.now() - 240_000).toISOString(),
    anomaly_score:      0.74,
    failure_probability: 0.62,
    predicted_fault:    'Disk Space',
    model_version:      'v1.4.2',
  },
  {
    system_id:          'cache-prod-02',
    prediction_time:    new Date(Date.now() - 60_000).toISOString(),
    anomaly_score:      0.95,
    failure_probability: 0.91,
    predicted_fault:    'Network Drop',
    model_version:      'v1.4.2',
  },
  {
    system_id:          'web-prod-01',
    prediction_time:    new Date(Date.now() - 360_000).toISOString(),
    anomaly_score:      0.41,
    failure_probability: 0.28,
    predicted_fault:    'High CPU',
    model_version:      'v1.4.2',
  },
  {
    system_id:          'api-prod-01',
    prediction_time:    new Date(Date.now() - 180_000).toISOString(),
    anomaly_score:      0.68,
    failure_probability: 0.55,
    predicted_fault:    'Auth Failure',
    model_version:      'v1.4.2',
  },
];

export async function fetchMLPredictions(limit = 100): Promise<MLPrediction[]> {
  return Promise.resolve(MOCK_ML_PREDICTIONS.slice(0, limit));
}

// ── Feature Snapshots ────────────────────────────────────

const MOCK_FEATURE_SNAPSHOTS: FeatureSnapshot[] = MOCK_SYSTEMS.map((sys) => ({
  system_id:          sys.system_id,
  snapshot_time:      new Date(Date.now() - 300_000).toISOString(),
  total_events:       sys.total_events,
  critical_count:     Math.floor(sys.total_events * 0.08),
  error_count:        Math.floor(sys.total_events * 0.15),
  warning_count:      Math.floor(sys.total_events * 0.25),
  info_count:         Math.floor(sys.total_events * 0.52),
  dominant_fault_type: FAULT_TYPES[MOCK_SYSTEMS.indexOf(sys) % FAULT_TYPES.length],
  avg_confidence:     0.55 + (MOCK_SYSTEMS.indexOf(sys) % 4) * 0.1,
  cpu_usage_percent:  sys.cpu_usage_percent,
  memory_usage_percent: sys.memory_usage_percent,
  disk_free_percent:  sys.disk_free_percent,
}));

export async function fetchFeatureSnapshots(system_id?: string, limit = 100): Promise<FeatureSnapshot[]> {
  const data = system_id
    ? MOCK_FEATURE_SNAPSHOTS.filter((s) => s.system_id === system_id)
    : MOCK_FEATURE_SNAPSHOTS;
  return Promise.resolve(data.slice(0, limit));
}

// ── ML Anomalies (v2-isof) ─────────────────────────────────────────────────

export async function fetchMLAnomalies(limit = 50, onlyAnomalies = false): Promise<MLAnomaly[]> {
  const data: MLAnomaly[] = MOCK_ML_PREDICTIONS.map((p) => ({
    system_id:           p.system_id,
    prediction_time:     p.prediction_time,
    anomaly_score:       p.anomaly_score,
    is_anomaly:          p.anomaly_score >= 0.7,
    failure_probability: p.failure_probability,
    predicted_fault:     p.predicted_fault,
    model_version:       p.model_version ?? 'v1.4.2',
    cluster_id:          p.cluster_id ?? null,
  }));
  const filtered = onlyAnomalies ? data.filter((d) => d.is_anomaly) : data;
  return Promise.resolve(filtered.slice(0, limit));
}

// ── ML Clusters (KMeans) ───────────────────────────────────────────────────

export async function fetchMLClusters(limit = 50): Promise<MLCluster[]> {
  const data: MLCluster[] = MOCK_ML_PREDICTIONS.map((p, i) => ({
    system_id:       p.system_id,
    prediction_time: p.prediction_time,
    cluster_id:      i % 3,            // 3 mock clusters: 0, 1, 2
    anomaly_score:   p.anomaly_score,
    is_anomaly:      p.anomaly_score >= 0.7,
    model_version:   p.model_version ?? 'v1.4.2',
  }));
  return Promise.resolve(data.slice(0, limit));
}
