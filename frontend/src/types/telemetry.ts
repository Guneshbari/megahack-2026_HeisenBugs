export type Severity = 'CRITICAL' | 'ERROR' | 'WARNING' | 'INFO';

export interface TelemetryEvent {
  system_id: string;
  hostname: string;
  event_record_id: number;
  provider_name: string;
  event_id: number;
  severity: Severity;
  fault_type: string;
  fault_description: string;
  cpu_usage_percent: number;
  memory_usage_percent: number;
  disk_free_percent: number;
  event_time: string;
  ingested_at?: string;
  event_hash?: string;
  raw_xml?: string;
  event_message?: string;
  parsed_message?: string;
  normalized_message?: string;
  fault_subtype?: string;
  confidence_score?: number;
  diagnostic_context?: Record<string, unknown>;
}

export type SystemStatus = 'online' | 'degraded' | 'offline';

export interface SystemInfo {
  system_id: string;
  hostname: string;
  status: SystemStatus;
  cpu_usage_percent: number;
  memory_usage_percent: number;
  /** Percentage of disk space that is FREE (not used). */
  disk_free_percent: number;
  os_version: string;
  last_seen: string;
  /** ISO timestamp of when the resource metrics (CPU/RAM/disk) were last recorded. */
  last_updated_at?: string;
  ip_address: string;
  total_events: number;
}

export interface Alert {
  alert_id: string;
  system_id: string;
  hostname: string;
  severity: Severity;
  rule: string;
  title: string;
  description: string;
  triggered_at: string;
  acknowledged: boolean;
  acknowledged_at?: string;
  acknowledged_by?: string;
  escalated?: boolean;
  escalated_at?: string;
  assigned_to?: string;
}

export interface MetricPoint {
  timestamp: string;
  system_id?: string;
  event_count: number;
  critical_count?: number;
  error_count?: number;
  warning_count?: number;
  info_count?: number;
  avg_cpu?: number;
  avg_memory?: number;
  avg_disk_free?: number;
  cpu_usage_percent?: number;
  memory_usage_percent?: number;
  disk_free_percent?: number;
}

export interface SeverityCount {
  severity: Severity;
  count: number;
}

export interface FaultTypeCount {
  fault_type: string;
  count: number;
}

export interface SystemFailureCount {
  hostname: string;
  system_id: string;
  failure_count: number;
}

/** ML prediction record from /ml/predictions (v1 legacy + v2 fields) */
export interface MLPrediction {
  id?: number;
  system_id: string;
  prediction_time: string;
  anomaly_score: number;          // 0–1, > 0.7 = high risk
  failure_probability: number;    // 0–1, > 0.6 = at risk
  predicted_fault: string;
  model_version?: string;
  /** v2-isof fields — null when scored by heuristic fallback */
  is_anomaly?: boolean | null;
  cluster_id?: number | null;
}

/**
 * Anomaly record from GET /ml/anomalies
 * One row per system (DISTINCT ON system_id), most recent prediction.
 */
export interface MLAnomaly {
  system_id: string;
  prediction_time: string;
  anomaly_score: number;           // 0–1
  is_anomaly: boolean | null;
  failure_probability: number;
  predicted_fault: string;
  model_version: string;
  cluster_id: number | null;
}

/**
 * Cluster record from GET /ml/clusters
 * One row per system (DISTINCT ON system_id), cluster_id is always set.
 */
export interface MLCluster {
  system_id: string;
  prediction_time: string;
  cluster_id: number;
  anomaly_score: number;
  is_anomaly: boolean | null;
  model_version: string;
}

/** Feature snapshot record from /feature-snapshots */
export interface FeatureSnapshot {
  system_id: string;
  snapshot_time: string;
  total_events: number;
  critical_count: number;
  error_count: number;
  warning_count: number;
  info_count: number;
  dominant_fault_type: string;
  avg_confidence: number;
  cpu_usage_percent: number;
  memory_usage_percent: number;
  disk_free_percent: number;
}
