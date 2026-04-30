/**
 * SentinelCore — Data Helpers
 *
 * Pure utility functions for formatting and computing derived data.
 * All data is now provided by DashboardContext (from the live API).
 * These functions accept data as parameters instead of importing mock JSON.
 */

import type {
  TelemetryEvent,
  SystemInfo,
  Alert,
  Severity,
  SeverityCount,
  FaultTypeCount,
  SystemFailureCount,
} from '../types/telemetry';

// ── Helper functions ───────────────────────────────────
// Accept data as parameters so they work with live API data

export function getEventsBySeverity(events: TelemetryEvent[], severity: Severity): TelemetryEvent[] {
  return events.filter((e) => e.severity === severity);
}

export function getSystemById(systems: SystemInfo[], id: string): SystemInfo | undefined {
  return systems.find((s) => s.system_id === id);
}

export function getActiveAlerts(alerts: Alert[]): Alert[] {
  return alerts.filter((a) => !a.acknowledged);
}

export function getAcknowledgedAlerts(alerts: Alert[]): Alert[] {
  return alerts.filter((a) => a.acknowledged);
}

export function getAlertsBySeverity(alerts: Alert[], severity: Severity): Alert[] {
  return alerts.filter((a) => a.severity === severity);
}

export function getSeverityDistribution(events: TelemetryEvent[]): SeverityCount[] {
  const counts: Record<Severity, number> = { CRITICAL: 0, ERROR: 0, WARNING: 0, INFO: 0 };
  events.forEach((e) => counts[e.severity]++);
  return Object.entries(counts).map(([severity, count]) => ({
    severity: severity as Severity,
    count,
  }));
}

export function getTopFaultTypes(events: TelemetryEvent[], limit = 5): FaultTypeCount[] {
  const counts: Record<string, number> = {};
  events.forEach((e) => {
    counts[e.fault_type] = (counts[e.fault_type] || 0) + 1;
  });
  return Object.entries(counts)
    .map(([fault_type, count]) => ({ fault_type, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, limit);
}

export function getTopFailingSystems(events: TelemetryEvent[], limit = 5): SystemFailureCount[] {
  const counts: Record<string, { hostname: string; count: number }> = {};
  events
    .filter((e) => e.severity === 'CRITICAL' || e.severity === 'ERROR')
    .forEach((e) => {
      if (!counts[e.system_id]) {
        counts[e.system_id] = { hostname: e.hostname, count: 0 };
      }
      counts[e.system_id].count++;
    });
  return Object.entries(counts)
    .map(([system_id, { hostname, count }]) => ({
      system_id,
      hostname,
      failure_count: count,
    }))
    .sort((a, b) => b.failure_count - a.failure_count)
    .slice(0, limit);
}

export function getOnlineSystems(systems: SystemInfo[]): number {
  return systems.filter((s) => s.status === 'online').length;
}

export function getDegradedSystems(systems: SystemInfo[]): number {
  return systems.filter((s) => s.status === 'degraded').length;
}

export function getOfflineSystems(systems: SystemInfo[]): number {
  return systems.filter((s) => s.status === 'offline').length;
}

export function getTotalEventCount(events: TelemetryEvent[]): number {
  return events.length;
}

export function getCriticalAlertCount(alerts: Alert[]): number {
  return getActiveAlerts(alerts).filter((a) => a.severity === 'CRITICAL').length;
}

// ── Formatting utilities (unchanged) ────────────────────

export function formatTimestamp(ts: string): string {
  const d = new Date(ts);
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

export function formatTimeShort(ts: string): string {
  const d = new Date(ts);
  return d.toLocaleString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

export function timeAgo(ts: string): string {
  const now = new Date();
  const then = new Date(ts);
  const diffMs = now.getTime() - then.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  return `${Math.floor(diffHr / 24)}d ago`;
}
