/**
 * dashboardDerived.ts
 *
 * Pure functions for heavy data processing from the orchestrator store.
 * Moved out of components/stores to improve readability and testability.
 */

import type { TelemetryEvent, Alert, SystemInfo } from '../types/telemetry';

export type TimeRange = '5m' | '15m' | '1h' | '6h' | '24h';
export type AutoRefresh = 'off' | '5s' | '10s' | '30s' | '1m';

export const TIME_RANGE_MS: Record<TimeRange, number> = {
  '5m': 5 * 60_000,
  '15m': 15 * 60_000,
  '1h': 60 * 60_000,
  '6h': 6 * 60 * 60_000,
  '24h': 24 * 60 * 60_000,
};

export const TIME_RANGE_WINDOW_MINUTES: Record<TimeRange, number> = {
  '5m': 5,
  '15m': 15,
  '1h': 60,
  '6h': 360,
  '24h': 1440,
};

export const REFRESH_MS: Record<AutoRefresh, number | null> = {
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

export type SystemHealthLevel = 'healthy' | 'warning' | 'error' | 'critical';

export interface SystemEventSummary {
  eventCount: number;
  criticalCount: number;
  errorCount: number;
  warningCount: number;
  healthScore: number;
  healthLevel: SystemHealthLevel;
  latestEvent: TelemetryEvent | null;
}

export function scoreToHealthLevel(score: number): SystemHealthLevel {
  if (score >= 10) return 'critical';
  if (score >= 5) return 'error';
  if (score >= 2) return 'warning';
  return 'healthy';
}

/**
 * Filter events based on time and search/filter criteria.
 */
export function deriveFilteredEvents(
  allEvents: TelemetryEvent[],
  timeRange: TimeRange,
  selectedSystems: string[],
  selectedSeverities: string[],
  selectedFaultTypes: string[],
  searchQuery: string,
): TelemetryEvent[] {
  const now = Date.now();
  const limitMs = now - TIME_RANGE_MS[timeRange];
  const term = searchQuery.toLowerCase();

  return allEvents.filter((e) => {
    const eventTime = new Date(e.event_time).getTime();
    if (eventTime < limitMs) return false;

    if (selectedSystems.length > 0 && !selectedSystems.includes(e.system_id) && !selectedSystems.includes(e.hostname)) return false;
    if (selectedSeverities.length > 0 && !selectedSeverities.includes(e.severity)) return false;
    if (selectedFaultTypes.length > 0 && !selectedFaultTypes.includes(e.fault_type)) return false;

    if (term) {
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
}

/**
 * Summarize event statistics per system.
 */
export function deriveSystemSummaries(filteredEvents: TelemetryEvent[]) {
  const eventsBySystemId: Record<string, TelemetryEvent[]> = {};
  const summariesBySystemId: Record<string, SystemEventSummary> = {};
  const eventsByHostname: Record<string, number> = {};

  for (const event of filteredEvents) {
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
  }

  // Update computed fields per system
  for (const [systemId, events] of Object.entries(eventsBySystemId)) {
    events.sort((left, right) => new Date(left.event_time).getTime() - new Date(right.event_time).getTime());
    const summary = summariesBySystemId[systemId];
    const healthScore = summary.criticalCount * 5 + summary.errorCount * 2 + summary.warningCount;
    summary.healthScore = healthScore;
    summary.healthLevel = scoreToHealthLevel(healthScore);
  }

  const topSystemsByEventVolume = Object.entries(eventsByHostname)
    .map(([name, events]) => ({ name, events }))
    .sort((left, right) => right.events - left.events)
    .slice(0, 6);

  return {
    filteredEventsBySystemId: eventsBySystemId,
    filteredSystemEventSummaries: summariesBySystemId,
    topSystemsByEventVolume,
  };
}

/**
 * Filter active alerts. Unacknowledged alerts bypass the time filter to prevent hiding active fires.
 */
export function deriveFilteredAlerts(
  alerts: Alert[],
  timeRange: TimeRange,
  selectedSystems: string[],
  selectedSeverities: string[],
  searchQuery: string,
): Alert[] {
  const now = Date.now();
  const limitMs = now - TIME_RANGE_MS[timeRange];
  const term = searchQuery.toLowerCase();

  const filtered = alerts.filter((a) => {
    const alertTime = new Date(a.triggered_at).getTime();
    if (a.acknowledged && alertTime < limitMs) return false;

    if (selectedSystems.length > 0 && !selectedSystems.includes(a.system_id) && !selectedSystems.includes(a.hostname)) return false;
    if (selectedSeverities.length > 0 && !selectedSeverities.includes(a.severity)) return false;
    
    if (term) {
      if (
        !a.rule.toLowerCase().includes(term) &&
        !a.title.toLowerCase().includes(term) &&
        !a.hostname.toLowerCase().includes(term)
      ) {
        return false;
      }
    }
    return true;
  });

  // Limit unacknowledged alerts to the most recent 50 to preserve UI performance
  let unacknowledgedCount = 0;
  return filtered.filter((a) => {
    if (!a.acknowledged) {
      unacknowledgedCount++;
      return unacknowledgedCount <= 50;
    }
    return true;
  });
}

/**
 * Filter systems based on global metadata (no time bounds on status).
 */
export function deriveFilteredSystems(
  systems: SystemInfo[],
  selectedSystems: string[],
  searchQuery: string,
): SystemInfo[] {
  const term = searchQuery.toLowerCase();
  
  return systems.filter((s) => {
    if (selectedSystems.length > 0 && !selectedSystems.includes(s.system_id) && !selectedSystems.includes(s.hostname)) return false;
    if (term) {
      if (
        !s.hostname.toLowerCase().includes(term) &&
        !s.system_id.toLowerCase().includes(term)
      ) {
        return false;
      }
    }
    return true;
  });
}
