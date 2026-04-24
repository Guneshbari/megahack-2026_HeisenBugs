/**
 * signalStore — Aggregated Signal Processing Layer
 *
 * Grouping key: system_id::fault_type::fault_subtype::2-min-bucket
 *
 * Rules:
 *  - Groups raw events per system × fault_type × fault_subtype × 2-min window
 *  - Collapses repeated events into spikes (count ≥ 3)
 *  - Tracks severity escalation within the group window
 *  - Stores ML predictions and feature snapshots alongside events
 *  - batchPush() is the only ingestion point (WebSocket + API)
 *  - Deduplication by event_hash / composite key
 *  - Max 5000 events retained (ring buffer behaviour)
 */
import { create } from 'zustand';
import type { TelemetryEvent, Severity, MLPrediction, FeatureSnapshot } from '../types/telemetry';

export interface GroupedSignal {
  id: string;
  fault_type: string;
  fault_subtype: string;
  severity: Severity;
  /** The primary system_id driving this signal — the system with the most events in the bucket */
  systemId: string;
  count: number;
  /** All unique systems involved in this signal group */
  systems: string[];
  firstSeen: string;
  lastSeen: string;
  /** true when count ≥ 3 within the 2-minute window */
  isSpike: boolean;
  /** human-readable age, e.g. "2m ago" */
  windowLabel: string;
  /** max confidence_score seen across events in this group */
  maxConfidence: number;
}

const SIGNAL_WINDOW_MS = 2 * 60 * 1000;   // 2-minute grouping window
const RETAIN_WINDOW_MS = 24 * 60 * 60 * 1000;  // keep signal derivation useful for sparse test data
const MAX_EVENTS        = 5_000;

/** Rank order for severity resolution (highest wins) */
const SEV_RANK: Record<Severity, number> = {
  CRITICAL: 4, ERROR: 3, WARNING: 2, INFO: 1,
};

function timeAgoLabel(ts: string): string {
  const ageMs = Date.now() - new Date(ts).getTime();
  const m = Math.round(ageMs / 60_000);
  if (m < 1) return 'now';
  if (m < 60) return `${m}m ago`;
  return `${Math.floor(m / 60)}h ago`;
}

export function groupIntoSignals(events: TelemetryEvent[]): GroupedSignal[] {
  const now = Date.now();
  const recent = events.filter(
    (e) => now - new Date(e.event_time).getTime() < RETAIN_WINDOW_MS,
  );

  // Map: key=`system_id::fault_type::fault_subtype::bucketIndex` → events
  const buckets = new Map<string, TelemetryEvent[]>();

  for (const ev of recent) {
    const t         = new Date(ev.event_time).getTime();
    const bucket    = Math.floor(t / SIGNAL_WINDOW_MS);
    const subtype   = ev.fault_subtype ?? 'general';
    const key       = `${ev.system_id}::${ev.fault_type}::${subtype}::${bucket}`;
    const arr       = buckets.get(key) ?? [];
    arr.push(ev);
    buckets.set(key, arr);
  }

  const signals: GroupedSignal[] = [];

  for (const [key, evts] of buckets) {
    const [systemId, fault_type, fault_subtype] = key.split('::');

    // Resolve highest severity across events in group
    const severity = evts.reduce<Severity>(
      (best, ev) => (SEV_RANK[ev.severity] > SEV_RANK[best] ? ev.severity : best),
      'INFO',
    );

    const times   = evts.map((e) => new Date(e.event_time).getTime());
    const lastMs  = Math.max(...times);
    const firstMs = Math.min(...times);

    // Primary system = most events (already grouped by system, so all share system_id)
    const systemCounts: Record<string, number> = {};
    for (const ev of evts) {
      systemCounts[ev.hostname] = (systemCounts[ev.hostname] ?? 0) + 1;
    }
    const primaryHostname = Object.entries(systemCounts).sort((a, b) => b[1] - a[1])[0]?.[0] ?? systemId;

    const maxConfidence = Math.max(
      ...evts.map((e) => e.confidence_score ?? 0),
      0,
    );

    signals.push({
      id:           key,
      fault_type,
      fault_subtype,
      severity,
      systemId,
      count:        evts.length,
      systems:      [...new Set(evts.map((e) => e.hostname))],
      firstSeen:    new Date(firstMs).toISOString(),
      lastSeen:     new Date(lastMs).toISOString(),
      isSpike:      evts.length >= 3,
      windowLabel:  timeAgoLabel(new Date(lastMs).toISOString()),
      maxConfidence,
    });

    void primaryHostname; // used in grouping display — kept for future label use
  }

  // Sort: most recent first, then by severity
  return signals.sort((a, b) => {
    const tDiff =
      new Date(b.lastSeen).getTime() - new Date(a.lastSeen).getTime();
    if (tDiff !== 0) return tDiff;
    return SEV_RANK[b.severity] - SEV_RANK[a.severity];
  });
}

/**
 * computeHealthScore — per-system or fleet-wide
 *
 * Penalties:
 *   CRITICAL × 10, capped at 40 pts
 *   ERROR    × 4,  capped at 30 pts
 *   WARNING  × 1,  capped at 20 pts
 *   Rate-of-change (worsening): up to 10 pts
 * Score is bounded [0, 100].
 */
export function computeHealthScore(
  events: TelemetryEvent[],
  systemId?: string,
  windowMs = 5 * 60 * 1000,
): number {
  const now       = Date.now();
  const src       = systemId ? events.filter((e) => e.system_id === systemId) : events;
  const inWin     = src.filter((e) => now - new Date(e.event_time).getTime() < windowMs);
  const prevWin   = src.filter((e) => {
    const age = now - new Date(e.event_time).getTime();
    return age >= windowMs && age < windowMs * 2;
  });

  const crit      = inWin.filter((e) => e.severity === 'CRITICAL').length;
  const err       = inWin.filter((e) => e.severity === 'ERROR').length;
  const warn      = inWin.filter((e) => e.severity === 'WARNING').length;
  const prevCrit  = prevWin.filter((e) => e.severity === 'CRITICAL').length;

  const critPen   = Math.min(crit * 10, 40);
  const errPen    = Math.min(err  * 4,  30);
  const warnPen   = Math.min(warn * 1,  20);

  // Rate-of-change penalty: penalize if current window is 50%+ worse
  const rocPen    = (prevCrit > 0 && crit > prevCrit * 1.5)
    ? Math.min((crit - prevCrit) * 2, 10)
    : (prevCrit === 0 && crit >= 3 ? 5 : 0);

  return Math.max(0, Math.min(100, 100 - critPen - errPen - warnPen - rocPen));
}

// ── Store interface ──────────────────────────────────────────────────

interface SignalState {
  events:           TelemetryEvent[];
  signals:          GroupedSignal[];
  mlPredictions:    MLPrediction[];
  featureSnapshots: FeatureSnapshot[];
  isConnected:      boolean;
  lastUpdated:      number | null;

  /** Replace all events (initial load from API) */
  setEvents:             (events: TelemetryEvent[]) => void;
  /** Merge new events from WebSocket batch */
  batchPush:             (newEvents: TelemetryEvent[]) => void;
  /** Replace ML predictions (called by DashboardContext on each poll) */
  setMLPredictions:      (preds: MLPrediction[]) => void;
  /** Replace feature snapshots (called by DashboardContext on each poll) */
  setFeatureSnapshots:   (snaps: FeatureSnapshot[]) => void;
  /** Merge live feature snapshots from WebSocket */
  mergeFeatureSnapshots:  (snaps: FeatureSnapshot[]) => void;
  setConnected:          (connected: boolean) => void;
  /** Force signal recompute (e.g. after time window rolls) */
  recompute:             () => void;
}

export const useSignalStore = create<SignalState>((set, get) => ({
  events:           [],
  signals:          [],
  mlPredictions:    [],
  featureSnapshots: [],
  isConnected:      false,
  lastUpdated:      null,

  setEvents: (events) => {
    set({
      events,
      signals:     groupIntoSignals(events),
      lastUpdated: Date.now(),
    });
  },

  batchPush: (newEvents) => {
    const current = get().events;

    // Deduplicate by event_hash or composite key
    const seen    = new Set<string>(
      current.map(
        (e) => e.event_hash ?? `${e.system_id}::${e.event_time}::${e.event_id}`,
      ),
    );
    const fresh = newEvents.filter((e) => {
      const key = e.event_hash ?? `${e.system_id}::${e.event_time}::${e.event_id}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    if (fresh.length === 0) return;

    // Ring-buffer: newest first, max MAX_EVENTS
    const merged = [...fresh, ...current].slice(0, MAX_EVENTS);
    set({
      events:      merged,
      signals:     groupIntoSignals(merged),
      lastUpdated: Date.now(),
    });
  },

  setMLPredictions: (mlPredictions) => set({ mlPredictions }),

  setFeatureSnapshots: (featureSnapshots) => set({ featureSnapshots }),

  mergeFeatureSnapshots: (snaps) => {
    if (snaps.length === 0) return;
    const merged = [...snaps, ...get().featureSnapshots]
      .sort((a, b) => new Date(b.snapshot_time).getTime() - new Date(a.snapshot_time).getTime());
    const seen = new Set<string>();
    const featureSnapshots = merged.filter((snap) => {
      const key = `${snap.system_id}::${snap.snapshot_time}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    }).slice(0, 500);
    set({ featureSnapshots });
  },

  setConnected: (isConnected) => set({ isConnected }),

  recompute: () => {
    const { events } = get();
    set({ signals: groupIntoSignals(events) });
  },
}));
