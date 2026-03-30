/**
 * feedbackStore — Operator Feedback Loop
 *
 * Tracks:
 *   resolutionLog     — incident resolution times (last 200)
 *   remediationLog    — remediation action outcomes (last 100)
 *
 * Derives:
 *   avgResolutionMsByRule    — used by adaptiveAlertStore to prevent suppression of slow-resolving rules
 *   actionEffectivenessRate  — used by actionEngine to rank actions by past success
 *
 * Both logs are ring-buffered to prevent memory creep.
 * recordResolution() feeds back into adaptiveAlertStore.updateRuleStat().
 */
import { create } from 'zustand';
import type { Incident } from './incidentStore';

// ── Types ─────────────────────────────────────────────────────────────

export interface ResolutionEntry {
  incident_id:        string;
  fault_type:         string;
  resolved_at:        string;
  resolution_time_ms: number;
  lifecycle_path:     string;  // e.g. "OPEN→ACKNOWLEDGED→RESOLVED"
}

export interface RemediationEntry {
  hook_id:     string;
  incident_id: string;
  outcome:     'success' | 'failed';
  timestamp:   string;
}

// ── Constants ─────────────────────────────────────────────────────────

const MAX_RESOLUTION_LOG  = 200;
const MAX_REMEDIATION_LOG = 100;

// ── Pure derivation helpers ───────────────────────────────────────────

function computeAvgResolutionMs(log: ResolutionEntry[]): Record<string, number> {
  const byRule: Record<string, number[]> = {};
  for (const entry of log) {
    if (!byRule[entry.fault_type]) byRule[entry.fault_type] = [];
    byRule[entry.fault_type].push(entry.resolution_time_ms);
  }
  const result: Record<string, number> = {};
  for (const [rule, times] of Object.entries(byRule)) {
    result[rule] = times.reduce((s, t) => s + t, 0) / times.length;
  }
  return result;
}

function computeEffectivenessRate(log: RemediationEntry[]): Record<string, number> {
  const byHook: Record<string, { success: number; total: number }> = {};
  for (const entry of log) {
    if (!byHook[entry.hook_id]) byHook[entry.hook_id] = { success: 0, total: 0 };
    byHook[entry.hook_id].total += 1;
    if (entry.outcome === 'success') byHook[entry.hook_id].success += 1;
  }
  const result: Record<string, number> = {};
  for (const [hookId, { success, total }] of Object.entries(byHook)) {
    result[hookId] = total > 0 ? success / total : 0;
  }
  return result;
}

// ── Store ─────────────────────────────────────────────────────────────

interface FeedbackState {
  resolutionLog:           ResolutionEntry[];
  remediationLog:          RemediationEntry[];
  /** Per-rule average resolution time in ms — consumed by adaptiveAlertStore */
  avgResolutionMsByRule:   Record<string, number>;
  /** Per-hookId success rate 0–1 — consumed by actionEngine for ranking */
  actionEffectivenessRate: Record<string, number>;

  recordResolution:  (incident: Incident) => void;
  recordRemediation: (hookId: string, incidentId: string, outcome: 'success' | 'failed') => void;
}

export const useFeedbackStore = create<FeedbackState>((set, get) => ({
  resolutionLog:           [],
  remediationLog:          [],
  avgResolutionMsByRule:   {},
  actionEffectivenessRate: {},

  recordResolution: (incident) => {
    const createdMs      = new Date(incident.created_at).getTime();
    const resolutionMs   = Date.now() - createdMs;

    const entry: ResolutionEntry = {
      incident_id:        incident.incident_id,
      fault_type:         incident.fault_type,
      resolved_at:        new Date().toISOString(),
      resolution_time_ms: resolutionMs,
      lifecycle_path:     incident.acknowledged_at
        ? 'OPEN→ACKNOWLEDGED→RESOLVED'
        : 'OPEN→RESOLVED',
    };

    const prev = get().resolutionLog;
    // Ring buffer: newest first, capped at MAX
    const next = [entry, ...prev].slice(0, MAX_RESOLUTION_LOG);
    const avgResolutionMsByRule = computeAvgResolutionMs(next);

    set({ resolutionLog: next, avgResolutionMsByRule });

    // Feed back into adaptiveAlertStore so it can update per-rule suppression thresholds.
    // Import is deferred to avoid circular dependency at module init time.
    import('./adaptiveAlertStore').then(({ useAdaptiveAlertStore }) => {
      useAdaptiveAlertStore.getState().updateRuleStat(incident.fault_type, resolutionMs);
    }).catch(() => {/* non-critical */});
  },

  recordRemediation: (hookId, incidentId, outcome) => {
    const entry: RemediationEntry = {
      hook_id:     hookId,
      incident_id: incidentId,
      outcome,
      timestamp:   new Date().toISOString(),
    };

    const prev = get().remediationLog;
    const next = [entry, ...prev].slice(0, MAX_REMEDIATION_LOG);
    const actionEffectivenessRate = computeEffectivenessRate(next);

    set({ remediationLog: next, actionEffectivenessRate });
  },
}));
