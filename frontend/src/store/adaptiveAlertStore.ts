/**
 * adaptiveAlertStore — Adaptive Alert State
 *
 * Maintains in-session AlertHistory (ruleStats Map) across poll cycles.
 * Calls processAdaptiveAlerts() after each process() call.
 *
 * Guard: empty alerts → clears adaptiveAlerts without touching history.
 * History accumulates across polls (session-persistent, lost on page reload).
 * updateRuleStat() is called by feedbackStore when an incident resolves.
 */
import { create } from 'zustand';
import type { Alert } from '../types/telemetry';
import {
  processAdaptiveAlerts,
  type AdaptiveAlert,
  type AlertHistory,
  type RuleStat,
} from '../lib/adaptiveAlertEngine';

// ── History builder ───────────────────────────────────────────────────

const NOISE_WINDOW_MS = 15 * 60_000;

function buildHistory(alerts: Alert[], prev: AlertHistory): AlertHistory {
  const now       = Date.now();
  const ruleStats = new Map<string, RuleStat>(prev.ruleStats);

  for (const alert of alerts) {
    const existing = ruleStats.get(alert.rule);

    if (!existing) {
      ruleStats.set(alert.rule, {
        count:           1,
        firstSeen:       alert.triggered_at,
        lastSeen:        alert.triggered_at,
        avgResolutionMs: 0,
      });
    } else {
      // Reset counter if outside the 15-min noise window
      const firstMs   = new Date(existing.firstSeen).getTime();
      const inWindow  = (now - firstMs) < NOISE_WINDOW_MS;

      ruleStats.set(alert.rule, {
        ...existing,
        count:     inWindow ? existing.count + 1 : 1,
        firstSeen: inWindow ? existing.firstSeen : alert.triggered_at,
        lastSeen:  alert.triggered_at,
      });
    }
  }

  return { ruleStats };
}

// ── Store ─────────────────────────────────────────────────────────────

interface AdaptiveAlertState {
  adaptiveAlerts: AdaptiveAlert[];
  /** Session-persistent rule stats — accumulated across poll cycles */
  history:        AlertHistory;

  /** Called each poll cycle after fetchAlerts() resolves */
  process: (alerts: Alert[], avgResolutionMsByRule: Record<string, number>) => void;
  /** Called by feedbackStore when an incident resolves — updates avgResolutionMs for a rule */
  updateRuleStat: (rule: string, resolvedMs: number) => void;
}

export const useAdaptiveAlertStore = create<AdaptiveAlertState>((set, get) => ({
  adaptiveAlerts: [],
  history:        { ruleStats: new Map() },

  process: (alerts, avgResolutionMsByRule) => {
    if (alerts.length === 0) {
      set({ adaptiveAlerts: [] });
      return;
    }

    const updatedHistory = buildHistory(alerts, get().history);
    const adaptiveAlerts = processAdaptiveAlerts(alerts, updatedHistory, avgResolutionMsByRule);

    set({ adaptiveAlerts, history: updatedHistory });
  },

  updateRuleStat: (rule, resolvedMs) => {
    const prev      = get().history;
    const ruleStats = new Map<string, RuleStat>(prev.ruleStats);
    const existing  = ruleStats.get(rule);

    if (existing) {
      // Rolling average: blend previous avg with new observation
      const prevAvg  = existing.avgResolutionMs;
      const nextAvg  = prevAvg === 0 ? resolvedMs : (prevAvg + resolvedMs) / 2;
      ruleStats.set(rule, { ...existing, avgResolutionMs: nextAvg });
      set({ history: { ruleStats } });
    }
  },
}));
