/**
 * adaptiveAlertEngine — Dynamic Severity Adjustment
 *
 * Pure functional module. AlertHistory is passed in (lives in adaptiveAlertStore, not here).
 *
 * Rules:
 *  1. Noise suppression — rule fired > 5× in last 15-min window
 *                         AND resolves quickly (< 5 min avg) → step severity DOWN
 *                         EXCEPT if avgResolutionMs > 30 min (slow-resolving = important, never suppress)
 *  2. Rare event boost  — rule not seen in last 60 min → step severity UP
 *  3. First-seen boost  — no history at all for this rule → step UP (unknown = elevated concern)
 *
 * Severity step order: INFO ↔ WARNING ↔ ERROR ↔ CRITICAL
 */
import type { Alert, Severity } from '../types/telemetry';

// ── Types ─────────────────────────────────────────────────────────────

export interface RuleStat {
  count:           number;
  firstSeen:       string;
  lastSeen:        string;
  avgResolutionMs: number;
}

export interface AlertHistory {
  ruleStats: Map<string, RuleStat>;
}

export interface AdaptiveAlert extends Alert {
  adaptive_severity: Severity;
  severity_delta:    'boosted' | 'suppressed' | 'nominal';
  suppression_count: number;
}

// ── Constants ─────────────────────────────────────────────────────────

const SEV_ORDER: Severity[]   = ['INFO', 'WARNING', 'ERROR', 'CRITICAL'];
const NOISE_COUNT_THRESHOLD   = 5;
const NOISE_WINDOW_MS         = 15 * 60_000;   // 15 min
const RARE_WINDOW_MS          = 60 * 60_000;   // 60 min — not seen in last hr = rare
const FAST_RESOLVE_THRESHOLD  = 5  * 60_000;   // 5 min → candidate for suppression
const SLOW_RESOLVE_THRESHOLD  = 30 * 60_000;   // 30 min → never suppress

// ── Helpers ───────────────────────────────────────────────────────────

function stepSeverity(sev: Severity, direction: 'up' | 'down'): Severity {
  const idx = SEV_ORDER.indexOf(sev);
  if (direction === 'up')   return SEV_ORDER[Math.min(idx + 1, SEV_ORDER.length - 1)];
  if (direction === 'down') return SEV_ORDER[Math.max(idx - 1, 0)];
  return sev;
}

// ── Main export ───────────────────────────────────────────────────────

export function processAdaptiveAlerts(
  alerts:                Alert[],
  history:               AlertHistory,
  avgResolutionMsByRule: Record<string, number>,
): AdaptiveAlert[] {
  if (alerts.length === 0) return [];

  const now = Date.now();

  return alerts.map((alert): AdaptiveAlert => {
    const stat = history.ruleStats.get(alert.rule);

    let adaptiveSeverity: Severity                   = alert.severity;
    let severityDelta: AdaptiveAlert['severity_delta'] = 'nominal';
    const suppressionCount                           = stat?.count ?? 0;

    if (!stat) {
      // Rule never seen before → first-seen boost (unknown = concern)
      adaptiveSeverity = stepSeverity(alert.severity, 'up');
      severityDelta    = 'boosted';
    } else {
      const lastSeenMs   = new Date(stat.lastSeen).getTime();
      const firstSeenMs  = new Date(stat.firstSeen).getTime();
      const windowMs     = now - firstSeenMs;

      // Use feedbackStore-derived avg if available; fall back to stat's recorded avg
      const avgResMs     = avgResolutionMsByRule[alert.rule] ?? stat.avgResolutionMs;

      // ── Rule 2: Rare event boost (checked first — overrides suppression) ──
      const isRare = (now - lastSeenMs) > RARE_WINDOW_MS;
      if (isRare) {
        adaptiveSeverity = stepSeverity(alert.severity, 'up');
        severityDelta    = 'boosted';
      }
      // ── Rule 1: Noise suppression (only if not rare) ──────────────────
      else {
        const isNoisy       = stat.count > NOISE_COUNT_THRESHOLD && windowMs < NOISE_WINDOW_MS;
        const isFastResolve = avgResMs < FAST_RESOLVE_THRESHOLD || avgResMs === 0;
        const isSlowResolve = avgResMs > SLOW_RESOLVE_THRESHOLD;

        if (isNoisy && isFastResolve && !isSlowResolve) {
          adaptiveSeverity = stepSeverity(alert.severity, 'down');
          severityDelta    = 'suppressed';
        }
      }
    }

    return {
      ...alert,
      adaptive_severity: adaptiveSeverity,
      severity_delta:    severityDelta,
      suppression_count: suppressionCount,
    };
  });
}
