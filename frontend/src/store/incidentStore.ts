/**
 * incidentStore — Operator Intelligence Store
 *
 * Derives from: signals + ML predictions + feature snapshots
 * Produces:
 *   incidents[]       — with priority score, lifecycle state, trend, impact
 *   correlations[]    — from correlationEngine
 *   systemHealthIndex — fleet-wide 0–100 score
 *
 * Priority formula: severity_pts + affected_systems*2 + confidence*5 + recency_bonus
 * Lifecycle states: OPEN → ACKNOWLEDGED → RESOLVED (manual transitions)
 * Noise reduction: incidents with priority < LOW_THRESHOLD collapsed by default
 */
import { create } from 'zustand';
import type { Severity, MLPrediction, FeatureSnapshot } from '../types/telemetry';
import type { GroupedSignal } from './signalStore';
import { correlate, type Correlation, type CorrelationInputs } from '../lib/correlationEngine';
// Deferred imports used at call-time to avoid circular init-time dependency
// useForecastStore  → forecastStore → forecastEngine (no cycle)
// useFeedbackStore  → feedbackStore → imports Incident type only (type-erased at runtime)

// ── Public types ─────────────────────────────────────────────────────

export type IncidentTrigger   = 'signal' | 'anomaly' | 'failure_prob';
export type IncidentLifecycle = 'OPEN' | 'ACKNOWLEDGED' | 'RESOLVED';
export type PriorityLabel     = 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';
export type TrendDirection    = 'rising' | 'stable' | 'falling';

export interface IncidentImpact {
  systems_affected:    number;
  avg_cpu:             string;   // e.g. "45%"
  error_rate_delta:    string;   // e.g. "+12%"
  dominant_fault:      string;
}

export interface Incident {
  incident_id:      string;
  title:            string;
  fault_type:       string;
  severity:         Severity;
  systems:          string[];
  event_count:      number;
  confidence:       number;          // 0–1
  priority_score:   number;          // raw numeric score
  priority_label:   PriorityLabel;   // HIGH / MEDIUM / LOW
  trigger:          IncidentTrigger;
  lifecycle:        IncidentLifecycle;
  signal?:          GroupedSignal;
  anomaly_score?:   number;
  failure_prob?:    number;
  predicted_fault?: string;
  trend:            TrendDirection;  // trend of anomaly_score / failure_prob
  impact:           IncidentImpact;
  created_at:       string;
  acknowledged_at?: string;
  resolved_at?:     string;
}

// ── Constants ────────────────────────────────────────────────────────

const SEV_RANK: Record<Severity, number> = { CRITICAL: 4, ERROR: 3, WARNING: 2, INFO: 1 };
const SEV_PRIORITY_PTS: Record<Severity, number> = { CRITICAL: 40, ERROR: 25, WARNING: 10, INFO: 2 };

const PRIORITY_THRESHOLDS = { HIGH: 50, MEDIUM: 25, LOW: 0 } as const;

// ── Priority scoring ─────────────────────────────────────────────────

function computePriority(
  severity: Severity,
  systemsCount: number,
  confidence: number,
  createdAt: string,
): { score: number; label: PriorityLabel } {
  const recencyMs     = Date.now() - new Date(createdAt).getTime();
  const recencyBonus  = recencyMs < 5 * 60_000 ? 10 : recencyMs < 15 * 60_000 ? 5 : 0;

  const score = SEV_PRIORITY_PTS[severity]
    + systemsCount * 2
    + Math.round(confidence * 5)
    + recencyBonus;

  const label: PriorityLabel =
    severity === 'CRITICAL' ? 'CRITICAL' :
    score >= PRIORITY_THRESHOLDS.HIGH   ? 'HIGH'   :
    score >= PRIORITY_THRESHOLDS.MEDIUM ? 'MEDIUM' : 'LOW';

  return { score, label };
}

// ── Trend detection ──────────────────────────────────────────────────
// We track a rolling snapshot of the last 2 deriveAll() calls in module scope
// to infer rising/falling/stable trends without a time-series DB.

const _prevScores = new Map<string, number>(); // system_id → anomaly_score

function detectTrend(systemId: string, currentScore: number): TrendDirection {
  const prev = _prevScores.get(systemId);
  _prevScores.set(systemId, currentScore);
  if (prev === undefined) return 'stable';
  if (currentScore > prev + 0.05) return 'rising';
  if (currentScore < prev - 0.05) return 'falling';
  return 'stable';
}

// ── Impact analysis ──────────────────────────────────────────────────

function computeImpact(
  systems:    string[],
  snapshots:  FeatureSnapshot[],
  faultType:  string,
): IncidentImpact {
  const relevantSnaps = snapshots.filter((s) => systems.includes(s.system_id));

  // avg CPU from feature snapshots
  const avgCpu = relevantSnaps.length > 0
    ? relevantSnaps.reduce((sum, s) => sum + s.cpu_usage_percent, 0) / relevantSnaps.length
    : 0;

  // Error rate: from snapshot critical+error ratios
  const avgErrRate = relevantSnaps.length > 0
    ? relevantSnaps.reduce((sum, s) => sum + (s.critical_count + s.error_count) / (s.total_events || 1), 0) / relevantSnaps.length
    : 0.1;

  const dominantFault = relevantSnaps.length > 0
    ? relevantSnaps.sort((a, b) => b.critical_count - a.critical_count)[0].dominant_fault_type
    : faultType;

  return {
    systems_affected:   systems.length,
    avg_cpu:            `${Math.round(avgCpu)}%`,
    error_rate_delta:   `+${Math.round(avgErrRate * 100)}%`,
    dominant_fault:     dominantFault,
  };
}

// ── Confidence helpers ───────────────────────────────────────────────

function signalIntensity(count: number): number {
  return Math.min(Math.log2(count) / Math.log2(100), 1);
}

function computeConfidence(sigCount: number, anomalyScore: number, failureProb: number): number {
  return Math.min(
    0.4 * signalIntensity(sigCount) + 0.3 * anomalyScore + 0.3 * failureProb,
    1,
  );
}

function dominantSeverity(sigSev: Severity, anomaly: number, failureProb: number): Severity {
  if (anomaly > 0.85 || failureProb > 0.80) return 'CRITICAL';
  if (anomaly > 0.70 || failureProb > 0.60) {
    if (sigSev === 'WARNING') return 'ERROR';
    if (sigSev === 'ERROR')   return 'CRITICAL';
  }
  return sigSev;
}

// ── System Health Index ──────────────────────────────────────────────

export function computeSystemHealthIndex(
  incidents:   Incident[],
  signals:     GroupedSignal[],
  mlPredictions: MLPrediction[],
): number {
  const totalIncidents = incidents.length;
  const spikes    = signals.filter((s) => s.isSpike).length;
  const highML    = mlPredictions.filter((p) => p.anomaly_score > 0.7 || p.failure_probability > 0.6).length;

  // Formula: 100 - (incidents*10 + signals*0.5 + ml_risk*30)
  const penalty = (totalIncidents * 10) + (spikes * 0.5) + (highML * 30);

  return Math.max(0, Math.min(100, Math.round(100 - penalty)));
}

// ── Main derivation ──────────────────────────────────────────────────

export function deriveIncidents(
  signals:          GroupedSignal[],
  mlPredictions:    MLPrediction[],
  featureSnapshots: FeatureSnapshot[],
): Incident[] {
  const incidents: Incident[] = [];
  const seen = new Set<string>();

  // Rule 1: Signal spikes (count > 5)
  for (const signal of signals) {
    if (signal.count <= 5) continue;

    const ml           = mlPredictions.find((p) => p.system_id === signal.systemId);
    const anomalyScore = ml?.anomaly_score        ?? 0;
    const failureProb  = ml?.failure_probability  ?? 0;
    const confidence   = computeConfidence(signal.count, anomalyScore, failureProb);
    const severity     = dominantSeverity(signal.severity, anomalyScore, failureProb);
    const trend        = detectTrend(signal.systemId, anomalyScore);
    const { score, label } = computePriority(severity, signal.systems.length, confidence, signal.lastSeen);
    const impact       = computeImpact(signal.systems, featureSnapshots, signal.fault_type);

    const id = `sig::${signal.systemId}::${signal.fault_type}`;
    if (seen.has(id)) continue;
    seen.add(id);

    incidents.push({
      incident_id:     `INC-SIG-${signal.id}`,
      title:           `${signal.fault_type} spike ×${signal.count}`,
      fault_type:      signal.fault_type,
      severity,
      systems:         signal.systems,
      event_count:     signal.count,
      confidence,
      priority_score:  score,
      priority_label:  label,
      trigger:         'signal',
      lifecycle:       'OPEN',
      signal,
      anomaly_score:   anomalyScore,
      failure_prob:    failureProb,
      predicted_fault: ml?.predicted_fault,
      trend,
      impact,
      created_at:      signal.lastSeen,
    });
  }

  // Rule 2: ML anomaly score > 0.7
  for (const pred of mlPredictions) {
    if (pred.anomaly_score <= 0.7) continue;

    const id = `anom::${pred.system_id}::${pred.predicted_fault}`;
    if (seen.has(id)) continue;
    seen.add(id);

    const snap       = featureSnapshots.find((s) => s.system_id === pred.system_id);
    const count      = snap?.total_events ?? 1;
    const confidence = computeConfidence(count, pred.anomaly_score, pred.failure_probability);
    const severity: Severity = pred.anomaly_score > 0.85 ? 'CRITICAL' : 'ERROR';
    const trend      = detectTrend(`${pred.system_id}:anom`, pred.anomaly_score);
    const { score, label } = computePriority(severity, 1, confidence, pred.prediction_time);
    const impact     = computeImpact([pred.system_id], featureSnapshots, pred.predicted_fault);

    incidents.push({
      incident_id:     `INC-ML-ANOM-${pred.system_id}`,
      title:           `ML anomaly: ${pred.predicted_fault} (${(pred.anomaly_score * 100).toFixed(0)}%)`,
      fault_type:      pred.predicted_fault,
      severity,
      systems:         [pred.system_id],
      event_count:     count,
      confidence,
      priority_score:  score,
      priority_label:  label,
      trigger:         'anomaly',
      lifecycle:       'OPEN',
      anomaly_score:   pred.anomaly_score,
      failure_prob:    pred.failure_probability,
      predicted_fault: pred.predicted_fault,
      trend,
      impact,
      created_at:      pred.prediction_time,
    });
  }

  // Rule 3: ML failure probability > 0.6
  for (const pred of mlPredictions) {
    if (pred.failure_probability <= 0.6) continue;
    const anomId = `anom::${pred.system_id}::${pred.predicted_fault}`;
    if (seen.has(anomId)) continue;
    const id = `failprob::${pred.system_id}::${pred.predicted_fault}`;
    if (seen.has(id)) continue;
    seen.add(id);

    const snap       = featureSnapshots.find((s) => s.system_id === pred.system_id);
    const count      = snap?.total_events ?? 1;
    const confidence = computeConfidence(count, pred.anomaly_score, pred.failure_probability);
    const severity: Severity = pred.failure_probability > 0.80 ? 'CRITICAL' : 'ERROR';
    const trend      = detectTrend(`${pred.system_id}:risk`, pred.failure_probability);
    const { score, label } = computePriority(severity, 1, confidence, pred.prediction_time);
    const impact     = computeImpact([pred.system_id], featureSnapshots, pred.predicted_fault);

    incidents.push({
      incident_id:     `INC-ML-RISK-${pred.system_id}`,
      title:           `Failure risk: ${pred.predicted_fault} (${(pred.failure_probability * 100).toFixed(0)}% prob)`,
      fault_type:      pred.predicted_fault,
      severity,
      systems:         [pred.system_id],
      event_count:     count,
      confidence,
      priority_score:  score,
      priority_label:  label,
      trigger:         'failure_prob',
      lifecycle:       'OPEN',
      anomaly_score:   pred.anomaly_score,
      failure_prob:    pred.failure_probability,
      predicted_fault: pred.predicted_fault,
      trend,
      impact,
      created_at:      pred.prediction_time,
    });
  }

  // Sort by priority_score DESC then severity then recency
  return incidents.sort((a, b) => {
    const pDiff = b.priority_score - a.priority_score;
    if (Math.abs(pDiff) > 2) return pDiff;
    const sDiff = SEV_RANK[b.severity] - SEV_RANK[a.severity];
    if (sDiff !== 0) return sDiff;
    return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
  });
}

// ── Forecast-based priority boost ────────────────────────────────────
// Reads forecastStore at derivation time (deferred import — avoids init cycle).
// Systems with risk_level === 'imminent' get +20 pts and at least MEDIUM label.

async function applyForecastBoost(incidents: Incident[]): Promise<Incident[]> {
  try {
    const { useForecastStore } = await import('./forecastStore');
    const forecasts = useForecastStore.getState().forecasts;
    const imminentIds = new Set(
      forecasts.filter((f) => f.risk_level === 'imminent').map((f) => f.system_id),
    );
    if (imminentIds.size === 0) return incidents;

    return incidents.map((inc) => {
      if (!inc.systems.some((s) => imminentIds.has(s))) return inc;
      const boostedScore = inc.priority_score + 20;
      const boostedLabel: PriorityLabel =
        inc.priority_label === 'LOW'    ? 'MEDIUM' :
        inc.priority_label === 'MEDIUM' ? 'HIGH'   : inc.priority_label;
      return { ...inc, priority_score: boostedScore, priority_label: boostedLabel };
    });
  } catch {
    return incidents;  // fallback: no boost if store unavailable
  }
}

// ── Store ────────────────────────────────────────────────────────────

interface IncidentState {
  incidents:         Incident[];
  correlations:      Correlation[];
  systemHealthIndex: number;
  /** Noise filter: incidents with priority_label === 'LOW' are collapsed in UI */
  showLowPriority:   boolean;

  deriveAll: (
    signals:          GroupedSignal[],
    mlPredictions:    MLPrediction[],
    featureSnapshots: FeatureSnapshot[],
    avgCpu:           number,
  ) => void;

  /** Lifecycle transitions */
  acknowledgeIncident: (id: string) => void;
  resolveIncident:     (id: string) => void;
  reopenIncident:      (id: string) => void;

  toggleLowPriority: () => void;
}

export const useIncidentStore = create<IncidentState>((set, get) => ({
  incidents:         [],
  correlations:      [],
  systemHealthIndex: 100,
  showLowPriority:   false,

  deriveAll: (signals, mlPredictions, featureSnapshots, avgCpu) => {
    // Guard: skip if inputs haven't changed meaningfully
    const prevIncidents = get().incidents;
    if (
      signals.length === 0 &&
      mlPredictions.length === 0 &&
      featureSnapshots.length === 0 &&
      prevIncidents.length === 0
    ) return;

    // Preserve existing lifecycle states across re-derivations
    const prevLifecycles = new Map(
      prevIncidents.map((i) => [i.incident_id, {
        lifecycle:       i.lifecycle,
        acknowledged_at: i.acknowledged_at,
        resolved_at:     i.resolved_at,
      }]),
    );

    const raw = deriveIncidents(signals, mlPredictions, featureSnapshots);

    // Restore lifecycle state for incidents that already existed
    const withLifecycle = raw.map((inc) => {
      const prev = prevLifecycles.get(inc.incident_id);
      if (!prev) return inc;
      return { ...inc, ...prev };
    });

    const corrInput: CorrelationInputs = { incidents: withLifecycle, mlPredictions, featureSnapshots, avgCpu };
    const correlations      = correlate(corrInput);
    const systemHealthIndex = computeSystemHealthIndex(withLifecycle, signals, mlPredictions);

    // Apply async forecast boost then commit — UI updates once when boost resolves
    applyForecastBoost(withLifecycle).then((incidents) => {
      set({ incidents, correlations, systemHealthIndex });
    }).catch(() => {
      set({ incidents: withLifecycle, correlations, systemHealthIndex });
    });
  },

  acknowledgeIncident: (id) => {
    set((s) => ({
      incidents: s.incidents.map((i) =>
        i.incident_id === id
          ? { ...i, lifecycle: 'ACKNOWLEDGED', acknowledged_at: new Date().toISOString() }
          : i,
      ),
    }));
  },

  resolveIncident: (id) => {
    const incident = get().incidents.find((i) => i.incident_id === id);
    // Feed resolution data back to feedbackStore (deferred import — avoids init cycle)
    if (incident) {
      import('./feedbackStore').then(({ useFeedbackStore }) => {
        useFeedbackStore.getState().recordResolution(incident);
      }).catch(() => {/* non-critical */});
    }
    set((s) => ({
      incidents: s.incidents.map((i) =>
        i.incident_id === id
          ? { ...i, lifecycle: 'RESOLVED', resolved_at: new Date().toISOString() }
          : i,
      ),
    }));
  },

  reopenIncident: (id) => {
    set((s) => ({
      incidents: s.incidents.map((i) =>
        i.incident_id === id
          ? { ...i, lifecycle: 'OPEN', acknowledged_at: undefined, resolved_at: undefined }
          : i,
      ),
    }));
  },

  toggleLowPriority: () => set((s) => ({ showLowPriority: !s.showLowPriority })),
}));
