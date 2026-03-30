/**
 * forecastEngine — Predictive Failure Forecasting
 *
 * Pure functional module. No side effects. No network calls.
 * Input:  per-system MLPrediction history (Map<system_id, MLPrediction[]>, last 10) + FeatureSnapshot[]
 * Output: SystemForecast[]
 *
 * Algorithm:
 *  1. Linear regression slope on failure_probability time series
 *  2. Extrapolate TTF (time to failure >= 0.90) when slope > 0
 *  3. Forecast confidence = 0.40 * R² + 0.35 * failure_prob_now + 0.25 * anomaly_score_latest
 *  4. risk_level: 'imminent' | 'elevated' | 'nominal'
 *
 * Edge-case safety:
 *  - No history → returns 'nominal' fallback
 *  - Single sample → no TTF, trend = 'stable', conf = failure_prob/2
 *  - NaN guards on all numeric operations
 */
import type { MLPrediction, FeatureSnapshot } from '../types/telemetry';

// ── Exported types ────────────────────────────────────────────────────

export interface SystemForecast {
  system_id:        string;
  trend:            'rising' | 'stable' | 'falling';
  failure_prob_now: number;
  /** null when not imminently at risk or sample too sparse to extrapolate */
  time_to_failure:  string | null;
  forecast_conf:    number;   // 0–1
  risk_level:       'imminent' | 'elevated' | 'nominal';
}

// ── Constants ─────────────────────────────────────────────────────────

/** Assumed interval between consecutive MLPrediction samples */
const SAMPLE_INTERVAL_MS = 30_000;   // 30s (matches DashboardContext auto-refresh default)
const FAILURE_THRESHOLD  = 0.90;
const SLOPE_RISING       = 0.05;     // per sample
const SLOPE_FALLING      = -0.05;
/** Only show TTF if failure is projected within this many samples */
const MAX_TTF_SAMPLES    = 120;      // = 60 min

// ── Linear regression ─────────────────────────────────────────────────

function linearRegression(y: number[]): { slope: number; r2: number } {
  const n = y.length;
  if (n < 2) return { slope: 0, r2: 0 };

  const meanX = (n - 1) / 2;
  const meanY = y.reduce((s, v) => s + v, 0) / n;

  let ssXX = 0, ssXY = 0, ssYY = 0;
  for (let i = 0; i < n; i++) {
    const dx = i - meanX;
    const dy = y[i] - meanY;
    ssXX += dx * dx;
    ssXY += dx * dy;
    ssYY += dy * dy;
  }

  const slope = ssXX === 0 ? 0 : ssXY / ssXX;
  const r2    = ssYY === 0 ? 1 : Math.min(Math.max((ssXY * ssXY) / (ssXX * ssYY), 0), 1);

  return { slope: Number.isFinite(slope) ? slope : 0, r2: Number.isFinite(r2) ? r2 : 0 };
}

// ── Duration formatter ────────────────────────────────────────────────

function formatDuration(ms: number): string {
  const minutes = Math.round(ms / 60_000);
  if (minutes < 1)  return '< 1 min';
  if (minutes < 60) return `~${minutes} min`;
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  return m > 0 ? `~${h}h ${m}m` : `~${h}h`;
}

// ── Per-system forecast ───────────────────────────────────────────────

function forecastSystem(
  systemId:  string,
  history:   MLPrediction[],
  snapshots: FeatureSnapshot[],
): SystemForecast {
  const NOMINAL: SystemForecast = {
    system_id:        systemId,
    trend:            'stable',
    failure_prob_now: 0,
    time_to_failure:  null,
    forecast_conf:    0,
    risk_level:       'nominal',
  };

  if (history.length === 0) return NOMINAL;

  // Sort oldest → newest for regression
  const sorted = [...history].sort(
    (a, b) => new Date(a.prediction_time).getTime() - new Date(b.prediction_time).getTime(),
  );

  const latest          = sorted[sorted.length - 1];
  const failureProbNow  = Math.min(Math.max(latest.failure_probability, 0), 1);
  const anomalyScore    = Math.min(Math.max(latest.anomaly_score, 0), 1);

  // ── Single-sample fallback ────────────────────────────────────────
  if (sorted.length < 2) {
    const riskLevel: SystemForecast['risk_level'] =
      failureProbNow > 0.80 ? 'imminent' :
      failureProbNow > 0.60 ? 'elevated' : 'nominal';
    return {
      system_id:        systemId,
      trend:            'stable',
      failure_prob_now: failureProbNow,
      time_to_failure:  null,
      forecast_conf:    Math.round((failureProbNow / 2) * 100) / 100,
      risk_level:       riskLevel,
    };
  }

  // ── Regression on full history ────────────────────────────────────
  const probSeries       = sorted.map((p) => p.failure_probability);
  const { slope, r2 }   = linearRegression(probSeries);

  const trend: SystemForecast['trend'] =
    slope > SLOPE_RISING  ? 'rising'  :
    slope < SLOPE_FALLING ? 'falling' : 'stable';

  // ── TTF extrapolation ─────────────────────────────────────────────
  let timeToFailure: string | null = null;
  if (slope > 0 && failureProbNow < FAILURE_THRESHOLD) {
    const samplesNeeded = (FAILURE_THRESHOLD - failureProbNow) / slope;
    if (samplesNeeded < MAX_TTF_SAMPLES && samplesNeeded > 0) {
      timeToFailure = formatDuration(samplesNeeded * SAMPLE_INTERVAL_MS);
    }
  }

  // ── Forecast confidence ───────────────────────────────────────────
  const baseConf    = 0.40 * r2 + 0.35 * failureProbNow + 0.25 * anomalyScore;
  // CPU context boost from feature snapshot
  const snap        = snapshots.find((s) => s.system_id === systemId);
  const cpuBoost    = (snap && snap.cpu_usage_percent > 85) ? 0.05 : 0;
  const forecastConf = Math.min(baseConf + cpuBoost, 1);

  // ── Risk level ────────────────────────────────────────────────────
  const imminentTTF = timeToFailure !== null;
  const riskLevel: SystemForecast['risk_level'] =
    (failureProbNow > 0.80 || (trend === 'rising' && imminentTTF))  ? 'imminent' :
    (failureProbNow > 0.60 || trend === 'rising')                    ? 'elevated' : 'nominal';

  return {
    system_id:        systemId,
    trend,
    failure_prob_now: failureProbNow,
    time_to_failure:  timeToFailure,
    forecast_conf:    Math.round(forecastConf * 100) / 100,
    risk_level:       riskLevel,
  };
}

// ── Main export ───────────────────────────────────────────────────────

const RISK_ORDER: Record<SystemForecast['risk_level'], number> = {
  imminent: 0, elevated: 1, nominal: 2,
};

/**
 * computeForecasts — produce a forecast per system in historyBySystem.
 * Systems with no history are absent from the output (not padded to nominal).
 * Caller should treat missing forecasts as nominal.
 */
export function computeForecasts(
  historyBySystem: Map<string, MLPrediction[]>,
  snapshots:       FeatureSnapshot[],
): SystemForecast[] {
  if (historyBySystem.size === 0) return [];

  const forecasts: SystemForecast[] = [];
  for (const [systemId, history] of historyBySystem) {
    forecasts.push(forecastSystem(systemId, history, snapshots));
  }

  return forecasts.sort((a, b) => RISK_ORDER[a.risk_level] - RISK_ORDER[b.risk_level]);
}
