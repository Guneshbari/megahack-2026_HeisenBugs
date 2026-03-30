/**
 * forecastStore — Forecast State Manager
 *
 * Maintains per-system ring buffer of last 10 MLPrediction snapshots (session-persistent).
 * Calls computeForecasts() after each ingest() — only when something actually changed.
 *
 * Guard: deduplicates by prediction_time — identical polls won't retrigger recompute.
 * Cap:   buffer.length > HISTORY_MAX → oldest removed (ring buffer via shift()).
 */
import { create } from 'zustand';
import type { MLPrediction, FeatureSnapshot } from '../types/telemetry';
import { computeForecasts, type SystemForecast } from '../lib/forecastEngine';

const HISTORY_MAX = 10;

interface ForecastState {
  forecasts:       SystemForecast[];
  /** Per-system ring buffer — last 10 MLPrediction snapshots, oldest first */
  historyBySystem: Map<string, MLPrediction[]>;

  /**
   * ingest — Called each poll cycle.
   * Adds new predictions to per-system buffers, caps at HISTORY_MAX,
   * then recomputes forecasts only if data changed.
   */
  ingest: (preds: MLPrediction[], snaps: FeatureSnapshot[]) => void;
}

export const useForecastStore = create<ForecastState>((set, get) => ({
  forecasts:       [],
  historyBySystem: new Map(),

  ingest: (preds, snaps) => {
    if (preds.length === 0) return;

    const prev    = get().historyBySystem;
    const next    = new Map(prev);
    let changed   = false;

    for (const pred of preds) {
      const buf = next.get(pred.system_id) ?? [];

      // Guard: skip if this prediction_time is already in the buffer
      if (buf.some((p) => p.prediction_time === pred.prediction_time)) continue;

      changed = true;
      const updated = [...buf, pred];   // append newest at end (oldest first)

      // Cap at HISTORY_MAX — remove oldest when over the limit
      if (updated.length > HISTORY_MAX) updated.shift();

      next.set(pred.system_id, updated);
    }

    // Guard: no new data → skip recompute entirely
    if (!changed) return;

    const forecasts = computeForecasts(next, snaps);
    set({ historyBySystem: next, forecasts });
  },
}));
