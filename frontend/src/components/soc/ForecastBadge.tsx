/**
 * ForecastBadge — Inline predictive risk indicator for a system
 *
 * Hidden for 'nominal' systems to avoid visual noise.
 * Reads from useForecastStore — granular selector on system_id.
 *
 * Props:
 *  systemId  — system_id to look up in the forecast store
 *  compact   — true = pill only, no TTF / confidence text
 */
import { useForecastStore } from '../../store/forecastStore';

interface ForecastBadgeProps {
  systemId: string;
  compact?: boolean;
}

export default function ForecastBadge({ systemId, compact = false }: ForecastBadgeProps) {
  // Granular selector — only re-renders when this system's forecast changes
  const forecast = useForecastStore(
    (s) => s.forecasts.find((f) => f.system_id === systemId) ?? null,
  );

  // No forecast data or nominal → render nothing (zero visual noise)
  if (!forecast || forecast.risk_level === 'nominal') return null;

  const isImminent = forecast.risk_level === 'imminent';

  const color  = isImminent ? '#EF4444' : '#F97316';
  const bg     = isImminent ? '#3B0A0A' : '#2A1200';
  const border = isImminent ? '#EF444455' : '#F9731655';
  const label  = isImminent ? '⚠ IMMINENT' : '↑ ELEVATED';
  const arrow  = forecast.trend === 'rising' ? '↑' : forecast.trend === 'falling' ? '↓' : '→';

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>

      {/* Risk level pill */}
      <span
        style={{
          fontFamily:    'JetBrains Mono, monospace',
          fontSize:      9,
          fontWeight:    700,
          color,
          background:    bg,
          border:        `1px solid ${border}`,
          padding:       '1px 6px',
          borderRadius:  2,
          letterSpacing: '0.06em',
          animation:     isImminent ? 'soc-pulse 1.6s ease-in-out infinite' : undefined,
          display:       'inline-flex',
          alignItems:    'center',
          gap:           4,
        }}
      >
        {label}
      </span>

      {/* TTF estimate — not shown in compact mode */}
      {!compact && forecast.time_to_failure && (
        <span style={{
          fontFamily: 'JetBrains Mono, monospace',
          fontSize:   9,
          color,
          opacity:    0.85,
        }}>
          TTF: {forecast.time_to_failure}
        </span>
      )}

      {/* Trend + confidence — not shown in compact mode */}
      {!compact && (
        <span style={{
          fontFamily: 'JetBrains Mono, monospace',
          fontSize:   9,
          color:      '#475569',
        }}>
          {arrow} {(forecast.forecast_conf * 100).toFixed(0)}% conf
        </span>
      )}
    </div>
  );
}
