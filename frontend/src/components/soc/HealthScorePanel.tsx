/**
 * HealthScorePanel — per-system health score display
 *
 * Score formula: 100 - critical×10 - error×4 - warning×1 - rate-of-change penalty
 * Bounded 0–100. Color bands: ≥85 green, ≥60 yellow, ≥30 orange, <30 red.
 * Large monospace numbers. No gradients. Minimal chrome.
 */
import { useMemo } from 'react';
import { useSignalStore, computeHealthScore } from '../../store/signalStore';
import { useDashboardStore } from '../../store/dashboardStore';
import { useUIStore } from '../../store/uiStore';

function scoreColor(score: number): string {
  if (score >= 85) return '#22C55E';
  if (score >= 60) return '#FACC15';
  if (score >= 30) return '#F97316';
  return '#DC2626';
}

function scoreLabel(score: number): string {
  if (score >= 85) return 'NOMINAL';
  if (score >= 60) return 'DEGRADED';
  if (score >= 30) return 'IMPAIRED';
  return 'CRITICAL';
}

interface SystemScore {
  systemId:  string;
  hostname:  string;
  score:     number;
  status:    string;
  cpu:       number;
  mem:       number;
  disk:      number;
}

export default function HealthScorePanel() {
  const events  = useSignalStore((s) => s.events);
  const systems = useDashboardStore((s) => s.systems);
  const highlightedSystems = useUIStore((s) => s.highlightedSystems);
  const hasHighlight = highlightedSystems.length > 0;

  const systemScores: SystemScore[] = useMemo(() => {
    return systems.map((sys) => ({
      systemId: sys.system_id,
      hostname: sys.hostname,
      score:    computeHealthScore(events, sys.system_id),
      status:   sys.status,
      cpu:      sys.cpu_usage_percent,
      mem:      sys.memory_usage_percent,
      disk:     sys.disk_free_percent,
    }));
  }, [events, systems]);

  const fleetScore = useMemo(() => {
    if (systemScores.length === 0) return 100;
    const sum = systemScores.reduce((acc, s) => acc + s.score, 0);
    return Math.round(sum / systemScores.length);
  }, [systemScores]);

  const fleetColor = scoreColor(fleetScore);
  const fleetLabel = scoreLabel(fleetScore);

  return (
    <div className="soc-panel flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="soc-panel-header">
        <span className="soc-panel-title">Fleet Health</span>
        <span className="font-mono text-[10px]" style={{ color: fleetColor }}>
          {fleetLabel}
        </span>
      </div>

      {/* Fleet composite score */}
      <div
        className="flex flex-col items-center justify-center border-b border-[#1E293B] py-3"
        style={{ flexShrink: 0 }}
      >
        <span
          className="font-mono font-bold leading-none"
          style={{ fontSize: 52, color: fleetColor, letterSpacing: '-2px' }}
        >
          {fleetScore}
        </span>
        <span className="font-mono text-[10px] text-[#475569] mt-1">fleet composite / 100</span>
      </div>

      {/* Per-system scores */}
      <div className="flex-1 overflow-y-auto">
        {/* Column labels */}
        <div
          className="flex items-center px-3 border-b border-[#1E293B]"
          style={{ height: 20, background: '#090e1a', flexShrink: 0 }}
        >
          <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider flex-1">System</span>
          <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider w-[30px] text-center">CPU</span>
          <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider w-[32px] text-center">MEM</span>
          <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider w-[36px] text-right">Score</span>
        </div>

        {systemScores.map((sys) => {
          const color    = scoreColor(sys.score);
          const isHighlighted = highlightedSystems.includes(sys.systemId) ||
            highlightedSystems.includes(sys.hostname);
          const isDimmed = hasHighlight && !isHighlighted;
          return (
            <div
              key={sys.systemId}
              className="flex items-center px-3 border-b border-[#151f2e] hover:bg-[#162032] transition-colors"
              style={{
                height:     32,
                opacity:    isDimmed ? 0.35 : 1,
                transition: 'opacity 200ms ease, background 80ms',
                boxShadow:  isHighlighted ? 'inset 0 0 0 1px #38BDF8' : undefined,
                background: isHighlighted ? '#0C1E33' : undefined,
              }}
            >
              {/* Status dot */}
              <span
                className={`soc-dot ${
                  sys.status === 'online'
                    ? 'soc-dot-online'
                    : sys.status === 'degraded'
                    ? 'soc-dot-degraded'
                    : 'soc-dot-offline'
                } mr-2`}
              />

              {/* Hostname */}
              <span className="font-mono text-[10px] text-[#94A3B8] flex-1 truncate">
                {sys.hostname.split('.')[0]} <span style={{ color: '#475569' }}>• {sys.systemId.substring(0, 5)}</span>
              </span>

              {/* CPU mini bar */}
              <div className="w-[30px] flex items-center justify-center">
                <div className="soc-bar-track">
                  <div
                    className="soc-bar-fill"
                    style={{
                      width:      `${Math.min(sys.cpu, 100)}%`,
                      background: sys.cpu > 90 ? '#DC2626' : sys.cpu > 70 ? '#F97316' : '#22C55E',
                    }}
                  />
                </div>
              </div>

              {/* MEM mini bar */}
              <div className="w-[32px] flex items-center justify-center">
                <div className="soc-bar-track">
                  <div
                    className="soc-bar-fill"
                    style={{
                      width:      `${Math.min(sys.mem, 100)}%`,
                      background: sys.mem > 90 ? '#DC2626' : sys.mem > 75 ? '#F97316' : '#38BDF8',
                    }}
                  />
                </div>
              </div>

              {/* Score */}
              <span
                className="font-mono text-[12px] font-bold w-[36px] text-right"
                style={{ color }}
              >
                {sys.score}
              </span>
            </div>
          );
        })}
      </div>

      {/* Footer */}
      <div
        className="flex items-center px-3 border-t border-[#1E293B]"
        style={{ height: 20, flexShrink: 0, background: '#090e1a' }}
      >
        <span className="font-mono text-[9px] text-[#334155]">
          {systemScores.filter((s) => s.score >= 85).length} nominal ·{' '}
          {systemScores.filter((s) => s.score < 85 && s.score >= 30).length} impaired ·{' '}
          {systemScores.filter((s) => s.score < 30).length} critical
        </span>
      </div>
    </div>
  );
}
