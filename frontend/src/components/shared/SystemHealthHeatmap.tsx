import { useNavigate } from 'react-router-dom';
import { useDashboardStore } from '../../store/dashboardStore';

const levelColors: Record<string, { bg: string; border: string; text: string; glow: string }> = {
  healthy: { bg: 'bg-[#22c55e]/10', border: 'border-[#22c55e]/30', text: 'text-[#22c55e]', glow: '' },
  warning: { bg: 'bg-[#ffd60a]/10', border: 'border-[#ffd60a]/30', text: 'text-[#ffd60a]', glow: '' },
  error: { bg: 'bg-[#ff7a18]/10', border: 'border-[#ff7a18]/30', text: 'text-[#ff7a18]', glow: 'shadow-[0_0_12px_rgba(255,122,24,0.15)]' },
  critical: { bg: 'bg-[#ff3b30]/10', border: 'border-[#ff3b30]/40', text: 'text-[#ff3b30]', glow: 'shadow-[0_0_16px_rgba(255,59,48,0.2)]' },
};

export default function SystemHealthHeatmap() {
  const navigate = useNavigate();
  const filteredSystems = useDashboardStore((s) => s.filteredSystems);
  const filteredSystemEventSummaries = useDashboardStore((s) => s.filteredSystemEventSummaries);

  const systemHealth = filteredSystems.map((s) => ({
    ...s,
    health: filteredSystemEventSummaries[s.system_id] ?? {
      eventCount: 0,
      criticalCount: 0,
      errorCount: 0,
      warningCount: 0,
      healthScore: 0,
      healthLevel: 'healthy',
      latestEvent: null,
    },
  }));

  return (
    <div className="glass-panel panel-glow rounded-xl p-5 animate-fade-in">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-semibold text-text-primary">System Health Map</h3>
          <p className="text-xs text-text-muted mt-0.5">Click a system to investigate events</p>
        </div>
        <div className="flex items-center gap-3 text-[10px] text-text-muted">
          {['healthy', 'warning', 'error', 'critical'].map((level) => (
            <span key={level} className="flex items-center gap-1">
              <span className={`w-2.5 h-2.5 rounded-sm ${levelColors[level].bg} border ${levelColors[level].border}`} />
              {level}
            </span>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-3 md:grid-cols-6 gap-2">
        {systemHealth.map((s) => {
          const colors = levelColors[s.health.healthLevel];
          return (
            <button
              key={s.system_id}
              onClick={() => navigate(`/events?system=${s.system_id}`)}
              className={`${colors.bg} border ${colors.border} ${colors.glow} rounded-lg p-3 text-left transition-all duration-200 hover:scale-[1.03] cursor-pointer`}
            >
              <p className={`text-xs font-bold ${colors.text} truncate`}>{s.hostname}</p>
              <p className="text-[10px] text-text-muted font-mono mt-0.5">{s.system_id}</p>
              <div className="flex items-center gap-2 mt-2 text-[10px] text-text-secondary">
                <span>CPU {s.cpu_usage_percent}%</span>
                <span>MEM {s.memory_usage_percent}%</span>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
