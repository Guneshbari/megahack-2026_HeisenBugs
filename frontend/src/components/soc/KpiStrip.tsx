/**
 * KpiStrip — inline SOC KPI bar
 *
 * Single horizontal bar, not a card grid.
 * Color-coded values only — no colored backgrounds.
 */
import { useMemo } from 'react';
import { useDashboardStore } from '../../store/dashboardStore';
import { useSignalStore } from '../../store/signalStore';
import { useIncidentStore } from '../../store/incidentStore';

interface KpiItem {
  label: string;
  value: string | number;
  color?: string;
}

export default function KpiStrip() {
  const systems = useDashboardStore((s) => s.systems);
  const filteredAlerts = useDashboardStore((s) => s.filteredAlerts);
  const pipelineHealth = useDashboardStore((s) => s.pipelineHealth);
  const isConnected    = useSignalStore((s) => s.isConnected);
  const signalCount    = useSignalStore((s) => s.signals.length);
  const incidentCount  = useIncidentStore((s) => s.incidents.length);
  const critIncidents  = useIncidentStore((s) => s.incidents.filter((i) => i.severity === 'CRITICAL').length);
  const mlIncidents    = useIncidentStore((s) => s.incidents.filter((i) => i.trigger !== 'signal').length);

  const kpis: KpiItem[] = useMemo(() => {
    const online   = systems.filter((s) => s.status === 'online').length;
    const degraded = systems.filter((s) => s.status === 'degraded').length;
    const offline  = systems.filter((s) => s.status === 'offline').length;
    const active   = filteredAlerts.filter((a) => !a.acknowledged).length;
    const eps      = pipelineHealth?.events_per_sec ?? 0;
    const latency  = pipelineHealth?.avg_latency_ms ?? 0;

    return [
      { label: 'SYSTEMS',   value: `${online}/${systems.length}`,  color: online < systems.length ? '#F97316' : '#22C55E' },
      { label: 'DEGRADED',  value: degraded, color: degraded > 0 ? '#F97316' : '#475569' },
      { label: 'OFFLINE',   value: offline,  color: offline  > 0 ? '#DC2626' : '#475569' },
      { label: 'INCIDENTS', value: incidentCount, color: critIncidents > 0 ? '#DC2626' : incidentCount > 0 ? '#F97316' : '#22C55E' },
      { label: 'ML-RISK',   value: mlIncidents, color: mlIncidents > 0 ? '#A855F7' : '#475569' },
      { label: 'ALERTS',    value: active,   color: active   > 0 ? '#F97316' : '#22C55E' },
      { label: 'SIGNALS',   value: signalCount, color: '#94A3B8' },
      { label: 'EPS',       value: eps.toFixed(1), color: '#38BDF8' },
      { label: 'LATENCY',   value: latency > 0 ? `${latency}ms` : '—', color: latency > 200 ? '#F97316' : '#94A3B8' },
      { label: 'TRANSPORT', value: isConnected ? 'LIVE' : 'MOCK', color: isConnected ? '#22C55E' : '#FACC15' },
    ];
  }, [systems, filteredAlerts, pipelineHealth, signalCount, isConnected, incidentCount, critIncidents, mlIncidents]);

  return (
    <div
      className="flex items-center border border-[#1E293B]"
      style={{ height: 28, background: '#0F172A', flexShrink: 0 }}
    >
      {kpis.map((kpi, i) => (
        <div
          key={kpi.label}
          className="flex items-center gap-2 h-full px-4"
          style={{
            borderRight: i < kpis.length - 1 ? '1px solid #1E293B' : undefined,
          }}
        >
          <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider">
            {kpi.label}
          </span>
          <span
            className="font-mono text-[11px] font-semibold"
            style={{ color: kpi.color ?? '#E2E8F0' }}
          >
            {kpi.value}
          </span>
        </div>
      ))}
    </div>
  );
}
