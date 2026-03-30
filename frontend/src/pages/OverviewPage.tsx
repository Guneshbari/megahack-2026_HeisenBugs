/**
 * OverviewPage — 12-column SOC grid
 *
 * Layout:
 *   [col-3] SmartEventStream  |  [col-6] IncidentBoard  |  [col-3] HealthScorePanel
 *   [col-12] MetricsCharts (ECharts, zoomable)
 *   [col-12] KpiStrip
 *
 * This page initializes the Zustand signal store from mock/API data on mount.
 */
import { useEffect } from 'react';
import SmartEventStream from '../components/soc/SmartEventStream';
import IncidentBoard    from '../components/soc/IncidentBoard';
import HealthScorePanel from '../components/soc/HealthScorePanel';
import MetricsCharts    from '../components/soc/MetricsCharts';
import KpiStrip         from '../components/soc/KpiStrip';
import { useSignalStore } from '../store/signalStore';


export default function OverviewPage() {

  // Periodic recompute to roll time windows (every 60s)
  const recompute = useSignalStore((s) => s.recompute);
  useEffect(() => {
    const id = setInterval(recompute, 60_000);
    return () => clearInterval(id);
  }, [recompute]);

  return (
    <div
      className="flex flex-col gap-1 h-full"
      style={{ minHeight: 0 }}
    >
      {/* KPI Strip — top */}
      <KpiStrip />

      {/* Main SOC grid: 7fr 3fr */}
      <div
        className="grid gap-1 flex-1 min-h-0"
        style={{ gridTemplateColumns: '7fr 3fr', minHeight: 0 }}
      >
        {/* LEFT — Incident board (Primary Focus) */}
        <IncidentBoard />

        {/* RIGHT — Health scores & Signal stream stacked */}
        <div className="flex flex-col gap-1 h-full min-h-0">
          <HealthScorePanel />
          <SmartEventStream />
        </div>
      </div>

      {/* Bottom — Event frequency chart */}
      <MetricsCharts />
    </div>
  );
}
