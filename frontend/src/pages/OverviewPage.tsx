import { Server, Activity, AlertTriangle, Zap, Download } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import DashboardCard from '../components/shared/DashboardCard';
import SystemHealthHeatmap from '../components/shared/SystemHealthHeatmap';
import LiveEventStream from '../components/shared/LiveEventStream';
import PipelineHealthPanel from '../components/shared/PipelineHealthPanel';
import EventRateChart from '../components/charts/EventRateChart';
import SeverityChart from '../components/charts/SeverityChart';
import FaultTypesChart from '../components/charts/FaultTypesChart';
import SeverityBadge from '../components/shared/SeverityBadge';
import {
  getActiveAlerts,
  getDegradedSystems,
  getCriticalAlertCount,
  formatTimestamp,
} from '../data/mockData';
import { useDashboard } from '../context/DashboardContext';
import { downloadReport } from '../lib/api';

export default function OverviewPage() {
  const navigate = useNavigate();
  const {
    filteredEvents,
    filteredSystems,
    filteredAlerts,
    dashboardMetrics,
    canUseAggregateViews,
  } = useDashboard();
  
  const degraded = getDegradedSystems(filteredSystems);
  const criticalAlerts = getCriticalAlertCount(filteredAlerts);
  const activeAlerts = getActiveAlerts(filteredAlerts).length;
  const eventVolume = canUseAggregateViews ? dashboardMetrics.total_events : filteredEvents.length;
  const eventVolumeSubtitle = canUseAggregateViews ? 'server-backed range total' : 'recent sample matching filters';

  const recentCritical = filteredEvents
    .filter((e) => e.severity === 'CRITICAL' || e.severity === 'ERROR')
    .sort((a, b) => new Date(b.event_time).getTime() - new Date(a.event_time).getTime())
    .slice(0, 8);

  return (
    <div className="space-y-4">
      {/* Page Header */}
      <div className="mb-2 flex items-center justify-between">
        <div>
          <h2 className="text-lg font-bold text-text-primary tracking-tight">Mission Control</h2>
          <p className="text-[11px] text-text-muted mt-0.5">System telemetry & alert activity overview</p>
        </div>
        <button onClick={() => downloadReport().catch(console.error)} className="flex items-center gap-1.5 px-3 py-1.5 rounded border border-border bg-bg-surface text-xs font-semibold text-text-primary hover:bg-bg-hover hover:-translate-y-0.5 transition-all">
          <Download className="w-4 h-4 text-signal-primary" />
          <span>Generate Report</span>
        </button>
      </div>

      {/* ── Top Row: KPI Strip ── */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <DashboardCard
          title="Systems Online"
          value={filteredSystems.filter(s => s.status === 'online').length}
          subtitle={degraded > 0 ? `${degraded} degraded` : 'All healthy'}
          subtitleColor={degraded > 0 ? 'text-accent-amber' : 'text-signal-highlight'}
          icon={<Server className="w-4 h-4 text-signal-primary" />}
          iconBg="bg-signal-primary/10"
        />
        <DashboardCard
          title="Systems Degraded"
          value={degraded}
          subtitle={degraded > 0 ? 'Requires attention' : 'None'}
          subtitleColor={degraded > 0 ? 'text-accent-orange' : 'text-signal-highlight'}
          icon={<Activity className="w-4 h-4 text-accent-orange" />}
          iconBg="bg-accent-orange/10"
        />
        <DashboardCard
          title="Critical Alerts"
          value={criticalAlerts}
          subtitle={`${activeAlerts} total active`}
          subtitleColor="text-accent-red"
          icon={<AlertTriangle className="w-4 h-4 text-accent-red" />}
          iconBg="bg-accent-red/10"
        />
        <DashboardCard
          title="Events In Range"
          value={eventVolume.toLocaleString()}
          subtitle={eventVolumeSubtitle}
          subtitleColor="text-text-secondary"
          icon={<Zap className="w-4 h-4 text-signal-primary" />}
          iconBg="bg-signal-primary/10"
        />
      </div>

      {/* ── Second Row Layout ── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left Col: Pipeline + Heatmap */}
        <div className="lg:col-span-2 space-y-4 flex flex-col">
          <PipelineHealthPanel />
          <div className="flex-1">
            <SystemHealthHeatmap />
          </div>
        </div>

        {/* Right Col: Live Event Stream */}
        <div className="lg:col-span-1 min-h-[400px]">
          <LiveEventStream />
        </div>
      </div>

      {/* ── Analytics Row ── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <EventRateChart />
        <SeverityChart />
        <FaultTypesChart />
      </div>

      {/* ── Recent Critical Events Strip ── */}
      {recentCritical.length > 0 && (
        <div className="glass-panel-solid rounded-md p-4 border border-border">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-[11px] font-bold text-text-muted uppercase tracking-wider">Recent Critical Events</h3>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2">
            {recentCritical.map((e) => (
              <div
                key={e.event_record_id}
                onClick={() => navigate(`/events?system=${e.system_id}`)}
                className="flex flex-col gap-2 p-3 rounded bg-bg-surface border border-border/50 hover:bg-bg-hover hover:border-border transition-colors cursor-pointer"
              >
                <div className="flex items-start justify-between">
                  <SeverityBadge severity={e.severity} />
                  <span className="text-[10px] text-text-muted">{formatTimestamp(e.event_time).split(', ')[1]}</span>
                </div>
                <div className="flex-1 min-w-0 mt-1">
                  <p className="text-xs font-semibold text-text-primary truncate">{e.fault_description || e.fault_type}</p>
                  <p className="text-[10px] text-text-secondary mt-0.5 font-mono">{e.hostname}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
