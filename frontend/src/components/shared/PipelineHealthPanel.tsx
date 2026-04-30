import { useMemo, useState } from 'react';
import { Activity, Database, Server, Zap, TrendingUp, BarChart3, Cpu } from 'lucide-react';
import { Area, AreaChart, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, LineChart, Line } from 'recharts';
import { useDashboardStore } from '../../store/dashboardStore';

type PanelTab = 'overview' | 'kafka' | 'system';

export default function PipelineHealthPanel() {
  const systems = useDashboardStore((s) => s.systems);
  const metrics = useDashboardStore((s) => s.metrics);
  const pipelineHealth = useDashboardStore((s) => s.pipelineHealth);
  const pipelineHealthError = useDashboardStore((s) => s.pipelineHealthError);
  const topSystemsByEventVolume = useDashboardStore((s) => s.topSystemsByEventVolume);
  const [activeTab, setActiveTab] = useState<PanelTab | null>(null);

  const eps = pipelineHealth?.events_per_sec ?? 0;
  const latency = pipelineHealth?.avg_latency_ms ?? 0;
  const kafkaLag = pipelineHealth?.kafka_lag ?? 0;
  const lagStatus = pipelineHealth?.lag_status ?? 'Unknown';
  const dbWriteRate = pipelineHealth?.db_write_rate ?? 0;
  const trendEps = pipelineHealth?.trend_eps ?? [];
  const trendLatency = pipelineHealth?.trend_latency ?? [];
  const epsChange = pipelineHealth?.eps_change_pct ?? 0;

  // Compute system health data for the tab
  const systemHealthData = useMemo(() => systems.map((s) => ({
    name: s.hostname,
    cpu: s.cpu_usage_percent,
    memory: s.memory_usage_percent,
    disk: s.disk_free_percent,
  })), [systems]);

  // Severity breakdown from metrics
  const severityTrend = useMemo(() => metrics.map((m) => ({
    time: new Date(m.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false }),
    Critical: m.critical_count,
    Error: m.error_count,
    Warning: m.warning_count,
    Info: m.info_count,
  })), [metrics]);

  const tabs: { id: PanelTab; label: string; color: string; hoverColor: string }[] = [
    { id: 'overview', label: 'Pipeline Overview', color: 'accent-blue', hoverColor: 'hover:border-[#3b82f6] hover:text-[#3b82f6]' },
    { id: 'kafka', label: 'Kafka', color: 'accent-purple', hoverColor: 'hover:border-[#8b5cf6] hover:text-[#8b5cf6]' },
    { id: 'system', label: 'System Health', color: 'accent-green', hoverColor: 'hover:border-[#22c55e] hover:text-[#22c55e]' },
  ];

  const tabActiveColors: Record<PanelTab, string> = {
    overview: 'border-[#3b82f6] text-[#3b82f6] bg-[#3b82f6]/10',
    kafka: 'border-[#8b5cf6] text-[#8b5cf6] bg-[#8b5cf6]/10',
    system: 'border-[#22c55e] text-[#22c55e] bg-[#22c55e]/10',
  };

  return (
    <div className="glass-panel rounded-xl p-5 border border-accent-blue/20">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-bold text-text-primary flex items-center gap-2">
            <Activity className="w-4 h-4 text-accent-blue" />
            Pipeline Health
          </h3>
          <p className="text-xs text-text-muted mt-0.5">Real-time metrics from Kafka & PostgreSQL</p>
        </div>

        {/* Tab Buttons */}
        <div className="flex gap-2">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(activeTab === tab.id ? null : tab.id)}
              className={`px-2 py-1 text-[10px] font-medium rounded border transition-colors flex items-center gap-1 ${
                activeTab === tab.id
                  ? tabActiveColors[tab.id]
                  : `bg-bg-surface border-border-light ${tab.hoverColor} text-text-secondary`
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* EPS */}
        <div className="bg-bg-surface/50 rounded-lg p-3 border border-border-light/50 flex flex-col justify-between">
          <div className="flex items-center gap-2 mb-2">
            <Zap className="w-4 h-4 text-accent-blue" />
            <span className="text-xs font-semibold text-text-secondary">Events/Sec</span>
          </div>
          <div className="flex items-end justify-between">
            <div>
              <span className="text-2xl font-bold text-text-primary">{eps}</span>
              {epsChange !== 0 && (
                <span className={`text-[10px] ml-2 ${epsChange > 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  {epsChange > 0 ? '+' : ''}{epsChange}%
                </span>
              )}
            </div>
            <div className="h-8 w-16">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={trendEps.slice(-20)}>
                  <defs>
                    <linearGradient id="colorEps" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <Area type="monotone" dataKey="value" stroke="#3b82f6" fillOpacity={1} fill="url(#colorEps)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Latency */}
        <div className="bg-bg-surface/50 rounded-lg p-3 border border-border-light/50 flex flex-col justify-between">
          <div className="flex items-center gap-2 mb-2">
            <Activity className="w-4 h-4 text-accent-green" />
            <span className="text-xs font-semibold text-text-secondary">Processing Latency</span>
          </div>
          <div className="flex items-end justify-between">
            <div>
              <span className="text-2xl font-bold text-text-primary">{latency}ms</span>
            </div>
            <div className="h-8 w-16">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={trendLatency.slice(-20)}>
                  <defs>
                    <linearGradient id="colorLat" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <Area type="monotone" dataKey="value" stroke="#10b981" fillOpacity={1} fill="url(#colorLat)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* Kafka Lag */}
        <div className="bg-bg-surface/50 rounded-lg p-3 border border-border-light/50 flex items-center justify-between">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Server className="w-4 h-4 text-accent-purple" />
              <span className="text-xs font-semibold text-text-secondary">Kafka Lag</span>
            </div>
            <span className="text-2xl font-bold text-text-primary">{kafkaLag}</span>
            <p className={`text-[10px] mt-1 ${lagStatus === 'Optimal' ? 'text-text-muted' : 'text-accent-red'}`}>{lagStatus}</p>
          </div>
          <div className={`w-10 h-10 rounded-full flex flex-col items-center justify-center border ${
            lagStatus === 'Optimal'
              ? 'bg-accent-purple/10 border-accent-purple/30'
              : 'bg-accent-red/10 border-accent-red/30'
          }`}>
            <div className={`w-2 h-2 rounded-full animate-pulse ${lagStatus === 'Optimal' ? 'bg-accent-purple' : 'bg-accent-red'}`}></div>
          </div>
        </div>

        {/* DB Write Rate */}
        <div className="bg-bg-surface/50 rounded-lg p-3 border border-border-light/50 flex items-center justify-between">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Database className="w-4 h-4 text-accent-cyan" />
              <span className="text-xs font-semibold text-text-secondary">DB Write Rate</span>
            </div>
            <span className="text-2xl font-bold text-text-primary">{dbWriteRate}/s</span>
            <p className="text-[10px] text-text-muted mt-1">PostgreSQL Sync</p>
          </div>
          <div className="w-10 h-10 rounded-full bg-accent-cyan/10 flex flex-col items-center justify-center border border-accent-cyan/30">
            <Database className="w-4 h-4 text-accent-cyan" />
          </div>
        </div>
      </div>

      {/* Error Banner */}
      {pipelineHealthError && (
        <div className="mt-3 px-3 py-2 rounded-lg bg-accent-red/10 border border-accent-red/30 text-xs text-accent-red">
          {pipelineHealthError}
        </div>
      )}

      {/* Expandable Tab Panels */}
      {activeTab && (
        <div className="mt-4 glass-panel rounded-lg p-4 border border-border-light/50 animate-fade-in">
          {activeTab === 'overview' && (
            <div>
              <h4 className="text-xs font-semibold text-text-primary uppercase tracking-wider mb-3 flex items-center gap-1.5">
                <TrendingUp className="w-3.5 h-3.5 text-[#3b82f6]" />
                Pipeline Overview — Event Flow Timeline
              </h4>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={severityTrend} margin={{ top: 5, right: 5, left: -20, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1a2230" strokeOpacity={0.25} vertical={false} />
                  <XAxis dataKey="time" tick={{ fill: '#556171', fontSize: 10 }} axisLine={{ stroke: '#1a2230' }} tickLine={false} />
                  <YAxis tick={{ fill: '#556171', fontSize: 10 }} axisLine={false} tickLine={false} />
                  <Tooltip contentStyle={{ backgroundColor: '#05080f', border: '1px solid #1a2230', borderRadius: 8, fontSize: 11, color: '#e6edf3' }} />
                  <Line type="monotone" dataKey="Critical" stroke="#ff3b30" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="Error" stroke="#ff7a18" strokeWidth={1.5} dot={false} />
                  <Line type="monotone" dataKey="Warning" stroke="#ffd60a" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
                  <Line type="monotone" dataKey="Info" stroke="#22c55e" strokeWidth={1} dot={false} strokeDasharray="4 2" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {activeTab === 'kafka' && (
            <div>
              <h4 className="text-xs font-semibold text-text-primary uppercase tracking-wider mb-3 flex items-center gap-1.5">
                <BarChart3 className="w-3.5 h-3.5 text-[#8b5cf6]" />
                Kafka — Events Per System
              </h4>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={topSystemsByEventVolume} margin={{ top: 5, right: 5, left: -10, bottom: 0 }}>
                  <defs>
                    <linearGradient id="kafkaGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#8b5cf6" />
                      <stop offset="100%" stopColor="#3b82f6" />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1a2230" strokeOpacity={0.25} horizontal={false} />
                  <XAxis dataKey="name" tick={{ fill: '#556171', fontSize: 9 }} axisLine={{ stroke: '#1a2230' }} tickLine={false} />
                  <YAxis tick={{ fill: '#556171', fontSize: 10 }} axisLine={false} tickLine={false} />
                  <Tooltip contentStyle={{ backgroundColor: '#05080f', border: '1px solid #1a2230', borderRadius: 8, fontSize: 11, color: '#e6edf3' }} />
                  <Bar dataKey="events" fill="url(#kafkaGrad)" radius={[4, 4, 0, 0]} barSize={28} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {activeTab === 'system' && (
            <div>
              <h4 className="text-xs font-semibold text-text-primary uppercase tracking-wider mb-3 flex items-center gap-1.5">
                <Cpu className="w-3.5 h-3.5 text-[#22c55e]" />
                System Health — Resource Utilization
              </h4>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={systemHealthData} margin={{ top: 5, right: 5, left: -10, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1a2230" strokeOpacity={0.25} horizontal={false} />
                  <XAxis dataKey="name" tick={{ fill: '#556171', fontSize: 9 }} axisLine={{ stroke: '#1a2230' }} tickLine={false} />
                  <YAxis tick={{ fill: '#556171', fontSize: 10 }} axisLine={false} tickLine={false} domain={[0, 100]} />
                  <Tooltip contentStyle={{ backgroundColor: '#05080f', border: '1px solid #1a2230', borderRadius: 8, fontSize: 11, color: '#e6edf3' }} />
                  <Bar dataKey="cpu" name="CPU %" fill="#00e5ff" radius={[4, 4, 0, 0]} barSize={14} />
                  <Bar dataKey="memory" name="Memory %" fill="#8b5cf6" radius={[4, 4, 0, 0]} barSize={14} />
                  <Bar dataKey="disk" name="Disk Free %" fill="#22c55e" radius={[4, 4, 0, 0]} barSize={14} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
