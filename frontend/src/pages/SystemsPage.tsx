/**
 * SystemsPage — dense SOC system grid
 *
 * Shows hostname, IP, status, CPU bar, MEM bar, disk bar, last seen, events.
 * No large cards — flat bordered rows at 32px height.
 * Hover highlights system; click shows detail modal inline.
 */
import { useState, useMemo } from 'react';
import type { EChartsOption } from 'echarts';
import { EChart } from '../components/soc/EChart';
import { useDashboardStore } from '../store/dashboardStore';
import { useSignalStore } from '../store/signalStore';
import { useUIStore } from '../store/uiStore';
import type { FeatureSnapshot, MetricPoint, SystemStatus } from '../types/telemetry';

const STATUS_DOT: Record<SystemStatus, string> = {
  online:   'soc-dot soc-dot-online',
  degraded: 'soc-dot soc-dot-degraded',
  offline:  'soc-dot soc-dot-offline',
};

const STATUS_COLOR: Record<SystemStatus, string> = {
  online:   '#00C853',
  degraded: '#FFD600',
  offline:  '#6B7C93',
};
const STATUS_ORDER: Record<SystemStatus, number> = { offline: 0, degraded: 1, online: 2 };

function MiniBar({ value, color }: { value: number; color: string }) {
  return (
    <div className="soc-bar-track" style={{ height: 6, background: '#1F2A37' }}>
      <div
        className="soc-bar-fill"
        style={{ width: `${Math.min(value, 100)}%`, background: color }}
      />
    </div>
  );
}

// User specified colors for resource bars:
// CPU: #3BA4FF, MEM: #FF8A00, DISK: #00C853
const CPU_COLOR = '#3BA4FF';
const MEM_COLOR = '#FF8A00';
const DISK_COLOR = '#00C853';

type TrendField = 'cpu_usage_percent' | 'memory_usage_percent' | 'disk_free_percent';
type SystemTrendPoint = Pick<
  FeatureSnapshot,
  'system_id' | 'snapshot_time' | 'cpu_usage_percent' | 'memory_usage_percent' | 'disk_free_percent' | 'total_events' | 'dominant_fault_type'
>;

const TREND_META: Record<TrendField, { label: string; color: string }> = {
  cpu_usage_percent: { label: 'CPU', color: CPU_COLOR },
  memory_usage_percent: { label: 'MEM', color: MEM_COLOR },
  disk_free_percent: { label: 'DISK FREE', color: DISK_COLOR },
};

function timeAgo(ts: string): string {
  const diff = Date.now() - new Date(ts).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60)  return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60)  return `${m}m`;
  return `${Math.floor(m / 60)}h`;
}

type SortKey = 'hostname' | 'status' | 'cpu' | 'mem' | 'events';

function ColumnHeader({
  align = 'left',
  id,
  isActive,
  isDescending,
  label,
  onToggle,
}: {
  align?: 'left' | 'right';
  id: SortKey;
  isActive: boolean;
  isDescending: boolean;
  label: string;
  onToggle: (key: SortKey) => void;
}) {
  return (
    <div
      onClick={() => onToggle(id)}
      className={`font-mono text-[9px] text-[#334155] uppercase tracking-wider cursor-pointer select-none flex items-center gap-1 ${align === 'right' ? 'justify-end' : ''}`}
    >
      {label}
      {isActive && <span>{isDescending ? 'v' : '^'}</span>}
    </div>
  );
}

function shortTime(ts: string): string {
  return new Date(ts).toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function systemLabel(hostname: string, systemId: string): string {
  return `${hostname} (${systemId.slice(0, 6)})`;
}

function metricToTrendPoint(metric: MetricPoint): SystemTrendPoint | null {
  if (!metric.system_id) return null;
  return {
    system_id: metric.system_id,
    snapshot_time: metric.timestamp,
    cpu_usage_percent: metric.cpu_usage_percent ?? metric.avg_cpu ?? 0,
    memory_usage_percent: metric.memory_usage_percent ?? metric.avg_memory ?? 0,
    disk_free_percent: metric.disk_free_percent ?? metric.avg_disk_free ?? 0,
    total_events: metric.event_count ?? 0,
    dominant_fault_type: 'metrics',
  };
}

function groupTrendsBySystem(
  snapshots: FeatureSnapshot[],
  metrics: MetricPoint[],
): Record<string, SystemTrendPoint[]> {
  const grouped: Record<string, Map<string, SystemTrendPoint>> = {};
  const add = (point: SystemTrendPoint) => {
    grouped[point.system_id] = grouped[point.system_id] ?? new Map<string, SystemTrendPoint>();
    grouped[point.system_id].set(point.snapshot_time, point);
  };

  metrics.map(metricToTrendPoint).forEach((point) => {
    if (point) add(point);
  });
  snapshots.forEach(add);

  return Object.fromEntries(
    Object.entries(grouped).map(([systemId, pointMap]) => [
      systemId,
      [...pointMap.values()]
        .sort((a, b) => new Date(a.snapshot_time).getTime() - new Date(b.snapshot_time).getTime())
        .slice(-100),
    ]),
  );
}

function buildTrendOption(
  points: SystemTrendPoint[],
  field: TrendField,
  compact = false,
): EChartsOption {
  const meta = TREND_META[field];
  const data = points.slice(compact ? -50 : -100);
  return {
    backgroundColor: 'transparent',
    animation: false,
    grid: compact
      ? { top: 4, right: 2, bottom: 4, left: 2 }
      : { top: 18, right: 10, bottom: 22, left: 34 },
    tooltip: compact
      ? { show: false }
      : {
          trigger: 'axis',
          backgroundColor: '#0F1720',
          borderColor: '#1F2A37',
          textStyle: { color: '#E6EDF3', fontSize: 11, fontFamily: 'JetBrains Mono,monospace' },
          formatter: (params) => {
            const first = Array.isArray(params) ? params[0] : params;
            const index = typeof first.dataIndex === 'number' ? first.dataIndex : 0;
            const point = data[index];
            return `${meta.label}<br/>${shortTime(point.snapshot_time)} ${Number(first.value).toFixed(1)}%`;
          },
        },
    xAxis: {
      type: 'category',
      data: data.map((p) => shortTime(p.snapshot_time)),
      boundaryGap: data.length === 1,
      axisLabel: { show: !compact, color: '#475569', fontSize: 9, interval: Math.max(0, Math.floor(data.length / 5) - 1) },
      axisLine: { show: !compact, lineStyle: { color: '#334155' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      min: 0,
      max: 100,
      axisLabel: { show: !compact, color: '#475569', fontSize: 9 },
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: { show: !compact, lineStyle: { color: '#151f2e', type: 'dashed' } },
    },
    series: [{
      name: meta.label,
      type: 'line',
      data: data.map((p) => Number(p[field].toFixed(2))),
      lineStyle: { color: meta.color, width: compact ? 1 : 1.5 },
      itemStyle: { color: meta.color },
      symbol: data.length <= 2 && !compact ? 'circle' : 'none',
      smooth: true,
    }],
  };
}

function trendStats(points: SystemTrendPoint[]) {
  const avg = (field: TrendField) => {
    if (points.length === 0) return 0;
    return points.reduce((sum, p) => sum + p[field], 0) / points.length;
  };
  return {
    cpuAvg: avg('cpu_usage_percent'),
    cpuPeak: Math.max(...points.map((p) => p.cpu_usage_percent), 0),
    memAvg: avg('memory_usage_percent'),
    diskMin: Math.min(...points.map((p) => p.disk_free_percent), 100),
  };
}

function Sparkline({ points, field }: { points: SystemTrendPoint[]; field: TrendField }) {
  const option = useMemo(() => buildTrendOption(points, field, true), [points, field]);
  return (
    <div className="h-[34px] min-w-0">
      {points.length > 0 ? <EChart option={option} /> : <div className="h-full bg-[#0A0F14]" />}
    </div>
  );
}

function TrendChart({ points, field }: { points: SystemTrendPoint[]; field: TrendField }) {
  const option = useMemo(() => buildTrendOption(points, field), [points, field]);
  const meta = TREND_META[field];
  return (
    <div className="border border-[#1F2A37] bg-[#0A0F14]" style={{ height: 118 }}>
      <div className="h-6 px-2 flex items-center justify-between border-b border-[#1F2A37]">
        <span className="font-mono text-[10px] font-semibold" style={{ color: meta.color }}>{meta.label}</span>
        <span className="font-mono text-[9px] text-[#6B7C93]">{points.length} pts</span>
      </div>
      <div style={{ height: 'calc(100% - 24px)' }}>
        {points.length > 0 ? <EChart option={option} /> : (
          <div className="h-full flex items-center justify-center font-mono text-[10px] text-[#6B7C93]">No trend data</div>
        )}
      </div>
    </div>
  );
}

export default function SystemsPage() {
  const systems = useDashboardStore((s) => s.systems);
  const metrics = useDashboardStore((s) => s.metrics);
  const featureSnapshots = useSignalStore((s) => s.featureSnapshots);
  const setHighlighted = useUIStore((s) => s.setHighlightedSystem);
  const highlighted    = useUIStore((s) => s.highlightedSystem);

  const [sortKey, setSortKey]   = useState<SortKey>('status');
  const [sortDesc, setSortDesc] = useState(true);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<SystemStatus | 'all'>('all');

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDesc((d) => !d);
    else { setSortKey(key); setSortDesc(true); }
  };

  const displayed = useMemo(() => {
    const base = statusFilter === 'all'
      ? systems
      : systems.filter((s) => s.status === statusFilter);

    return [...base].sort((a, b) => {
      let cmp = 0;
      if (sortKey === 'hostname') cmp = a.hostname.localeCompare(b.hostname);
      if (sortKey === 'status')   cmp = STATUS_ORDER[a.status] - STATUS_ORDER[b.status];
      if (sortKey === 'cpu')      cmp = a.cpu_usage_percent - b.cpu_usage_percent;
      if (sortKey === 'mem')      cmp = a.memory_usage_percent - b.memory_usage_percent;
      if (sortKey === 'events')   cmp = a.total_events - b.total_events;
      return sortDesc ? -cmp : cmp;
    });
  }, [systems, sortKey, sortDesc, statusFilter]);

  const selected = useMemo(
    () => displayed.find((s) => s.system_id === selectedId) ?? null,
    [displayed, selectedId],
  );
  const trendsBySystem = useMemo(
    () => groupTrendsBySystem(featureSnapshots, metrics),
    [featureSnapshots, metrics],
  );
  const selectedTrend = selected ? trendsBySystem[selected.system_id] ?? [] : [];
  const selectedStats = useMemo(() => trendStats(selectedTrend), [selectedTrend]);

  const onlineCount   = systems.filter((s) => s.status === 'online').length;
  const degradedCount = systems.filter((s) => s.status === 'degraded').length;
  const offlineCount  = systems.filter((s) => s.status === 'offline').length;

  return (
    <div className="flex flex-col h-full gap-1" style={{ minHeight: 0 }}>
      {/* Toolbar */}
      <div
        className="flex items-center justify-between px-3 border border-[#1F2A37] flex-shrink-0 bg-[#0F1720]"
        style={{ height: 32 }}
      >
        <div className="flex items-center gap-3">
          <span className="font-mono text-[10px] text-[#E6EDF3] font-semibold uppercase tracking-wider">
            Systems
          </span>
          {(['all', 'online', 'degraded', 'offline'] as const).map((f) => (
            <button
              key={f}
              onClick={() => setStatusFilter(f)}
              className={`soc-btn ${statusFilter === f ? 'soc-btn-active' : ''}`}
            >
              {f === 'online'   ? `Online ${onlineCount}`   :
               f === 'degraded' ? `Degraded ${degradedCount}` :
               f === 'offline'  ? `Offline ${offlineCount}`  :
               `All ${systems.length}`}
            </button>
          ))}
        </div>
        <span className="font-mono text-[9px] text-[#6B7C93]">hover → highlight | click → detail</span>
      </div>

      <div className="flex flex-1 min-h-0 gap-1">
        {/* System table */}
        <div className="flex-1 flex flex-col min-h-0 border border-[#1F2A37]">
          {/* Column headers */}
          <div
            className="flex items-center px-4 gap-4 flex-shrink-0"
            style={{ height: 28, background: '#111927', borderBottom: '1px solid #1F2A37' }}
          >
            <div className="w-4 flex-shrink-0" style={{ position: 'sticky', left: 0 }} />
            <div className="flex-1 min-w-[120px]" style={{ position: 'sticky', left: 24 }}><ColumnHeader label="Hostname" id="hostname" isActive={sortKey === 'hostname'} isDescending={sortDesc} onToggle={toggleSort} /></div>
            <div className="w-[80px] flex-shrink-0"><ColumnHeader label="Status" id="status" isActive={sortKey === 'status'} isDescending={sortDesc} onToggle={toggleSort} /></div>
            <div className="w-[100px] flex-shrink-0"><ColumnHeader label="CPU" id="cpu" isActive={sortKey === 'cpu'} isDescending={sortDesc} onToggle={toggleSort} /></div>
            <div className="w-[100px] flex-shrink-0"><ColumnHeader label="MEM" id="mem" isActive={sortKey === 'mem'} isDescending={sortDesc} onToggle={toggleSort} /></div>
            <div className="w-[60px] flex-shrink-0 flex justify-end">
              <span className="font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">DISK</span>
            </div>
            <div className="w-[100px] flex-shrink-0 flex justify-end">
              <span className="font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">IP</span>
            </div>
            <div className="w-[60px] flex-shrink-0 flex justify-end">
              <span className="font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">Seen</span>
            </div>
            <div className="w-[60px] flex-shrink-0 flex justify-end"><ColumnHeader label="Events" id="events" align="right" isActive={sortKey === 'events'} isDescending={sortDesc} onToggle={toggleSort} /></div>
          </div>

          {/* Rows */}
          <div className="flex-1 overflow-y-auto">
            {displayed.map((sys) => {
              const isSelected    = selectedId === sys.system_id;
              const isHighlighted = highlighted === sys.system_id;

              return (
                <div
                  key={sys.system_id}
                  onClick={() => setSelectedId(isSelected ? null : sys.system_id)}
                  onMouseEnter={() => setHighlighted(sys.system_id)}
                  onMouseLeave={() => setHighlighted(null)}
                  className={`flex items-center px-4 gap-4 border-b border-[#1F2A37] cursor-pointer transition-colors even:bg-[#0F1720] ${isHighlighted && !isSelected ? 'bg-[#162131]' : ''}`}
                  style={{
                    height: 36,
                    background: isSelected ? '#1E293B' : undefined,
                    borderLeft: `3px solid ${STATUS_COLOR[sys.status]}`,
                  }}
                >
                  {/* Status dot */}
                  <div className="w-4 flex-shrink-0 flex items-center justify-center" style={{ position: 'sticky', left: 0 }}>
                    <span className={`${STATUS_DOT[sys.status]}`} />
                  </div>

                  {/* Hostname */}
                  <div className="flex-1 min-w-[120px] truncate" style={{ position: 'sticky', left: 24 }}>
                    <span className="font-mono text-[11px] text-[#E6EDF3] font-medium">
                      {sys.hostname.split('.')[0]} <span style={{ color: '#6B7C93', fontWeight: 400 }}>({sys.system_id.slice(0, 6)})</span>
                    </span>
                    <span className="text-[#6B7C93] text-[10px] ml-1">
                      .{sys.hostname.split('.').slice(1).join('.')}
                    </span>
                  </div>

                  {/* Status text */}
                  <div
                    className="font-mono text-[10px] w-[80px] flex-shrink-0"
                    style={{ color: STATUS_COLOR[sys.status] }}
                  >
                    {sys.status.toUpperCase()}
                  </div>

                  {/* CPU */}
                  <div className="w-[100px] flex items-center justify-between gap-2 flex-shrink-0">
                    <div className="flex-1">
                      <MiniBar value={sys.cpu_usage_percent} color={CPU_COLOR} />
                    </div>
                    <span
                      className="font-mono text-[10px] text-[#E6EDF3] w-[32px] text-right inline-block"
                    >
                      {sys.cpu_usage_percent.toFixed(0)}%
                    </span>
                  </div>

                  {/* MEM */}
                  <div className="w-[100px] flex items-center justify-between gap-2 flex-shrink-0">
                    <div className="flex-1">
                      <MiniBar value={sys.memory_usage_percent} color={MEM_COLOR} />
                    </div>
                    <span
                      className="font-mono text-[10px] text-[#E6EDF3] w-[32px] text-right inline-block"
                    >
                      {sys.memory_usage_percent.toFixed(0)}%
                    </span>
                  </div>

                  {/* Disk (free%) */}
                  <div className="w-[60px] flex items-center justify-end flex-shrink-0">
                    <span className="font-mono text-[10px] text-[#E6EDF3]">
                      {sys.disk_free_percent.toFixed(0)}%
                    </span>
                  </div>

                  {/* IP */}
                  <div className="w-[100px] flex items-center justify-end flex-shrink-0">
                    <span className="font-mono text-[10px] text-[#6B7C93]">
                      {sys.ip_address}
                    </span>
                  </div>

                  {/* Last seen */}
                  <div className="w-[60px] flex items-center justify-end flex-shrink-0">
                    <span className="font-mono text-[10px] text-[#6B7C93]">
                      {timeAgo(sys.last_seen)}
                    </span>
                  </div>

                  {/* Events */}
                  <div className="w-[60px] flex items-center justify-end flex-shrink-0">
                    <span className="font-mono text-[10px] text-[#9FB3C8]">
                      {sys.total_events.toLocaleString()}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Detail pane */}
        {selected && (
          <div
            className="w-[440px] flex-shrink-0 border border-[#1F2A37] flex flex-col bg-[#111927]"
          >
            <div className="soc-panel-header">
              <span className="font-mono text-[11px] text-[#E6EDF3] font-semibold">System Detail</span>
              <button onClick={() => setSelectedId(null)} className="soc-btn px-1.5 py-0 h-5">✕</button>
            </div>
            <div className="flex-1 overflow-y-auto p-4 flex flex-col gap-4">
              
              <div>
                <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">Info</div>
                <div className="grid grid-cols-2 gap-x-8 gap-y-2">
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">ID: </span>{selected.system_id}</div>
                  <div className="font-mono" style={{ color: STATUS_COLOR[selected.status] }}><span className="text-[#6B7C93]">STATUS: </span>{selected.status.toUpperCase()}</div>
                </div>
              </div>

              <div>
                <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">System</div>
                <div className="grid grid-cols-2 gap-x-8 gap-y-2">
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">HOST: </span>{systemLabel(selected.hostname, selected.system_id)}</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">IP: </span>{selected.ip_address}</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">OS: </span>{selected.os_version}</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">SEEN: </span>{timeAgo(selected.last_seen)}</div>
                </div>
              </div>

              <div>
                <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">Metrics</div>
                <div className="grid grid-cols-[72px_1fr_48px] gap-x-2 gap-y-2 items-center">
                  <div className="font-mono text-[#6B7C93]">CPU</div>
                  <Sparkline points={selectedTrend} field="cpu_usage_percent" />
                  <div className="font-mono text-[#E6EDF3] text-right">{selected.cpu_usage_percent.toFixed(1)}%</div>
                  <div className="font-mono text-[#6B7C93]">MEM</div>
                  <Sparkline points={selectedTrend} field="memory_usage_percent" />
                  <div className="font-mono text-[#E6EDF3] text-right">{selected.memory_usage_percent.toFixed(1)}%</div>
                  <div className="font-mono text-[#6B7C93]">DISK</div>
                  <Sparkline points={selectedTrend} field="disk_free_percent" />
                  <div className="font-mono text-[#E6EDF3] text-right">{selected.disk_free_percent.toFixed(1)}%</div>
                </div>
              </div>

              <div>
                <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">Trend Analytics</div>
                <div className="grid grid-cols-4 gap-2">
                  <div className="border border-[#1F2A37] bg-[#0A0F14] p-2">
                    <div className="font-mono text-[9px] text-[#6B7C93]">AVG CPU</div>
                    <div className="font-mono text-[13px] text-[#E6EDF3]">{selectedStats.cpuAvg.toFixed(1)}%</div>
                  </div>
                  <div className="border border-[#1F2A37] bg-[#0A0F14] p-2">
                    <div className="font-mono text-[9px] text-[#6B7C93]">PEAK CPU</div>
                    <div className="font-mono text-[13px] text-[#E6EDF3]">{selectedStats.cpuPeak.toFixed(1)}%</div>
                  </div>
                  <div className="border border-[#1F2A37] bg-[#0A0F14] p-2">
                    <div className="font-mono text-[9px] text-[#6B7C93]">AVG MEM</div>
                    <div className="font-mono text-[13px] text-[#E6EDF3]">{selectedStats.memAvg.toFixed(1)}%</div>
                  </div>
                  <div className="border border-[#1F2A37] bg-[#0A0F14] p-2">
                    <div className="font-mono text-[9px] text-[#6B7C93]">MIN DISK</div>
                    <div className="font-mono text-[13px] text-[#E6EDF3]">{selectedStats.diskMin.toFixed(1)}%</div>
                  </div>
                </div>
              </div>

              <div>
                <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">Charts</div>
                <div className="flex flex-col gap-2">
                  <TrendChart points={selectedTrend} field="cpu_usage_percent" />
                  <TrendChart points={selectedTrend} field="memory_usage_percent" />
                  <TrendChart points={selectedTrend} field="disk_free_percent" />
                </div>
              </div>

              <div>
                <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">Last 10 Snapshots</div>
                <div className="border border-[#1F2A37] bg-[#0A0F14]">
                  {selectedTrend.slice(-10).reverse().map((point) => (
                    <div key={`${point.system_id}-${point.snapshot_time}`} className="grid grid-cols-[52px_44px_44px_54px_1fr] gap-2 px-2 py-1 border-b border-[#1F2A37] last:border-b-0">
                      <span className="font-mono text-[10px] text-[#6B7C93]">{shortTime(point.snapshot_time)}</span>
                      <span className="font-mono text-[10px] text-[#E6EDF3]">{point.cpu_usage_percent.toFixed(0)}% CPU</span>
                      <span className="font-mono text-[10px] text-[#E6EDF3]">{point.memory_usage_percent.toFixed(0)}% MEM</span>
                      <span className="font-mono text-[10px] text-[#E6EDF3]">{point.disk_free_percent.toFixed(0)}% FREE</span>
                      <span className="font-mono text-[10px] text-[#6B7C93] truncate">{point.dominant_fault_type}</span>
                    </div>
                  ))}
                  {selectedTrend.length === 0 && (
                    <div className="px-2 py-3 font-mono text-[10px] text-[#6B7C93]">No snapshots in current window</div>
                  )}
                </div>
              </div>

              <div>
                <details className="cursor-pointer group">
                  <summary className="text-[11px] text-[#6B7C93] uppercase tracking-wider font-mono font-semibold select-none group-hover:text-[#E6EDF3] transition-colors">
                    Raw Data <span className="text-[9px] opacity-70">▼</span>
                  </summary>
                  <pre className="mt-2 p-2 bg-[#0A0F14] border border-[#1F2A37] text-[10px] text-[#9FB3C8] font-mono overflow-x-auto whitespace-pre-wrap">
                    {JSON.stringify(selected, null, 2)}
                  </pre>
                </details>
              </div>

            </div>
          </div>
        )}
      </div>

      {/* Footer */}
      <div
        className="flex items-center px-3 border border-[#1F2A37] flex-shrink-0 bg-[#0F1720]"
        style={{ height: 24 }}
      >
        <span className="font-mono text-[9px] text-[#6B7C93]">
          {displayed.length} system{displayed.length !== 1 ? 's' : ''} · {onlineCount} online · {degradedCount} degraded · {offlineCount} offline
        </span>
      </div>
    </div>
  );
}

