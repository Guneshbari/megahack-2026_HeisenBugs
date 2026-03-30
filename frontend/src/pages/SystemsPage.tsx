/**
 * SystemsPage — dense SOC system grid
 *
 * Shows hostname, IP, status, CPU bar, MEM bar, disk bar, last seen, events.
 * No large cards — flat bordered rows at 32px height.
 * Hover highlights system; click shows detail modal inline.
 */
import { useState, useMemo } from 'react';
import { useDashboardStore } from '../store/dashboardStore';
import { useUIStore } from '../store/uiStore';
import type { SystemStatus } from '../types/telemetry';

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

function timeAgo(ts: string): string {
  const diff = Date.now() - new Date(ts).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60)  return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60)  return `${m}m`;
  return `${Math.floor(m / 60)}h`;
}

type SortKey = 'hostname' | 'status' | 'cpu' | 'mem' | 'events';

export default function SystemsPage() {
  const systems = useDashboardStore((s) => s.systems);
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

  const statusOrder: Record<SystemStatus, number> = { offline: 0, degraded: 1, online: 2 };

  const displayed = useMemo(() => {
    const base = statusFilter === 'all'
      ? systems
      : systems.filter((s) => s.status === statusFilter);

    return [...base].sort((a, b) => {
      let cmp = 0;
      if (sortKey === 'hostname') cmp = a.hostname.localeCompare(b.hostname);
      if (sortKey === 'status')   cmp = statusOrder[a.status] - statusOrder[b.status];
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

  const onlineCount   = systems.filter((s) => s.status === 'online').length;
  const degradedCount = systems.filter((s) => s.status === 'degraded').length;
  const offlineCount  = systems.filter((s) => s.status === 'offline').length;

  const ColHeader = ({ label, id }: { label: string; id: SortKey }) => (
    <div
      onClick={() => toggleSort(id)}
      className="font-mono text-[9px] text-[#334155] uppercase tracking-wider cursor-pointer select-none"
    >
      {label}{sortKey === id ? (sortDesc ? ' ↓' : ' ↑') : ''}
    </div>
  );

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
            className="flex items-center px-4 gap-2 flex-shrink-0"
            style={{ height: 24, background: '#111927', borderBottom: '1px solid #1F2A37' }}
          >
            <div className="w-6" style={{ position: 'sticky', left: 0 }} />
            <div className="flex-1" style={{ position: 'sticky', left: 32 }}><ColHeader label="Hostname" id="hostname" /></div>
            <div className="w-[100px]"><ColHeader label="Status" id="status" /></div>
            <div className="w-[80px]"><ColHeader label="CPU" id="cpu" /></div>
            <div className="w-[80px]"><ColHeader label="MEM" id="mem" /></div>
            <div className="w-[60px]">
              <span className="font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">DISK</span>
            </div>
            <div className="w-[80px]">
              <span className="font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">IP</span>
            </div>
            <div className="w-[52px]">
              <span className="font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">Seen</span>
            </div>
            <div className="w-[52px]"><ColHeader label="Events" id="events" /></div>
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
                  className={`flex items-center px-4 gap-2 border-b border-[#1F2A37] cursor-pointer transition-colors even:bg-[#0F1720] ${isHighlighted && !isSelected ? 'bg-[#162131]' : ''}`}
                  style={{
                    height: 30,
                    background: isSelected ? '#1E293B' : undefined,
                    borderLeft: `3px solid ${STATUS_COLOR[sys.status]}`,
                  }}
                >
                  {/* Status dot */}
                  <span className={`${STATUS_DOT[sys.status]}`} style={{ position: 'sticky', left: 0 }} />

                  {/* Hostname */}
                  <span className="font-mono text-[11px] text-[#E6EDF3] flex-1 truncate font-medium" style={{ position: 'sticky', left: 32 }}>
                    {sys.hostname.split('.')[0]}
                    <span className="text-[#6B7C93] text-[10px] ml-1">
                      .{sys.hostname.split('.').slice(1).join('.')}
                    </span>
                  </span>

                  {/* Status text */}
                  <span
                    className="font-mono text-[10px] w-[100px] flex-shrink-0"
                    style={{ color: STATUS_COLOR[sys.status] }}
                  >
                    {sys.status.toUpperCase()}
                  </span>

                  {/* CPU */}
                  <div className="w-[80px] flex items-center gap-2 flex-shrink-0">
                    <MiniBar value={sys.cpu_usage_percent} color={CPU_COLOR} />
                    <span
                      className="font-mono text-[10px] text-[#E6EDF3]"
                      style={{ minWidth: 28, textAlign: 'right' }}
                    >
                      {sys.cpu_usage_percent.toFixed(0)}%
                    </span>
                  </div>

                  {/* MEM */}
                  <div className="w-[80px] flex items-center gap-2 flex-shrink-0">
                    <MiniBar value={sys.memory_usage_percent} color={MEM_COLOR} />
                    <span
                      className="font-mono text-[10px] text-[#E6EDF3]"
                      style={{ minWidth: 28, textAlign: 'right' }}
                    >
                      {sys.memory_usage_percent.toFixed(0)}%
                    </span>
                  </div>

                  {/* Disk (free%) */}
                  <div className="w-[60px] flex items-center gap-1 flex-shrink-0 text-[#E6EDF3]">
                    <span className="font-mono text-[10px]">
                      {sys.disk_free_percent.toFixed(0)}%
                    </span>
                  </div>

                  {/* IP */}
                  <span className="font-mono text-[10px] text-[#6B7C93] w-[80px] flex-shrink-0">
                    {sys.ip_address}
                  </span>

                  {/* Last seen */}
                  <span className="font-mono text-[10px] text-[#6B7C93] w-[52px] text-right flex-shrink-0">
                    {timeAgo(sys.last_seen)}
                  </span>

                  {/* Events */}
                  <span className="font-mono text-[10px] text-[#9FB3C8] w-[52px] text-right flex-shrink-0">
                    {sys.total_events.toLocaleString()}
                  </span>
                </div>
              );
            })}
          </div>
        </div>

        {/* Detail pane */}
        {selected && (
          <div
            className="w-[320px] flex-shrink-0 border border-[#1F2A37] flex flex-col bg-[#111927]"
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
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">HOST: </span>{selected.hostname.split('.')[0]}</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">IP: </span>{selected.ip_address}</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">OS: </span>{selected.os_version}</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">SEEN: </span>{timeAgo(selected.last_seen)}</div>
                </div>
              </div>

              <div>
                <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">Metrics</div>
                <div className="grid grid-cols-2 gap-x-8 gap-y-2">
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">CPU: </span>{selected.cpu_usage_percent.toFixed(1)}%</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">MEM: </span>{selected.memory_usage_percent.toFixed(1)}%</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">DISK FREE: </span>{selected.disk_free_percent.toFixed(1)}%</div>
                  <div className="font-mono text-[#E6EDF3]"><span className="text-[#6B7C93]">EVENTS: </span>{selected.total_events.toLocaleString()}</div>
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
