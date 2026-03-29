import { useState, useMemo, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowUpDown, Terminal, RefreshCw, Plus, X } from 'lucide-react';
import { timeAgo } from '../data/mockData';
import { useDashboard } from '../context/DashboardContext';
import { registerSystem, executeSystemCommand } from '../lib/api';
import type { SystemStatus, SystemInfo } from '../types/telemetry';

type SortKey = 'hostname' | 'status' | 'cpu' | 'memory' | 'disk' | 'alerts' | 'last_event' | 'last_seen';
type SortDir = 'asc' | 'desc';

const statusConfig: Record<SystemStatus, { color: string; bg: string; label: string }> = {
  online: { color: 'text-signal-highlight', bg: 'bg-signal-highlight', label: 'Online' },
  degraded: { color: 'text-accent-amber', bg: 'bg-accent-amber', label: 'Degraded' },
  offline: { color: 'text-accent-red', bg: 'bg-accent-red', label: 'Offline' },
};

interface SortHeaderProps {
  label: string;
  sortId: SortKey;
  activeSortKey: SortKey;
  onToggleSort: (sortKey: SortKey) => void;
  align?: 'left' | 'right' | 'center';
}

function SortHeader({ label, sortId, activeSortKey, onToggleSort, align = 'left' }: SortHeaderProps) {
  return (
    <th
      onClick={() => onToggleSort(sortId)}
      className={`text-${align} text-[10px] font-semibold text-text-muted uppercase tracking-wider py-3 px-4 cursor-pointer hover:text-text-primary transition-colors select-none`}
    >
      <span className={`inline-flex items-center gap-1.5 ${align === 'right' ? 'flex-row-reverse' : ''}`}>
        {label}
        <ArrowUpDown className={`w-3 h-3 ${activeSortKey === sortId ? 'text-signal-primary' : 'opacity-30'}`} />
      </span>
    </th>
  );
}

export default function SystemsPage() {
  const navigate = useNavigate();
  const { filteredSystems, filteredSystemEventSummaries } = useDashboard();
  const [sortKey, setSortKey] = useState<SortKey>('status');
  const [sortDir, setSortDir] = useState<SortDir>('asc');

  const onlineCount = filteredSystems.filter((s) => s.status === 'online').length;
  const degradedCount = filteredSystems.filter((s) => s.status === 'degraded').length;

  const [showAddSystem, setShowAddSystem] = useState(false);
  const [addSystemForm, setAddSystemForm] = useState({ hostname: '', ip_address: '', agent_key: '' });
  
  const [terminalSystem, setTerminalSystem] = useState<string | null>(null);
  const [terminalCommand, setTerminalCommand] = useState('');
  const [terminalOutput, setTerminalOutput] = useState('');
  const [isCommanding, setIsCommanding] = useState(false);
  
  const [restartSystem, setRestartSystem] = useState<SystemInfo | null>(null);

  const handleAddSystem = async () => {
    const { hostname, ip_address, agent_key } = addSystemForm;
    if (!hostname || !ip_address) return;
    try {
      await registerSystem(hostname, ip_address, agent_key);
      setShowAddSystem(false);
      setAddSystemForm({ hostname: '', ip_address: '', agent_key: '' });
    } catch (e) {
      console.error(e);
    }
  };

  const terminalEndRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (terminalEndRef.current) terminalEndRef.current.scrollIntoView({ behavior: 'smooth' });
  }, [terminalOutput]);

  const handleExecuteCommand = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!terminalSystem || !terminalCommand) return;
    setIsCommanding(true);
    try {
      const res = await executeSystemCommand(terminalSystem, terminalCommand);
      setTerminalOutput(prev => prev + `\n> ${terminalCommand}\n` + (res.output || 'Command failed.'));
      setTerminalCommand('');
    } catch (e) {
      setTerminalOutput(prev => prev + `\n> ${terminalCommand}\nError executing command.`);
    } finally {
      setIsCommanding(false);
    }
  };

  const handleRestart = async () => {
    if (!restartSystem) return;
    try {
      await executeSystemCommand(restartSystem.system_id, 'systemctl restart sentinel-agent');
      setRestartSystem(null);
    } catch (e) {
      console.error(e);
    }
  };

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  };

  const sortedSystems = useMemo(() => {
    return [...filteredSystems].sort((a, b) => {
      let aVal: any = a[sortKey as keyof SystemInfo];
      let bVal: any = b[sortKey as keyof SystemInfo];

      const aSummary = filteredSystemEventSummaries[a.system_id];
      const bSummary = filteredSystemEventSummaries[b.system_id];

      if (sortKey === 'status') {
        const order = { offline: 0, degraded: 1, online: 2 };
        aVal = order[a.status];
        bVal = order[b.status];
      } else if (sortKey === 'cpu') {
        aVal = a.cpu_usage_percent;
        bVal = b.cpu_usage_percent;
      } else if (sortKey === 'memory') {
        aVal = a.memory_usage_percent;
        bVal = b.memory_usage_percent;
      } else if (sortKey === 'disk') {
        aVal = a.disk_free_percent;
        bVal = b.disk_free_percent;
      } else if (sortKey === 'alerts') {
        aVal = aSummary ? aSummary.criticalCount + aSummary.errorCount : 0;
        bVal = bSummary ? bSummary.criticalCount + bSummary.errorCount : 0;
      } else if (sortKey === 'last_event') {
        aVal = aSummary?.latestEvent ? new Date(aSummary.latestEvent.event_time).getTime() : 0;
        bVal = bSummary?.latestEvent ? new Date(bSummary.latestEvent.event_time).getTime() : 0;
      } else if (sortKey === 'last_seen') {
        aVal = new Date(a.last_seen).getTime();
        bVal = new Date(b.last_seen).getTime();
      }

      if (aVal < bVal) return sortDir === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
  }, [filteredSystems, filteredSystemEventSummaries, sortKey, sortDir]);

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-bold text-text-primary tracking-tight">Systems Monitor</h2>
          <p className="text-[11px] text-text-muted mt-0.5">Fleet health and resource utilization</p>
        </div>
        <div className="flex items-center gap-4 text-xs">
          <span className="flex items-center gap-1.5 text-text-secondary">
            <span className="w-1.5 h-1.5 rounded-full bg-signal-highlight" />
            <span className="font-semibold text-text-primary">{onlineCount}</span> Online
          </span>
          <span className="flex items-center gap-1.5 text-text-secondary">
            <span className="w-1.5 h-1.5 rounded-full bg-accent-amber" />
            <span className="font-semibold text-text-primary">{degradedCount}</span> Degraded
          </span>
          <span className="px-2 py-0.5 rounded bg-bg-surface border border-border text-text-secondary text-[11px] font-medium mr-2">
            {filteredSystems.length} Total
          </span>
          <button
            onClick={() => setShowAddSystem(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-signal-primary/20 text-signal-primary text-xs font-semibold rounded border border-signal-primary/30 hover:bg-signal-primary/30 transition-colors"
          >
            <Plus className="w-3.5 h-3.5" /> Add System
          </button>
        </div>
      </div>

      {/* Systems Table */}
      <div className="glass-panel-solid rounded-md overflow-hidden border border-border">
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse whitespace-nowrap">
            <thead>
              <tr className="bg-bg-surface border-b border-border/60">
                <SortHeader label="Hostname" sortId="hostname" activeSortKey={sortKey} onToggleSort={handleSort} />
                <SortHeader label="Status" sortId="status" activeSortKey={sortKey} onToggleSort={handleSort} />
                <SortHeader label="Last Seen" sortId="last_seen" activeSortKey={sortKey} onToggleSort={handleSort} />
                <SortHeader label="CPU" sortId="cpu" activeSortKey={sortKey} onToggleSort={handleSort} align="right" />
                <SortHeader label="Memory" sortId="memory" activeSortKey={sortKey} onToggleSort={handleSort} align="right" />
                <SortHeader label="Disk Free" sortId="disk" activeSortKey={sortKey} onToggleSort={handleSort} align="right" />
                <SortHeader label="Active Alerts" sortId="alerts" activeSortKey={sortKey} onToggleSort={handleSort} align="right" />
                <SortHeader label="Last Event" sortId="last_event" activeSortKey={sortKey} onToggleSort={handleSort} align="right" />
                <th className="px-4 py-3 bg-bg-surface text-right w-24"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/40">
              {sortedSystems.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-5 py-8 text-center text-sm text-text-muted">
                    No systems reporting in the selected criteria.
                  </td>
                </tr>
              ) : (
                sortedSystems.map((system) => {
                  const status = statusConfig[system.status];
                  const eventSummary = filteredSystemEventSummaries[system.system_id];
                  const activeAlerts = eventSummary ? eventSummary.criticalCount + eventSummary.errorCount : 0;
                  const recentEvent = eventSummary?.latestEvent;
                  const isDegraded = system.status !== 'online';
                  
                      const isStale = (() => {
                        const updatedAt = system.last_updated_at ?? system.last_seen;
                        if (!updatedAt) return false;
                        return (Date.now() - new Date(updatedAt).getTime()) > 2 * 60 * 1000;
                      })();
                      const staleTitle = isStale ? ' (reading may be stale — collector not reporting)' : '';
                      return (
                        <tr
                          key={system.system_id}
                          onClick={() => navigate(`/events?system=${system.system_id}`)}
                          className={`hover:bg-bg-hover cursor-pointer transition-colors ${isDegraded ? 'bg-accent-red/5' : ''}`}
                        >
                          <td className="px-4 py-2.5">
                            <div className="flex flex-col">
                              <span className="text-[13px] font-semibold text-text-primary">{system.hostname}</span>
                              <span className="text-[10px] font-mono text-text-muted">{system.system_id}</span>
                            </div>
                          </td>
                          <td className="px-4 py-2.5">
                            <span className={`inline-flex items-center gap-1.5 text-[11px] font-medium ${status.color}`}>
                              <span className={`w-1.5 h-1.5 rounded-full ${status.bg}`} />
                              {status.label}
                            </span>
                          </td>
                          <td className="px-4 py-2.5 text-xs text-text-secondary">
                            {timeAgo(system.last_seen)}
                          </td>
                          <td className="px-4 py-2.5 text-right font-mono text-xs text-text-secondary" title={`CPU usage${staleTitle}`}>
                            <span className="inline-flex items-center justify-end gap-1">
                              {isStale && <span className="w-1.5 h-1.5 rounded-full bg-accent-amber opacity-70" title="Reading may be stale" />}
                              {system.cpu_usage_percent.toFixed(1)}%
                            </span>
                          </td>
                          <td className="px-4 py-2.5 text-right font-mono text-xs text-text-secondary" title={`Memory usage${staleTitle}`}>
                            <span className="inline-flex items-center justify-end gap-1">
                              {isStale && <span className="w-1.5 h-1.5 rounded-full bg-accent-amber opacity-70" title="Reading may be stale" />}
                              {system.memory_usage_percent.toFixed(1)}%
                            </span>
                          </td>
                          <td className="px-4 py-2.5 text-right font-mono text-xs text-text-secondary" title={`Disk free${staleTitle}`}>
                            <span className="inline-flex items-center justify-end gap-1">
                              {isStale && <span className="w-1.5 h-1.5 rounded-full bg-accent-amber opacity-70" title="Reading may be stale" />}
                              {system.disk_free_percent.toFixed(1)}% free
                            </span>
                          </td>
                      <td className="px-4 py-2.5 text-right">
                        {activeAlerts > 0 ? (
                          <span className="inline-flex items-center justify-center min-w-[20px] h-5 px-1.5 rounded bg-accent-red/10 text-accent-red text-xs font-bold">
                            {activeAlerts}
                          </span>
                        ) : (
                          <span className="text-text-muted text-xs">—</span>
                        )}
                      </td>
                      <td className="px-4 py-2.5 text-right text-xs text-text-secondary w-40 truncate">
                        {recentEvent ? recentEvent.fault_type : <span className="text-text-muted">None</span>}
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <div className="flex items-center justify-end gap-2">
                          <button
                            onClick={(e) => { e.stopPropagation(); setTerminalSystem(system.system_id); setTerminalOutput(`Connected to ${system.hostname}...\nSentinelCore Terminal v2.0.0\n`); }}
                            className="p-1.5 rounded bg-bg-surface border border-border text-text-secondary hover:text-text-primary hover:bg-bg-hover transition-colors"
                            title="Terminal Session"
                          >
                            <Terminal className="w-3.5 h-3.5 inline" />
                          </button>
                          <button
                            onClick={(e) => { e.stopPropagation(); setRestartSystem(system); }}
                            className="p-1.5 rounded bg-bg-surface border border-border text-text-secondary hover:text-accent-orange hover:border-accent-orange/30 transition-colors"
                            title="Restart Agent"
                          >
                            <RefreshCw className="w-3.5 h-3.5 inline" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Add System Modal */}
      {showAddSystem && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm animate-fade-in" onClick={(e) => { if (e.target === e.currentTarget) setShowAddSystem(false); }}>
          <div className="w-[450px] bg-bg-primary rounded-xl shadow-[0_0_30px_rgba(0,0,0,0.8)] border border-border overflow-hidden">
            <div className="flex items-center justify-between px-5 py-4 border-b border-border bg-bg-surface">
              <h3 className="text-sm font-bold text-text-primary">Register New System</h3>
              <button onClick={() => setShowAddSystem(false)} className="text-text-muted hover:text-text-primary"><X className="w-4 h-4" /></button>
            </div>
            <div className="p-5 space-y-4">
              <div>
                <label className="block text-xs font-semibold text-text-secondary mb-1">Hostname</label>
                <input type="text" value={addSystemForm.hostname} onChange={(e) => setAddSystemForm({ ...addSystemForm, hostname: e.target.value })} className="w-full bg-bg-surface border border-border rounded px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-signal-primary" placeholder="e.g. prod-db-03" />
              </div>
              <div>
                <label className="block text-xs font-semibold text-text-secondary mb-1">IP Address</label>
                <input type="text" value={addSystemForm.ip_address} onChange={(e) => setAddSystemForm({ ...addSystemForm, ip_address: e.target.value })} className="w-full bg-bg-surface border border-border rounded px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-signal-primary" placeholder="e.g. 192.168.1.10" />
              </div>
              <div>
                <label className="block text-xs font-semibold text-text-secondary mb-1">Agent Key</label>
                <input type="password" value={addSystemForm.agent_key} onChange={(e) => setAddSystemForm({ ...addSystemForm, agent_key: e.target.value })} className="w-full bg-bg-surface border border-border rounded px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-signal-primary" placeholder="Shared secret" />
              </div>
              <div className="pt-2 flex justify-end gap-2">
                <button onClick={() => setShowAddSystem(false)} className="px-4 py-2 text-xs font-semibold text-text-secondary hover:text-text-primary">Cancel</button>
                <button onClick={handleAddSystem} className="px-4 py-2 bg-signal-primary text-[#0f172a] text-xs font-bold rounded hover:opacity-90">Register</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Terminal Modal */}
      {terminalSystem && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm animate-fade-in" onClick={(e) => { if (e.target === e.currentTarget) { setTerminalSystem(null); setTerminalOutput(''); } }}>
          <div className="w-[700px] h-[500px] bg-[#050505] rounded-xl shadow-[0_0_30px_rgba(0,0,0,0.8)] border border-border overflow-hidden flex flex-col">
            <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-[#111]">
              <div className="flex items-center gap-2">
                <Terminal className="w-4 h-4 text-signal-primary" />
                <h3 className="text-xs font-mono font-bold text-text-primary">{terminalSystem}</h3>
              </div>
              <button onClick={() => { setTerminalSystem(null); setTerminalOutput(''); }} className="text-text-muted hover:text-text-primary"><X className="w-4 h-4" /></button>
            </div>
            <div className="flex-1 p-4 overflow-y-auto font-mono text-[11px] text-[#A0AEC0] whitespace-pre-wrap flex flex-col">
              <div className="flex-1">
                {terminalOutput}
                <div ref={terminalEndRef} />
              </div>
            </div>
            <form onSubmit={handleExecuteCommand} className="flex px-4 py-3 bg-[#111] border-t border-border">
              <span className="text-signal-primary font-mono text-xs mr-2 mt-0.5">$</span>
              <input
                type="text"
                autoFocus
                value={terminalCommand}
                onChange={(e) => setTerminalCommand(e.target.value)}
                disabled={isCommanding}
                className="flex-1 bg-transparent border-none outline-none font-mono text-xs text-white"
                placeholder="Enter command..."
              />
            </form>
          </div>
        </div>
      )}

      {/* Restart Modal */}
      {restartSystem && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm animate-fade-in" onClick={(e) => { if (e.target === e.currentTarget) setRestartSystem(null); }}>
          <div className="w-[400px] bg-bg-primary rounded-xl shadow-[0_0_30px_rgba(0,0,0,0.8)] border border-border overflow-hidden">
            <div className="p-5">
              <div className="flex items-center gap-3 mb-3 text-accent-orange">
                <RefreshCw className="w-5 h-5" />
                <h3 className="text-sm font-bold">Restart Collector Agent</h3>
              </div>
              <p className="text-xs text-text-secondary leading-relaxed">
                Are you sure you want to trigger an agent restart on <span className="font-semibold text-text-primary">{restartSystem.hostname}</span>? This will briefly suspend telemetry collection.
              </p>
              <div className="mt-5 flex justify-end gap-3">
                <button onClick={() => setRestartSystem(null)} className="px-4 py-2 text-xs font-semibold text-text-secondary hover:text-text-primary">Cancel</button>
                <button onClick={handleRestart} className="px-4 py-2 bg-accent-orange text-white text-xs font-bold rounded hover:opacity-90">Confirm Restart</button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
