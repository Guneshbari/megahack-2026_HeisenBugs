import { useState, useMemo } from 'react';
import { Check, ArrowUpRight, ArrowUpDown, Plus, X } from 'lucide-react';
import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import SeverityBadge from '../components/shared/SeverityBadge';
import { timeAgo, formatTimestamp, getActiveAlerts, getAcknowledgedAlerts } from '../data/mockData';
import { useDashboard } from '../context/DashboardContext';
import { alertAction, createAlertRule } from '../lib/api';
import type { Alert, Severity } from '../types/telemetry';

type SortKey = 'severity' | 'system' | 'title' | 'age' | 'status';
type SortDir = 'asc' | 'desc';

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
      className={`text-${align} text-[10px] font-semibold text-text-muted uppercase tracking-wider py-3 px-4 cursor-pointer hover:text-text-primary transition-colors select-none bg-bg-surface sticky top-0 z-10`}
    >
      <span className={`inline-flex items-center gap-1.5 ${align === 'right' ? 'flex-row-reverse' : ''}`}>
        {label}
        <ArrowUpDown className={`w-3 h-3 ${activeSortKey === sortId ? 'text-signal-primary' : 'opacity-30'}`} />
      </span>
    </th>
  );
}

const severityValue = { CRITICAL: 4, ERROR: 3, WARNING: 2, INFO: 1 };

export default function AlertsPage() {
  const { filteredAlerts, filteredEventsBySystemId, refreshTick: _refreshTick } = useDashboard();
  const [tab, setTab] = useState<'active' | 'acknowledged'>('active');
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null);
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);

  const [sortKey, setSortKey] = useState<SortKey>('severity');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  const showToast = (message: string, type: 'success' | 'error') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  };

  const [showAddRule, setShowAddRule] = useState(false);
  const [addRuleForm, setAddRuleForm] = useState({ rule_name: '', condition: '', severity: 'WARNING' as Severity, threshold: 1 });

  const handleAddRule = async () => {
    if (!addRuleForm.rule_name || !addRuleForm.condition) return;
    try {
      await createAlertRule(addRuleForm.rule_name, addRuleForm.condition, addRuleForm.severity, addRuleForm.threshold);
      setShowAddRule(false);
      setAddRuleForm({ rule_name: '', condition: '', severity: 'WARNING', threshold: 1 });
      showToast('Alert rule created', 'success');
    } catch (e) {
      showToast('Failed to create rule', 'error');
    }
  };

  const handleAction = async (action: 'acknowledge' | 'escalate', alert: Alert, e?: React.MouseEvent) => {
    if (e) e.stopPropagation();
    try {
      const data = await alertAction(action, alert.alert_id);
      if (data.success) {
        showToast(`Alert ${action}d`, 'success');
        // Re-fetch data from the server so the UI reflects true DB state
        // The DashboardContext auto-refresh will pick this up on the next tick
        // but we trigger an immediate re-evaluation by updating local alert state
        const updated = { ...alert, [action === 'acknowledge' ? 'acknowledged' : 'escalated']: true };
        if (selectedAlert?.alert_id === alert.alert_id) setSelectedAlert(updated);
      } else {
        showToast(`Failed to ${action}`, 'error');
      }
    } catch (err) {
      // alertAction throws on auth failure or network error
      const msg = err instanceof Error ? err.message : 'Network error';
      showToast(msg, 'error');
    }
  };

  const activeAlerts = useMemo(() => getActiveAlerts(filteredAlerts), [filteredAlerts]);
  const ackAlerts = useMemo(() => getAcknowledgedAlerts(filteredAlerts), [filteredAlerts]);
  const targetAlerts = tab === 'active' ? activeAlerts : ackAlerts;

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDir('desc'); }
  };

  const sortedAlerts = useMemo(() => {
    return [...targetAlerts].sort((a, b) => {
      let aV: any, bV: any;
      if (sortKey === 'severity') { aV = severityValue[a.severity]; bV = severityValue[b.severity]; }
      else if (sortKey === 'system') { aV = a.hostname; bV = b.hostname; }
      else if (sortKey === 'title') { aV = a.title; bV = b.title; }
      else if (sortKey === 'age') { aV = new Date(a.triggered_at).getTime(); bV = new Date(b.triggered_at).getTime(); }
      else if (sortKey === 'status') { aV = a.acknowledged ? 1 : a.escalated ? 2 : 0; bV = b.acknowledged ? 1 : b.escalated ? 2 : 0; }
      
      if (aV < bV) return sortDir === 'asc' ? -1 : 1;
      if (aV > bV) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
  }, [targetAlerts, sortKey, sortDir]);

  const correlatedEvents = useMemo(() => {
    if (!selectedAlert) return [];
    return (filteredEventsBySystemId[selectedAlert.system_id] ?? []).slice(-8);
  }, [selectedAlert, filteredEventsBySystemId]);

  const correlationData = correlatedEvents.map((e) => ({
    time: formatTimestamp(e.event_time).split(', ')[1] || formatTimestamp(e.event_time),
    cpu: e.cpu_usage_percent,
    memory: e.memory_usage_percent,
  }));

  return (
    <div className="flex h-[calc(100vh-100px)] gap-4">
      {/* Main Queue Queue */}
      <div className="flex-1 flex flex-col min-w-0">
        <div className="flex items-center justify-between mb-4 shrink-0">
          <div>
            <h2 className="text-lg font-bold text-text-primary tracking-tight">Incident Queue</h2>
            <p className="text-[11px] text-text-muted mt-0.5">Active alerts & priority triage</p>
          </div>
          <div className="flex items-center gap-4">
            <button
              onClick={() => setShowAddRule(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-signal-primary/20 text-signal-primary text-[11px] font-semibold rounded border border-signal-primary/30 hover:bg-signal-primary/30 transition-colors"
            >
              <Plus className="w-3.5 h-3.5" /> Add Rule
            </button>
            <div className="flex glass-panel-solid rounded-md overflow-hidden border border-border">
              <button
                onClick={() => { setTab('active'); setSelectedAlert(null); }}
                className={`px-4 py-1.5 text-xs font-semibold transition-colors ${
                  tab === 'active' ? 'bg-signal-primary/20 text-signal-primary' : 'text-text-secondary hover:text-text-primary'
                }`}
              >Active ({activeAlerts.length})</button>
              <button
                onClick={() => { setTab('acknowledged'); setSelectedAlert(null); }}
                className={`px-4 py-1.5 text-xs font-semibold transition-colors border-l border-border ${
                  tab === 'acknowledged' ? 'bg-signal-primary/20 text-signal-primary' : 'text-text-secondary hover:text-text-primary'
                }`}
              >Acknowledged ({ackAlerts.length})</button>
            </div>
          </div>
        </div>

        {/* Table Wrapper */}
        <div className="flex-1 glass-panel-solid rounded-md border border-border flex flex-col overflow-hidden">
          <div className="flex-1 overflow-y-auto">
            <table className="w-full text-left border-collapse whitespace-nowrap">
              <thead>
                <tr className="border-b border-border/80">
                  <SortHeader label="Severity" sortId="severity" activeSortKey={sortKey} onToggleSort={handleSort} />
                  <SortHeader label="Title" sortId="title" activeSortKey={sortKey} onToggleSort={handleSort} />
                  <SortHeader label="System" sortId="system" activeSortKey={sortKey} onToggleSort={handleSort} />
                  <SortHeader label="Age" sortId="age" activeSortKey={sortKey} onToggleSort={handleSort} />
                  <SortHeader label="Status" sortId="status" activeSortKey={sortKey} onToggleSort={handleSort} />
                  <th className="px-4 py-3 bg-bg-surface sticky top-0 z-10 w-32" />
                </tr>
              </thead>
              <tbody className="divide-y divide-border/40">
                {sortedAlerts.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-5 py-12 text-center text-sm text-text-muted">
                      No alerts found in this queue.
                    </td>
                  </tr>
                ) : (
                  sortedAlerts.map((alert) => (
                    <tr
                      key={alert.alert_id}
                      onClick={() => setSelectedAlert(alert)}
                      className={`cursor-pointer transition-colors ${
                        selectedAlert?.alert_id === alert.alert_id ? 'bg-signal-primary/10' : 
                        !alert.acknowledged ? 'bg-accent-red/5 hover:bg-bg-hover' : 'hover:bg-bg-hover'
                      }`}
                    >
                      <td className="px-4 py-2"><SeverityBadge severity={alert.severity} size="sm" /></td>
                      <td className="px-4 py-2 max-w-[240px] truncate text-xs font-semibold text-text-primary">
                        {alert.title}
                      </td>
                      <td className="px-4 py-2">
                        <div className="flex flex-col">
                          <span className="text-xs text-text-primary">{alert.hostname}</span>
                          <span className="text-[10px] text-text-muted font-mono">{alert.system_id}</span>
                        </div>
                      </td>
                      <td className="px-4 py-2 text-xs text-text-secondary">{timeAgo(alert.triggered_at)}</td>
                      <td className="px-4 py-2 text-xs text-text-secondary">
                        {alert.acknowledged ? 'Acknowledged' : alert.escalated ? 'Escalated' : 'Open'}
                      </td>
                      <td className="px-4 py-2 text-right">
                        {!alert.acknowledged && (
                          <div className="flex items-center justify-end gap-2">
                            <button
                              onClick={(e) => handleAction('acknowledge', alert, e)}
                              className="p-1.5 rounded bg-signal-primary/10 text-signal-primary hover:bg-signal-primary/25 transition-colors"
                              title="Acknowledge"
                            >
                              <Check className="w-3.5 h-3.5" />
                            </button>
                            <button
                              onClick={(e) => handleAction('escalate', alert, e)}
                              className="p-1.5 rounded bg-bg-surface border border-border text-text-secondary hover:text-text-primary transition-colors"
                              title="Escalate"
                            >
                              <ArrowUpRight className="w-3.5 h-3.5" />
                            </button>
                          </div>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Detail Drawer (Right Side) */}
      {selectedAlert && (
        <div className="w-[400px] shrink-0 flex flex-col glass-panel-solid rounded-md border border-border animate-slide-in overflow-hidden">
          <div className="flex items-center justify-between px-4 py-3 border-b border-border bg-bg-surface shrink-0">
            <h3 className="text-xs font-bold uppercase tracking-wider text-text-primary">Alert Details</h3>
            <button onClick={() => setSelectedAlert(null)} className="text-text-muted hover:text-text-primary text-xs">✕</button>
          </div>
          <div className="flex-1 overflow-y-auto p-4 space-y-5">
            <div>
              <div className="flex items-center gap-2 mb-2">
                <SeverityBadge severity={selectedAlert.severity} />
                <span className="text-[10px] text-text-muted">{timeAgo(selectedAlert.triggered_at)}</span>
              </div>
              <h4 className="text-sm font-bold text-text-primary leading-snug">{selectedAlert.title}</h4>
              <p className="text-xs text-text-secondary mt-2 leading-relaxed">{selectedAlert.description}</p>
            </div>

            <div className="space-y-1 text-[11px]">
              <div className="flex justify-between py-1.5 border-b border-border/40">
                <span className="text-text-muted">Host</span>
                <span className="text-text-primary font-medium">{selectedAlert.hostname}</span>
              </div>
              <div className="flex justify-between py-1.5 border-b border-border/40">
                <span className="text-text-muted">Rule</span>
                <span className="text-text-primary font-mono">{selectedAlert.rule}</span>
              </div>
              <div className="flex justify-between py-1.5 border-b border-border/40">
                <span className="text-text-muted">Status</span>
                <span className="text-text-primary">{selectedAlert.acknowledged ? 'Acknowledged' : selectedAlert.escalated ? 'Escalated' : 'Open'}</span>
              </div>
            </div>

            {/* Actions block in drawer */}
            {!selectedAlert.acknowledged && (
              <div className="flex gap-2 pt-2">
                <button 
                  onClick={() => handleAction('acknowledge', selectedAlert)}
                  className="flex-1 flex justify-center items-center gap-1.5 py-1.5 bg-signal-primary/20 text-signal-primary text-xs font-semibold rounded border border-signal-primary/30 hover:bg-signal-primary/30 transition-colors"
                >
                  <Check className="w-3.5 h-3.5" /> Ack
                </button>
                <button 
                  onClick={() => handleAction('escalate', selectedAlert)}
                  className="flex-1 flex justify-center items-center gap-1.5 py-1.5 bg-bg-surface text-text-secondary text-xs font-semibold rounded border border-border hover:text-text-primary hover:bg-bg-hover transition-colors"
                >
                  <ArrowUpRight className="w-3.5 h-3.5" /> Escalate
                </button>
              </div>
            )}

            {/* Flattened Correlation Chart */}
            {correlationData.length > 0 && (
              <div className="pt-2">
                <h5 className="text-[10px] font-semibold text-text-muted uppercase tracking-wider mb-3">Resource Trends</h5>
                <div className="h-[120px] w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={correlationData} margin={{ top: 0, right: 0, left: -25, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="2 2" stroke="#1e293b" vertical={false} />
                      <XAxis dataKey="time" tick={{ fill: '#64748b', fontSize: 9 }} axisLine={false} tickLine={false} />
                      <YAxis tick={{ fill: '#64748b', fontSize: 9 }} axisLine={false} tickLine={false} domain={[0, 100]} />
                      <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: 4, fontSize: 10, color: '#f8fafc' }} />
                      <Area type="step" dataKey="cpu" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.1} strokeWidth={1} />
                      <Line type="step" dataKey="memory" stroke="#8b5cf6" strokeWidth={1} dot={false} />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Add Rule Modal */}
      {showAddRule && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm animate-fade-in" onClick={(e) => { if (e.target === e.currentTarget) setShowAddRule(false); }}>
          <div className="w-[450px] bg-bg-primary rounded-xl shadow-[0_0_30px_rgba(0,0,0,0.8)] border border-border overflow-hidden">
            <div className="flex items-center justify-between px-5 py-4 border-b border-border bg-bg-surface">
              <h3 className="text-sm font-bold text-text-primary">Create Alert Rule</h3>
              <button onClick={() => setShowAddRule(false)} className="text-text-muted hover:text-text-primary"><X className="w-4 h-4" /></button>
            </div>
            <div className="p-5 space-y-4">
              <div>
                <label className="block text-xs font-semibold text-text-secondary mb-1">Rule Name</label>
                <input type="text" value={addRuleForm.rule_name} onChange={(e) => setAddRuleForm({ ...addRuleForm, rule_name: e.target.value })} className="w-full bg-bg-surface border border-border rounded px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-signal-primary" placeholder="High CPU Usage" />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-semibold text-text-secondary mb-1">Severity</label>
                  <select value={addRuleForm.severity} onChange={(e) => setAddRuleForm({ ...addRuleForm, severity: e.target.value as Severity })} className="w-full bg-bg-surface border border-border rounded px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-signal-primary">
                    <option value="CRITICAL">Critical</option>
                    <option value="ERROR">Error</option>
                    <option value="WARNING">Warning</option>
                    <option value="INFO">Info</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-semibold text-text-secondary mb-1">Threshold (Count)</label>
                  <input type="number" min="1" value={addRuleForm.threshold} onChange={(e) => setAddRuleForm({ ...addRuleForm, threshold: Number(e.target.value) })} className="w-full bg-bg-surface border border-border rounded px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-signal-primary" />
                </div>
              </div>
              <div>
                <label className="block text-xs font-semibold text-text-secondary mb-1">Condition</label>
                <textarea rows={3} value={addRuleForm.condition} onChange={(e) => setAddRuleForm({ ...addRuleForm, condition: e.target.value })} className="w-full bg-bg-surface border border-border rounded px-3 py-2 text-sm font-mono text-text-primary focus:outline-none focus:border-signal-primary" placeholder="cpu_usage > 90% FOR 5m" />
              </div>
              <div className="pt-2 flex justify-end gap-2">
                <button onClick={() => setShowAddRule(false)} className="px-4 py-2 text-xs font-semibold text-text-secondary hover:text-text-primary">Cancel</button>
                <button onClick={handleAddRule} className="px-4 py-2 bg-signal-primary text-[#0f172a] text-xs font-bold rounded hover:opacity-90">Create Rule</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Floating Toast Notification */}
      {toast && (
        <div className={`fixed bottom-6 right-6 px-4 py-3 rounded shadow-lg border flex items-center gap-3 animate-fade-in z-50 ${
          toast.type === 'success' ? 'bg-[#0f172a] border-signal-primary/50 text-signal-primary' : 'bg-[#0f172a] border-accent-red/50 text-accent-red'
        }`}>
          <div className="text-[11px] font-semibold">{toast.message}</div>
        </div>
      )}
    </div>
  );
}
