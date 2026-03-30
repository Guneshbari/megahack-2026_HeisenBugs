/**
 * AlertsPage — dense flat SOC alert table
 * Replaces card-based layout with flat rows.
 * Acknowledge/escalate via inline buttons.
 */
import { useState, useMemo, useCallback } from 'react';
import { Check, ArrowUpRight, Plus, X } from 'lucide-react';
import { useDashboardStore } from '../store/dashboardStore';
import { alertAction, createAlertRule } from '../lib/api';
import type { Alert, Severity } from '../types/telemetry';

const SEV_COLORS: Record<Severity, string> = {
  CRITICAL: '#FF3B3B',
  ERROR:    '#FF8A00',
  WARNING:  '#FFD600',
  INFO:     '#3BA4FF',
};

function timeAgo(ts: string): string {
  const diff = Date.now() - new Date(ts).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1)   return 'now';
  if (m < 60)  return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24)  return `${h}h`;
  return `${Math.floor(h / 24)}d`;
}

type Tab = 'active' | 'acknowledged';
type SortKey = 'severity' | 'age' | 'system';

export default function AlertsPage() {
  const filteredAlerts = useDashboardStore((s) => s.filteredAlerts);

  const [tab, setTab]                 = useState<Tab>('active');
  const [sortKey, setSortKey]         = useState<SortKey>('severity');
  const [sortDesc, setSortDesc]       = useState(true);
  const [selectedId, setSelectedId]   = useState<string | null>(null);
  const [toast, setToast]             = useState<{ msg: string; ok: boolean } | null>(null);
  const [showAddRule, setShowAddRule] = useState(false);
  const [ruleForm, setRuleForm]       = useState({ name: '', condition: '', severity: 'WARNING' as Severity, threshold: 1 });
  const [localStates, setLocalStates] = useState<Record<string, Partial<Alert>>>({});

  const showToast = (msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 3000);
  };

  const mergedAlerts: Alert[] = useMemo(
    () => filteredAlerts.map((a) => ({ ...a, ...localStates[a.alert_id] })),
    [filteredAlerts, localStates],
  );

  const sevOrder: Record<Severity, number> = { CRITICAL: 4, ERROR: 3, WARNING: 2, INFO: 1 };

  const displayed = useMemo(() => {
    const base = tab === 'active'
      ? mergedAlerts.filter((a) => !a.acknowledged)
      : mergedAlerts.filter((a) => a.acknowledged);

    return [...base].sort((a, b) => {
      let cmp = 0;
      if (sortKey === 'severity') cmp = sevOrder[a.severity] - sevOrder[b.severity];
      if (sortKey === 'age')      cmp = new Date(a.triggered_at).getTime() - new Date(b.triggered_at).getTime();
      if (sortKey === 'system')   cmp = a.hostname.localeCompare(b.hostname);
      return sortDesc ? -cmp : cmp;
    });
  }, [mergedAlerts, tab, sortKey, sortDesc]);

  const toggleSort = useCallback((key: SortKey) => {
    if (sortKey === key) setSortDesc((d) => !d);
    else { setSortKey(key); setSortDesc(true); }
  }, [sortKey]);

  const handleAction = useCallback(async (
    action: 'acknowledge' | 'escalate',
    alert: Alert,
    e: React.MouseEvent,
  ) => {
    e.stopPropagation();
    try {
      await alertAction(action, alert.alert_id);
      setLocalStates((prev) => ({
        ...prev,
        [alert.alert_id]: {
          ...prev[alert.alert_id],
          ...(action === 'acknowledge' ? { acknowledged: true } : { escalated: true }),
        },
      }));
      showToast(`${action}d`, true);
    } catch (err) {
      showToast(err instanceof Error ? err.message : 'Failed', false);
    }
  }, []);

  const handleAddRule = async () => {
    if (!ruleForm.name || !ruleForm.condition) return;
    try {
      await createAlertRule(ruleForm.name, ruleForm.condition, ruleForm.severity, ruleForm.threshold);
      setShowAddRule(false);
      setRuleForm({ name: '', condition: '', severity: 'WARNING', threshold: 1 });
      showToast('Rule created', true);
    } catch {
      showToast('Failed to create rule', false);
    }
  };

  const ColHeader = ({ label, id }: { label: string; id: SortKey }) => (
    <div
      onClick={() => toggleSort(id)}
      className="font-mono text-[9px] text-[#334155] uppercase tracking-wider cursor-pointer select-none flex items-center gap-1"
      style={{ userSelect: 'none' }}
    >
      {label}
      {sortKey === id && <span>{sortDesc ? '↓' : '↑'}</span>}
    </div>
  );

  const activeCount = mergedAlerts.filter((a) => !a.acknowledged).length;
  const ackCount    = mergedAlerts.filter((a) => a.acknowledged).length;

  return (
    <div className="flex flex-col h-full gap-1" style={{ minHeight: 0 }}>
      {/* Toast */}
      {toast && (
        <div
          className={`fixed top-8 right-4 z-50 font-mono text-[11px] px-3 py-1.5 border ${
            toast.ok ? 'border-[#00C853] text-[#00C853] bg-[#0F1720]' : 'border-[#FF3B3B] text-[#FF3B3B] bg-[#0F1720]'
          }`}
          style={{ animation: 'soc-fadein 100ms' }}
        >
          {toast.ok ? '✓' : '✕'} {toast.msg}
        </div>
      )}

      {/* Toolbar */}
      <div
        className="flex items-center justify-between px-3 border border-[#1F2A37] flex-shrink-0 bg-[#0F1720]"
        style={{ height: 32 }}
      >
        <div className="flex items-center gap-3">
          <span className="font-mono text-[10px] text-[#E6EDF3] font-semibold uppercase tracking-wider">
            Alert Manager
          </span>
          {/* Tabs */}
          <button
            onClick={() => setTab('active')}
            className={`soc-btn ${tab === 'active' ? 'soc-btn-active' : ''}`}
          >
            Active {activeCount > 0 && <span className="ml-1 text-[#DC2626]">{activeCount}</span>}
          </button>
          <button
            onClick={() => setTab('acknowledged')}
            className={`soc-btn ${tab === 'acknowledged' ? 'soc-btn-active' : ''}`}
          >
            Acknowledged {ackCount}
          </button>
        </div>
        <button
          onClick={() => setShowAddRule(true)}
          className="soc-btn"
        >
          <Plus className="w-3 h-3" />
          New Rule
        </button>
      </div>

      {/* Add rule overlay */}
      {showAddRule && (
        <div
          className="fixed inset-0 z-40 flex items-center justify-center"
          style={{ background: 'rgba(0,0,0,0.7)' }}
        >
          <div className="soc-panel w-[380px] p-4" style={{ gap: 10, display: 'flex', flexDirection: 'column' }}>
            <div className="flex items-center justify-between mb-2">
              <span className="font-mono text-[11px] text-[#E2E8F0] font-semibold">NEW ALERT RULE</span>
              <button onClick={() => setShowAddRule(false)} className="soc-btn p-0 w-5 h-5 flex items-center justify-center">
                <X className="w-3 h-3" />
              </button>
            </div>
            {[
              { key: 'name', label: 'Rule Name', type: 'text', ph: 'High CPU Alert' },
              { key: 'condition', label: 'Condition', type: 'text', ph: 'cpu > 90' },
              { key: 'threshold', label: 'Threshold', type: 'number', ph: '1' },
            ].map(({ key, label, type, ph }) => (
              <div key={key}>
                <div className="font-mono text-[9px] text-[#475569] uppercase mb-1">{label}</div>
                <input
                  type={type}
                  className="soc-input w-full"
                  placeholder={ph}
                  value={String(ruleForm[key as keyof typeof ruleForm])}
                  onChange={(e) => setRuleForm((f) => ({ ...f, [key]: type === 'number' ? Number(e.target.value) : e.target.value }))}
                />
              </div>
            ))}
            <div>
              <div className="font-mono text-[9px] text-[#475569] uppercase mb-1">Severity</div>
              <select
                className="soc-input w-full"
                value={ruleForm.severity}
                onChange={(e) => setRuleForm((f) => ({ ...f, severity: e.target.value as Severity }))}
              >
                {['CRITICAL', 'ERROR', 'WARNING', 'INFO'].map((s) => (
                  <option key={s} value={s} style={{ background: '#0B1220' }}>{s}</option>
                ))}
              </select>
            </div>
            <button onClick={handleAddRule} className="soc-btn soc-btn-active w-full justify-center mt-1">
              Create Rule
            </button>
          </div>
        </div>
      )}

      {/* Column headers */}
      <div
        className="border border-[#1F2A37] flex-shrink-0 bg-[#111927]"
      >
        <div className="flex items-center px-2 gap-3" style={{ height: 24 }}>
          <div className="w-[68px]" style={{ position: 'sticky', left: 0 }}><ColHeader label="SEV" id="severity" /></div>
          <div className="w-[140px]" style={{ position: 'sticky', left: 76 }}><ColHeader label="System" id="system" /></div>
          <div className="flex-1 font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">Title</div>
          <div className="w-[52px]"><ColHeader label="Age" id="age" /></div>
          <div className="w-[100px]" />
        </div>
      </div>

      {/* Alert rows */}
      <div className="flex-1 overflow-y-auto border border-[#1E293B]">
        {displayed.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full gap-1">
            <span className="font-mono text-[10px] text-[#22C55E]">● ALL CLEAR</span>
            <span className="font-mono text-[9px] text-[#475569]">No {tab} alerts</span>
          </div>
        ) : (
          displayed.map((alert) => {
            const isSelected = selectedId === alert.alert_id;
            const sevColor   = SEV_COLORS[alert.severity];
            return (
              <div key={alert.alert_id}>
                <div
                  onClick={() => setSelectedId(isSelected ? null : alert.alert_id)}
                  className={`flex items-center gap-3 px-2 border-b border-[#1F2A37] border-l-[3px] cursor-pointer transition-colors ${
                    isSelected ? 'bg-[#1E293B]' :
                    alert.severity === 'CRITICAL' ? 'bg-[#2A0F10]' :
                    alert.severity === 'ERROR' ? 'bg-[#2A1A0F]' :
                    'hover:bg-[#162131] even:bg-[#0F1720]'
                  }`}
                  style={{ height: 30, borderLeftColor: sevColor }}
                >
                  {/* Severity badge */}
                  <span
                    className={`soc-badge soc-badge-${alert.severity.toLowerCase()} w-[68px] justify-center`}
                    style={{ position: 'sticky', left: 0 }}
                  >
                    {alert.severity}
                  </span>

                  {/* System (Pinned next to severity) */}
                  <span 
                    className="font-mono text-[11px] text-[#9FB3C8] w-[140px] truncate flex-shrink-0"
                    style={{ position: 'sticky', left: 76 }}
                  >
                    {alert.hostname.split('.')[0]}
                  </span>

                  {/* Title */}
                  <span className="font-mono text-[11px] text-[#E6EDF3] flex-1 truncate font-medium">
                    {alert.title}
                  </span>

                  {/* Age */}
                  <span className="font-mono text-[10px] text-[#6B7C93] w-[52px] text-right flex-shrink-0">
                    {timeAgo(alert.triggered_at)}
                  </span>

                  {/* Actions */}
                  <div className="flex items-center gap-1 flex-shrink-0 w-[100px] justify-end">
                    {!alert.acknowledged && (
                      <button
                        onClick={(e) => handleAction('acknowledge', alert, e)}
                        className="soc-btn h-[20px] px-2 text-[9px]"
                        style={{ color: '#00C853', borderColor: '#00C853' }}
                      >
                        <Check className="w-2.5 h-2.5" />
                        ACK
                      </button>
                    )}
                    {!alert.escalated && (
                      <button
                        onClick={(e) => handleAction('escalate', alert, e)}
                        className="soc-btn h-[20px] px-2 text-[9px]"
                        style={{ color: '#FF8A00', borderColor: '#FF8A00' }}
                      >
                        <ArrowUpRight className="w-2.5 h-2.5" />
                        ESC
                      </button>
                    )}
                    {alert.escalated && (
                      <span className="font-mono text-[9px] text-[#FF8A00]">↑ ESC</span>
                    )}
                  </div>
                </div>

                {/* Expanded detail */}
                {isSelected && (
                  <div className="px-4 py-3 border-b border-[#1F2A37] bg-[#111927]" style={{ fontSize: 11 }}>
                    {/* DETAILS Section */}
                    <div className="mb-4">
                      <div className="text-[11px] text-[#6B7C93] uppercase tracking-wider mb-2 font-mono font-semibold">Details</div>
                      <div className="grid grid-cols-2 gap-x-8 gap-y-2">
                        <div className="font-mono text-[#E6EDF3]">
                          <span className="text-[#6B7C93]">Rule: </span>{alert.rule}
                        </div>
                        <div className="font-mono text-[#E6EDF3]">
                          <span className="text-[#6B7C93]">Description: </span>{alert.description}
                        </div>
                        {alert.acknowledged_by && (
                          <div className="font-mono text-[#E6EDF3]">
                            <span className="text-[#6B7C93]">Acked by: </span>{alert.acknowledged_by}
                          </div>
                        )}
                      </div>
                    </div>

                    {/* RAW DATA Section */}
                    <details className="cursor-pointer group">
                      <summary className="text-[11px] text-[#6B7C93] uppercase tracking-wider font-mono font-semibold select-none group-hover:text-[#E6EDF3] transition-colors">
                        Raw Data <span className="text-[9px] opacity-70">▼</span>
                      </summary>
                      <pre className="mt-2 p-2 bg-[#0A0F14] border border-[#1F2A37] text-[10px] text-[#9FB3C8] font-mono overflow-x-auto whitespace-pre-wrap">
                        {JSON.stringify(alert, null, 2)}
                      </pre>
                    </details>
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>

      {/* Footer */}
      <div
        className="flex items-center px-3 border border-[#1F2A37] flex-shrink-0 bg-[#0F1720]"
        style={{ height: 24 }}
      >
        <span className="font-mono text-[9px] text-[#6B7C93]">
          {displayed.length} alert{displayed.length !== 1 ? 's' : ''} · click row to expand
        </span>
      </div>
    </div>
  );
}
