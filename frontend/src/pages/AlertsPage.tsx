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
import { ENABLE_PRETEXT_OPTIMIZATION, measureText, useDebouncedElementWidth } from '../utils/textLayout';

const SEV_COLORS: Record<Severity, string> = {
  CRITICAL: '#FF3B3B',
  ERROR:    '#FF8A00',
  WARNING:  '#FFD600',
  INFO:     '#3BA4FF',
};
const SEV_ORDER: Record<Severity, number> = { CRITICAL: 4, ERROR: 3, WARNING: 2, INFO: 1 };

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
const ALERT_ROW_MIN_HEIGHT = 36;
const ALERT_TITLE_FONT = '11px Inter';
const ALERT_TITLE_LINE_HEIGHT = 16;

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
      style={{ userSelect: 'none' }}
    >
      {label}
      {isActive && <span>{isDescending ? 'v' : '^'}</span>}
    </div>
  );
}

export default function AlertsPage() {
  const filteredAlerts = useDashboardStore((s) => s.filteredAlerts);

  const [tab, setTab]                 = useState<Tab>('active');
  const [sortKey, setSortKey]         = useState<SortKey>('severity');
  const [sortDesc, setSortDesc]       = useState(true);
  const [selectedId, setSelectedId]   = useState<string | null>(null);
  const [toast, setToast]             = useState<{ msg: string; ok: boolean } | null>(null);
  const [showAddRule, setShowAddRule] = useState(false);
  const [listRef, listWidth] = useDebouncedElementWidth<HTMLDivElement>(90);
  const [ruleForm, setRuleForm]       = useState({
    name: '',
    condition: '',
    severity: 'WARNING' as Severity,
    threshold: 1,
    cooldownMinutes: 30,
    escalationTarget: '',
  });
  const [localStates, setLocalStates] = useState<Record<string, Partial<Alert>>>({});

  const showToast = (msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 3000);
  };

  const mergedAlerts: Alert[] = useMemo(
    () => filteredAlerts.map((a) => ({ ...a, ...localStates[a.alert_id] })),
    [filteredAlerts, localStates],
  );

  const displayed = useMemo(() => {
    const base = tab === 'active'
      ? mergedAlerts.filter((a) => !a.acknowledged)
      : mergedAlerts.filter((a) => a.acknowledged);

    return [...base].sort((a, b) => {
      let cmp = 0;
      if (sortKey === 'severity') cmp = SEV_ORDER[a.severity] - SEV_ORDER[b.severity];
      if (sortKey === 'age')      cmp = new Date(a.triggered_at).getTime() - new Date(b.triggered_at).getTime();
      if (sortKey === 'system')   cmp = a.hostname.localeCompare(b.hostname);
      return sortDesc ? -cmp : cmp;
    });
  }, [mergedAlerts, tab, sortKey, sortDesc]);
  const alertTitleWidth = Math.max(listWidth - 440, 140);
  const alertRowHeights = useMemo(
    () => displayed.reduce<Record<string, number>>((acc, alert) => {
      if (!ENABLE_PRETEXT_OPTIMIZATION) {
        acc[alert.alert_id] = ALERT_ROW_MIN_HEIGHT;
        return acc;
      }
      const measured = measureText(alert.title, alertTitleWidth, {
        font: ALERT_TITLE_FONT,
        lineHeight: ALERT_TITLE_LINE_HEIGHT,
      });
      acc[alert.alert_id] = Math.max(ALERT_ROW_MIN_HEIGHT, measured.height + 16);
      return acc;
    }, {}),
    [alertTitleWidth, displayed],
  );

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
      await createAlertRule(
        ruleForm.name,
        ruleForm.condition,
        ruleForm.severity,
        ruleForm.threshold,
        ruleForm.cooldownMinutes,
        ruleForm.escalationTarget,
      );
      setShowAddRule(false);
      setRuleForm({
        name: '',
        condition: '',
        severity: 'WARNING',
        threshold: 1,
        cooldownMinutes: 30,
        escalationTarget: '',
      });
      showToast('Rule created', true);
    } catch {
      showToast('Failed to create rule', false);
    }
  };

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
              { key: 'cooldownMinutes', label: 'Ack Cooldown (min)', type: 'number', ph: '30' },
              { key: 'escalationTarget', label: 'Escalation Webhook', type: 'text', ph: 'https://example.com/webhook' },
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
        <div className="flex items-center px-4 gap-4" style={{ height: 28 }}>
          <div className="w-[68px]" style={{ position: 'sticky', left: 0 }}><ColumnHeader label="SEV" id="severity" isActive={sortKey === 'severity'} isDescending={sortDesc} onToggle={toggleSort} /></div>
          <div className="w-[140px]" style={{ position: 'sticky', left: 84 }}><ColumnHeader label="System" id="system" isActive={sortKey === 'system'} isDescending={sortDesc} onToggle={toggleSort} /></div>
          <div className="flex-1 font-mono text-[9px] text-[#6B7C93] uppercase tracking-wider">Title</div>
          <div className="w-[60px] flex justify-end"><ColumnHeader label="Age" id="age" align="right" isActive={sortKey === 'age'} isDescending={sortDesc} onToggle={toggleSort} /></div>
          <div className="w-[124px]" />
        </div>
      </div>

      {/* Alert rows */}
      <div ref={listRef} className="flex-1 overflow-y-auto border border-[#1E293B]">
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
                  className={`flex items-center gap-4 px-4 border-b border-[#1F2A37] border-l-[3px] cursor-pointer transition-colors ${
                    isSelected ? 'bg-[#1E293B]' :
                    alert.severity === 'CRITICAL' ? 'bg-[#2A0F10]' :
                    alert.severity === 'ERROR' ? 'bg-[#2A1A0F]' :
                    'hover:bg-[#162131] even:bg-[#0F1720]'
                  }`}
                  style={{ minHeight: alertRowHeights[alert.alert_id] ?? ALERT_ROW_MIN_HEIGHT, borderLeftColor: sevColor }}
                >
                  {/* Severity badge */}
                  <span
                    className={`soc-badge soc-badge-${alert.severity.toLowerCase()} w-[68px] justify-center flex-shrink-0`}
                    style={{ position: 'sticky', left: 0 }}
                  >
                    {alert.severity}
                  </span>

                  {/* System (Pinned next to severity) */}
                  <span 
                    className="font-mono text-[11px] text-[#9FB3C8] w-[140px] truncate flex-shrink-0"
                    style={{ position: 'sticky', left: 84 }}
                  >
                    {alert.hostname.split('.')[0]}
                  </span>

                  {/* Title */}
                  <span className="font-mono text-[11px] text-[#E6EDF3] flex-1 font-medium" style={{ whiteSpace: 'normal', lineHeight: '16px' }}>
                    {alert.title}
                  </span>

                  {/* Age */}
                  <div className="w-[60px] flex justify-end flex-shrink-0">
                    <span className="font-mono text-[10px] text-[#6B7C93] text-right">
                      {timeAgo(alert.triggered_at)}
                    </span>
                  </div>

                  {/* Actions */}
                  <div className="flex items-center justify-end gap-2 flex-shrink-0 w-[124px]">
                    {!alert.acknowledged && (
                      <button
                        onClick={(e) => handleAction('acknowledge', alert, e)}
                        className="soc-btn h-[24px] px-2 text-[10px] flex items-center justify-center gap-1.5 flex-1 w-[58px]"
                        style={{ color: '#00C853', borderColor: '#00C853' }}
                      >
                        <Check className="w-3 h-3" />
                        ACK
                      </button>
                    )}
                    {!alert.escalated && (
                      <button
                        onClick={(e) => handleAction('escalate', alert, e)}
                        className="soc-btn h-[24px] px-2 text-[10px] flex items-center justify-center gap-1.5 flex-1 w-[58px]"
                        style={{ color: '#FF8A00', borderColor: '#FF8A00' }}
                      >
                        <ArrowUpRight className="w-3 h-3" />
                        ESC
                      </button>
                    )}
                    {alert.escalated && (
                      <span className="font-mono text-[10px] text-[#FF8A00] flex-1 text-right flex items-center justify-end gap-1">
                        <ArrowUpRight className="w-3 h-3" /> ESC
                      </span>
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

