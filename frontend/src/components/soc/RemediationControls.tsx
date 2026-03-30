/**
 * RemediationControls — Manual remediation action panel
 *
 * Rendered inside RootCausePanel for the selected incident.
 *
 * Safety features:
 *  - 30s cooldown per hookId (enforced via cooldownMapRef)
 *  - Dedup guard: inFlightSetRef prevents double-execution
 *  - Manual trigger only — no auto-execution, ever
 *
 * UX flow:
 *  [Button] → [Inline confirm strip] → [Running…] → [✓ Done | ✗ Failed + log]
 */
import { useState, useCallback, useRef, useEffect } from 'react';
import type { Incident } from '../../store/incidentStore';
import { getSuggestedActions, executeRemediation, type RemediationResult } from '../../lib/actionEngine';
import { useFeedbackStore } from '../../store/feedbackStore';

// ── Types ─────────────────────────────────────────────────────────────

interface HookState {
  status:   'idle' | 'confirming' | 'running' | 'done' | 'failed';
  result?:  RemediationResult;
  cooldown: number;  // seconds remaining in cooldown
}

const COOLDOWN_S = 30;

// ── Component ─────────────────────────────────────────────────────────

export default function RemediationControls({ incident }: { incident: Incident }) {
  const recordRemediation  = useFeedbackStore((s) => s.recordRemediation);
  const effectivenessRates = useFeedbackStore((s) => s.actionEffectivenessRate);

  // Get actions with effectiveness-ranked remediations
  const allActions = getSuggestedActions(incident.fault_type, incident.trigger !== 'signal', effectivenessRates);
  const hooks      = allActions.map((a) => a.remediation).filter(Boolean);

  // Component-owned refs — not global state, so each mounting is independent
  const cooldownMapRef = useRef<Map<string, number>>(new Map());
  const inFlightSetRef = useRef<Set<string>>(new Set());

  const [hookStates, setHookStates] = useState<Record<string, HookState>>(() =>
    Object.fromEntries(hooks.map((h) => [h!.id, { status: 'idle', cooldown: 0 }])),
  );

  // ── Cooldown countdown ticker ─────────────────────────────────────
  useEffect(() => {
    const interval = setInterval(() => {
      const now = Date.now();
      setHookStates((prev) => {
        let changed = false;
        const next  = { ...prev };
        for (const hookId of Object.keys(next)) {
          const lastRun = cooldownMapRef.current.get(hookId);
          if (lastRun === undefined) continue;
          const elapsedS  = Math.floor((now - lastRun) / 1000);
          const remaining = Math.max(0, COOLDOWN_S - elapsedS);
          if (remaining !== next[hookId].cooldown) {
            next[hookId] = { ...next[hookId], cooldown: remaining };
            changed = true;
          }
        }
        return changed ? next : prev;
      });
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const setHookStatus = useCallback((hookId: string, update: Partial<HookState>) => {
    setHookStates((prev) => ({
      ...prev,
      [hookId]: { ...prev[hookId], ...update },
    }));
  }, []);

  // ── Handlers ──────────────────────────────────────────────────────
  const handleConfirm = useCallback(async (hookId: string) => {
    setHookStatus(hookId, { status: 'running' });

    const result = await executeRemediation(
      hookId,
      cooldownMapRef.current,
      inFlightSetRef.current,
      (id, res) => recordRemediation(id, incident.incident_id, res.success ? 'success' : 'failed'),
    );

    const lastRun = cooldownMapRef.current.get(hookId);
    setHookStatus(hookId, {
      status:   result.success ? 'done' : 'failed',
      result,
      cooldown: lastRun ? COOLDOWN_S : 0,
    });
  }, [incident.incident_id, recordRemediation, setHookStatus]);

  const handleCancel = useCallback((hookId: string) => {
    setHookStatus(hookId, { status: 'idle' });
  }, [setHookStatus]);

  if (hooks.length === 0) return null;

  return (
    <div style={{ padding: '6px 12px 8px', background: '#060c18', borderBottom: '1px solid #1E293B' }}>

      {/* Section header */}
      <div style={{
        fontFamily:    'JetBrains Mono, monospace',
        fontSize:      9,
        color:         '#334155',
        textTransform: 'uppercase',
        letterSpacing: '0.1em',
        marginBottom:  6,
      }}>
        Remediation
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {hooks.map((hook) => {
          if (!hook) return null;
          const state         = hookStates[hook.id] ?? { status: 'idle', cooldown: 0 };
          const isCoolingDown = state.cooldown > 0;
          const isDisabled    = state.status === 'running' || (isCoolingDown && state.status !== 'idle');

          return (
            <div key={hook.id} style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>

              {/* ── Primary button row ── */}
              {state.status !== 'confirming' && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <button
                    id={`remediate-${hook.id}`}
                    disabled={isDisabled}
                    onClick={() => !isDisabled && state.status === 'idle' && setHookStatus(hook.id, { status: 'confirming' })}
                    style={{
                      fontFamily:   'JetBrains Mono, monospace',
                      fontSize:     9,
                      fontWeight:   600,
                      color:        isDisabled ? '#334155' : '#38BDF8',
                      background:   'transparent',
                      border:       `1px solid ${isDisabled ? '#1E293B' : '#38BDF855'}`,
                      borderRadius: 2,
                      padding:      '2px 10px',
                      cursor:       isDisabled ? 'not-allowed' : 'pointer',
                      transition:   'all 80ms',
                      letterSpacing: '0.04em',
                    }}
                    onMouseEnter={(e) => { if (!isDisabled) (e.currentTarget as HTMLButtonElement).style.background = '#38BDF818'; }}
                    onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.background = 'transparent'; }}
                  >
                    {state.status === 'running' ? '◌ Running…' : `⚡ ${hook.label}`}
                  </button>

                  {/* Status feedback — done */}
                  {state.status === 'done' && (
                    <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9, color: '#22C55E' }}>
                      ✓ Done
                      {isCoolingDown && (
                        <span style={{ color: '#334155', marginLeft: 4 }}>· ready in {state.cooldown}s</span>
                      )}
                    </span>
                  )}

                  {/* Status feedback — failed */}
                  {state.status === 'failed' && (
                    <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 9, color: '#F97316' }}>
                      ✗ Failed
                      {isCoolingDown && (
                        <span style={{ color: '#334155', marginLeft: 4 }}>· ready in {state.cooldown}s</span>
                      )}
                    </span>
                  )}
                </div>
              )}

              {/* ── Inline confirmation strip ── */}
              {state.status === 'confirming' && (
                <div style={{
                  background:    '#0F1E30',
                  border:        '1px solid #1E3A5F',
                  borderRadius:  2,
                  padding:       '5px 8px',
                  display:       'flex',
                  flexDirection: 'column',
                  gap:           5,
                }}>
                  <span style={{
                    fontFamily: 'JetBrains Mono, monospace',
                    fontSize:   9,
                    color:      '#94A3B8',
                    lineHeight: 1.45,
                  }}>
                    {hook.confirm}
                  </span>
                  <div style={{ display: 'flex', gap: 6 }}>
                    <button
                      onClick={() => handleConfirm(hook.id)}
                      style={{
                        fontFamily:   'JetBrains Mono, monospace',
                        fontSize:     9,
                        fontWeight:   700,
                        color:        '#EF4444',
                        background:   '#2A0A0A',
                        border:       '1px solid #EF444455',
                        borderRadius: 2,
                        padding:      '2px 10px',
                        cursor:       'pointer',
                      }}
                    >
                      Confirm
                    </button>
                    <button
                      onClick={() => handleCancel(hook.id)}
                      style={{
                        fontFamily:   'JetBrains Mono, monospace',
                        fontSize:     9,
                        color:        '#475569',
                        background:   'transparent',
                        border:       '1px solid #1E293B',
                        borderRadius: 2,
                        padding:      '2px 10px',
                        cursor:       'pointer',
                      }}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}

              {/* ── Result log line ── */}
              {(state.status === 'done' || state.status === 'failed') && state.result && (
                <div style={{
                  fontFamily:  'JetBrains Mono, monospace',
                  fontSize:    9,
                  color:       state.status === 'done' ? '#22C55E' : '#F97316',
                  background:  state.status === 'done' ? '#071A0E' : '#1A0C00',
                  padding:     '2px 6px',
                  borderLeft:  `2px solid ${state.status === 'done' ? '#22C55E' : '#F97316'}`,
                  marginLeft:  8,
                  lineHeight:  1.4,
                }}>
                  {state.result.log}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
