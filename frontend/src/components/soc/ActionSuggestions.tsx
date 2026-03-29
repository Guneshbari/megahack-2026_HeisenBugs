/**
 * ActionSuggestions — heuristic operator playbook
 *
 * Maps fault_type → actionable remediation steps.
 * Rendered below RootCausePanel for selected incidents.
 * Severity-gated: only shows steps matching incident severity.
 */
import type { Incident } from '../../store/incidentStore';

import { getSuggestedActions } from '../../lib/actionEngine';

const PRIORITY_COLOR = {
  immediate:   '#DC2626',
  investigate: '#F97316',
  monitor:     '#FACC15',
};

const PRIORITY_DOT_LABEL = {
  immediate:   '●',
  investigate: '●',
  monitor:     '○',
};

export default function ActionSuggestions({ incident }: { incident: Incident }) {
  const actions = getSuggestedActions(incident.fault_type, incident.trigger !== 'signal');

  // Filter: for LOW priority show only monitor; for HIGH show all; for MEDIUM skip monitor
  const filtered = actions.filter((a) => {
    if (incident.priority_label === 'LOW')    return a.priority === 'monitor';
    if (incident.priority_label === 'MEDIUM') return a.priority !== 'monitor';
    return true; // CRITICAL / HIGH → all steps
  });

  return (
    <div style={{ padding: '8px 12px 10px', background: '#06090f', borderBottom: '1px solid #1E293B' }}>
      {/* Header */}
      <div style={{
        fontFamily:    'JetBrains Mono, monospace',
        fontSize:      9,
        color:         '#334155',
        textTransform: 'uppercase',
        letterSpacing: '0.1em',
        marginBottom:  7,
        display:       'flex',
        alignItems:    'center',
        gap:           6,
      }}>
        <span>Action Playbook</span>
        <span style={{
          padding:      '0 5px',
          background:   '#0F172A',
          border:       '1px solid #1E293B',
          borderRadius: 2,
          fontSize:     8,
          color:        '#475569',
        }}>
          {incident.fault_type}
        </span>
      </div>

      {/* Action rows */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {filtered.map((action, i) => (
          <div key={i} style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: 6 }}>
              {/* Priority indicator */}
              <span style={{
                fontFamily: 'JetBrains Mono, monospace',
                fontSize:   9,
                color:      PRIORITY_COLOR[action.priority],
                flexShrink: 0,
                marginTop:  1,
              }}>
                {PRIORITY_DOT_LABEL[action.priority]}
              </span>

              {/* Step text */}
              <span style={{
                fontFamily: 'JetBrains Mono, monospace',
                fontSize:   10,
                color:      '#94A3B8',
                lineHeight: '1.35',
                flex:       1,
              }}>
                {action.step}
              </span>

              {/* Priority label */}
              <span style={{
                fontFamily:    'JetBrains Mono, monospace',
                fontSize:      8,
                color:         PRIORITY_COLOR[action.priority],
                textTransform: 'uppercase',
                flexShrink:    0,
                opacity:       0.7,
              }}>
                {action.priority}
              </span>
            </div>

            {/* Command hint */}
            {action.command && (
              <div style={{
                marginLeft:  16,
                fontFamily:  'JetBrains Mono, monospace',
                fontSize:    9,
                color:       '#22C55E',
                background:  '#0A1208',
                padding:     '1px 6px',
                borderLeft:  '2px solid #166534',
                borderRadius: 1,
                overflow:    'hidden',
                textOverflow: 'ellipsis',
                whiteSpace:  'nowrap',
              }}>
                $ {action.command}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
