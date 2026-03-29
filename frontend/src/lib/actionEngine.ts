/**
 * actionEngine — Heuristic operator playbook
 *
 * Maps fault patterns to actionable remediation steps.
 * Rules:
 *  - CPU → scale instance
 *  - DB → check connections
 *  - Network → check connectivity
 *  - High anomaly → investigate spike
 */

export interface Action {
  step:        string;
  command?:    string;
  priority:    'immediate' | 'investigate' | 'monitor';
}

const PLAYBOOK: Record<string, Action[]> = {
  'cpu': [
    { step: 'Scale instance / add nodes to cluster', priority: 'immediate' },
    { step: 'Identify top CPU-consuming processes',  command: 'top -b -n 1 | head -n 15', priority: 'immediate' },
    { step: 'Review recent deployments for regression', priority: 'investigate' },
  ],
  'db': [
    { step: 'Check active database connections', command: 'show processlist;', priority: 'immediate' },
    { step: 'Verify connection pool limits', priority: 'investigate' },
    { step: 'Monitor slow queries', priority: 'monitor' },
  ],
  'network': [
    { step: 'Check upstream physical connectivity', command: 'ping -c 4 8.8.8.8', priority: 'immediate' },
    { step: 'Verify firewall drop rules', priority: 'investigate' },
    { step: 'Monitor packet loss rate', priority: 'monitor' },
  ],
  'auth': [
    { step: 'Review auth service brute-force logs', priority: 'immediate' },
    { step: 'Check token validity expiration metrics', priority: 'investigate' },
  ],
  'crash': [
    { step: 'Check system logs for OOM killer', command: 'dmesg -T | grep -i oom', priority: 'immediate' },
    { step: 'Restart affected service', command: 'systemctl restart service', priority: 'immediate' },
  ],
  'anomaly': [
    { step: 'Investigate ML-detected spike context', priority: 'immediate' },
    { step: 'Compare feature snapshots against baseline', priority: 'investigate' },
  ],
};

const FALLBACK_ACTIONS: Action[] = [
  { step: 'Capture current system state snapshot', priority: 'immediate' },
  { step: 'Review recent changes in change log', priority: 'investigate' },
  { step: 'Escalate if not resolved within 15 min', priority: 'monitor' },
];

export function getSuggestedActions(faultType: string, isAnomaly: boolean): Action[] {
  const faultLower = faultType.toLowerCase();

  if (isAnomaly) {
    return PLAYBOOK['anomaly'];
  }

  // Find partial match in keys
  for (const [key, actions] of Object.entries(PLAYBOOK)) {
    if (faultLower.includes(key)) {
      return actions;
    }
  }

  return FALLBACK_ACTIONS;
}
