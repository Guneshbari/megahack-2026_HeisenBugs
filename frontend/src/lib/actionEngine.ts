/**
 * actionEngine — Heuristic operator playbook (extended)
 *
 * Maps fault patterns to actionable remediation steps.
 * Now includes RemediationHook for executable actions and
 * effectiveness-based ranking via feedbackStore data.
 *
 * Rules:
 *  - CPU     → scale instance
 *  - DB      → check DB connections
 *  - Network → check connectivity
 *  - Crash   → restart service
 *  - Anomaly → restart service + investigate
 */

// ── Types ─────────────────────────────────────────────────────────────

export interface RemediationHook {
  id:      string;   // unique key used for cooldown/dedup tracking
  label:   string;   // button label shown in RemediationControls UI
  confirm: string;   // confirmation text shown in the inline mini-strip
}

export interface Action {
  step:         string;
  command?:     string;
  priority:     'immediate' | 'investigate' | 'monitor';
  remediation?: RemediationHook;
}

export interface RemediationResult {
  success: boolean;
  log:     string;
}

// ── Remediation hook catalog ──────────────────────────────────────────

const HOOKS: Record<string, RemediationHook> = {
  'restart-service': {
    id:      'restart-service',
    label:   'Restart Service',
    confirm: 'Restart the affected service? Ensure no active transactions are currently in flight.',
  },
  'scale-instance': {
    id:      'scale-instance',
    label:   'Scale Instance',
    confirm: 'Scale compute capacity for this system? This will add resources to the cluster.',
  },
  'check-db-connections': {
    id:      'check-db-connections',
    label:   'Check DB Connections',
    confirm: 'Run a DB connection health check? This is a read-only diagnostic — no writes will occur.',
  },
};

// ── Playbook ──────────────────────────────────────────────────────────

const PLAYBOOK: Record<string, Action[]> = {
  'cpu': [
    { step: 'Scale instance / add nodes to cluster',           priority: 'immediate',   remediation: HOOKS['scale-instance'] },
    { step: 'Identify top CPU-consuming processes',            command: 'top -b -n 1 | head -n 15', priority: 'immediate' },
    { step: 'Review recent deployments for regression',        priority: 'investigate' },
  ],
  'db': [
    { step: 'Check active database connections',               command: 'show processlist;', priority: 'immediate', remediation: HOOKS['check-db-connections'] },
    { step: 'Verify connection pool limits',                   priority: 'investigate' },
    { step: 'Monitor slow queries',                            priority: 'monitor' },
  ],
  'network': [
    { step: 'Check upstream physical connectivity',            command: 'ping -c 4 8.8.8.8', priority: 'immediate' },
    { step: 'Verify firewall drop rules',                      priority: 'investigate' },
    { step: 'Monitor packet loss rate',                        priority: 'monitor' },
  ],
  'auth': [
    { step: 'Review auth service brute-force logs',            priority: 'immediate' },
    { step: 'Check token validity expiration metrics',         priority: 'investigate' },
  ],
  'crash': [
    { step: 'Check system logs for OOM killer',                command: 'dmesg -T | grep -i oom', priority: 'immediate' },
    { step: 'Restart affected service',                        command: 'systemctl restart service', priority: 'immediate', remediation: HOOKS['restart-service'] },
  ],
  'anomaly': [
    { step: 'Investigate ML-detected spike context',           priority: 'immediate',   remediation: HOOKS['restart-service'] },
    { step: 'Compare feature snapshots against baseline',      priority: 'investigate' },
  ],
  'disk': [
    { step: 'Identify large files / log growth',               command: 'du -sh /* | sort -rh | head -20', priority: 'immediate' },
    { step: 'Rotate or archive aged logs',                     priority: 'investigate' },
    { step: 'Monitor disk write rate',                         priority: 'monitor' },
  ],
};

const FALLBACK_ACTIONS: Action[] = [
  { step: 'Capture current system state snapshot',             priority: 'immediate' },
  { step: 'Review recent changes in change log',               priority: 'investigate' },
  { step: 'Escalate if not resolved within 15 min',            priority: 'monitor' },
];

// ── Main: getSuggestedActions ─────────────────────────────────────────

/**
 * getSuggestedActions — Returns ordered action list for a fault type.
 *
 * @param faultType          - Incident fault_type string
 * @param isAnomaly          - true when trigger is 'anomaly' or 'failure_prob'
 * @param effectivenessRates - Optional Map<hookId, 0–1> from feedbackStore.
 *                             When provided, actions with high-success remediations float to top.
 */
export function getSuggestedActions(
  faultType:           string,
  isAnomaly:           boolean,
  effectivenessRates?: Record<string, number>,
): Action[] {
  const faultLower = faultType.toLowerCase();
  let actions: Action[];

  if (isAnomaly) {
    actions = PLAYBOOK['anomaly'];
  } else {
    const match = Object.entries(PLAYBOOK).find(([key]) => faultLower.includes(key));
    actions = match ? match[1] : FALLBACK_ACTIONS;
  }

  // Effectiveness ranking: float actions with high-success remediations to top
  if (effectivenessRates && Object.keys(effectivenessRates).length > 0) {
    return [...actions].sort((a, b) => {
      const rateA = a.remediation ? (effectivenessRates[a.remediation.id] ?? 0) : 0;
      const rateB = b.remediation ? (effectivenessRates[b.remediation.id] ?? 0) : 0;
      if (rateA !== rateB) return rateB - rateA;
      return 0;  // preserve original order otherwise
    });
  }

  return actions;
}

// ── Simulated remediation execution ──────────────────────────────────

/**
 * executeRemediation — Frontend-simulated async remediation.
 *
 * No real system calls are made. Safe for any environment.
 * Enforces a 30s cooldown per hookId and deduplicates in-flight executions.
 *
 * @param hookId       - Hook to execute
 * @param cooldownMap  - Component-owned Map<hookId, lastExecutedMs> for cooldown tracking
 * @param inFlightSet  - Component-owned Set<hookId> of currently running hooks
 * @param onComplete   - Callback after execution (use to record to feedbackStore)
 */
const COOLDOWN_MS = 30_000;

export async function executeRemediation(
  hookId:      string,
  cooldownMap: Map<string, number>,
  inFlightSet: Set<string>,
  onComplete?: (hookId: string, result: RemediationResult) => void,
): Promise<RemediationResult> {
  // ── Cooldown guard ────────────────────────────────────────────────
  const lastRun = cooldownMap.get(hookId);
  if (lastRun !== undefined) {
    const elapsed = Date.now() - lastRun;
    if (elapsed < COOLDOWN_MS) {
      const remaining = Math.ceil((COOLDOWN_MS - elapsed) / 1000);
      return { success: false, log: `Cooldown active — ready in ${remaining}s` };
    }
  }

  // ── Dedup guard ───────────────────────────────────────────────────
  if (inFlightSet.has(hookId)) {
    return { success: false, log: 'Already executing — please wait' };
  }

  const hook = HOOKS[hookId];
  if (!hook) {
    return { success: false, log: `Unknown remediation hook: ${hookId}` };
  }

  // ── Execute (simulated) ───────────────────────────────────────────
  inFlightSet.add(hookId);

  const delayMs = 1_000 + Math.random() * 1_000;
  await new Promise<void>((resolve) => setTimeout(resolve, delayMs));

  const success = Math.random() > 0.10;  // 90% simulated success rate
  const result: RemediationResult = {
    success,
    log: success
      ? `${hook.label} completed successfully at ${new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}`
      : `${hook.label} failed — review system logs for details`,
  };

  // Record execution timestamp for cooldown
  cooldownMap.set(hookId, Date.now());
  inFlightSet.delete(hookId);

  onComplete?.(hookId, result);
  return result;
}
