/**
 * SmartEventStream — virtualized signal list
 *
 * Rendering rules:
 *  - Consumes ONLY grouped signals from Zustand (never raw events)
 *  - Row height: 28px, virtualized via @tanstack/react-virtual
 *  - Left border colored by severity
 *  - Spike format: "Auth Failure ×23 (2m ago)"
 *  - Click selects incident via uiStore
 *  - No animations longer than 150ms
 */
import { useRef, useMemo, useCallback, useState, memo } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { useSignalStore } from '../../store/signalStore';
import { useUIStore } from '../../store/uiStore';
import type { GroupedSignal } from '../../store/signalStore';
import type { Severity } from '../../types/telemetry';

const SEV_BORDER: Record<Severity, string> = {
  CRITICAL: 'soc-border-critical',
  ERROR:    'soc-border-error',
  WARNING:  'soc-border-warning',
  INFO:     'soc-border-info',
};

const SEV_TEXT: Record<Severity, string> = {
  CRITICAL: 'text-[#DC2626]',
  ERROR:    'text-[#F97316]',
  WARNING:  'text-[#FACC15]',
  INFO:     'text-[#38BDF8]',
};

const ROW_HEIGHT = 40;

const SignalRow = memo(({ signal, isSelected, onClick }: {
  signal:     GroupedSignal;
  isSelected: boolean;
  onClick:    () => void;
}) => {
  const isRecent = Date.now() - new Date(signal.lastSeen).getTime() < 60000;
  return (
    <div
      onClick={onClick}
      className={`soc-signal-row ${SEV_BORDER[signal.severity]} ${isSelected ? 'bg-[#1E3A5F]' : ''}`}
      style={{ paddingLeft: 8, paddingRight: 6, opacity: isSelected || isRecent ? 1 : 0.65 }}
    >
    {/* Fault type label */}
    <span className={`font-mono text-[11px] font-medium flex-1 truncate ${SEV_TEXT[signal.severity]}`}>
      {signal.fault_type}
    </span>

    {/* Spike badge */}
    {signal.isSpike && (
      <span className="soc-spike">×{signal.count}</span>
    )}

    {/* System count */}
    <span className="font-mono text-[10px] text-[#475569] ml-1 flex-shrink-0">
      {signal.systems.length > 1
        ? `${signal.systems.length} sys`
        : signal.systems[0]?.split('.')[0] ?? ''}
    </span>

    {/* Age */}
    <span className="font-mono text-[10px] text-[#475569] ml-2 flex-shrink-0 w-[48px] text-right">
      {signal.windowLabel}
    </span>
  </div>
  );
});

export default function SmartEventStream() {
  const allSignals        = useSignalStore((s) => s.signals);
  const isConnected       = useSignalStore((s) => s.isConnected);
  const selectedIncidentId    = useUIStore((s) => s.selectedIncidentId);
  const setSelectedIncidentId = useUIStore((s) => s.setSelectedIncidentId);

  const [showNoise, setShowNoise] = useState(false);

  // Noise reduction: Hide signals with count < 3 (non-spikes) unless expanded
  const signals = useMemo(() => 
    showNoise ? allSignals : allSignals.filter((s) => s.isSpike || s.severity === 'CRITICAL'),
  [allSignals, showNoise]);

  const hiddenCount = allSignals.length - signals.length;

  const parentRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count:           signals.length,
    getScrollElement: () => parentRef.current,
    estimateSize:    () => ROW_HEIGHT,
    overscan:        10,
  });

  const handleClick = useCallback((id: string) => {
    setSelectedIncidentId(selectedIncidentId === id ? null : id);
  }, [selectedIncidentId, setSelectedIncidentId]);

  // Stats for header
  const stats = useMemo(() => {
    const spikes = allSignals.filter((s) => s.isSpike).length;
    const crits  = allSignals.filter((s) => s.severity === 'CRITICAL').length;
    return { spikes, crits };
  }, [allSignals]);

  const items = virtualizer.getVirtualItems();

  return (
    <div className="soc-panel flex flex-col h-full overflow-hidden">
      {/* Header */}
      <div className="soc-panel-header">
        <span className="soc-panel-title">Signal Stream</span>
        <div className="flex items-center gap-3">
          {stats.crits > 0 && (
            <span className="font-mono text-[10px] text-[#DC2626]">
              {stats.crits} CRIT
            </span>
          )}
          {stats.spikes > 0 && (
            <span className="font-mono text-[10px] text-[#FACC15]">
              {stats.spikes} spikes
            </span>
          )}
          <span className={isConnected ? 'soc-ws-live' : 'soc-ws-dead'}>
            {isConnected ? '● LIVE' : '○ MOCK'}
          </span>
        </div>
      </div>

      {/* Sub-header: column labels */}
      <div
        className="flex items-center px-2 border-b border-[#1E293B]"
        style={{ height: 20, background: '#090e1a', flexShrink: 0 }}
      >
        <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider flex-1">Signal</span>
        <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider w-[48px] text-right">Age</span>
      </div>

      {/* Virtualized signal list */}
      <div
        ref={parentRef}
        className="flex-1 overflow-y-auto overflow-x-hidden"
      >
        {signals.length === 0 ? (
          <div className="flex items-center justify-center h-full text-[#475569] font-mono text-xs">
            no signals
          </div>
        ) : (
          <div
            style={{ height: virtualizer.getTotalSize(), position: 'relative' }}
          >
            {items.map((item) => {
              const signal = signals[item.index];
              return (
                <div
                  key={signal.id}
                  style={{
                    position: 'absolute',
                    top:    item.start,
                    left:   0,
                    right:  0,
                    height: ROW_HEIGHT,
                  }}
                >
                  <SignalRow
                    signal={signal}
                    isSelected={selectedIncidentId === signal.id}
                    onClick={() => handleClick(signal.id)}
                  />
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Footer count & noise toggle */}
      <div
        className="flex items-center justify-between px-2 border-t border-[#1E293B]"
        style={{ height: 22, flexShrink: 0, background: '#090e1a' }}
      >
        <span className="font-mono text-[9px] text-[#334155]">
          {signals.length} signal{signals.length !== 1 ? 's' : ''}
        </span>

        {hiddenCount > 0 && (
          <button
            onClick={() => setShowNoise(!showNoise)}
            className="font-mono text-[8px] bg-transparent border-none cursor-pointer p-0"
            style={{ color: showNoise ? '#FACC15' : '#475569' }}
          >
            {showNoise ? `▼ hide ${hiddenCount} noise` : `▶ +${hiddenCount} noise`}
          </button>
        )}
      </div>
    </div>
  );
}
