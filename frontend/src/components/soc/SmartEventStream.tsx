/**
 * SmartEventStream - virtualized signal list
 *
 * Rendering rules:
 *  - Consumes only grouped signals from Zustand
 *  - Row height: 40px, virtualized via @tanstack/react-virtual
 *  - Left border colored by severity
 *  - Click selects incident via uiStore
 *  - No animations longer than 150ms
 */
import { useMemo, useCallback, useState, memo, useEffect } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { useSignalStore } from '../../store/signalStore';
import { useUIStore } from '../../store/uiStore';
import type { GroupedSignal } from '../../store/signalStore';
import type { Severity } from '../../types/telemetry';
import { getTransportStatusLabel } from '../../lib/api';
import { ENABLE_PRETEXT_OPTIMIZATION, measureText, useDebouncedElementWidth } from '../../utils/textLayout';

const SEV_BORDER: Record<Severity, string> = {
  CRITICAL: 'soc-border-critical',
  ERROR: 'soc-border-error',
  WARNING: 'soc-border-warning',
  INFO: 'soc-border-info',
};

const SEV_TEXT: Record<Severity, string> = {
  CRITICAL: 'text-[#DC2626]',
  ERROR: 'text-[#F97316]',
  WARNING: 'text-[#FACC15]',
  INFO: 'text-[#38BDF8]',
};

const ROW_HEIGHT = 40;
const ROW_PADDING_Y = 10;
const SIGNAL_FONT = '11px JetBrains Mono';
const SIGNAL_LINE_HEIGHT = 16;

function isSignalRecent(lastSeen: string, now: number): boolean {
  return now - new Date(lastSeen).getTime() < 60_000;
}

const SignalRow = memo(({ now, onClick, isSelected, signal }: {
  now: number;
  onClick: () => void;
  isSelected: boolean;
  signal: GroupedSignal;
}) => {
  const isRecent = isSignalRecent(signal.lastSeen, now);

  return (
    <div
      onClick={onClick}
      className={`soc-signal-row ${SEV_BORDER[signal.severity]} ${isSelected ? 'bg-[#1E3A5F]' : ''}`}
      style={{ paddingLeft: 8, paddingRight: 6, opacity: isSelected || isRecent ? 1 : 0.65 }}
    >
      <span className={`font-mono text-[11px] font-medium flex-1 ${SEV_TEXT[signal.severity]}`} style={{ whiteSpace: 'normal', lineHeight: '16px' }}>
        {signal.fault_type}
      </span>

      {signal.isSpike && (
        <span className="soc-spike">x{signal.count}</span>
      )}

      <span className="font-mono text-[10px] text-[#475569] ml-1 flex-shrink-0">
        {signal.systems.length > 1 ? `${signal.systems.length} sys` : signal.systems[0]?.split('.')[0] ?? ''}
      </span>

      <span className="font-mono text-[10px] text-[#475569] ml-2 flex-shrink-0 w-[48px] text-right">
        {signal.windowLabel}
      </span>
    </div>
  );
});

export default function SmartEventStream() {
  const allSignals = useSignalStore((s) => s.signals);
  const isConnected = useSignalStore((s) => s.isConnected);
  const selectedIncidentId = useUIStore((s) => s.selectedIncidentId);
  const setSelectedIncidentId = useUIStore((s) => s.setSelectedIncidentId);
  const [showNoise, setShowNoise] = useState(false);
  const [now, setNow] = useState(() => Date.now());

  const signals = useMemo(
    () => (showNoise ? allSignals : allSignals.filter((s) => s.isSpike || s.severity === 'CRITICAL')),
    [allSignals, showNoise],
  );
  const hiddenCount = allSignals.length - signals.length;
  const [parentRef, parentWidth] = useDebouncedElementWidth<HTMLDivElement>(90);
  const transportLabel = getTransportStatusLabel(isConnected);
  const contentWidth = Math.max(parentWidth - 110, 120);
  const rowHeights = useMemo(
    () => signals.map((signal) => {
      if (!ENABLE_PRETEXT_OPTIMIZATION) return ROW_HEIGHT;
      const measured = measureText(signal.fault_type, contentWidth, {
        font: SIGNAL_FONT,
        lineHeight: SIGNAL_LINE_HEIGHT,
      });
      return Math.max(ROW_HEIGHT, measured.height + ROW_PADDING_Y * 2);
    }),
    [contentWidth, signals],
  );

  const virtualizer = useVirtualizer({
    count: signals.length,
    getScrollElement: () => parentRef.current,
    estimateSize: (index) => rowHeights[index] ?? ROW_HEIGHT,
    overscan: 10,
  });

  const handleClick = useCallback((id: string) => {
    setSelectedIncidentId(selectedIncidentId === id ? null : id);
  }, [selectedIncidentId, setSelectedIncidentId]);

  useEffect(() => {
    setNow(Date.now());
  }, [signals]);

  const stats = useMemo(() => {
    const spikes = allSignals.filter((s) => s.isSpike).length;
    const crits = allSignals.filter((s) => s.severity === 'CRITICAL').length;
    return { spikes, crits };
  }, [allSignals]);

  const items = virtualizer.getVirtualItems();

  return (
    <div className="soc-panel flex flex-col h-full overflow-hidden">
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
          <span className={transportLabel === 'LIVE' ? 'soc-ws-live' : 'soc-ws-dead'}>
            {transportLabel}
          </span>
        </div>
      </div>

      <div
        className="flex items-center px-2 border-b border-[#1E293B]"
        style={{ height: 20, background: '#090e1a', flexShrink: 0 }}
      >
        <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider flex-1">Signal</span>
        <span className="font-mono text-[9px] text-[#334155] uppercase tracking-wider w-[48px] text-right">Age</span>
      </div>

      <div
        ref={parentRef}
        className="flex-1 overflow-y-auto overflow-x-hidden"
      >
        {signals.length === 0 ? (
          <div className="flex items-center justify-center h-full text-[#475569] font-mono text-xs">
            no signals
          </div>
        ) : (
          <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
            {items.map((item) => {
              const signal = signals[item.index];
              return (
                <div
                  key={signal.id}
                  style={{
                    position: 'absolute',
                    top: item.start,
                    left: 0,
                    right: 0,
                    height: item.size,
                  }}
                >
                  <SignalRow
                    signal={signal}
                    isSelected={selectedIncidentId === signal.id}
                    onClick={() => handleClick(signal.id)}
                    now={now}
                  />
                </div>
              );
            })}
          </div>
        )}
      </div>

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
            {showNoise ? `hide ${hiddenCount} noise` : `+${hiddenCount} noise`}
          </button>
        )}
      </div>
    </div>
  );
}
