import { useState, useMemo, useRef } from 'react';
import { Radio } from 'lucide-react';
import SeverityBadge from './SeverityBadge';
import EventDetailInspector from './EventDetailInspector';
import { formatTimestamp } from '../../data/mockData';
import { useDashboardStore } from '../../store/dashboardStore';
import type { TelemetryEvent } from '../../types/telemetry';

const MAX_VISIBLE = 12;

export default function LiveEventStream() {
  const filteredEvents = useDashboardStore((s) => s.filteredEvents);
  const sortedRecentEvents = useMemo(
    () => [...filteredEvents]
      .sort((a, b) => new Date(b.event_time).getTime() - new Date(a.event_time).getTime())
      .slice(0, MAX_VISIBLE),
    [filteredEvents],
  );
  const [isLive, setIsLive] = useState(true);
  const [pausedEvents, setPausedEvents] = useState<TelemetryEvent[]>([]);
  const [selectedEvent, setSelectedEvent] = useState<TelemetryEvent | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const streamEvents = isLive ? sortedRecentEvents : pausedEvents;

  function toggleLiveState() {
    if (isLive) {
      setPausedEvents(sortedRecentEvents);
      setIsLive(false);
      return;
    }

    setIsLive(true);
  }

  return (
    <div className="glass-panel-solid rounded-md border border-border flex flex-col h-full bg-bg-surface overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-border">
        <div className="flex items-center gap-2">
          <Radio className={`w-4 h-4 ${isLive ? 'text-signal-highlight neon-green' : 'text-text-muted'}`} />
          <h3 className="text-xs font-semibold text-text-primary uppercase tracking-wider">Live Event Stream</h3>
          {isLive && (
            <span className="flex items-center gap-1 text-[10px] text-signal-highlight font-medium">
              <span className="w-1.5 h-1.5 rounded-full bg-signal-highlight" />
              LIVE
            </span>
          )}
        </div>
        <button
          onClick={toggleLiveState}
          className={`px-2.5 py-1 text-[10px] font-semibold rounded-md transition-colors ${
            isLive
              ? 'bg-accent-red/15 text-accent-red hover:bg-accent-red/25'
              : 'bg-signal-highlight/15 text-signal-highlight hover:bg-signal-highlight/25'
          }`}
        >
          {isLive ? 'PAUSE' : 'RESUME'}
        </button>
      </div>

      {/* Stream */}
      <div ref={containerRef} className="max-h-[220px] overflow-y-auto">
        {streamEvents.map((e, i) => (
          <div
            key={`${e.event_record_id}-${i}`}
            onClick={() => setSelectedEvent(e)}
            className={`flex items-center gap-3 px-4 py-1 border-b border-border/30 text-[11px] hover:bg-bg-hover transition-colors cursor-pointer ${
              i === 0 ? 'bg-bg-hover/50' : ''
            }`}
          >
            <span className="text-text-muted font-mono whitespace-nowrap w-[110px] shrink-0">
              {formatTimestamp(e.event_time)}
            </span>
            <span className="font-mono text-signal-primary w-[65px] shrink-0">{e.system_id}</span>
            <span className="w-[70px] shrink-0">
              <SeverityBadge severity={e.severity} />
            </span>
            <span className="text-text-secondary w-[100px] shrink-0 truncate">{e.fault_type}</span>
            <span className="text-text-muted truncate flex-1">{e.fault_description || e.provider_name}</span>
          </div>
        ))}
      </div>

      {/* Event Detail Modal Overlay */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm animate-fade-in" onClick={(e) => {
          if (e.target === e.currentTarget) setSelectedEvent(null);
        }}>
          <div className="w-full max-w-3xl max-h-[90vh] overflow-y-auto bg-bg-primary rounded-xl shadow-[0_0_30px_rgba(0,0,0,0.8)] border border-border">
            <EventDetailInspector event={selectedEvent} onClose={() => setSelectedEvent(null)} />
          </div>
        </div>
      )}
    </div>
  );
}
