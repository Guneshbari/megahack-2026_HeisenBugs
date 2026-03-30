/**
 * EventsPage — EventTable + detail pane
 * Uses TanStack Table + virtual via EventTable component.
 */
import { useState } from 'react';
import { Download, Pause, Play } from 'lucide-react';
import EventTable from '../components/soc/EventTable';
import EventDetailInspector from '../components/shared/EventDetailInspector';
import { useDashboardStore } from '../store/dashboardStore';
import { useUIStore } from '../store/uiStore';
import type { TelemetryEvent } from '../types/telemetry';

export default function EventsPage() {
  const filteredEvents = useDashboardStore((s) => s.filteredEvents);
  const selectedEvent    = useUIStore((s) => s.selectedEvent);
  const setSelectedEvent = useUIStore((s) => s.setSelectedEvent);

  const [isPaused, setIsPaused]       = useState(false);
  const [frozenEvents, setFrozenEvents] = useState<TelemetryEvent[]>([]);

  const displayEvents = isPaused ? frozenEvents : filteredEvents;

  const togglePause = () => {
    if (isPaused) {
      setIsPaused(false);
    } else {
      setFrozenEvents(filteredEvents);
      setIsPaused(true);
    }
  };

  const exportCsv = () => {
    const headers = ['Time', 'Severity', 'System', 'Provider', 'Fault Type', 'Message'];
    const rows    = displayEvents.map((e) => [
      new Date(e.event_time).toISOString(),
      e.severity,
      e.hostname,
      e.provider_name ?? '',
      e.fault_type,
      `"${(e.fault_description ?? e.fault_type ?? '').replace(/"/g, '""')}"`,
    ]);
    const csv  = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `sentinel_events_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="flex flex-col h-full gap-1" style={{ minHeight: 0 }}>
      {/* Toolbar */}
      <div
        className="flex items-center justify-between px-3 border border-[#1F2A37] flex-shrink-0 bg-[#0F1720]"
        style={{ height: 32 }}
      >
        <div className="flex items-center gap-3">
          <span className="font-mono text-[10px] text-[#E6EDF3] font-semibold uppercase tracking-wider">
            Event Console
          </span>
          <span className="font-mono text-[10px] text-[#6B7C93]">
            {displayEvents.length.toLocaleString()} events
          </span>
          {isPaused && (
            <span className="font-mono text-[9px] text-[#FACC15] border border-[#FACC15] px-1">PAUSED</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={togglePause}
            className={`soc-btn ${isPaused ? 'soc-btn-active' : ''}`}
          >
            {isPaused ? <Play className="w-3 h-3" /> : <Pause className="w-3 h-3" />}
            {isPaused ? 'Resume' : 'Pause'}
          </button>
          <button onClick={exportCsv} className="soc-btn">
            <Download className="w-3 h-3" />
            CSV
          </button>
        </div>
      </div>

      {/* Table + detail pane */}
      <div className="flex flex-1 min-h-0 gap-1">
        {/* Main table */}
        <div className="flex-1 min-h-0 border border-[#1F2A37]" style={{ overflow: 'hidden' }}>
          <EventTable events={displayEvents} />
        </div>

        {/* Detail pane */}
        {selectedEvent && (
          <div className="w-[420px] flex-shrink-0 border border-[#1F2A37]" style={{ overflow: 'hidden' }}>
            <EventDetailInspector
              event={selectedEvent}
              onClose={() => setSelectedEvent(null)}
            />
          </div>
        )}
      </div>
    </div>
  );
}
