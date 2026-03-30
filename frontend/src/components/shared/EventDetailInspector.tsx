import { X } from 'lucide-react';
import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { formatTimestamp } from '../../data/mockData';
import { useDashboardStore } from '../../store/dashboardStore';
import type { TelemetryEvent } from '../../types/telemetry';

interface EventDetailInspectorProps {
  readonly event: TelemetryEvent | null;
  readonly onClose: () => void;
}

const SectionHeader = ({ title }: { title: string }) => (
  <h4 className="flex items-center gap-2 text-[10px] font-bold text-[#6B7C93] uppercase tracking-wider mb-2 pb-1 border-b border-[#1F2A37]">
    {title}
  </h4>
);

export default function EventDetailInspector({ event, onClose }: EventDetailInspectorProps) {
  const filteredEventsBySystemId = useDashboardStore((s) => s.filteredEventsBySystemId);
  if (!event) return null;

  const systemEvents = filteredEventsBySystemId[event.system_id] ?? [];

  const correlationData = systemEvents.map((e) => ({
    time: formatTimestamp(e.event_time).split(', ')[1] || formatTimestamp(e.event_time),
    cpu: e.cpu_usage_percent,
    memory: e.memory_usage_percent,
    disk: e.disk_free_percent,
  }));

  const fields = [
    { label: 'System ID', value: event.system_id, mono: true },
    { label: 'Hostname', value: event.hostname },
    { label: 'Event ID', value: String(event.event_id), mono: true },
    { label: 'Provider', value: event.provider_name },
    { label: 'Fault Type', value: event.fault_type },
    { label: 'Event Hash', value: event.event_hash || '—', mono: true },
  ];

  return (
    <div className="flex flex-col h-full bg-[#111927] overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 bg-[#0F1720] border-b border-[#1F2A37] shrink-0">
        <h3 className="text-[11px] font-bold text-[#E6EDF3] uppercase tracking-wider">Event Details</h3>
        <button
          onClick={onClose}
          className="p-1 rounded text-[#6B7C93] hover:text-[#E6EDF3] hover:bg-[#162131] transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-6 bg-[#111927]">
        {/* INFO Section */}
        <section>
          <SectionHeader title="INFO" />
          <div className="flex items-center gap-2 mb-2">
            <span className={`soc-badge soc-badge-${event.severity.toLowerCase()} w-[56px] justify-center`}>{event.severity}</span>
            <span className="text-[10px] text-[#6B7C93] font-mono">{formatTimestamp(event.event_time)}</span>
          </div>
          <p className="text-[13px] font-medium text-[#E6EDF3] leading-relaxed">
            {event.fault_description || event.fault_type}
          </p>
        </section>

        {/* EVENT Section */}
        <section>
          <SectionHeader title="EVENT" />
          <div className="space-y-1 bg-[#0A0F14] border border-[#1F2A37] rounded p-2">
            {fields.map((f) => (
              <div key={f.label} className="flex justify-between py-1 border-b border-[#1F2A37] last:border-0 text-[11px]">
                <span className="text-[#6B7C93]">{f.label}</span>
                <span className={`text-[#E6EDF3] ${f.mono ? 'font-mono text-[10px]' : 'font-medium'}`}>
                  {f.value}
                </span>
              </div>
            ))}
          </div>
        </section>

        {/* METRICS Section */}
        <section>
          <SectionHeader title="METRICS" />
          {correlationData.length > 0 ? (
            <div className="h-[140px] w-full border border-[#1F2A37] rounded bg-[#0A0F14] p-2">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={correlationData} margin={{ top: 5, right: 0, left: -25, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="2 2" stroke="#1F2A37" vertical={false} />
                  <XAxis dataKey="time" tick={{ fill: '#6B7C93', fontSize: 9 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: '#6B7C93', fontSize: 9 }} axisLine={false} tickLine={false} domain={[0, 100]} />
                  <Tooltip
                    contentStyle={{ backgroundColor: '#0F1720', border: '1px solid #1F2A37', borderRadius: 4, fontSize: 10, color: '#E6EDF3' }}
                  />
                  <Line type="stepAfter" dataKey="cpu" stroke="#3BA4FF" strokeWidth={1.5} dot={false} name="CPU %" />
                  <Line type="stepAfter" dataKey="memory" stroke="#FF8A00" strokeWidth={1.5} dot={false} name="Memory %" />
                  <Area type="monotone" dataKey="disk" fill="#00C853" fillOpacity={0.1} stroke="#00C853" strokeWidth={1} strokeDasharray="3 3" name="Disk %" />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="flex items-center justify-center p-4 border border-dashed border-[#1F2A37] rounded text-[10px] text-[#475569] font-mono">
              NO METRICS AVAILABLE
            </div>
          )}
        </section>

        {/* RAW DATA Section */}
        <section>
          <SectionHeader title="RAW DATA" />
          {event.diagnostic_context && Object.keys(event.diagnostic_context).length > 0 ? (
            <pre className="text-[10px] text-[#9FB3C8] font-mono bg-[#0A0F14] border border-[#1F2A37] rounded p-3 overflow-x-auto">
              {JSON.stringify(event.diagnostic_context, null, 2)}
            </pre>
          ) : (
            <div className="flex items-center justify-center p-4 border border-dashed border-[#1F2A37] rounded text-[10px] text-[#475569] font-mono">
              NO DIAGNOSTIC CONTEXT
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
