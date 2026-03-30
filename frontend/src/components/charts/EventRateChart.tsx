import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { formatTimeShort } from '../../data/mockData';
import { useDashboardStore } from '../../store/dashboardStore';

export default function EventRateChart() {
  const metrics = useDashboardStore((s) => s.metrics);
  const data = metrics.map((m) => ({
    time: formatTimeShort(m.timestamp),
    events: m.event_count,
  }));

  return (
    <div className="glass-panel-solid rounded-md border border-border p-4 bg-bg-surface">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-[11px] font-bold text-text-primary uppercase tracking-wider">Event Rate</h3>
          <p className="text-[10px] text-text-muted mt-0.5">Events per hour over the last 24 hours</p>
        </div>
      </div>
      <div>
        <ResponsiveContainer width="100%" height={240}>
          <AreaChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" strokeOpacity={0.25} vertical={false} />
            <XAxis dataKey="time" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={{ stroke: '#1e293b' }} tickLine={false} />
            <YAxis tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
            <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: 4, fontSize: 10, color: '#f8fafc' }} />
            <Area type="monotone" dataKey="events" stroke="#3b82f6" strokeWidth={1.5} fill="#3b82f6" fillOpacity={0.1} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
