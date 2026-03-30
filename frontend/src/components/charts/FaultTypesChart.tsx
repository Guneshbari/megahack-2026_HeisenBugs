import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { useDashboardStore } from '../../store/dashboardStore';

export default function FaultTypesChart() {
  const faultDistribution = useDashboardStore((s) => s.faultDistribution);
  const data = faultDistribution.slice(0, 5);
  const subtitle = 'Most common categories across the selected time range';

  return (
    <div className="glass-panel-solid rounded-md border border-border p-4 bg-bg-surface">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-[11px] font-bold text-text-primary uppercase tracking-wider">Top Fault Types</h3>
          <p className="text-[10px] text-text-muted mt-0.5">{subtitle}</p>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={data} layout="vertical" margin={{ top: 0, right: 10, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" strokeOpacity={0.25} horizontal={false} />
          <XAxis type="number" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={{ stroke: '#1e293b' }} tickLine={false} />
          <YAxis type="category" dataKey="fault_type" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} width={110} />
          <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: 4, fontSize: 10, color: '#f8fafc' }} />
          <Bar dataKey="count" fill="#3b82f6" radius={[0, 4, 4, 0]} barSize={20} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
