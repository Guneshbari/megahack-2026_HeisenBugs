import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
  Legend,
} from 'recharts';
import { useDashboardStore } from '../../store/dashboardStore';
import type { Severity } from '../../types/telemetry';

const COLORS: Record<Severity, string> = {
  CRITICAL: '#ef4444',
  ERROR: '#f97316',
  WARNING: '#f59e0b',
  INFO: '#3b82f6',
};

export default function SeverityChart() {
  const severityDistribution = useDashboardStore((s) => s.severityDistribution);
  const data = severityDistribution;
  const subtitle = 'Server-backed breakdown for the selected time range';

  return (
    <div className="glass-panel-solid rounded-md border border-border p-4 bg-bg-surface">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-[11px] font-bold text-text-primary uppercase tracking-wider">Severity Distribution</h3>
          <p className="text-[10px] text-text-muted mt-0.5">{subtitle}</p>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={240}>
        <PieChart>
          <Pie
            data={data}
            dataKey="count"
            nameKey="severity"
            cx="50%"
            cy="50%"
            innerRadius={55}
            outerRadius={85}
            strokeWidth={2}
            stroke="#000000"
          >
            {data.map((entry) => (
              <Cell key={entry.severity} fill={COLORS[entry.severity]} />
            ))}
          </Pie>
          <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: 4, fontSize: 10, color: '#f8fafc' }} />
          <Legend verticalAlign="bottom" height={36} formatter={(value: string) => <span className="text-[10px] text-text-secondary">{value}</span>} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}
