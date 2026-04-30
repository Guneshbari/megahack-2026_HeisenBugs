/**
 * AnalyticsPage — ECharts-powered analytics
 *
 * Charts:
 *  - Event Frequency (multi-series line, matches Overview)
 *  - Severity Distribution (horizontal bar)
 *  - Top Fault Types (bar chart)
 *  - Resource Utilization (CPU/MEM avg line)
 */
import { useMemo } from 'react';
import type { EChartsOption } from 'echarts';
import { EChart } from '../components/soc/EChart';
import { useDashboardStore } from '../store/dashboardStore';
import type { FaultTypeCount, MetricPoint, SeverityCount } from '../types/telemetry';

const DARK   = '#0B1220';
const PANEL  = '#0F172A';
const BORDER = '#1E293B';
const MUTED  = '#334155';
const DIM    = '#475569';
const TEXT   = '#94A3B8';

const COLORS: Record<string, string> = {
  CRITICAL: '#DC2626',
  ERROR:    '#F97316',
  WARNING:  '#FACC15',
  INFO:     '#38BDF8',
  CPU:      '#F97316',
  MEM:      '#38BDF8',
};

const TOOLTIP_STYLE = {
  backgroundColor: PANEL,
  borderColor:     BORDER,
  borderWidth:     1,
  textStyle:       { color: '#E2E8F0', fontSize: 11, fontFamily: 'JetBrains Mono,monospace' },
  padding:         [6, 10] as [number, number],
};

const AXIS_LABEL = { color: DIM, fontSize: 9, fontFamily: 'JetBrains Mono,monospace' };
const SPLIT_LINE = { lineStyle: { color: '#151f2e', type: 'dashed' as const } };

function buildFreqOption(metrics: MetricPoint[]): EChartsOption {
  const isSparse = metrics.length <= 2;
  return {
    backgroundColor: DARK,
    animation: false,
    grid: { top: 28, right: 12, bottom: 44, left: 36, containLabel: false },
    legend: {
      top: 4, right: 12,
      textStyle: { color: DIM, fontSize: 9, fontFamily: 'JetBrains Mono,monospace' },
      itemWidth: 10, itemHeight: 2,
    },
    tooltip: { trigger: 'axis', axisPointer: { lineStyle: { color: BORDER } }, ...TOOLTIP_STYLE },
    xAxis: {
      type: 'category',
      boundaryGap: metrics.length === 1,
      data: metrics.map((m) => new Date(m.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })),
      axisLabel: { ...AXIS_LABEL, interval: Math.max(0, Math.floor(metrics.length / 6) - 1) },
      axisLine: { lineStyle: { color: MUTED } },
      splitLine: { show: false },
      axisTick: { show: false },
    },
    yAxis: { type: 'value', axisLabel: AXIS_LABEL, axisLine: { show: false }, axisTick: { show: false }, splitLine: SPLIT_LINE },
    dataZoom: [{ type: 'inside' }, { type: 'slider', height: 12, bottom: 2, showDetail: false }],
    series: [
      { name: 'CRITICAL', type: 'line', data: metrics.map((m) => m.critical_count), lineStyle: { color: '#DC2626', width: 1 }, symbol: isSparse ? 'circle' : 'none', symbolSize: isSparse ? 7 : 4, showSymbol: isSparse },
      { name: 'ERROR',    type: 'line', data: metrics.map((m) => m.error_count),    lineStyle: { color: '#F97316', width: 1 }, symbol: isSparse ? 'circle' : 'none', symbolSize: isSparse ? 7 : 4, showSymbol: isSparse },
      { name: 'WARNING',  type: 'line', data: metrics.map((m) => m.warning_count),  lineStyle: { color: '#FACC15', width: 1 }, symbol: isSparse ? 'circle' : 'none', symbolSize: isSparse ? 7 : 4, showSymbol: isSparse },
      { name: 'INFO',     type: 'line', data: metrics.map((m) => m.info_count),     lineStyle: { color: '#38BDF8', width: 1, opacity: 0.5 }, symbol: isSparse ? 'circle' : 'none', symbolSize: isSparse ? 7 : 4, showSymbol: isSparse },
    ],
  };
}

function buildSevOption(severities: SeverityCount[]): EChartsOption {
  const total = severities.reduce((s, x) => s + x.count, 0) || 1;
  return {
    backgroundColor: DARK,
    animation: false,
    grid: { top: 16, right: 16, bottom: 16, left: 8, containLabel: true },
    tooltip: { trigger: 'axis', ...TOOLTIP_STYLE },
    xAxis: { type: 'value', axisLabel: AXIS_LABEL, axisLine: { show: false }, axisTick: { show: false }, splitLine: SPLIT_LINE },
    yAxis: {
      type: 'category',
      data: severities.map((s) => s.severity),
      axisLabel: { ...AXIS_LABEL, color: TEXT },
      axisLine: { show: false },
      axisTick: { show: false },
    },
    series: [{
      type: 'bar',
      data: severities.map((s) => ({
        value: s.count,
        itemStyle: { color: COLORS[s.severity] ?? '#475569' },
        label: {
          show: true, position: 'right',
          formatter: `{c} (${((s.count / total) * 100).toFixed(0)}%)`,
          color: DIM, fontSize: 9, fontFamily: 'JetBrains Mono,monospace',
        },
      })),
      barMaxWidth: 16,
    }],
  };
}

function buildFaultOption(faults: FaultTypeCount[]): EChartsOption {
  const sorted = [...faults].sort((a, b) => b.count - a.count).slice(0, 8);
  return {
    backgroundColor: DARK,
    animation: false,
    grid: { top: 16, right: 40, bottom: 16, left: 8, containLabel: true },
    tooltip: { trigger: 'axis', ...TOOLTIP_STYLE },
    xAxis: { type: 'value', axisLabel: AXIS_LABEL, axisLine: { show: false }, axisTick: { show: false }, splitLine: SPLIT_LINE },
    yAxis: {
      type: 'category',
      data: sorted.map((f) => f.fault_type),
      axisLabel: { ...AXIS_LABEL, color: TEXT, overflow: 'truncate', width: 100 },
      axisLine: { show: false }, axisTick: { show: false },
    },
    series: [{
      type: 'bar',
      data: sorted.map((f) => ({
        value: f.count,
        itemStyle: { color: '#38BDF8' },
        label: {
          show: true, position: 'right',
          formatter: '{c}', color: DIM, fontSize: 9, fontFamily: 'JetBrains Mono,monospace',
        },
      })),
      barMaxWidth: 14,
    }],
  };
}

function buildResourceOption(metrics: MetricPoint[]): EChartsOption {
  const isSparse = metrics.length <= 2;
  return {
    backgroundColor: DARK,
    animation: false,
    grid: { top: 28, right: 12, bottom: 30, left: 36, containLabel: false },
    legend: {
      top: 4, right: 12,
      textStyle: { color: DIM, fontSize: 9, fontFamily: 'JetBrains Mono,monospace' },
      itemWidth: 10, itemHeight: 2,
    },
    tooltip: { trigger: 'axis', ...TOOLTIP_STYLE },
    xAxis: {
      type: 'category',
      boundaryGap: metrics.length === 1,
      data: metrics.map((m) => new Date(m.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })),
      axisLabel: { ...AXIS_LABEL, interval: Math.max(0, Math.floor(metrics.length / 6) - 1) },
      axisLine: { lineStyle: { color: MUTED } },
      splitLine: { show: false }, axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      min: 0, max: 100,
      axisLabel: { ...AXIS_LABEL, formatter: '{value}%' },
      axisLine: { show: false }, axisTick: { show: false }, splitLine: SPLIT_LINE,
    },
    series: [
      { name: 'CPU',  type: 'line', data: metrics.map((m) => Number(m.avg_cpu ?? 0)),    lineStyle: { color: '#F97316', width: 1 }, symbol: isSparse ? 'circle' : 'none', symbolSize: isSparse ? 7 : 4, showSymbol: isSparse },
      { name: 'MEM',  type: 'line', data: metrics.map((m) => Number(m.avg_memory ?? 0)), lineStyle: { color: '#38BDF8', width: 1 }, symbol: isSparse ? 'circle' : 'none', symbolSize: isSparse ? 7 : 4, showSymbol: isSparse },
    ],
  };
}

function ChartPanel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="soc-panel flex flex-col overflow-hidden" style={{ minHeight: 0 }}>
      <div className="soc-panel-header">
        <span className="soc-panel-title">{title}</span>
      </div>
      <div style={{ flex: 1, minHeight: 0 }}>
        {children}
      </div>
    </div>
  );
}

export default function AnalyticsPage() {
  const metrics = useDashboardStore((s) => s.metrics);
  const severityDistribution = useDashboardStore((s) => s.severityDistribution);
  const faultDistribution = useDashboardStore((s) => s.faultDistribution);

  const freqOption     = useMemo(() => buildFreqOption(metrics),              [metrics]);
  const sevOption      = useMemo(() => buildSevOption(severityDistribution),  [severityDistribution]);
  const faultOption    = useMemo(() => buildFaultOption(faultDistribution),   [faultDistribution]);
  const resourceOption = useMemo(() => buildResourceOption(metrics),          [metrics]);

  return (
    <div className="flex flex-col h-full gap-1" style={{ minHeight: 0 }}>
      {/* Title bar */}
      <div
        className="flex items-center px-3 border border-[#1E293B] flex-shrink-0"
        style={{ height: 28, background: '#0F172A' }}
      >
        <span className="font-mono text-[10px] text-[#E2E8F0] font-semibold uppercase tracking-wider">
          Analytics
        </span>
        <span className="font-mono text-[10px] text-[#475569] ml-3">
          {metrics.length} time buckets · scroll/zoom enabled
        </span>
      </div>

      {/* Top row: event freq (spans full width) */}
      <ChartPanel title="Event Frequency  (CRITICAL · ERROR · WARNING · INFO)">
        <div style={{ height: 180 }}>
          <EChart option={freqOption} />
        </div>
      </ChartPanel>

      {/* Bottom row: 3 charts */}
      <div
        className="grid gap-1 flex-1 min-h-0"
        style={{ gridTemplateColumns: '1fr 1fr 1fr', minHeight: 0 }}
      >
        <ChartPanel title="Severity Distribution">
          <EChart option={sevOption} />
        </ChartPanel>

        <ChartPanel title="Top Fault Types">
          <EChart option={faultOption} />
        </ChartPanel>

        <ChartPanel title="Resource Utilization (CPU / MEM avg %)">
          <EChart option={resourceOption} />
        </ChartPanel>
      </div>
    </div>
  );
}
