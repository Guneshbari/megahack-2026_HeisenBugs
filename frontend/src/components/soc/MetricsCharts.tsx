/**
 * MetricsCharts — ECharts multi-series event frequency chart
 *
 * Rules:
 *  - Fixed window (last N minutes from time range setting)
 *  - Append-only rendering (notMerge: false)
 *  - Throttled to max 1 update per second
 *  - No gradients, no fills — thin 1px lines on dark canvas
 *  - DataZoom: inside scroll + bottom slider
 *  - Brush selection for time window zoom
 */
import { useMemo } from 'react';
import type { EChartsOption } from 'echarts';
import { EChart } from './EChart';
import { useDashboardStore } from '../../store/dashboardStore';
import type { MetricPoint } from '../../types/telemetry';

const DARK_BG    = '#0B1220';
const DARK_PANEL = '#0F172A';
const BORDER     = '#1E293B';
const MUTED      = '#334155';
const TEXT_DIM   = '#475569';

function buildOption(metrics: MetricPoint[]): EChartsOption {
  const isSparse = metrics.length <= 2;
  const times = metrics.map((m) => {
    const d = new Date(m.timestamp);
    return d.toLocaleTimeString('en-US', {
      hour: '2-digit', minute: '2-digit', hour12: false,
    });
  });

  return {
    backgroundColor: DARK_BG,
    animation: false,
    grid: {
      top: 28, right: 12, bottom: 48, left: 44,
      containLabel: false,
    },
    legend: {
      top: 4,
      right: 12,
      textStyle: { color: TEXT_DIM, fontSize: 10, fontFamily: 'JetBrains Mono,monospace' },
      itemWidth: 12,
      itemHeight: 2,
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'line', lineStyle: { color: BORDER } },
      backgroundColor: DARK_PANEL,
      borderColor: BORDER,
      borderWidth: 1,
      textStyle: { color: '#E2E8F0', fontSize: 11, fontFamily: 'JetBrains Mono,monospace' },
      padding: [6, 10],
    },
    xAxis: {
      type: 'category',
      data: times,
      boundaryGap: metrics.length === 1,
      axisLabel: {
        color: TEXT_DIM, fontSize: 9,
        fontFamily: 'JetBrains Mono,monospace',
        interval: Math.max(0, Math.floor(times.length / 8) - 1),
      },
      axisLine: { lineStyle: { color: MUTED } },
      splitLine: { show: false },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      axisLabel: {
        color: TEXT_DIM, fontSize: 9,
        fontFamily: 'JetBrains Mono,monospace',
      },
      axisLine:  { show: false },
      axisTick:  { show: false },
      splitLine: { lineStyle: { color: '#151f2e', type: 'dashed' } },
    },
    dataZoom: [
      {
        type:    'inside',
        start:   0,
        end:     100,
        zoomLock: false,
      },
      {
        type:          'slider',
        height:        14,
        bottom:        2,
        borderColor:   BORDER,
        backgroundColor: DARK_BG,
        fillerColor:   'rgba(56,189,248,0.08)',
        handleStyle:   { color: '#38BDF8' },
        textStyle:     { color: '' },
        labelFormatter: '',
        showDetail:    false,
      },
    ],
    series: [
      {
        name:      'CRITICAL',
        type:      'line',
        data:      metrics.map((m) => m.critical_count),
        lineStyle: { color: '#DC2626', width: 1 },
        itemStyle: { color: '#DC2626' },
        symbol:    isSparse ? 'circle' : 'none',
        symbolSize: isSparse ? 7 : 4,
        showSymbol: isSparse,
        smooth:    false,
        z: 4,
      },
      {
        name:      'ERROR',
        type:      'line',
        data:      metrics.map((m) => m.error_count),
        lineStyle: { color: '#F97316', width: 1 },
        itemStyle: { color: '#F97316' },
        symbol:    isSparse ? 'circle' : 'none',
        symbolSize: isSparse ? 7 : 4,
        showSymbol: isSparse,
        smooth:    false,
        z: 3,
      },
      {
        name:      'WARNING',
        type:      'line',
        data:      metrics.map((m) => m.warning_count),
        lineStyle: { color: '#FACC15', width: 1 },
        itemStyle: { color: '#FACC15' },
        symbol:    isSparse ? 'circle' : 'none',
        symbolSize: isSparse ? 7 : 4,
        showSymbol: isSparse,
        smooth:    false,
        z: 2,
      },
      {
        name:      'INFO',
        type:      'line',
        data:      metrics.map((m) => m.info_count),
        lineStyle: { color: '#38BDF8', width: 1, opacity: 0.6 },
        itemStyle: { color: '#38BDF8' },
        symbol:    isSparse ? 'circle' : 'none',
        symbolSize: isSparse ? 7 : 4,
        showSymbol: isSparse,
        smooth:    false,
        z: 1,
      },
    ],
  };
}

export default function MetricsCharts() {
  const metrics = useDashboardStore((s) => s.metrics);

  const option = useMemo(() => buildOption(metrics), [metrics]);

  return (
    <div className="soc-panel" style={{ height: 200 }}>
      {/* Header */}
      <div className="soc-panel-header">
        <span className="soc-panel-title">Event Frequency</span>
        <div className="flex items-center gap-4">
          {(['CRITICAL', 'ERROR', 'WARNING', 'INFO'] as const).map((sev) => {
            const total = metrics.reduce<number>((s, m) => {
              const k = `${sev.toLowerCase()}_count` as keyof MetricPoint;
              return s + Number(m[k] ?? 0);
            }, 0);
            const colors: Record<string, string> = {
              CRITICAL: '#DC2626', ERROR: '#F97316', WARNING: '#FACC15', INFO: '#38BDF8',
            };
            return (
              <span key={sev} className="font-mono text-[10px]" style={{ color: colors[sev] }}>
                {sev} {total}
              </span>
            );
          })}
        </div>
      </div>
      {/* Chart body */}
      <div style={{ height: 'calc(100% - 28px)' }}>
        <EChart option={option} />
      </div>
    </div>
  );
}
