/**
 * SOC DashboardLayout
 *
 * Layout: 160px sidebar (fixed-left) + full-height main content area
 * Topbar: 32px single-line strip
 * Main: full remaining height, zero padding, scroll per-page
 */
import { Outlet, NavLink } from 'react-router-dom';
import { useEffect } from 'react';
import {
  LayoutDashboard,
  Search,
  Server,
  AlertTriangle,
  BarChart3,
  Shield,
  LogOut,
} from 'lucide-react';
import { useAuth } from '../../context/AuthContext';
import { useDashboardStore } from '../../store/dashboardStore';
import { useSignalStore } from '../../store/signalStore';

const NAV_ITEMS = [
  { to: '/',          icon: LayoutDashboard, label: 'Overview'   },
  { to: '/events',    icon: Search,          label: 'Events'     },
  { to: '/systems',   icon: Server,          label: 'Systems'    },
  { to: '/alerts',    icon: AlertTriangle,   label: 'Alerts'     },
  { to: '/analytics', icon: BarChart3,       label: 'Analytics'  },
];

export default function DashboardLayout() {
  const auth = useAuth();
  const filteredAlerts = useDashboardStore((s) => s.filteredAlerts);
  const pipelineHealth = useDashboardStore((s) => s.pipelineHealth);
  const allEvents = useDashboardStore((s) => s.allEvents);
  const systems = useDashboardStore((s) => s.systems);
  const loadData = useDashboardStore((s) => s.loadData);
  const tickRefresh = useDashboardStore((s) => s.tickRefresh);
  const autoRefresh = useDashboardStore((s) => s.autoRefresh);
  const isConnected = useSignalStore((s) => s.isConnected);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    if (!autoRefresh || autoRefresh === 'off') return;
    // Map autoRefresh to ms manually or import REFRESH_MS from lib
    const msMap: Record<string, number> = { '5s': 5000, '10s': 10000, '30s': 30000, '1m': 60000 };
    const interval = msMap[autoRefresh];
    if (!interval) return;
    const id = setInterval(() => {
      tickRefresh();
    }, interval);
    return () => clearInterval(id);
  }, [autoRefresh, tickRefresh]);

  const criticalCount = filteredAlerts.filter((a) => !a.acknowledged && a.severity === 'CRITICAL').length;

  return (
    <div
      style={{
        display:         'grid',
        gridTemplateColumns: '160px 1fr',
        gridTemplateRows:    '32px 1fr',
        height:          '100vh',
        background:      '#0A0F14',
        overflow:        'hidden',
      }}
    >
      {/* ── Top bar (spans both columns) ──────────────── */}
      <div
        style={{
          gridColumn:    '1 / -1',
          gridRow:       1,
          display:       'flex',
          alignItems:    'center',
          justifyContent:'space-between',
          padding:       '0 12px',
          borderBottom:  '1px solid #1F2A37',
          background:    '#111927',
          flexShrink:    0,
          zIndex:        50,
        }}
      >
        {/* Left: wordmark */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Shield style={{ width: 14, height: 14, color: '#38BDF8' }} />
          <span style={{
            fontFamily: 'JetBrains Mono, monospace',
            fontSize:   11,
            fontWeight: 700,
            color:      '#E2E8F0',
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
          }}>
            SentinelCore
          </span>
          <span style={{
            fontFamily: 'JetBrains Mono, monospace',
            fontSize:   9,
            color:      '#6B7C93',
            letterSpacing: '0.1em',
            marginLeft: 4,
          }}>
            SOC CONSOLE
          </span>
        </div>

        {/* Middle: Pipeline stats */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          {pipelineHealth && (
            <>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 9, color: '#6B7C93' }}>EPS</span>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 10, color: '#E6EDF3', fontWeight: 600 }}>{pipelineHealth.events_per_sec.toFixed(1)}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 9, color: '#6B7C93' }}>KAFKA LAG</span>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 10, color: pipelineHealth.kafka_lag > 5000 ? '#FF8A00' : '#E6EDF3', fontWeight: 600 }}>{pipelineHealth.kafka_lag.toLocaleString()}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 9, color: '#6B7C93' }}>DB WRITE</span>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 10, color: '#E6EDF3', fontWeight: 600 }}>{pipelineHealth.db_write_rate.toFixed(1)}/s</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 9, color: '#6B7C93' }}>CONSUMER</span>
                <span style={{ fontFamily: 'Inter,monospace', fontSize: 10, color: pipelineHealth.lag_status === 'healthy' ? '#00C853' : '#FF3B3B', textTransform: 'uppercase', fontWeight: 600 }}>{pipelineHealth.lag_status}</span>
              </div>
            </>
          )}
        </div>

        {/* Right: status + logout */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          {criticalCount > 0 && (
            <span style={{
              fontFamily: 'Inter,monospace',
              fontSize: 10,
              fontWeight: 700,
              color: '#FF3B3B',
              animation: 'soc-blink 1s step-end infinite',
            }}>
              ● {criticalCount} Critical Alert{criticalCount !== 1 && 's'}
            </span>
          )}
          <span style={{
            fontFamily: 'Inter,monospace',
            fontSize: 9,
            color: isConnected ? '#00C853' : '#FFD600',
          }}>
            {isConnected ? '● LIVE' : '◌ MOCK'}
          </span>
          <span style={{
            fontFamily: 'Inter,monospace',
            fontSize: 9,
            color: '#6B7C93',
          }}>
            {new Date().toLocaleTimeString('en-US', { hour12: false })}
          </span>
          <button
            onClick={() => auth.logout()}
            style={{
              display: 'flex', alignItems: 'center', gap: 4,
              background: 'none', border: '1px solid #1F2A37',
              padding: '0 8px', height: 20,
              fontFamily: 'Inter,monospace', fontSize: 9,
              color: '#9FB3C8', cursor: 'pointer',
            }}
          >
            <LogOut style={{ width: 10, height: 10 }} />
            SIGN OUT
          </button>
        </div>
      </div>

      {/* ── Sidebar ─────────────────────────────────────── */}
      <aside
        style={{
          gridColumn: 1,
          gridRow:    2,
          borderRight: '1px solid #1F2A37',
          background:  '#111927',
          display:     'flex',
          flexDirection: 'column',
          overflow:    'hidden',
        }}
      >
        <nav style={{ flex: 1, padding: '10px 0', display: 'flex', flexDirection: 'column', gap: 4 }}>
          {NAV_ITEMS.map(({ to, icon: Icon, label }) => {
            let countStr = '';
            if (label === 'Alerts' && filteredAlerts.length > 0) countStr = String(filteredAlerts.length);
            if (label === 'Events' && allEvents.length > 0) countStr = String(allEvents.length);
            if (label === 'Systems' && systems.length > 0) countStr = String(systems.length);

            return (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                style={({ isActive }) => ({
                  display:     'flex',
                  alignItems:  'center',
                  gap:         12,
                  padding:     '10px 14px',
                  margin:      '0 8px',
                  borderRadius: 4,
                  fontFamily:  'Inter, sans-serif',
                  fontSize:    13,
                  fontWeight:  500,
                  color:       isActive ? '#E6EDF3' : '#9FB3C8',
                  background:  isActive ? '#1E293B' : 'transparent',
                  textDecoration: 'none',
                  transition:  'background 80ms, color 80ms',
                })}
              >
                <Icon style={{ width: 16, height: 16, flexShrink: 0, color: '#6B7C93' }} />
                {label}
                {countStr && (
                  <span style={{
                    marginLeft: 'auto',
                    fontFamily: 'Inter,monospace',
                    fontSize: 10,
                    color: label === 'Alerts' && criticalCount > 0 ? '#FF3B3B' : '#6B7C93',
                    fontWeight: 600,
                  }}>
                    {countStr}
                  </span>
                )}
              </NavLink>
            );
          })}
        </nav>

        {/* Footer */}
        <div
          style={{
            padding: '10px 14px',
            borderTop: '1px solid #1F2A37',
            flexShrink: 0,
          }}
        >
          <div style={{ fontFamily: 'Inter,monospace', fontSize: 10, color: '#6B7C93' }}>
            v0.3.0 · mock mode
          </div>
        </div>
      </aside>

      {/* ── Main content ─────────────────────────────────── */}
      <main
        style={{
          gridColumn: 2,
          gridRow:    2,
          overflow:   'hidden',
          display:    'flex',
          flexDirection: 'column',
          padding:    0,
          background: '#0A0F14',
        }}
      >
        <Outlet />
      </main>
    </div>
  );
}
