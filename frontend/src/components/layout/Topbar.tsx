import { useState, useRef, useEffect } from 'react';
import { Bell, Clock, RefreshCw, ChevronDown, Server, Activity, AlertTriangle, Zap, LogOut } from 'lucide-react';
import {
  getOnlineSystems,
  getDegradedSystems,
  getCriticalAlertCount,
  getTotalEventCount,
} from '../../data/mockData';
import { fetchRecentAlerts } from '../../lib/api';
import { TIME_RANGE_LABELS, REFRESH_LABELS, type TimeRange, type AutoRefresh } from '../../lib/dashboardDerived';
import { useDashboardStore } from '../../store/dashboardStore';
import { useAuth } from '../../context/AuthContext';

const TIME_RANGES: TimeRange[] = ['5m', '15m', '1h', '6h', '24h'];
const REFRESH_OPTIONS: AutoRefresh[] = ['off', '5s', '10s', '30s', '1m'];

export default function Topbar() {
  const systems = useDashboardStore((s) => s.systems);
  const alerts = useDashboardStore((s) => s.alerts);
  const allEvents = useDashboardStore((s) => s.allEvents);
  const timeRange = useDashboardStore((s) => s.timeRange);
  const setTimeRange = useDashboardStore((s) => s.setTimeRange);
  const autoRefresh = useDashboardStore((s) => s.autoRefresh);
  const setAutoRefresh = useDashboardStore((s) => s.setAutoRefresh);
  const online = getOnlineSystems(systems);
  const degraded = getDegradedSystems(systems);
  const criticals = getCriticalAlertCount(alerts);
  const totalEvents = getTotalEventCount(allEvents);
  const { user, logout } = useAuth();

  const [showTimeDropdown, setShowTimeDropdown] = useState(false);
  const [showRefreshDropdown, setShowRefreshDropdown] = useState(false);
  const [showUserDropdown, setShowUserDropdown] = useState(false);
  const [showAlertsDropdown, setShowAlertsDropdown] = useState(false);
  const [pipelineStatus, setPipelineStatus] = useState<{ status: string; delay_seconds: number }>({ status: 'OK', delay_seconds: 0 });
  const timeRef = useRef<HTMLDivElement>(null);
  const refreshRef = useRef<HTMLDivElement>(null);
  const userRef = useRef<HTMLDivElement>(null);
  const notificationRef = useRef<HTMLDivElement>(null);
  const [recentAlerts, setRecentAlerts] = useState<any[]>([]);

  useEffect(() => {
    const fetchHealthAndAlerts = async () => {
      try {
        const res = await fetch('http://localhost:8000/pipeline-health/status');
        const data = await res.json();
        setPipelineStatus(data);
      } catch (e) {
        setPipelineStatus({ status: 'DOWN', delay_seconds: 999 });
      }

      try {
        const fetchAlertData = await fetchRecentAlerts();
        setRecentAlerts(fetchAlertData);
      } catch(e) {}
    };
    fetchHealthAndAlerts();
    const interval = setInterval(fetchHealthAndAlerts, 10000);
    return () => clearInterval(interval);
  }, []);

  // Get user initials
  const initials = user?.name
    ? user.name.split(' ').map((w) => w[0]).join('').toUpperCase().slice(0, 2)
    : 'U';

  // Close dropdowns on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (timeRef.current && !timeRef.current.contains(e.target as Node)) setShowTimeDropdown(false);
      if (refreshRef.current && !refreshRef.current.contains(e.target as Node)) setShowRefreshDropdown(false);
      if (userRef.current && !userRef.current.contains(e.target as Node)) setShowUserDropdown(false);
      if (notificationRef.current && !notificationRef.current.contains(e.target as Node)) setShowAlertsDropdown(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  return (
    <header className="fixed top-0 left-[220px] right-0 h-[48px] bg-bg-surface border-b border-border flex items-center justify-between px-5 z-40">
      {/* Status Summary Strip */}
      <div className="flex items-center gap-4">
        {/* Systems Online */}
        <div className="flex items-center gap-1.5">
          <Server className="w-3.5 h-3.5 text-signal-highlight" />
          <span className="text-xs font-semibold text-signal-highlight">{online}</span>
          <span className="text-[10px] text-text-muted">online</span>
        </div>

        <span className="w-px h-4 bg-border" />

        {/* Degraded */}
        <div className="flex items-center gap-1.5">
          <Activity className="w-3.5 h-3.5 text-accent-amber" />
          <span className="text-xs font-semibold text-accent-amber">{degraded}</span>
          <span className="text-[10px] text-text-muted">degraded</span>
        </div>

        <span className="w-px h-4 bg-border" />

        {/* Critical Alerts */}
        <div className="flex items-center gap-1.5">
          <AlertTriangle className={`w-3.5 h-3.5 ${criticals > 0 ? 'text-accent-red' : 'text-text-muted'}`} />
          <span className={`text-xs font-semibold ${criticals > 0 ? 'text-accent-red' : 'text-text-muted'}`}>{criticals}</span>
          <span className="text-[10px] text-text-muted">critical</span>
        </div>

        <span className="w-px h-4 bg-border" />

        {/* Events */}
        <div className="flex items-center gap-1.5">
          <Zap className="w-3.5 h-3.5 text-signal-primary" />
          <span className="text-xs font-semibold text-signal-primary">{totalEvents}</span>
          <span className="text-[10px] text-text-muted">events</span>
        </div>

        <span className="w-px h-4 bg-border" />

        {/* Pipeline health */}
        <div className="flex items-center gap-1.5" title={`Lag: ${pipelineStatus.delay_seconds}s`}>
          <span className={`w-2 h-2 rounded-full ${
            pipelineStatus.status === 'OK' ? 'bg-signal-highlight' :
            pipelineStatus.status === 'DEGRADED' ? 'bg-accent-amber' :
            'bg-accent-red'
          }`} />
          <span className="text-[10px] text-text-muted">Pipeline {pipelineStatus.status}</span>
        </div>
      </div>

      {/* Right side — Time controls + notifications */}
      <div className="flex items-center gap-3">
        {/* Time Range Selector */}
        <div ref={timeRef} className="relative">
          <button
            onClick={() => { setShowTimeDropdown(!showTimeDropdown); setShowRefreshDropdown(false); setShowUserDropdown(false); }}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded border border-border bg-bg-surface text-xs text-text-primary hover:bg-bg-hover transition-colors"
          >
            <Clock className="w-3.5 h-3.5 text-signal-primary" />
            <span className="font-medium">{TIME_RANGE_LABELS[timeRange]}</span>
            <ChevronDown className="w-3 h-3 opacity-50" />
          </button>
          {showTimeDropdown && (
            <div className="absolute right-0 top-full mt-1 w-40 bg-bg-surface border border-border rounded shadow-lg z-50 animate-fade-in py-1">
              {TIME_RANGES.map((r) => (
                <button
                  key={r}
                  onClick={() => { setTimeRange(r); setShowTimeDropdown(false); }}
                  className={`w-full text-left px-3 py-2 text-xs transition-colors ${
                    timeRange === r
                      ? 'text-signal-primary bg-signal-primary/10'
                      : 'text-text-secondary hover:text-text-primary hover:bg-bg-hover'
                  }`}
                >
                  {TIME_RANGE_LABELS[r]}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Auto-Refresh Selector */}
        <div ref={refreshRef} className="relative">
          <button
            onClick={() => { setShowRefreshDropdown(!showRefreshDropdown); setShowTimeDropdown(false); setShowUserDropdown(false); }}
            className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded border text-xs transition-colors ${
              autoRefresh !== 'off'
                ? 'border-signal-highlight/40 bg-signal-highlight/10 text-signal-highlight'
                : 'border-border bg-bg-surface text-text-primary hover:bg-bg-hover'
            }`}
          >
            <RefreshCw className={`w-3.5 h-3.5 ${autoRefresh !== 'off' ? 'animate-spin' : ''}`} style={autoRefresh !== 'off' ? { animationDuration: '3s' } : {}} />
            <span className="font-medium">{autoRefresh === 'off' ? 'Auto' : REFRESH_LABELS[autoRefresh]}</span>
          </button>
          {showRefreshDropdown && (
            <div className="absolute right-0 top-full mt-1 w-32 bg-bg-surface border border-border rounded shadow-lg z-50 animate-fade-in py-1">
              {REFRESH_OPTIONS.map((r) => (
                <button
                  key={r}
                  onClick={() => { setAutoRefresh(r); setShowRefreshDropdown(false); }}
                  className={`w-full text-left px-3 py-2 text-xs transition-colors ${
                    autoRefresh === r
                      ? 'text-signal-primary bg-signal-primary/10'
                      : 'text-text-secondary hover:text-text-primary hover:bg-bg-hover'
                  }`}
                >
                  {r === 'off' ? 'Off' : `Every ${REFRESH_LABELS[r]}`}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Notifications */}
        <div className="relative" ref={notificationRef}>
          <button 
            onClick={() => { setShowAlertsDropdown(!showAlertsDropdown); setShowTimeDropdown(false); setShowRefreshDropdown(false); setShowUserDropdown(false); }}
            className="relative p-1.5 rounded text-text-secondary hover:text-text-primary hover:bg-bg-hover transition-colors"
          >
            <Bell className="w-4 h-4" />
            <span className="absolute -top-0.5 -right-0.5 w-4 h-4 bg-accent-red rounded-full flex items-center justify-center text-[9px] font-bold text-white">
              {criticals + degraded}
            </span>
          </button>
          
          {showAlertsDropdown && (
            <div className="absolute right-0 top-full mt-1 w-80 bg-bg-surface border border-border rounded shadow-lg z-50 animate-fade-in max-h-[400px] overflow-y-auto py-1">
              <h4 className="px-4 py-2 text-xs font-semibold border-b border-border text-text-primary">Recent Alerts</h4>
              <div className="flex flex-col">
                {recentAlerts.length > 0 ? recentAlerts.map(a => (
                  <div key={a.alert_id} className="px-4 py-3 border-b border-border/50 hover:bg-bg-hover text-left flex flex-col gap-1 transition-colors">
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-bold text-text-primary truncate pr-2">{a.title}</span>
                      <span className={`text-[9px] uppercase font-bold px-1.5 py-0.5 rounded ${a.severity === 'CRITICAL' ? 'bg-accent-red/20 text-accent-red' : a.severity === 'ERROR' ? 'bg-accent-orange/20 text-accent-orange' : 'bg-accent-amber/20 text-accent-amber'}`}>{a.severity}</span>
                    </div>
                    <span className="text-[10px] text-text-muted">{a.hostname} • {new Date(a.triggered_at).toLocaleTimeString()}</span>
                  </div>
                )) : (
                  <div className="px-4 py-4 text-xs text-text-muted text-center">No unacknowledged recent alerts</div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* User Avatar with Dropdown */}
        <div ref={userRef} className="relative">
          <button
            onClick={() => { setShowUserDropdown(!showUserDropdown); setShowTimeDropdown(false); setShowRefreshDropdown(false); }}
            className="w-7 h-7 rounded flex items-center justify-center text-[10px] font-semibold text-signal-primary border border-border bg-bg-surface cursor-pointer hover:bg-bg-hover transition-colors"
          >
            {initials}
          </button>
          {showUserDropdown && (
            <div className="absolute right-0 top-full mt-1 w-52 bg-bg-surface border border-border rounded shadow-lg z-50 animate-fade-in py-1">
              <div className="px-3 py-2 border-b border-border">
                <p className="text-xs font-semibold text-text-primary">{user?.name}</p>
                <p className="text-[10px] text-text-muted mt-0.5">{user?.email}</p>
              </div>
              <button
                onClick={() => { setShowUserDropdown(false); logout(); }}
                className="w-full flex items-center gap-2 px-3 py-2 mt-1 text-xs text-text-secondary hover:text-accent-red hover:bg-bg-hover transition-colors"
              >
                <LogOut className="w-3.5 h-3.5" />
                Sign Out
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

