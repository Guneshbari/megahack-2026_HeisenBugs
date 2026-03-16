import { useState, useRef, useEffect } from 'react';
import { Bell, Clock, RefreshCw, ChevronDown, Server, Activity, AlertTriangle, Zap, LogOut } from 'lucide-react';
import {
  getOnlineSystems,
  getDegradedSystems,
  getCriticalAlertCount,
  getTotalEventCount,
} from '../../data/mockData';
import { useDashboard, TIME_RANGE_LABELS, REFRESH_LABELS, type TimeRange, type AutoRefresh } from '../../context/DashboardContext';
import { useAuth } from '../../context/AuthContext';

const TIME_RANGES: TimeRange[] = ['5m', '15m', '1h', '6h', '24h'];
const REFRESH_OPTIONS: AutoRefresh[] = ['off', '5s', '10s', '30s', '1m'];

export default function Topbar() {
  const { systems, alerts, allEvents, timeRange, setTimeRange, autoRefresh, setAutoRefresh } = useDashboard();
  const online = getOnlineSystems(systems);
  const degraded = getDegradedSystems(systems);
  const criticals = getCriticalAlertCount(alerts);
  const totalEvents = getTotalEventCount(allEvents);
  const { user, logout } = useAuth();

  const [showTimeDropdown, setShowTimeDropdown] = useState(false);
  const [showRefreshDropdown, setShowRefreshDropdown] = useState(false);
  const [showUserDropdown, setShowUserDropdown] = useState(false);
  const timeRef = useRef<HTMLDivElement>(null);
  const refreshRef = useRef<HTMLDivElement>(null);
  const userRef = useRef<HTMLDivElement>(null);

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
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  return (
    <header className="fixed top-0 left-[220px] right-0 h-[56px] bg-bg-surface/70 backdrop-blur-xl border-b border-border flex items-center justify-between px-5 z-40">
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
          <AlertTriangle className={`w-3.5 h-3.5 ${criticals > 0 ? 'text-accent-red neon-red' : 'text-text-muted'}`} />
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
        <div className="flex items-center gap-1.5">
          <span className="w-2 h-2 rounded-full bg-signal-highlight shadow-[0_0_6px_rgba(34,197,94,0.5)]" />
          <span className="text-[10px] text-text-muted">Pipeline OK</span>
        </div>
      </div>

      {/* Right side — Time controls + notifications */}
      <div className="flex items-center gap-3">
        {/* Time Range Selector */}
        <div ref={timeRef} className="relative">
          <button
            onClick={() => { setShowTimeDropdown(!showTimeDropdown); setShowRefreshDropdown(false); setShowUserDropdown(false); }}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-border bg-bg-primary/50 text-xs text-text-secondary hover:text-text-primary hover:border-signal-primary/30 transition-all"
          >
            <Clock className="w-3.5 h-3.5 text-signal-primary" />
            <span className="font-medium">{TIME_RANGE_LABELS[timeRange]}</span>
            <ChevronDown className="w-3 h-3 opacity-50" />
          </button>
          {showTimeDropdown && (
            <div className="absolute right-0 top-full mt-1.5 w-40 glass-panel rounded-lg py-1 shadow-xl shadow-black/40 z-50 animate-fade-in">
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
            className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg border text-xs transition-all ${
              autoRefresh !== 'off'
                ? 'border-signal-highlight/40 bg-signal-highlight/10 text-signal-highlight'
                : 'border-border bg-bg-primary/50 text-text-secondary hover:text-text-primary hover:border-signal-primary/30'
            }`}
          >
            <RefreshCw className={`w-3.5 h-3.5 ${autoRefresh !== 'off' ? 'animate-spin' : ''}`} style={autoRefresh !== 'off' ? { animationDuration: '3s' } : {}} />
            <span className="font-medium">{autoRefresh === 'off' ? 'Auto' : REFRESH_LABELS[autoRefresh]}</span>
          </button>
          {showRefreshDropdown && (
            <div className="absolute right-0 top-full mt-1.5 w-32 glass-panel rounded-lg py-1 shadow-xl shadow-black/40 z-50 animate-fade-in">
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
        <button className="relative p-2 rounded-lg text-text-secondary hover:text-text-primary hover:bg-bg-hover transition-colors">
          <Bell className="w-4 h-4" />
          <span className="absolute -top-0.5 -right-0.5 w-4 h-4 bg-accent-red rounded-full flex items-center justify-center text-[9px] font-bold text-white shadow-[0_0_8px_rgba(255,59,48,0.4)]">
            {criticals + degraded}
          </span>
        </button>

        {/* User Avatar with Dropdown */}
        <div ref={userRef} className="relative">
          <button
            onClick={() => { setShowUserDropdown(!showUserDropdown); setShowTimeDropdown(false); setShowRefreshDropdown(false); }}
            className="w-7 h-7 rounded-full bg-signal-primary/20 flex items-center justify-center text-[10px] font-semibold text-signal-primary border border-signal-primary/30 cursor-pointer hover:bg-signal-primary/30 transition-colors"
          >
            {initials}
          </button>
          {showUserDropdown && (
            <div className="absolute right-0 top-full mt-1.5 w-52 glass-panel rounded-lg py-2 shadow-xl shadow-black/40 z-50 animate-fade-in">
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

