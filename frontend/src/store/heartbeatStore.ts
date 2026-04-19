/**
 * heartbeatStore — Real-time system metrics from WS heartbeat frames
 *
 * Updated every ~1 second by the WebSocket transport layer.
 * Drives:
 *  - System stats bar (CPU / Memory / Disk)
 *  - Connection status indicator (green/red)
 *  - Idle banner ("System running normally. No new events.")
 */
import { create } from 'zustand';

export interface HeartbeatFrame {
  timestamp: string;
  cpu: number;
  memory: number;
  disk: number;
}

interface HeartbeatState {
  /** Latest heartbeat payload. null = no heartbeat received yet */
  latest:        HeartbeatFrame | null;
  /** Epoch ms of last heartbeat receipt on the client */
  lastHeartbeat: number | null;
  /** Epoch ms of the last received event message */
  lastEventTime: number | null;
  /** True when lastHeartbeat is within the last 5 seconds */
  isAlive:       boolean;
  /** True when no event has been seen for IDLE_THRESHOLD_MS */
  isIdle:        boolean;

  /** Called by websocket.ts on every heartbeat frame */
  onHeartbeat:   (frame: HeartbeatFrame) => void;
  /** Called by websocket.ts whenever an event batch arrives */
  onEventReceived: () => void;
}

/** Show idle banner when no events for > 30 seconds */
const IDLE_THRESHOLD_MS = 30_000;
/** Mark connection dead when no heartbeat for > 5 seconds */
const ALIVE_THRESHOLD_MS = 5_000;

let _aliveTimer: ReturnType<typeof setInterval> | null = null;

export const useHeartbeatStore = create<HeartbeatState>((set, get) => {
  // Periodic tick to recompute isAlive / isIdle based on wall clock
  function startAliveTimer() {
    if (_aliveTimer !== null) return;
    _aliveTimer = setInterval(() => {
      const { lastHeartbeat, lastEventTime } = get();
      const now = Date.now();
      set({
        isAlive: lastHeartbeat !== null && now - lastHeartbeat < ALIVE_THRESHOLD_MS,
        isIdle:  lastEventTime === null   || now - lastEventTime > IDLE_THRESHOLD_MS,
      });
    }, 1_000);
  }

  return {
    latest:        null,
    lastHeartbeat: null,
    lastEventTime: null,
    isAlive:       false,
    isIdle:        true,

    onHeartbeat: (frame) => {
      startAliveTimer();
      const now = Date.now();
      set({
        latest:        frame,
        lastHeartbeat: now,
        isAlive:       true,
      });
    },

    onEventReceived: () => {
      const now = Date.now();
      set({ lastEventTime: now, isIdle: false });
    },
  };
});
