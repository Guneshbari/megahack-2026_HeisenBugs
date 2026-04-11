/**
 * WebSocket transport layer for SentinelCore
 *
 * Rules:
 *  - Incoming messages buffered, flushed every 300ms
 *  - Reconnects with exponential backoff (1s → 30s max)
 *  - Disconnected gracefully — does NOT crash when backend is offline
 *  - Updates go direct into Zustand signalStore via batchPush()
 *  - USE_MOCK_DATA = true → WebSocket is stubbed (no connection attempt)
 */
import { useSignalStore } from '../store/signalStore';
import { isApiSessionAuthenticated, USE_MOCK_DATA } from './api';
import { auth } from './firebase';
import type { TelemetryEvent } from '../types/telemetry';

const WS_URL             = (import.meta.env.VITE_SENTINEL_WS_URL ?? 'ws://localhost:8000/ws/events').trim();
const BATCH_INTERVAL_MS  = 300;
const INITIAL_RETRY_MS   = 1_000;
const MAX_RETRY_MS       = 30_000;

class SentinelWebSocket {
  private ws:           WebSocket | null = null;
  private buffer:       TelemetryEvent[] = [];
  private flushTimer:   ReturnType<typeof setTimeout> | null = null;
  private retryDelay:   number = INITIAL_RETRY_MS;
  private stopped:      boolean = false;
  private url:          string;

  constructor(url: string) {
    this.url = url;
  }

  async connect(): Promise<void> {
    if (this.stopped) return;

    let connectionUrl = this.url;
    if (isApiSessionAuthenticated()) {
      const user = auth.currentUser;
      if (!user) {
        useSignalStore.getState().setConnected(false);
        this.scheduleReconnect();
        return;
      }
      try {
        const wsUrl = new URL(this.url);
        wsUrl.searchParams.set('token', await user.getIdToken());
        connectionUrl = wsUrl.toString();
      } catch (error) {
        console.warn('SentinelCore websocket auth token unavailable:', error);
        useSignalStore.getState().setConnected(false);
        this.scheduleReconnect();
        return;
      }
    }

    try {
      this.ws = new WebSocket(connectionUrl);
    } catch {
      // WebSocket constructor can throw if URL is invalid
      this.scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      this.retryDelay = INITIAL_RETRY_MS;
      useSignalStore.getState().setConnected(true);
    };

    this.ws.onmessage = (ev: MessageEvent) => {
      try {
        const data = JSON.parse(ev.data as string);
        if (Array.isArray(data)) {
          this.buffer.push(...(data as TelemetryEvent[]));
        } else {
          this.buffer.push(data as TelemetryEvent);
        }
        this.scheduleFlush();
      } catch {
        // Ignore malformed frames
      }
    };

    this.ws.onerror = () => {
      useSignalStore.getState().setConnected(false);
    };

    this.ws.onclose = () => {
      useSignalStore.getState().setConnected(false);
      if (!this.stopped) this.scheduleReconnect();
    };
  }

  private scheduleFlush(): void {
    if (this.flushTimer !== null) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      const batch = this.buffer.splice(0);
      if (batch.length > 0) {
        useSignalStore.getState().batchPush(batch);
      }
    }, BATCH_INTERVAL_MS);
  }

  private scheduleReconnect(): void {
    setTimeout(() => {
      this.retryDelay = Math.min(this.retryDelay * 2, MAX_RETRY_MS);
      this.connect().catch((error) => {
        console.warn('SentinelCore websocket reconnect failed:', error);
      });
    }, this.retryDelay);
  }

  disconnect(): void {
    this.stopped = true;
    if (this.flushTimer !== null) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.ws?.close();
    this.ws = null;
  }
}

let _client: SentinelWebSocket | null = null;

/** Call once at app startup (after signal store is ready) */
export function initWebSocket(): void {
  if (USE_MOCK_DATA) {
    // In mock mode, mark as "connected" so the UI doesn't show an error dot
    setTimeout(() => useSignalStore.getState().setConnected(true), 500);
    return;
  }
  if (_client) return;
  _client = new SentinelWebSocket(WS_URL);
  _client.connect().catch((error) => {
    console.warn('SentinelCore websocket startup failed:', error);
  });
}

export function disconnectWebSocket(): void {
  _client?.disconnect();
  _client = null;
}
