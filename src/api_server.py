"""
SentinelCore — FastAPI Backend
Version: 2.0.0

Hardening:
  - timeout_wrapper + retry_with_backoff on every DB call
  - Structured per-request JSON logging (endpoint, latency_ms, status, count)
  - Safe fallback returns ([] / {}) instead of raising HTTP 500
  - /metrics and /dashboard-metrics fall back to feature_snapshots
  - New: /feature-snapshots  (ML vectors, zero-NULL guarantee)
  - New: /live-status        (lightweight heartbeat-only ping)
"""

import json
import logging
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import psycopg2
import psycopg2.pool
import psycopg2.extras
import psycopg2.sql as pgsql

try:
    import firebase_admin
    from firebase_admin import auth as firebase_auth, credentials as firebase_credentials
    _FIREBASE_ADMIN_AVAILABLE = True
except ImportError:
    _FIREBASE_ADMIN_AVAILABLE = False

from shared_constants import (
    DB_CONFIG,
    FIREBASE_AUTH_ENABLED,
    API_RESPONSE_TIMEOUT_SECONDS,
    API_CACHE_TTL_SECONDS,
    API_CORS_ALLOWED_ORIGINS,
    API_MAX_EVENTS_LIMIT,
    DB_QUERY_TIMEOUT_SECONDS,
    DB_POOL_MIN_CONN,
    DB_POOL_MAX_CONN,
)
from sentinel_utils import (
    retry_with_backoff,
    timeout_wrapper,
    structured_log,
)

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api_server")

_TOTAL_TIMEOUT = API_RESPONSE_TIMEOUT_SECONDS + DB_QUERY_TIMEOUT_SECONDS


def _log_req(endpoint: str, latency_ms: float, status: str, count: int = 0) -> None:
    structured_log(
        "api_server",
        {
            "endpoint":   endpoint,
            "latency_ms": round(latency_ms, 2),
            "status":     status,
            "count":      count,
        },
        log=logger,
    )


def _log_failure(endpoint: str, operation: str, error: Any) -> None:
    """Emit a structured failure log for a failed API or DB operation."""
    structured_log(
        "api_server",
        {
            "endpoint": endpoint,
            "operation": operation,
            "status": "failed",
            "error": str(error),
        },
        log=logger,
    )


# ============================================================================
# FIREBASE ADMIN SDK INITIALISATION
# ============================================================================

def _init_firebase_admin() -> bool:
    """Initialise Firebase Admin SDK from a service account file or environment."""
    import os
    if not _FIREBASE_ADMIN_AVAILABLE:
        return False
    try:
        if firebase_admin._apps:  # already initialised
            return True
        sa_path = os.getenv("SENTINEL_FIREBASE_SERVICE_ACCOUNT_PATH", "")
        if sa_path:
            cred = firebase_credentials.Certificate(sa_path)
        else:
            # Fall back to Application Default Credentials (GCP / Cloud Run)
            cred = firebase_credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
        return True
    except Exception as exc:
        logger.error("Firebase Admin SDK init failed: %s", exc)
        return False


_FIREBASE_ADMIN_READY = False


# ============================================================================
# APP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialise on startup, tear down on shutdown."""
    global _pool, _FIREBASE_ADMIN_READY
    _FIREBASE_ADMIN_READY = _init_firebase_admin()
    if FIREBASE_AUTH_ENABLED:
        if _FIREBASE_ADMIN_READY:
            logger.info("[startup] Firebase Admin SDK ready — token verification enabled")
        else:
            logger.error(
                "[startup] Firebase Admin SDK unavailable — set SENTINEL_FIREBASE_SERVICE_ACCOUNT_PATH "
                "or install firebase-admin.  Auth is DISABLED."
            )
    _log_security_posture()
    yield
    # Shutdown
    if _pool:
        _pool.closeall()
        _pool = None


app = FastAPI(
    title="SentinelCore API",
    description="Live telemetry data API for the SentinelCore dashboard",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=API_CORS_ALLOWED_ORIGINS,
    allow_credentials=API_CORS_ALLOWED_ORIGINS != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# DB CONNECTION POOL
# ============================================================================

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


class TimedResponseCache:
    """Small in-memory TTL cache for hot read endpoints."""

    def __init__(self, max_size: int = 1000) -> None:
        self._entries: Dict[str, Tuple[float, Any]] = {}
        self._inflight: Dict[str, threading.Event] = {}
        self._max_size = max_size
        self._lock = threading.Lock()

    def get_or_set(self, key: str, ttl_seconds: int, loader: Callable[[], Any]) -> Any:
        """
        Return a cached value when fresh, otherwise compute it once per key.

        This prevents cache stampedes on hot endpoints when an entry expires
        under concurrent load.
        """
        while True:
            should_load = False
            with self._lock:
                now = time.time()
                cached_entry = self._entries.get(key)
                if cached_entry and cached_entry[0] > now:
                    return cached_entry[1]

                inflight_event = self._inflight.get(key)
                if inflight_event is None:
                    inflight_event = threading.Event()
                    self._inflight[key] = inflight_event
                    should_load = True

            if should_load:
                try:
                    value = loader()
                except Exception:
                    with self._lock:
                        self._inflight.pop(key, None)
                        inflight_event.set()
                    raise

                with self._lock:
                    if len(self._entries) >= self._max_size:
                        now = time.time()
                        expired = [k for k, v in self._entries.items() if v[0] <= now]
                        for k in expired:
                            del self._entries[k]
                        if len(self._entries) >= self._max_size:
                            # Evict key with earliest expiry
                            oldest = min(self._entries.keys(), key=lambda k: self._entries[k][0])
                            del self._entries[oldest]

                    self._entries[key] = (time.time() + ttl_seconds, value)
                    self._inflight.pop(key, None)
                    inflight_event.set()
                return value

            inflight_event.wait()


_response_cache = TimedResponseCache()


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """
    Thread-safe connection pool.
    FastAPI runs sync endpoints in a thread pool — SimpleConnectionPool is
    NOT safe there.  ThreadedConnectionPool uses an internal lock so each
    thread gets its own connection safely.
    Sized from shared_constants so it can be tuned via env var without
    touching code.
    """
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:          # double-checked locking
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=DB_POOL_MIN_CONN,
                    maxconn=DB_POOL_MAX_CONN,
                    **DB_CONFIG,
                )
    return _pool


@contextmanager
def _get_db() -> Iterator[Any]:
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if conn:
            pool.putconn(conn)


@app.middleware("http")
async def _security_middleware(request: Request, call_next: Callable[..., Any]) -> Any:
    """Firebase ID token verification + baseline security response headers."""
    # Health endpoint is always public (used by load balancers)
    if request.url.path == "/health":
        response = await call_next(request)
        _add_security_headers(response)
        return response

    if FIREBASE_AUTH_ENABLED:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            _log_failure(request.url.path, "auth", "missing_bearer_token")
            return PlainTextResponse("Unauthorized", status_code=401)

        id_token = auth_header.split(" ", 1)[1]
        if not _FIREBASE_ADMIN_READY:
            # SDK failed to initialise — fail secure
            _log_failure(request.url.path, "auth", "firebase_sdk_not_ready")
            return PlainTextResponse("Service unavailable: auth backend not ready", status_code=503)

        try:
            firebase_auth.verify_id_token(id_token, check_revoked=True)
        except firebase_auth.RevokedIdTokenError:
            _log_failure(request.url.path, "auth", "revoked_token")
            return PlainTextResponse("Unauthorized: token revoked", status_code=401)
        except firebase_auth.ExpiredIdTokenError:
            _log_failure(request.url.path, "auth", "expired_token")
            return PlainTextResponse("Unauthorized: token expired", status_code=401)
        except Exception as exc:
            _log_failure(request.url.path, "auth", str(exc))
            return PlainTextResponse("Unauthorized", status_code=401)

    response = await call_next(request)
    _add_security_headers(response)
    return response


def _add_security_headers(response: Any) -> None:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("X-XSS-Protection", "1; mode=block")


# ============================================================================
# QUERY HELPERS
# ============================================================================

def _exec_query(sql: str, params: Any = None, endpoint: str = "query") -> List[Dict]:
    """
    Execute a SELECT query with timeout + retry protection.
    Returns a list of row dicts on success, [] on any failure.
    """
    def _run() -> List[Dict]:
        with _get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]

    def _with_retry() -> List[Dict]:
        result, ok = retry_with_backoff(_run, label=endpoint)
        if not ok:
            _log_failure(endpoint, "query_retry", "retries exhausted")
        return result if (ok and result is not None) else []

    result, ok = timeout_wrapper(
        _with_retry,
        timeout_secs=float(_TOTAL_TIMEOUT),
        label=endpoint,
    )
    if not ok or result is None:
        _log_failure(endpoint, "query_timeout", "query returned no result before timeout")
    return result if (ok and result is not None) else []


def _exec_one(sql: str, params: Any = None, endpoint: str = "query") -> Dict:
    """
    Execute a SELECT query expecting one row.
    Returns a dict on success, {} on failure.
    """
    def _run() -> Dict:
        with _get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else {}

    def _with_retry() -> Dict:
        result, ok = retry_with_backoff(_run, label=endpoint)
        if not ok:
            _log_failure(endpoint, "query_retry", "retries exhausted")
        return result if (ok and result is not None) else {}

    result, ok = timeout_wrapper(
        _with_retry,
        timeout_secs=float(_TOTAL_TIMEOUT),
        label=endpoint,
    )
    if not ok or result is None:
        _log_failure(endpoint, "query_timeout", "query returned no result before timeout")
    return result if (ok and result is not None) else {}


def _parse_diag(raw: Any):
    """Parse diagnostic JSON and extract the best human-readable description."""
    diag = raw
    if isinstance(diag, str):
        try:
            diag = json.loads(diag)
        except Exception:
            diag = None
    desc = ""
    if isinstance(diag, dict):
        desc = (
            diag.get("message") or diag.get("description") or
            diag.get("summary") or diag.get("error") or
            diag.get("detail") or ""
        )
    return diag, desc


def _iso(val: Any) -> Any:
    """Convert datetimes to ISO strings without mutating other values."""
    return val.isoformat() if isinstance(val, datetime) else val


def _coerce_float(val: Any, fallback: float = 0.0) -> float:
    """Coerce values to float for stable API output types."""
    try:
        return float(val) if val is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _coerce_int(val: Any, fallback: int = 0) -> int:
    """Coerce values to int for stable API output types."""
    try:
        return int(val) if val is not None else fallback
    except (TypeError, ValueError):
        return fallback


_f = _coerce_float
_i = _coerce_int


def _cache_key(endpoint: str, **params: Any) -> str:
    """Build a deterministic cache key from an endpoint and parameters."""
    parts = [endpoint]
    for key in sorted(params):
        parts.append(f"{key}={params[key]}")
    return "|".join(parts)


def _bounded_limit(limit: int) -> int:
    """Clamp externally supplied limits to a safe, env-tunable range."""
    try:
        requested_limit = int(limit)
    except (TypeError, ValueError):
        requested_limit = 100
    return max(1, min(requested_limit, API_MAX_EVENTS_LIMIT))


def _log_security_posture() -> None:
    """Emit warnings when running with permissive API defaults."""
    if API_CORS_ALLOWED_ORIGINS == ["*"]:
        _log_failure("/startup", "security_posture", "wildcard_cors_origins_enabled")

    if DB_CONFIG.get("password", "") in ("", "changeme123"):
        _log_failure("/startup", "security_posture", "weak_or_default_database_password_in_use")

    if not FIREBASE_AUTH_ENABLED:
        _log_failure("/startup", "security_posture", "firebase_auth_disabled_all_endpoints_public")


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/events")
def get_events(limit: int = 100, include_raw_xml: bool = False) -> List[Dict]:
    t0 = time.time()
    try:
        limit = _bounded_limit(limit)
        rows = _exec_query("""
            SELECT id, system_id, fault_type, severity, provider_name, event_id,
                   cpu_usage_percent, memory_usage_percent, disk_free_percent,
                   event_hash, diagnostic_context, raw_xml, ingested_at,
                   event_message, parsed_message, normalized_message,
                   fault_subtype, confidence_score
            FROM events
            ORDER BY ingested_at DESC
            LIMIT %s
        """, (limit,), endpoint="/events")

        for row in rows:
            row["cpu_usage_percent"]    = _f(row.get("cpu_usage_percent"))
            row["memory_usage_percent"] = _f(row.get("memory_usage_percent"))
            row["disk_free_percent"]    = _f(row.get("disk_free_percent"))
            row["confidence_score"]     = _f(row.get("confidence_score"), 0.20)
            row["event_message"]        = row.get("event_message") or ""
            row["parsed_message"]       = row.get("parsed_message") or ""
            row["normalized_message"]   = row.get("normalized_message") or ""
            row["fault_subtype"]        = row.get("fault_subtype") or ""
            row["event_record_id"]      = row["id"]
            row["hostname"]             = row.get("system_id", "")
            row["event_time"]           = _iso(row.get("ingested_at"))
            diag, desc = _parse_diag(row.get("diagnostic_context"))
            if diag is not None:
                row["diagnostic_context"] = diag
            row["fault_description"] = desc
            row["ingested_at"] = _iso(row.get("ingested_at"))
            if not include_raw_xml:
                row.pop("raw_xml", None)

        _log_req("/events", (time.time() - t0) * 1000, "ok", len(rows))
        return rows

    except Exception as exc:
        logger.error("/events error: %s", exc)
        _log_failure("/events", "endpoint", exc)
        _log_req("/events", (time.time() - t0) * 1000, "error")
        return []


@app.get("/systems")
def get_systems() -> List[Dict]:
    t0 = time.time()
    try:
        def load_systems() -> List[Dict]:
            rows = _exec_query("""
                WITH counts AS (
                    SELECT system_id, COUNT(*) AS total_events
                    FROM events
                    GROUP BY system_id
                ),
                critical_counts AS (
                    SELECT system_id, COUNT(*) AS critical_count
                    FROM events
                    WHERE severity = 'CRITICAL'
                      AND ingested_at > NOW() - INTERVAL '1 hour'
                    GROUP BY system_id
                )
                SELECT
                    h.system_id, h.hostname,
                    h.cpu_usage_percent, h.memory_usage_percent, h.disk_free_percent,
                    h.os_version, h.last_seen,
                    COALESCE(c.total_events, 0)    AS total_events,
                    COALESCE(cc.critical_count, 0) AS critical_count
                FROM system_heartbeats h
                LEFT JOIN counts          c  ON c.system_id  = h.system_id
                LEFT JOIN critical_counts cc ON cc.system_id = h.system_id
                ORDER BY h.system_id
            """, endpoint="/systems")

            systems: List[Dict] = []
            for row in rows:
                crit = _i(row.get("critical_count"))
                cpu = _f(row.get("cpu_usage_percent"))
                mem = _f(row.get("memory_usage_percent"))
                disk = _f(row.get("disk_free_percent"))
                status = "degraded" if (crit >= 3 or cpu > 90 or mem > 95) else "online"

                last_seen = row.get("last_seen")
                if isinstance(last_seen, datetime):
                    diff = (
                        datetime.now(timezone.utc) -
                        last_seen.replace(tzinfo=timezone.utc)
                    ).total_seconds()
                    if diff > 120:
                        status = "offline"
                    last_seen = last_seen.isoformat()

                systems.append({
                    "system_id":            row["system_id"],
                    "hostname":             row.get("hostname", ""),
                    "status":               status,
                    "cpu_usage_percent":    cpu,
                    "memory_usage_percent": mem,
                    "disk_free_percent":    disk,
                    "os_version":           row.get("os_version", "Windows"),
                    "last_seen":            last_seen,
                    "ip_address":           "",
                    "total_events":         _i(row.get("total_events")),
                })
            return systems

        systems = _response_cache.get_or_set(
            _cache_key("/systems"),
            API_CACHE_TTL_SECONDS,
            load_systems,
        )

        _log_req("/systems", (time.time() - t0) * 1000, "ok", len(systems))
        return systems

    except Exception as exc:
        logger.error("/systems error: %s", exc)
        _log_failure("/systems", "endpoint", exc)
        _log_req("/systems", (time.time() - t0) * 1000, "error")
        return []


@app.get("/alerts")
def get_alerts() -> List[Dict]:
    t0 = time.time()
    try:
        rows = _exec_query("""
            SELECT
                system_id, system_id AS hostname, severity, fault_type,
                provider_name, diagnostic_context,
                ingested_at AS event_time, id AS event_record_id,
                acknowledged, escalated
            FROM events
            WHERE severity IN ('CRITICAL', 'ERROR', 'WARNING')
            ORDER BY ingested_at DESC
            LIMIT 50
        """, endpoint="/alerts")

        alerts: List[Dict] = []
        for i, row in enumerate(rows):
            _, desc = _parse_diag(row.get("diagnostic_context"))
            alerts.append({
                "alert_id":     f"ALERT-{row.get('event_record_id', i)}",
                "system_id":    row["system_id"],
                "hostname":     row.get("hostname", ""),
                "severity":     row["severity"],
                "rule":         f"{row.get('fault_type', 'Unknown')} Detection",
                "title":        (
                    f"{row['severity']}: {row.get('fault_type', 'Unknown')}"
                    f" on {row.get('hostname', 'Unknown')}"
                ),
                "description":  desc,
                "triggered_at": _iso(row.get("event_time")),
                "acknowledged": bool(row.get("acknowledged")),
                "escalated":    bool(row.get("escalated")),
                "status":       "acknowledged" if row.get("acknowledged") else "active",
            })

        _log_req("/alerts", (time.time() - t0) * 1000, "ok", len(alerts))
        return alerts

    except Exception as exc:
        logger.error("/alerts error: %s", exc)
        _log_failure("/alerts", "endpoint", exc)
        _log_req("/alerts", (time.time() - t0) * 1000, "error")
        return []


class AlertActionRequest(BaseModel):
    alert_id: str

@app.post("/alerts/acknowledge")
def acknowledge_alert(req: AlertActionRequest) -> Dict:
    t0 = time.time()
    try:
        record_id = int(req.alert_id.replace("ALERT-", ""))
        def _run_ack() -> bool:
            with _get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE events SET acknowledged = TRUE, acknowledged_at = NOW() WHERE id = %s",
                        (record_id,)
                    )
                conn.commit()
            return True
        result, ok = retry_with_backoff(_run_ack, label="/alerts/acknowledge")
        success = bool(ok and result)
        _log_req("/alerts/acknowledge", (time.time() - t0) * 1000, "ok" if success else "error")
        return {"success": success}
    except Exception as exc:
        _log_failure("/alerts/acknowledge", "endpoint", exc)
        return {"success": False}

@app.post("/alerts/escalate")
def escalate_alert(req: AlertActionRequest) -> Dict:
    t0 = time.time()
    try:
        record_id = int(req.alert_id.replace("ALERT-", ""))
        def _run_esc() -> bool:
            with _get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE events SET escalated = TRUE, escalated_at = NOW() WHERE id = %s",
                        (record_id,)
                    )
                conn.commit()
            return True
        result, ok = retry_with_backoff(_run_esc, label="/alerts/escalate")
        success = bool(ok and result)
        _log_req("/alerts/escalate", (time.time() - t0) * 1000, "ok" if success else "error")
        return {"success": success}
    except Exception as exc:
        _log_failure("/alerts/escalate", "endpoint", exc)
        return {"success": False}

@app.get("/pipeline-health/status")
def get_pipeline_health_status() -> Dict:
    t0 = time.time()
    try:
        row = _exec_one("SELECT MAX(ingested_at) AS latest FROM events", endpoint="/pipeline-health/status")
        latest = row.get("latest")
        
        delay_seconds = 0
        status = "OK"
        
        if isinstance(latest, datetime):
            delay_seconds = (datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)).total_seconds()
            if delay_seconds < 60:
                status = "OK"
            elif delay_seconds < 300:
                status = "DEGRADED"
            else:
                status = "DOWN"
        
        _log_req("/pipeline-health/status", (time.time() - t0) * 1000, "ok")
        return {"status": status, "delay_seconds": int(delay_seconds)}
    except Exception as exc:
        _log_failure("/pipeline-health/status", "endpoint", exc)
        return {"status": "DOWN", "delay_seconds": 999}


@app.get("/metrics")
def get_metrics() -> List[Dict]:
    """Time-bucketed metric points. Falls back to feature_snapshots if no recent events."""
    t0 = time.time()
    try:
        def load_metrics() -> List[Dict]:
            rows = _exec_query("""
                SELECT
                    date_trunc('hour', ingested_at)                             AS bucket,
                    COUNT(*)                                                     AS event_count,
                    COUNT(*) FILTER (WHERE severity = 'CRITICAL')               AS critical_count,
                    COUNT(*) FILTER (WHERE severity = 'ERROR')                  AS error_count,
                    COUNT(*) FILTER (WHERE severity = 'WARNING')                AS warning_count,
                    COUNT(*) FILTER (WHERE severity = 'INFO')                   AS info_count,
                    ROUND(AVG(cpu_usage_percent)::numeric, 1)                   AS avg_cpu,
                    ROUND(AVG(memory_usage_percent)::numeric, 1)                AS avg_memory,
                    ROUND(AVG(disk_free_percent)::numeric, 1)                   AS avg_disk_free
                FROM events
                WHERE ingested_at > NOW() - INTERVAL '24 hours'
                GROUP BY bucket
                ORDER BY bucket ASC
            """, endpoint="/metrics")

            if not rows:
                rows = _exec_query("""
                    SELECT
                        date_trunc('hour', snapshot_time)       AS bucket,
                        SUM(total_events)                        AS event_count,
                        SUM(critical_count)                      AS critical_count,
                        SUM(error_count)                         AS error_count,
                        SUM(warning_count)                       AS warning_count,
                        SUM(info_count)                          AS info_count,
                        ROUND(AVG(cpu_usage_percent), 1)         AS avg_cpu,
                        ROUND(AVG(memory_usage_percent), 1)      AS avg_memory,
                        ROUND(AVG(disk_free_percent), 1)         AS avg_disk_free
                    FROM feature_snapshots
                    WHERE snapshot_time > NOW() - INTERVAL '24 hours'
                    GROUP BY bucket
                    ORDER BY bucket ASC
                """, endpoint="/metrics_fallback")

            return [{
                "timestamp":      _iso(r.get("bucket")),
                "event_count":    _i(r.get("event_count")),
                "critical_count": _i(r.get("critical_count")),
                "error_count":    _i(r.get("error_count")),
                "warning_count":  _i(r.get("warning_count")),
                "info_count":     _i(r.get("info_count")),
                "avg_cpu":        _f(r.get("avg_cpu")),
                "avg_memory":     _f(r.get("avg_memory")),
                "avg_disk_free":  _f(r.get("avg_disk_free")),
            } for r in rows]

        metrics = _response_cache.get_or_set(
            _cache_key("/metrics"),
            API_CACHE_TTL_SECONDS,
            load_metrics,
        )

        _log_req("/metrics", (time.time() - t0) * 1000, "ok", len(metrics))
        return metrics

    except Exception as exc:
        logger.error("/metrics error: %s", exc)
        _log_failure("/metrics", "endpoint", exc)
        _log_req("/metrics", (time.time() - t0) * 1000, "error")
        return []


@app.get("/dashboard-metrics")
def get_dashboard_metrics(window_minutes: Optional[int] = None) -> Dict:
    """KPI summary. Falls back to feature_snapshots when events table is empty."""
    t0      = time.time()
    default = {"total_events": 0, "critical_events": 0, "warning_events": 0}
    try:
        def load_dashboard_metrics() -> Dict:
            params: List = []
            if window_minutes and window_minutes > 0:
                time_filter = pgsql.SQL("WHERE ingested_at >= NOW() - (%s * INTERVAL '1 minute')")
                params.append(window_minutes)
            else:
                time_filter = pgsql.SQL("")

            query = pgsql.SQL("""
                SELECT
                    COUNT(*)                                       AS total_events,
                    COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical_events,
                    COUNT(*) FILTER (WHERE severity = 'WARNING')  AS warning_events
                FROM events
                {time_filter}
            """).format(time_filter=time_filter)

            row = _exec_one(query, tuple(params), endpoint="/dashboard-metrics")

            if not row or _i(row.get("total_events")) == 0:
                snap = _exec_one("""
                    SELECT
                        SUM(total_events)   AS total_events,
                        SUM(critical_count) AS critical_events,
                        SUM(warning_count)  AS warning_events
                    FROM feature_snapshots
                """, endpoint="/dashboard-metrics_snap")
                if snap:
                    row = snap

            return {
                "total_events":    _i(row.get("total_events")),
                "critical_events": _i(row.get("critical_events")),
                "warning_events":  _i(row.get("warning_events")),
            }

        result = _response_cache.get_or_set(
            _cache_key("/dashboard-metrics", window_minutes),
            API_CACHE_TTL_SECONDS,
            load_dashboard_metrics,
        )
        _log_req("/dashboard-metrics", (time.time() - t0) * 1000, "ok")
        return result

    except Exception as exc:
        logger.error("/dashboard-metrics error: %s", exc)
        _log_failure("/dashboard-metrics", "endpoint", exc)
        _log_req("/dashboard-metrics", (time.time() - t0) * 1000, "error")
        return default


@app.get("/fault-distribution")
def get_fault_distribution(window_minutes: Optional[int] = None) -> List[Dict]:
    t0 = time.time()
    try:
        params: List = []
        if window_minutes and window_minutes > 0:
            time_filter = pgsql.SQL("WHERE ingested_at >= NOW() - (%s * INTERVAL '1 minute')")
            params.append(window_minutes)
        else:
            time_filter = pgsql.SQL("")

        query = pgsql.SQL(
            "SELECT fault_type, COUNT(*) AS count FROM events {time_filter} "
            "GROUP BY fault_type ORDER BY count DESC"
        ).format(time_filter=time_filter)

        rows = _exec_query(query, tuple(params), endpoint="/fault-distribution")
        _log_req("/fault-distribution", (time.time() - t0) * 1000, "ok", len(rows))
        return rows
    except Exception as exc:
        logger.error("/fault-distribution error: %s", exc)
        _log_failure("/fault-distribution", "endpoint", exc)
        return []


@app.get("/severity-distribution")
def get_severity_distribution(window_minutes: Optional[int] = None) -> List[Dict]:
    t0 = time.time()
    try:
        params: List = []
        if window_minutes and window_minutes > 0:
            time_filter = pgsql.SQL("WHERE ingested_at >= NOW() - (%s * INTERVAL '1 minute')")
            params.append(window_minutes)
        else:
            time_filter = pgsql.SQL("")

        query = pgsql.SQL(
            "SELECT severity, COUNT(*) AS count FROM events {time_filter} GROUP BY severity"
        ).format(time_filter=time_filter)

        rows = _exec_query(query, tuple(params), endpoint="/severity-distribution")
        _log_req("/severity-distribution", (time.time() - t0) * 1000, "ok", len(rows))
        return rows
    except Exception as exc:
        logger.error("/severity-distribution error: %s", exc)
        _log_failure("/severity-distribution", "endpoint", exc)
        return []


@app.get("/system-failures")
def get_system_failures(
    limit: int = 6,
    window_minutes: Optional[int] = None,
) -> List[Dict]:
    t0 = time.time()
    try:
        limit = max(1, min(limit, 25))
        conditions = [pgsql.SQL("e.severity IN ('CRITICAL', 'ERROR')")]
        params: List = []
        if window_minutes and window_minutes > 0:
            conditions.append(pgsql.SQL("e.ingested_at >= NOW() - (%s * INTERVAL '1 minute')"))
            params.append(window_minutes)

        params.append(limit)
        query = pgsql.SQL("""
            SELECT
                e.system_id,
                COALESCE(h.hostname, e.system_id) AS hostname,
                COUNT(*) AS failure_count
            FROM events e
            LEFT JOIN system_heartbeats h ON h.system_id = e.system_id
            WHERE {conditions}
            GROUP BY e.system_id, hostname
            ORDER BY failure_count DESC, e.system_id
            LIMIT %s
        """).format(conditions=pgsql.SQL(" AND ").join(conditions))

        rows = _exec_query(query, tuple(params), endpoint="/system-failures")
        _log_req("/system-failures", (time.time() - t0) * 1000, "ok", len(rows))
        return rows
    except Exception as exc:
        logger.error("/system-failures error: %s", exc)
        _log_failure("/system-failures", "endpoint", exc)
        return []


@app.get("/system-metrics")
def get_system_metrics() -> Dict:
    t0      = time.time()
    default = {"avg_cpu": 0.0, "avg_memory": 0.0, "avg_disk": 0.0}
    try:
        row = _exec_one("""
            SELECT
                ROUND(AVG(cpu_usage_percent)::numeric, 1)    AS avg_cpu,
                ROUND(AVG(memory_usage_percent)::numeric, 1) AS avg_memory,
                ROUND(AVG(disk_free_percent)::numeric, 1)    AS avg_disk
            FROM events
        """, endpoint="/system-metrics")
        result = row if row else default
        _log_req("/system-metrics", (time.time() - t0) * 1000, "ok")
        return result
    except Exception as exc:
        logger.error("/system-metrics error: %s", exc)
        _log_failure("/system-metrics", "endpoint", exc)
        return default


@app.get("/pipeline-health")
def get_pipeline_health() -> Dict:
    t0      = time.time()
    default = {
        "events_per_sec": 0.0, "eps_change_pct": 0,
        "avg_latency_ms": 0,   "kafka_lag": 0,
        "lag_status": "Unknown", "db_write_rate": 0.0,
        "trend_eps": [], "trend_latency": [],
    }
    try:
        eps_row = _exec_one("""
            SELECT COUNT(*) AS total_recent,
                   EXTRACT(EPOCH FROM (MAX(ingested_at) - MIN(ingested_at))) AS span_seconds
            FROM events WHERE ingested_at > NOW() - INTERVAL '5 minutes'
        """, endpoint="/pipeline-health/eps")

        total_recent   = _i(eps_row.get("total_recent"))
        span_sec       = _f(eps_row.get("span_seconds"))
        events_per_sec = round(total_recent / span_sec, 1) if span_sec > 0 else 0.0

        lat_row = _exec_one("""
            WITH ordered AS (
                SELECT ingested_at,
                       LAG(ingested_at) OVER (PARTITION BY system_id ORDER BY ingested_at) AS prev_at
                FROM events WHERE ingested_at > NOW() - INTERVAL '5 minutes'
            )
            SELECT ROUND(AVG(EXTRACT(EPOCH FROM (ingested_at - prev_at)) * 1000)::numeric, 0)
                   AS avg_latency_ms
            FROM ordered WHERE prev_at IS NOT NULL
        """, endpoint="/pipeline-health/lat")
        avg_latency_ms = _i(lat_row.get("avg_latency_ms"))

        wr_row          = _exec_one(
            "SELECT COUNT(*) AS writes_last_min FROM events "
            "WHERE ingested_at > NOW() - INTERVAL '1 minute'",
            endpoint="/pipeline-health/wr",
        )
        db_write_rate   = round(_i(wr_row.get("writes_last_min")) / 60.0, 1)

        latest_row      = _exec_one(
            "SELECT MAX(ingested_at) AS latest FROM events",
            endpoint="/pipeline-health/latest",
        )
        latest          = latest_row.get("latest")
        kafka_lag       = 0
        lag_status      = "Optimal"
        if isinstance(latest, datetime):
            age = (
                datetime.now(timezone.utc) -
                latest.replace(tzinfo=timezone.utc)
            ).total_seconds()
            if age > 300:
                kafka_lag  = int(age)
                lag_status = "Degraded"

        trend_rows = _exec_query("""
            WITH ordered AS (
                SELECT ingested_at, date_trunc('minute', ingested_at) AS bucket,
                       LAG(ingested_at) OVER (ORDER BY ingested_at) AS prev_at
                FROM events WHERE ingested_at > NOW() - INTERVAL '20 minutes'
            )
            SELECT bucket, COUNT(*) AS cnt,
                   ROUND(AVG(EXTRACT(EPOCH FROM (ingested_at - prev_at)) * 1000)::numeric, 0) AS bucket_lat
            FROM ordered WHERE prev_at IS NOT NULL
            GROUP BY bucket ORDER BY bucket ASC
        """, endpoint="/pipeline-health/trend")

        trend_eps     = []
        trend_latency = []
        for r in trend_rows:
            ts   = _iso(r.get("bucket"))
            cnt  = _i(r.get("cnt"))
            blat = _i(r.get("bucket_lat"))
            trend_eps.append({"time": ts, "value": cnt})
            trend_latency.append({"time": ts, "value": blat if blat > 0 else avg_latency_ms})

        _log_req("/pipeline-health", (time.time() - t0) * 1000, "ok")
        return {
            "events_per_sec": events_per_sec,
            "eps_change_pct": 0,
            "avg_latency_ms": avg_latency_ms,
            "kafka_lag":      kafka_lag,
            "lag_status":     lag_status,
            "db_write_rate":  db_write_rate,
            "trend_eps":      trend_eps,
            "trend_latency":  trend_latency,
        }

    except Exception as exc:
        logger.error("/pipeline-health error: %s", exc)
        _log_failure("/pipeline-health", "endpoint", exc)
        _log_req("/pipeline-health", (time.time() - t0) * 1000, "error")
        return default


@app.get("/feature-snapshots")
def get_feature_snapshots(
    system_id: Optional[str] = None, limit: int = 100
) -> List[Dict]:
    """Pre-aggregated ML feature vectors — zero-NULL guarantee."""
    t0 = time.time()
    try:
        if system_id:
            rows = _exec_query("""
                SELECT * FROM feature_snapshots
                WHERE system_id = %s
                ORDER BY snapshot_time DESC LIMIT %s
            """, (system_id, limit), endpoint="/feature-snapshots")
        else:
            rows = _exec_query("""
                SELECT * FROM feature_snapshots
                ORDER BY snapshot_time DESC LIMIT %s
            """, (limit,), endpoint="/feature-snapshots")

        for row in rows:
            row["snapshot_time"]        = _iso(row.get("snapshot_time"))
            row["cpu_usage_percent"]    = _f(row.get("cpu_usage_percent"))
            row["memory_usage_percent"] = _f(row.get("memory_usage_percent"))
            row["disk_free_percent"]    = _f(row.get("disk_free_percent"), 100.0)
            row["avg_confidence"]       = _f(row.get("avg_confidence"), 0.20)
            row["total_events"]         = _i(row.get("total_events"))
            row["critical_count"]       = _i(row.get("critical_count"))
            row["error_count"]          = _i(row.get("error_count"))
            row["warning_count"]        = _i(row.get("warning_count"))
            row["info_count"]           = _i(row.get("info_count"))
            row["dominant_fault_type"]  = row.get("dominant_fault_type") or "NONE"

        _log_req("/feature-snapshots", (time.time() - t0) * 1000, "ok", len(rows))
        return rows

    except Exception as exc:
        logger.error("/feature-snapshots error: %s", exc)
        _log_failure("/feature-snapshots", "endpoint", exc)
        _log_req("/feature-snapshots", (time.time() - t0) * 1000, "error")
        return []


@app.get("/live-status")
def get_live_status() -> List[Dict]:
    """Lightweight heartbeat-only status — no join with events table."""
    t0 = time.time()
    try:
        rows = _exec_query("""
            SELECT system_id, hostname,
                   cpu_usage_percent, memory_usage_percent, disk_free_percent,
                   os_version, agent_version, ip_address, uptime_seconds, last_seen
            FROM system_heartbeats
            ORDER BY system_id
        """, endpoint="/live-status")

        result: List[Dict] = []
        for row in rows:
            last_seen = row.get("last_seen")
            online    = True
            if isinstance(last_seen, datetime):
                age    = (
                    datetime.now(timezone.utc) -
                    last_seen.replace(tzinfo=timezone.utc)
                ).total_seconds()
                online    = age <= 120
                last_seen = last_seen.isoformat()

            result.append({
                "system_id":            row["system_id"],
                "hostname":             row.get("hostname", ""),
                "online":               online,
                "cpu_usage_percent":    _f(row.get("cpu_usage_percent")),
                "memory_usage_percent": _f(row.get("memory_usage_percent")),
                "disk_free_percent":    _f(row.get("disk_free_percent"), 100.0),
                "os_version":           row.get("os_version", ""),
                "agent_version":        row.get("agent_version", ""),
                "ip_address":           row.get("ip_address", ""),
                "uptime_seconds":       _i(row.get("uptime_seconds")),
                "last_seen":            last_seen,
            })

        _log_req("/live-status", (time.time() - t0) * 1000, "ok", len(result))
        return result

    except Exception as exc:
        logger.error("/live-status error: %s", exc)
        _log_failure("/live-status", "endpoint", exc)
        _log_req("/live-status", (time.time() - t0) * 1000, "error")
        return []


@app.get("/metrics-export")
def prometheus_metrics() -> PlainTextResponse:
    """Prometheus-compatible text metrics endpoint for Grafana scraping."""
    t0 = time.time()
    try:
        row = _exec_one("""
            SELECT
                COUNT(*)                                             AS total_events,
                COUNT(*) FILTER (WHERE severity = 'CRITICAL')       AS critical_events,
                COUNT(*) FILTER (WHERE severity = 'ERROR')          AS error_events,
                COUNT(*) FILTER (WHERE severity = 'WARNING')        AS warning_events,
                COUNT(*) FILTER (WHERE severity = 'INFO')           AS info_events,
                ROUND(AVG(cpu_usage_percent)::numeric, 2)           AS avg_cpu,
                ROUND(AVG(memory_usage_percent)::numeric, 2)        AS avg_memory,
                ROUND(AVG(disk_free_percent)::numeric, 2)           AS avg_disk_free,
                COUNT(DISTINCT system_id)                           AS total_systems
            FROM events
        """, endpoint="/metrics-export")

        online_row = _exec_one("""
            SELECT COUNT(DISTINCT system_id) AS online_systems
            FROM events
            WHERE ingested_at > NOW() - INTERVAL '2 hours'
        """, endpoint="/metrics-export/online")

        r = row or {}
        o = online_row or {}
        lines = [
            "# HELP sentinel_total_events Total telemetry events.",
            "# TYPE sentinel_total_events gauge",
            f"sentinel_total_events {_i(r.get('total_events'))}",
            "# HELP sentinel_critical_events CRITICAL severity events.",
            "# TYPE sentinel_critical_events gauge",
            f"sentinel_critical_events {_i(r.get('critical_events'))}",
            "# HELP sentinel_error_events ERROR severity events.",
            "# TYPE sentinel_error_events gauge",
            f"sentinel_error_events {_i(r.get('error_events'))}",
            "# HELP sentinel_warning_events WARNING severity events.",
            "# TYPE sentinel_warning_events gauge",
            f"sentinel_warning_events {_i(r.get('warning_events'))}",
            "# HELP sentinel_info_events INFO severity events.",
            "# TYPE sentinel_info_events gauge",
            f"sentinel_info_events {_i(r.get('info_events'))}",
            "# HELP sentinel_avg_cpu Average CPU usage percent.",
            "# TYPE sentinel_avg_cpu gauge",
            f"sentinel_avg_cpu {_f(r.get('avg_cpu'))}",
            "# HELP sentinel_avg_memory Average memory usage percent.",
            "# TYPE sentinel_avg_memory gauge",
            f"sentinel_avg_memory {_f(r.get('avg_memory'))}",
            "# HELP sentinel_avg_disk_free Average disk free percent.",
            "# TYPE sentinel_avg_disk_free gauge",
            f"sentinel_avg_disk_free {_f(r.get('avg_disk_free'))}",
            "# HELP sentinel_total_systems Total distinct systems.",
            "# TYPE sentinel_total_systems gauge",
            f"sentinel_total_systems {_i(r.get('total_systems'))}",
            "# HELP sentinel_online_systems Systems active in last 2 hours.",
            "# TYPE sentinel_online_systems gauge",
            f"sentinel_online_systems {_i(o.get('online_systems'))}",
        ]
        _log_req("/metrics-export", (time.time() - t0) * 1000, "ok")
        return PlainTextResponse(
            "\n".join(lines) + "\n",
            media_type="text/plain; version=0.0.4",
        )

    except Exception as exc:
        logger.error("/metrics-export error: %s", exc)
        _log_failure("/metrics-export", "endpoint", exc)
        return PlainTextResponse(
            "# error generating metrics\n",
            media_type="text/plain; version=0.0.4",
        )


@app.get("/health")
def health_check() -> Dict:
    t0 = time.time()
    try:
        _exec_one("SELECT 1 AS ok", endpoint="/health")
        _log_req("/health", (time.time() - t0) * 1000, "ok")
        return {"status": "healthy", "database": "connected"}
    except Exception as exc:
        _log_failure("/health", "endpoint", exc)
        _log_req("/health", (time.time() - t0) * 1000, "error")
        return {"status": "degraded", "database": str(exc)}
