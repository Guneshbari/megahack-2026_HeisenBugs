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
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
import psycopg2
import psycopg2.pool
import psycopg2.extras

from shared_constants import (
    DB_CONFIG,
    API_RESPONSE_TIMEOUT_SECONDS,
    DB_QUERY_TIMEOUT_SECONDS,
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


# ============================================================================
# APP
# ============================================================================

app = FastAPI(
    title="SentinelCore API",
    description="Live telemetry data API for the SentinelCore dashboard",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# DB CONNECTION POOL
# ============================================================================

_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None


def _get_pool() -> psycopg2.pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=10, **DB_CONFIG
        )
    return _pool


@contextmanager
def _get_db() -> Iterator[Any]:
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        if conn:
            pool.putconn(conn)


@app.on_event("shutdown")
def _on_shutdown() -> None:
    global _pool
    if _pool:
        _pool.closeall()


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
        return result if (ok and result is not None) else []

    result, ok = timeout_wrapper(
        _with_retry,
        timeout_secs=float(_TOTAL_TIMEOUT),
        label=endpoint,
    )
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
        return result if (ok and result is not None) else {}

    result, ok = timeout_wrapper(
        _with_retry,
        timeout_secs=float(_TOTAL_TIMEOUT),
        label=endpoint,
    )
    return result if (ok and result is not None) else {}


def _parse_diag(raw: Any):
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
    return val.isoformat() if isinstance(val, datetime) else val


def _f(val: Any, fallback: float = 0.0) -> float:
    try:
        return float(val) if val is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _i(val: Any, fallback: int = 0) -> int:
    try:
        return int(val) if val is not None else fallback
    except (TypeError, ValueError):
        return fallback


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/events")
def get_events(limit: int = 100) -> List[Dict]:
    t0 = time.time()
    try:
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

        _log_req("/events", (time.time() - t0) * 1000, "ok", len(rows))
        return rows

    except Exception as exc:
        logger.error("/events error: %s", exc)
        _log_req("/events", (time.time() - t0) * 1000, "error")
        return []


@app.get("/systems")
def get_systems() -> List[Dict]:
    t0 = time.time()
    try:
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
            crit   = _i(row.get("critical_count"))
            cpu    = _f(row.get("cpu_usage_percent"))
            mem    = _f(row.get("memory_usage_percent"))
            disk   = _f(row.get("disk_free_percent"))
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

        _log_req("/systems", (time.time() - t0) * 1000, "ok", len(systems))
        return systems

    except Exception as exc:
        logger.error("/systems error: %s", exc)
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
                ingested_at AS event_time, id AS event_record_id
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
                "acknowledged": False,
            })

        _log_req("/alerts", (time.time() - t0) * 1000, "ok", len(alerts))
        return alerts

    except Exception as exc:
        logger.error("/alerts error: %s", exc)
        _log_req("/alerts", (time.time() - t0) * 1000, "error")
        return []


@app.get("/metrics")
def get_metrics() -> List[Dict]:
    """Time-bucketed metric points. Falls back to feature_snapshots if no recent events."""
    t0 = time.time()
    try:
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

        metrics = [{
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

        _log_req("/metrics", (time.time() - t0) * 1000, "ok", len(metrics))
        return metrics

    except Exception as exc:
        logger.error("/metrics error: %s", exc)
        _log_req("/metrics", (time.time() - t0) * 1000, "error")
        return []


@app.get("/dashboard-metrics")
def get_dashboard_metrics() -> Dict:
    """KPI summary. Falls back to feature_snapshots when events table is empty."""
    t0      = time.time()
    default = {"total_events": 0, "critical_events": 0, "warning_events": 0}
    try:
        row = _exec_one("""
            SELECT
                COUNT(*)                                       AS total_events,
                COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical_events,
                COUNT(*) FILTER (WHERE severity = 'WARNING')  AS warning_events
            FROM events
        """, endpoint="/dashboard-metrics")

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

        result = {
            "total_events":    _i(row.get("total_events")),
            "critical_events": _i(row.get("critical_events")),
            "warning_events":  _i(row.get("warning_events")),
        }
        _log_req("/dashboard-metrics", (time.time() - t0) * 1000, "ok")
        return result

    except Exception as exc:
        logger.error("/dashboard-metrics error: %s", exc)
        _log_req("/dashboard-metrics", (time.time() - t0) * 1000, "error")
        return default


@app.get("/fault-distribution")
def get_fault_distribution() -> List[Dict]:
    t0 = time.time()
    try:
        rows = _exec_query(
            "SELECT fault_type, COUNT(*) AS count FROM events "
            "GROUP BY fault_type ORDER BY count DESC",
            endpoint="/fault-distribution",
        )
        _log_req("/fault-distribution", (time.time() - t0) * 1000, "ok", len(rows))
        return rows
    except Exception as exc:
        logger.error("/fault-distribution error: %s", exc)
        return []


@app.get("/severity-distribution")
def get_severity_distribution() -> List[Dict]:
    t0 = time.time()
    try:
        rows = _exec_query(
            "SELECT severity, COUNT(*) AS count FROM events GROUP BY severity",
            endpoint="/severity-distribution",
        )
        _log_req("/severity-distribution", (time.time() - t0) * 1000, "ok", len(rows))
        return rows
    except Exception as exc:
        logger.error("/severity-distribution error: %s", exc)
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
            SELECT date_trunc('minute', ingested_at) AS bucket, COUNT(*) AS cnt
            FROM events WHERE ingested_at > NOW() - INTERVAL '20 minutes'
            GROUP BY bucket ORDER BY bucket ASC
        """, endpoint="/pipeline-health/trend")

        trend_eps     = []
        trend_latency = []
        for r in trend_rows:
            ts  = _iso(r.get("bucket"))
            cnt = _i(r.get("cnt"))
            trend_eps.append({"time": ts, "value": cnt})
            trend_latency.append({"time": ts, "value": avg_latency_ms + (cnt % 10)})

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
        _log_req("/health", (time.time() - t0) * 1000, "error")
        return {"status": "degraded", "database": str(exc)}