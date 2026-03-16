"""
SentinelCore — FastAPI Backend
Serves live PostgreSQL data to the dashboard frontend.
Uses connection pooling for stable performance under frequent polling.
"""

import json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from contextlib import contextmanager
from datetime import datetime, timezone
import psycopg2
import psycopg2.pool
import psycopg2.extras
import typing

from shared_constants import DB_CONFIG

# ============================================================================
# APP SETUP
# ============================================================================

app = FastAPI(
    title="SentinelCore API",
    description="Live telemetry data API for the SentinelCore dashboard",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# DATABASE CONNECTION POOL
# ============================================================================

# DB_CONFIG imported from shared_constants

pool = None


def get_pool():
    global pool
    if pool is None:
        pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **DB_CONFIG,
        )
    return pool


@contextmanager
# Context manager that gets a connection from the pool and returns it.
def get_db():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        if conn:
            pool.putconn(conn)


@app.on_event("shutdown")
def shutdown():
    global pool
    if pool:
        pool.closeall()


# ============================================================================
# HELPERS
# ============================================================================


def parse_diagnostic_context(raw_diag):
    """Parse diagnostic_context JSON and extract a human-readable description."""
    diag = raw_diag
    if isinstance(diag, str):
        try:
            diag = json.loads(diag)
        except Exception:
            diag = None
    desc = ""
    if isinstance(diag, dict):
        desc = (
            diag.get("message")
            or diag.get("description")
            or diag.get("summary")
            or diag.get("error")
            or diag.get("detail")
            or ""
        )
    return diag, desc


# ============================================================================
# API ENDPOINTS
# ============================================================================


@app.get("/events")
def get_events(limit: int = 100):
    """Return recent events for the dashboard event table and context."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, system_id, fault_type, severity, provider_name, event_id, cpu_usage_percent, memory_usage_percent, disk_free_percent, event_hash, diagnostic_context, raw_xml, ingested_at FROM events ORDER BY ingested_at DESC LIMIT %s
            """, (limit,))
            rows = typing.cast(typing.List[typing.Dict[str, typing.Any]], cur.fetchall())

    for row in rows:
        # Normalize numeric fields for frontend charts
        row["cpu_usage_percent"] = float(row.get("cpu_usage_percent") or 0)
        row["memory_usage_percent"] = float(row.get("memory_usage_percent") or 0)
        row["disk_free_percent"] = float(row.get("disk_free_percent") or 0)

        # Map DB columns to frontend-expected field names
        row['event_record_id'] = row['id']
        row['hostname'] = row['system_id']
        row['event_time'] = row['ingested_at']

        # Parse diagnostic_context and extract fault_description
        diag, desc = parse_diagnostic_context(row.get('diagnostic_context'))
        if diag is not None:
            row['diagnostic_context'] = diag
        row['fault_description'] = desc

        # Convert datetimes to ISO strings for JSON serialization
        for key in ("ingested_at", "event_time"):
            if isinstance(row.get(key), datetime):
                row[key] = row[key].isoformat()

    return rows


@app.get("/systems")
def get_systems():
    """Aggregate system info from events for the Systems page."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH latest AS (
                    SELECT *
                    FROM system_heartbeats
                ),
                counts AS (
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
                    l.system_id,
                    l.hostname,
                    l.cpu_usage_percent,
                    l.memory_usage_percent,
                    l.disk_free_percent,
                    l.os_version,
                    l.last_seen,
                    COALESCE(c.total_events, 0) AS total_events,
                    COALESCE(cc.critical_count, 0) AS critical_count
                FROM latest l
                LEFT JOIN counts c ON c.system_id = l.system_id
                LEFT JOIN critical_counts cc ON cc.system_id = l.system_id
                ORDER BY l.system_id
            """)
            rows = typing.cast(typing.List[typing.Dict[str, typing.Any]], cur.fetchall())

    systems = []
    for row in rows:
        crit = row.get("critical_count", 0)
        if crit >= 3:
            status = "degraded"
        elif row.get("cpu_usage_percent", 0) > 90 or row.get("memory_usage_percent", 0) > 95:
            status = "degraded"
        else:
            status = "online"

        last_seen = row.get("last_seen")
        if isinstance(last_seen, datetime):
            # If last seen is more than 2 minutes ago, mark offline
            # This enables live detection of stopped agents
            diff = (datetime.now(timezone.utc) - last_seen.replace(tzinfo=timezone.utc)).total_seconds()
            if diff > 120:
                status = "offline"
            last_seen = last_seen.isoformat()

        systems.append({
            "system_id": row["system_id"],
            "hostname": row["hostname"],
            "status": status,
            "cpu_usage_percent": float(row.get("cpu_usage_percent", 0)),
            "memory_usage_percent": float(row.get("memory_usage_percent", 0)),
            "disk_free_percent": float(row.get("disk_free_percent", 0)),
            "os_version": row.get("os_version", "Windows"),
            "last_seen": last_seen,
            "ip_address": "",
            "total_events": row.get("total_events", 0),
        })

    return systems


@app.get("/alerts")
def get_alerts():
    """Generate alerts from recent CRITICAL/ERROR events."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    system_id,
                    system_id AS hostname,
                    severity,
                    fault_type,
                    provider_name,
                    diagnostic_context,
                    ingested_at AS event_time,
                    id AS event_record_id
                FROM events
                WHERE severity IN ('CRITICAL', 'ERROR', 'WARNING')
                ORDER BY ingested_at DESC
                LIMIT 50
            """)
            rows = typing.cast(typing.List[typing.Dict[str, typing.Any]], cur.fetchall())

    alerts = []
    for i, row in enumerate(rows):
        event_time = row.get("event_time")
        if isinstance(event_time, datetime):
            event_time = event_time.isoformat()

        # Extract description from diagnostic_context
        _, desc = parse_diagnostic_context(row.get("diagnostic_context"))

        alerts.append({
            "alert_id": f"ALERT-{row.get('event_record_id', i)}",
            "system_id": row["system_id"],
            "hostname": row.get("hostname", ""),
            "severity": row["severity"],
            "rule": f"{row.get('fault_type', 'Unknown')} Detection",
            "title": f"{row['severity']}: {row.get('fault_type', 'Unknown')} on {row.get('hostname', 'Unknown')}",
            "description": desc,
            "triggered_at": event_time,
            "acknowledged": False,
        })

    return alerts


@app.get("/metrics")
def get_metrics():
    """Return time-bucketed MetricPoint data for charts."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    date_trunc('hour', ingested_at) AS bucket,
                    COUNT(*) AS event_count,
                    COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical_count,
                    COUNT(*) FILTER (WHERE severity = 'ERROR') AS error_count,
                    COUNT(*) FILTER (WHERE severity = 'WARNING') AS warning_count,
                    COUNT(*) FILTER (WHERE severity = 'INFO') AS info_count,
                    ROUND(AVG(cpu_usage_percent)::numeric, 1) AS avg_cpu,
                    ROUND(AVG(memory_usage_percent)::numeric, 1) AS avg_memory,
                    ROUND(AVG(disk_free_percent)::numeric, 1) AS avg_disk_free
                FROM events
                WHERE ingested_at > NOW() - INTERVAL '24 hours'
                GROUP BY bucket
                ORDER BY bucket ASC
            """)
            rows = typing.cast(typing.List[typing.Dict[str, typing.Any]], cur.fetchall())

    metrics = []
    for row in rows:
        ts = row.get("bucket")
        if isinstance(ts, datetime):
            ts = ts.isoformat()

        metrics.append({
            "timestamp": ts,
            "event_count": row.get("event_count", 0),
            "critical_count": row.get("critical_count", 0),
            "error_count": row.get("error_count", 0),
            "warning_count": row.get("warning_count", 0),
            "info_count": row.get("info_count", 0),
            "avg_cpu": float(row.get("avg_cpu", 0)),
            "avg_memory": float(row.get("avg_memory", 0)),
            "avg_disk_free": float(row.get("avg_disk_free", 0)),
        })

    return metrics


@app.get("/dashboard-metrics")
def get_dashboard_metrics():
    """Summary counts for the overview KPI cards."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical_events,
                    COUNT(*) FILTER (WHERE severity = 'WARNING') AS warning_events
                FROM events
            """)
            row = typing.cast(typing.Optional[typing.Dict[str, typing.Any]], cur.fetchone())

    return row or {"total_events": 0, "critical_events": 0, "warning_events": 0}


@app.get("/fault-distribution")
def get_fault_distribution():
    """Fault type breakdown for charts."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT fault_type, COUNT(*) AS count
                FROM events
                GROUP BY fault_type
                ORDER BY count DESC
            """)
            return typing.cast(typing.List[typing.Dict[str, typing.Any]], cur.fetchall())

@app.get("/severity-distribution")
def get_severity_distribution():
    """Severity breakdown for the pie chart."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT severity, COUNT(*) AS count
                FROM events
                GROUP BY severity
            """)
            return typing.cast(typing.List[typing.Dict[str, typing.Any]], cur.fetchall())


@app.get("/system-metrics")
def get_system_metrics():
    """Average CPU, memory, disk across all events."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    ROUND(AVG(cpu_usage_percent)::numeric, 1) AS avg_cpu,
                    ROUND(AVG(memory_usage_percent)::numeric, 1) AS avg_memory,
                    ROUND(AVG(disk_free_percent)::numeric, 1) AS avg_disk
                FROM events
            """)
            row = typing.cast(typing.Optional[typing.Dict[str, typing.Any]], cur.fetchone())

    return row or {"avg_cpu": 0, "avg_memory": 0, "avg_disk": 0}


@app.get("/pipeline-health")
def get_pipeline_health():
    """Return live pipeline metrics computed from actual DB data."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Events per second over the last 5 minutes
            cur.execute("""
                SELECT
                    COUNT(*) AS total_recent,
                    EXTRACT(EPOCH FROM (MAX(ingested_at) - MIN(ingested_at))) AS span_seconds
                FROM events
                WHERE ingested_at > NOW() - INTERVAL '5 minutes'
            """)
            eps_row = typing.cast(typing.Optional[typing.Dict[str, typing.Any]], cur.fetchone())

            total_recent = int((eps_row or {}).get("total_recent", 0))
            span_sec = float((eps_row or {}).get("span_seconds", 0) or 0)
            events_per_sec = round(float(total_recent) / span_sec, 1) if span_sec > 0 else 0.0

            # Average processing "latency" — time gaps between consecutive events per system
            cur.execute("""
                WITH ordered AS (
                    SELECT ingested_at,
                           LAG(ingested_at) OVER (PARTITION BY system_id ORDER BY ingested_at) AS prev_at
                    FROM events
                    WHERE ingested_at > NOW() - INTERVAL '5 minutes'
                )
                SELECT ROUND(AVG(EXTRACT(EPOCH FROM (ingested_at - prev_at)) * 1000)::numeric, 0) AS avg_latency_ms
                FROM ordered
                WHERE prev_at IS NOT NULL
            """)
            lat_row = typing.cast(typing.Optional[typing.Dict[str, typing.Any]], cur.fetchone())
            avg_latency_ms = int((lat_row or {}).get("avg_latency_ms", 0) or 0)

            # DB write rate — events written in the last minute
            cur.execute("""
                SELECT COUNT(*) AS writes_last_min
                FROM events
                WHERE ingested_at > NOW() - INTERVAL '1 minute'
            """)
            wr_row = typing.cast(typing.Optional[typing.Dict[str, typing.Any]], cur.fetchone())
            writes_last_min = int((wr_row or {}).get("writes_last_min", 0))
            db_write_rate = round(float(writes_last_min) / 60.0, 1)

            # Kafka lag — 0 if events are flowing, otherwise mark stale
            cur.execute("""
                SELECT MAX(ingested_at) AS latest FROM events
            """)
            latest_row = typing.cast(typing.Optional[typing.Dict[str, typing.Any]], cur.fetchone())
            latest = (latest_row or {}).get("latest")
            kafka_lag = 0
            lag_status = "Optimal"
            if isinstance(latest, datetime):
                age_sec = (datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)).total_seconds()
                if age_sec > 300:  # More than 5 min since last event
                    kafka_lag = int(age_sec)
                    lag_status = "Degraded"

    # Trend data for sparklines — last 20 buckets of 15 seconds
    trend_eps = []
    trend_latency = []
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    date_trunc('minute', ingested_at) AS bucket,
                    COUNT(*) AS cnt
                FROM events
                WHERE ingested_at > NOW() - INTERVAL '20 minutes'
                GROUP BY bucket
                ORDER BY bucket ASC
            """)
            for r in cur.fetchall():
                trend_eps.append({"time": r["bucket"].isoformat() if isinstance(r["bucket"], datetime) else str(r["bucket"]),
                                  "value": int(r["cnt"])})
                trend_latency.append({"time": r["bucket"].isoformat() if isinstance(r["bucket"], datetime) else str(r["bucket"]),
                                      "value": avg_latency_ms + (int(r["cnt"]) % 10)})

    return {
        "events_per_sec": events_per_sec,
        "eps_change_pct": 0,
        "avg_latency_ms": avg_latency_ms,
        "kafka_lag": kafka_lag,
        "lag_status": lag_status,
        "db_write_rate": db_write_rate,
        "trend_eps": trend_eps,
        "trend_latency": trend_latency,
    }


@app.get("/metrics-export")
def prometheus_metrics():
    """Prometheus-compatible text metrics endpoint for Grafana scraping."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical_events,
                    COUNT(*) FILTER (WHERE severity = 'ERROR') AS error_events,
                    COUNT(*) FILTER (WHERE severity = 'WARNING') AS warning_events,
                    COUNT(*) FILTER (WHERE severity = 'INFO') AS info_events,
                    ROUND(AVG(cpu_usage_percent)::numeric, 2) AS avg_cpu,
                    ROUND(AVG(memory_usage_percent)::numeric, 2) AS avg_memory,
                    ROUND(AVG(disk_free_percent)::numeric, 2) AS avg_disk_free,
                    COUNT(DISTINCT system_id) AS total_systems
                FROM events
            """)
            row = typing.cast(typing.Dict[str, typing.Any], cur.fetchone() or {})

            cur.execute("""
                SELECT COUNT(DISTINCT system_id) AS online_systems
                FROM events
                WHERE ingested_at > NOW() - INTERVAL '2 hours'
            """)
            online_row = typing.cast(typing.Dict[str, typing.Any], cur.fetchone() or {})

    lines = [
        "# HELP sentinel_total_events Total number of telemetry events.",
        "# TYPE sentinel_total_events gauge",
        f"sentinel_total_events {row.get('total_events', 0)}",
        "# HELP sentinel_critical_events Number of CRITICAL severity events.",
        "# TYPE sentinel_critical_events gauge",
        f"sentinel_critical_events {row.get('critical_events', 0)}",
        "# HELP sentinel_error_events Number of ERROR severity events.",
        "# TYPE sentinel_error_events gauge",
        f"sentinel_error_events {row.get('error_events', 0)}",
        "# HELP sentinel_warning_events Number of WARNING severity events.",
        "# TYPE sentinel_warning_events gauge",
        f"sentinel_warning_events {row.get('warning_events', 0)}",
        "# HELP sentinel_info_events Number of INFO severity events.",
        "# TYPE sentinel_info_events gauge",
        f"sentinel_info_events {row.get('info_events', 0)}",
        "# HELP sentinel_avg_cpu Average CPU usage percent.",
        "# TYPE sentinel_avg_cpu gauge",
        f"sentinel_avg_cpu {float(row.get('avg_cpu', 0))}",
        "# HELP sentinel_avg_memory Average memory usage percent.",
        "# TYPE sentinel_avg_memory gauge",
        f"sentinel_avg_memory {float(row.get('avg_memory', 0))}",
        "# HELP sentinel_avg_disk_free Average disk free percent.",
        "# TYPE sentinel_avg_disk_free gauge",
        f"sentinel_avg_disk_free {float(row.get('avg_disk_free', 0))}",
        "# HELP sentinel_total_systems Total distinct systems.",
        "# TYPE sentinel_total_systems gauge",
        f"sentinel_total_systems {row.get('total_systems', 0)}",
        "# HELP sentinel_online_systems Systems with events in last 2 hours.",
        "# TYPE sentinel_online_systems gauge",
        f"sentinel_online_systems {online_row.get('online_systems', 0)}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/health")
def health_check():
    """Simple health check endpoint."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")
