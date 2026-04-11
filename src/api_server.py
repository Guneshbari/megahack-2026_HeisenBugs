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

import asyncio
import json
import hashlib
import logging
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from urllib import error as urllib_error, request as urllib_request

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
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

from shared.resilience_constants import (
    API_RESPONSE_TIMEOUT_SECONDS,
    DB_QUERY_TIMEOUT_SECONDS,
)
from shared.api_constants import (
    FIREBASE_AUTH_ENABLED,
    API_CACHE_TTL_SECONDS,
    API_CORS_ALLOWED_ORIGINS,
    API_MAX_EVENTS_LIMIT,
    ALERT_ACK_COOLDOWN_MINUTES,
    ALERT_ESCALATION_TIMEOUT_SECONDS,
    ALERT_ESCALATION_WEBHOOK_URL,
    ALERT_RULE_LOOKBACK_MINUTES,
)
from shared.db_constants import (
    get_db_config,
    DB_POOL_MIN_CONN,
    DB_POOL_MAX_CONN,
)

_DB_CONFIG = get_db_config()
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
            "latency_ms": float(f"{float(latency_ms):.2f}"),
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

    # Ensure audit_logs table exists
    try:
        with _get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS audit_logs (
                        id SERIAL PRIMARY KEY,
                        user_id VARCHAR(100),
                        action VARCHAR(100),
                        resource_id VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE IF NOT EXISTS systems (
                        system_id VARCHAR(100) PRIMARY KEY,
                        hostname VARCHAR(255),
                        ip_address VARCHAR(100),
                        agent_key VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE IF NOT EXISTS alert_rules (
                        id SERIAL PRIMARY KEY,
                        rule_name VARCHAR(255) NOT NULL,
                        condition TEXT NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        threshold INT NOT NULL DEFAULT 1,
                        cooldown_minutes INT NOT NULL DEFAULT 30,
                        escalation_target TEXT,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE IF NOT EXISTS alerts (
                        id SERIAL PRIMARY KEY,
                        source_event_id INTEGER,
                        alert_key VARCHAR(64) NOT NULL,
                        source_type VARCHAR(20) NOT NULL DEFAULT 'native',
                        rule_id INTEGER,
                        rule_name VARCHAR(255) NOT NULL,
                        system_id VARCHAR(100) NOT NULL,
                        hostname VARCHAR(255) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        title TEXT NOT NULL,
                        description TEXT NOT NULL,
                        occurrence_count INT NOT NULL DEFAULT 1,
                        first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
                        acknowledged_at TIMESTAMP WITH TIME ZONE,
                        escalated BOOLEAN NOT NULL DEFAULT FALSE,
                        escalated_at TIMESTAMP WITH TIME ZONE,
                        assigned_to VARCHAR(100),
                        suppressed_until TIMESTAMP WITH TIME ZONE,
                        escalation_status VARCHAR(20) NOT NULL DEFAULT 'pending',
                        escalation_target TEXT,
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb
                    );
                """)
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_alerts_last_seen
                        ON alerts(last_seen_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_alerts_system_seen
                        ON alerts(system_id, last_seen_at DESC);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_active_key
                        ON alerts(alert_key)
                        WHERE acknowledged = FALSE;
                    """
                )
                for column_name, column_type, default_value in [
                    ("cooldown_minutes", "INT", "30"),
                    ("escalation_target", "TEXT", "NULL"),
                    ("enabled", "BOOLEAN", "TRUE"),
                    ("created_at", "TIMESTAMP WITH TIME ZONE", "CURRENT_TIMESTAMP"),
                ]:
                    cur.execute(
                        f"ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS {column_name} {column_type} DEFAULT {default_value};"
                    )
            conn.commit()
    except Exception as exc:
        logger.error("[startup] Failed to create audit_logs table: %s", exc)

    yield
    # Shutdown
    if _pool:
        _pool.closeall()
        _pool = None


import os as _os
_is_dev = _os.getenv("SENTINEL_ENV", "production").lower() == "development"

app = FastAPI(
    title="SentinelCore API",
    description="Live telemetry data API for the SentinelCore dashboard",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs" if _is_dev else None,
    redoc_url="/redoc" if _is_dev else None,
    openapi_url="/openapi.json" if _is_dev else None,
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
                            self._entries.pop(k, None)
                        if len(self._entries) >= self._max_size:
                            # Evict key with earliest expiry
                            oldest = min(self._entries.keys(), key=lambda k: self._entries[k][0])
                            self._entries.pop(oldest, None)

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
                    **_DB_CONFIG,
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
            decoded_token = firebase_auth.verify_id_token(id_token, check_revoked=True)
            request.state.uid = decoded_token.get("uid", "unknown")
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


def _verify_websocket_token(id_token: str) -> Tuple[bool, Optional[str], str]:
    """Validate Firebase token for websocket connections."""
    token = _safe_text(id_token)
    if not token:
        return False, None, "missing_websocket_token"
    if not _FIREBASE_ADMIN_READY:
        return False, None, "firebase_sdk_not_ready"
    try:
        decoded_token = firebase_auth.verify_id_token(token, check_revoked=True)
        uid = _safe_text(decoded_token.get("uid"))
        if not uid:
            return False, None, "missing_uid"
        return True, uid, "ok"
    except firebase_auth.RevokedIdTokenError:
        return False, None, "revoked_token"
    except firebase_auth.ExpiredIdTokenError:
        return False, None, "expired_token"
    except Exception as exc:
        return False, None, str(exc)


# ============================================================================
# QUERY HELPERS
# ============================================================================

def _exec_query(sql: str, params: Any = None, endpoint: str = "query") -> List[Dict[str, Any]]:
    """
    Execute a SELECT query with timeout + retry protection.
    Returns a list of row dicts on success, [] on any failure.
    """
    def _run() -> List[Dict[str, Any]]:
        with _get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
        return []

    def _with_retry() -> List[Dict[str, Any]]:
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


def _exec_one(sql: str, params: Any = None, endpoint: str = "query") -> Dict[str, Any]:
    """
    Execute a SELECT query expecting one row.
    Returns a dict on success, {} on failure.
    """
    def _run() -> Dict[str, Any]:
        with _get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else {}
        return {}

    def _with_retry() -> Dict[str, Any]:
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


def _format_event_row(row: Dict[str, Any], include_raw_xml: bool = False) -> Dict[str, Any]:
    """Normalize an event row into the frontend event shape."""
    row["cpu_usage_percent"] = _f(row.get("cpu_usage_percent"))
    row["memory_usage_percent"] = _f(row.get("memory_usage_percent"))
    row["disk_free_percent"] = _f(row.get("disk_free_percent"))
    row["confidence_score"] = _f(row.get("confidence_score"), 0.20)
    row["event_message"] = row.get("event_message") or ""
    row["parsed_message"] = row.get("parsed_message") or ""
    row["normalized_message"] = row.get("normalized_message") or ""
    row["fault_subtype"] = row.get("fault_subtype") or ""
    row["event_record_id"] = row["id"]
    row["hostname"] = row.get("hostname") or row.get("system_id", "")
    row["event_time"] = _iso(row.get("ingested_at"))
    diag, desc = _parse_diag(row.get("diagnostic_context"))
    if diag is not None:
        row["diagnostic_context"] = diag
    row["fault_description"] = desc or row.get("parsed_message") or row.get("event_message") or row.get("fault_type") or ""
    row["ingested_at"] = _iso(row.get("ingested_at"))
    if not include_raw_xml:
        row.pop("raw_xml", None)
    return row


def _format_alert_row(row: Dict[str, Any], index: int = 0) -> Dict[str, Any]:
    """Convert DB alert rows into the frontend alert shape."""
    metadata = row.get("metadata")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    if not isinstance(metadata, dict):
        metadata = {}

    return {
        "alert_id": f"ALERT-{row.get('id', index)}",
        "system_id": row.get("system_id", "unknown"),
        "hostname": row.get("hostname") or row.get("system_id") or "unknown",
        "severity": row.get("severity", "WARNING"),
        "rule": row.get("rule_name", "Unknown Rule"),
        "title": row.get("title", "Untitled Alert"),
        "description": row.get("description", ""),
        "triggered_at": _iso(row.get("first_seen_at") or row.get("last_seen_at")),
        "acknowledged": bool(row.get("acknowledged")),
        "acknowledged_at": _iso(row.get("acknowledged_at")),
        "escalated": bool(row.get("escalated")),
        "escalated_at": _iso(row.get("escalated_at")),
        "assigned_to": row.get("assigned_to"),
        "occurrence_count": row.get("occurrence_count", 1),
        "acknowledged_by": metadata.get("acknowledged_by"),
        "status": "acknowledged" if row.get("acknowledged") else "active",
    }


def _send_escalation_webhook(payload: Dict[str, Any], target_url: str) -> Tuple[bool, str]:
    """Best-effort webhook delivery for escalated alerts."""
    if not target_url:
        return False, "no_target_configured"

    req = urllib_request.Request(
        target_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=ALERT_ESCALATION_TIMEOUT_SECONDS) as response:
            status_code = getattr(response, "status", 200)
            return 200 <= status_code < 300, f"http_{status_code}"
    except urllib_error.URLError as exc:
        return False, str(exc.reason)
    except Exception as exc:
        return False, str(exc)


def _safe_text(value: Any, fallback: str = "") -> str:
    """Normalize arbitrary values into trimmed strings."""
    if value is None:
        return fallback
    return str(value).strip()


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    """Convert values to float with a deterministic fallback."""
    try:
        return float(value) if value is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _parse_rule_condition(condition: str) -> Optional[Tuple[str, str, str]]:
    """Parse simple rule expressions such as `cpu > 90`."""
    import re
    match = re.match(r"^\s*([a-zA-Z0-9_\.]+)\s*(>=|<=|!=|=|==|>|<|contains)\s*(.+?)\s*$", condition or "")
    if not match:
        return None
    field_name, operator, raw_value = match.groups()
    return field_name.strip(), operator.strip().lower(), raw_value.strip().strip("'\"")


def _event_rule_value(event_row: Dict[str, Any], field_name: str) -> Any:
    """Resolve field aliases for rule evaluation."""
    aliases = {
        "cpu": "cpu_usage_percent",
        "cpu_usage": "cpu_usage_percent",
        "memory": "memory_usage_percent",
        "mem": "memory_usage_percent",
        "disk": "disk_free_percent",
        "disk_free": "disk_free_percent",
        "message": "normalized_message",
        "provider": "provider_name",
    }
    resolved = aliases.get(field_name.strip().lower(), field_name.strip().lower())
    if resolved in event_row:
        return event_row.get(resolved)
    diagnostic_context = event_row.get("diagnostic_context")
    if isinstance(diagnostic_context, str):
        try:
            diagnostic_context = json.loads(diagnostic_context)
        except json.JSONDecodeError:
            diagnostic_context = {}
    if isinstance(diagnostic_context, dict):
        return diagnostic_context.get(resolved)
    return None


def _rule_matches_event(rule_row: Dict[str, Any], event_row: Dict[str, Any]) -> bool:
    """Evaluate a simple rule against an event record."""
    parsed = _parse_rule_condition(_safe_text(rule_row.get("condition")))
    if not parsed:
        return False
    field_name, operator, expected_raw = parsed
    actual_value = _event_rule_value(event_row, field_name)
    if actual_value is None:
        return False
    if operator == "contains":
        return expected_raw.lower() in _safe_text(actual_value).lower()
    actual_number = _safe_float(actual_value, float("nan"))
    expected_number = _safe_float(expected_raw, float("nan"))
    both_numeric = not any(map(lambda value: value != value, [actual_number, expected_number]))
    if both_numeric:
        comparisons = {
            ">": actual_number > expected_number,
            "<": actual_number < expected_number,
            ">=": actual_number >= expected_number,
            "<=": actual_number <= expected_number,
            "=": actual_number == expected_number,
            "==": actual_number == expected_number,
            "!=": actual_number != expected_number,
        }
        return comparisons.get(operator, False)
    actual_text = _safe_text(actual_value).lower()
    expected_text = expected_raw.lower()
    comparisons = {
        "=": actual_text == expected_text,
        "==": actual_text == expected_text,
        "!=": actual_text != expected_text,
        ">": actual_text > expected_text,
        "<": actual_text < expected_text,
        ">=": actual_text >= expected_text,
        "<=": actual_text <= expected_text,
    }
    return comparisons.get(operator, False)


def _build_native_alert_key(event_row: Dict[str, Any]) -> str:
    """Build the same stable native alert key used during live ingestion."""
    key_parts = [
        "native",
        _safe_text(event_row.get("system_id"), "unknown"),
        _safe_text(event_row.get("severity"), "INFO"),
        _safe_text(event_row.get("fault_type"), "UNKNOWN"),
        _safe_text(event_row.get("fault_subtype"), "UNKNOWN"),
        _safe_text(event_row.get("provider_name"), "unknown"),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()


def _build_rule_alert_key(rule_row: Dict[str, Any], event_row: Dict[str, Any]) -> str:
    """Build the same stable rule alert key used during live ingestion."""
    key_parts = [
        "rule",
        str(rule_row.get("id", "0")),
        _safe_text(event_row.get("system_id"), "unknown"),
        _safe_text(event_row.get("severity"), "INFO"),
        _safe_text(event_row.get("fault_type"), "UNKNOWN"),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()


def _backfill_native_alerts(limit: int = 500) -> int:
    """Populate the alerts table from recent severe events when alerts are missing."""
    with _get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id,
                    system_id,
                    COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                    severity,
                    fault_type,
                    fault_subtype,
                    provider_name,
                    event_message,
                    parsed_message,
                    ingested_at
                FROM events
                WHERE severity IN ('CRITICAL', 'ERROR', 'WARNING')
                ORDER BY ingested_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            recent_events = [dict(row) for row in cur.fetchall()]
            newest_by_key: Dict[str, Dict[str, Any]] = {}
            for event_row in recent_events:
                alert_key = _build_native_alert_key(event_row)
                newest_by_key.setdefault(alert_key, event_row)

            inserted = 0
            for alert_key, event_row in newest_by_key.items():
                cur.execute(
                    """
                    INSERT INTO alerts (
                        source_event_id,
                        alert_key,
                        source_type,
                        rule_name,
                        system_id,
                        hostname,
                        severity,
                        title,
                        description,
                        occurrence_count,
                        first_seen_at,
                        last_seen_at,
                        metadata
                    )
                    SELECT
                        %s, %s, 'native', %s, %s, %s, %s, %s, %s, 1, %s, %s, %s::jsonb
                    WHERE NOT EXISTS (
                        SELECT 1 FROM alerts WHERE alert_key = %s AND acknowledged = FALSE
                    )
                    """,
                    (
                        event_row.get("id"),
                        alert_key,
                        f"{_safe_text(event_row.get('fault_type'), 'Unknown')} Detection",
                        _safe_text(event_row.get("system_id"), "unknown"),
                        _safe_text(event_row.get("hostname"), _safe_text(event_row.get("system_id"), "unknown")),
                        _safe_text(event_row.get("severity"), "WARNING"),
                        f"{_safe_text(event_row.get('severity'), 'WARNING')}: {_safe_text(event_row.get('fault_type'), 'Unknown')} on {_safe_text(event_row.get('hostname'), _safe_text(event_row.get('system_id'), 'unknown'))}",
                        _safe_text(event_row.get("parsed_message")) or _safe_text(event_row.get("event_message")) or _safe_text(event_row.get("fault_type"), "No description"),
                        event_row.get("ingested_at"),
                        event_row.get("ingested_at"),
                        json.dumps({"provider_name": event_row.get("provider_name")}),
                        alert_key,
                    ),
                )
                inserted += cur.rowcount or 0
        conn.commit()
    return inserted


def _backfill_rule_alerts(rule_id: int) -> int:
    """Evaluate a newly created rule against recent events to seed alert rows."""
    with _get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, rule_name, condition, severity, threshold, cooldown_minutes, escalation_target
                FROM alert_rules
                WHERE id = %s AND enabled = TRUE
                """,
                (rule_id,),
            )
            rule_row = dict(cur.fetchone() or {})
            if not rule_row:
                return 0

            cur.execute(
                """
                SELECT
                    id,
                    system_id,
                    COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                    severity,
                    fault_type,
                    fault_subtype,
                    provider_name,
                    diagnostic_context,
                    cpu_usage_percent,
                    memory_usage_percent,
                    disk_free_percent,
                    event_message,
                    parsed_message,
                    normalized_message,
                    ingested_at
                FROM events
                WHERE ingested_at >= NOW() - (%s || ' minutes')::interval
                ORDER BY ingested_at DESC
                LIMIT 500
                """,
                (str(ALERT_RULE_LOOKBACK_MINUTES),),
            )
            recent_events = [dict(row) for row in cur.fetchall()]
            by_system: Dict[str, List[Dict[str, Any]]] = {}
            for event_row in recent_events:
                if _rule_matches_event(rule_row, event_row):
                    by_system.setdefault(event_row.get("system_id", "unknown"), []).append(event_row)

            inserted = 0
            for system_id, matches in by_system.items():
                if len(matches) < max(1, int(rule_row.get("threshold") or 1)):
                    continue
                newest = matches[0]
                alert_key = _build_rule_alert_key(rule_row, newest)
                cur.execute(
                    """
                    INSERT INTO alerts (
                        source_event_id,
                        alert_key,
                        source_type,
                        rule_id,
                        rule_name,
                        system_id,
                        hostname,
                        severity,
                        title,
                        description,
                        occurrence_count,
                        first_seen_at,
                        last_seen_at,
                        escalation_target,
                        metadata
                    )
                    SELECT
                        %s, %s, 'rule', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    WHERE NOT EXISTS (
                        SELECT 1 FROM alerts WHERE alert_key = %s AND acknowledged = FALSE
                    )
                    """,
                    (
                        newest.get("id"),
                        alert_key,
                        rule_row.get("id"),
                        rule_row.get("rule_name"),
                        newest.get("system_id"),
                        newest.get("hostname"),
                        _safe_text(rule_row.get("severity"), newest.get("severity") or "WARNING"),
                        f"{_safe_text(rule_row.get('severity'), newest.get('severity') or 'WARNING')}: {_safe_text(rule_row.get('rule_name'), 'Custom Rule')} on {_safe_text(newest.get('hostname'), newest.get('system_id') or 'unknown')}",
                        f"Rule matched: {_safe_text(rule_row.get('condition'))}",
                        len(matches),
                        newest.get("ingested_at"),
                        newest.get("ingested_at"),
                        rule_row.get("escalation_target"),
                        json.dumps({"backfilled": True, "match_count": len(matches)}),
                        alert_key,
                    ),
                )
                inserted += cur.rowcount or 0
        conn.commit()
    return inserted


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

    if _DB_CONFIG.get("password", "") in ("", "changeme123"):
        _log_failure("/startup", "security_posture", "weak_or_default_database_password_in_use")

    if not FIREBASE_AUTH_ENABLED:
        _log_failure("/startup", "security_posture", "firebase_auth_disabled_all_endpoints_public")


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/events")
def get_events(
    limit: int = 100, 
    include_raw_xml: bool = False,
    system_id: Optional[str] = None,
    severity: Optional[str] = None,
    fault_type: Optional[str] = None,
    search: Optional[str] = None
) -> List[Dict]:
    t0 = time.time()
    try:
        limit = _bounded_limit(limit)
        
        conditions = []
        params: List[Any] = []
        if system_id:
            conditions.append(pgsql.SQL("system_id = %s"))
            params.append(system_id)
        if severity:
            conditions.append(pgsql.SQL("severity = %s"))
            params.append(severity)
        if fault_type:
            conditions.append(pgsql.SQL("fault_type = %s"))
            params.append(fault_type)
        if search:
            search_term = f"%{search}%"
            conditions.append(pgsql.SQL("""
                (
                    fault_type ILIKE %s OR
                    system_id ILIKE %s OR
                    COALESCE(hostname, '') ILIKE %s OR
                    provider_name ILIKE %s OR
                    COALESCE(event_message, '') ILIKE %s OR
                    COALESCE(parsed_message, '') ILIKE %s OR
                    COALESCE(normalized_message, '') ILIKE %s OR
                    COALESCE(CAST(diagnostic_context AS TEXT), '') ILIKE %s
                )
            """))
            params.extend([search_term] * 8)
            
        where_clause = pgsql.SQL("")
        if conditions:
            where_clause = pgsql.SQL("WHERE ") + pgsql.SQL(" AND ").join(conditions)

        query = pgsql.SQL("""
            SELECT id, system_id,
                   COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                   fault_type, severity, provider_name, event_id,
                   cpu_usage_percent, memory_usage_percent, disk_free_percent,
                   event_hash, diagnostic_context, raw_xml, ingested_at,
                   event_message, parsed_message, normalized_message,
                   fault_subtype, confidence_score
            FROM events
            {where_clause}
            ORDER BY ingested_at DESC
            LIMIT %s
        """).format(where_clause=where_clause)
        params.append(limit)

        rows = _exec_query(query, tuple(params), endpoint="/events")

        rows = [_format_event_row(row, include_raw_xml=include_raw_xml) for row in rows]

        _log_req("/events", (time.time() - t0) * 1000, "ok", len(rows))
        return rows

    except Exception as exc:
        logger.error("/events error: %s", exc)
        _log_failure("/events", "endpoint", exc)
        _log_req("/events", (time.time() - t0) * 1000, "error")
        return []


@app.websocket("/ws/events")
async def stream_events(websocket: WebSocket) -> None:
    """Best-effort live event stream used by the dashboard for incremental updates."""
    if FIREBASE_AUTH_ENABLED:
        ok, _, auth_error = _verify_websocket_token(websocket.query_params.get("token", ""))
        if not ok:
            _log_failure("/ws/events", "auth", auth_error)
            await websocket.close(code=1013 if auth_error == "firebase_sdk_not_ready" else 4401)
            return

    await websocket.accept()
    last_event_id = 0

    try:
        bootstrap_rows = _exec_query(
            """
            SELECT id, system_id,
                   COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                   fault_type, severity, provider_name, event_id,
                   cpu_usage_percent, memory_usage_percent, disk_free_percent,
                   event_hash, diagnostic_context, raw_xml, ingested_at,
                   event_message, parsed_message, normalized_message,
                   fault_subtype, confidence_score
            FROM events
            ORDER BY id DESC
            LIMIT 50
            """,
            endpoint="/ws/events/bootstrap",
        )
        if bootstrap_rows:
            bootstrap_rows.reverse()
            last_event_id = max(_i(row.get("id")) for row in bootstrap_rows)
            await websocket.send_json([_format_event_row(dict(row), include_raw_xml=False) for row in bootstrap_rows])

        while True:
            rows = _exec_query(
                """
                SELECT id, system_id,
                       COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                       fault_type, severity, provider_name, event_id,
                       cpu_usage_percent, memory_usage_percent, disk_free_percent,
                       event_hash, diagnostic_context, raw_xml, ingested_at,
                       event_message, parsed_message, normalized_message,
                       fault_subtype, confidence_score
                FROM events
                WHERE id > %s
                ORDER BY id ASC
                LIMIT 250
                """,
                (last_event_id,),
                endpoint="/ws/events/poll",
            )

            if rows:
                last_event_id = max(_i(row.get("id")) for row in rows)
                await websocket.send_json([_format_event_row(dict(row), include_raw_xml=False) for row in rows])

            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
    except Exception as exc:
        _log_failure("/ws/events", "websocket", exc)
        try:
            await websocket.close(code=1011)
        except Exception:
            pass


@app.get("/systems")
def get_systems() -> List[Dict]:
    t0 = time.time()
    try:
        def load_systems() -> List[Dict]:
            rows = _exec_query("""
                WITH counts AS (
                    SELECT system_id, SUM(total_events) AS total_events
                    FROM feature_snapshots
                    GROUP BY system_id
                ),
                critical_counts AS (
                    SELECT system_id, SUM(critical_count) AS critical_count
                    FROM feature_snapshots
                    WHERE snapshot_time > NOW() - INTERVAL '1 hour'
                    GROUP BY system_id
                )
                SELECT
                    h.system_id, h.hostname,
                    h.cpu_usage_percent, h.memory_usage_percent, h.disk_free_percent,
                    h.os_version, h.ip_address, h.last_seen,
                    COALESCE(c.total_events, 0)    AS total_events,
                    COALESCE(cc.critical_count, 0) AS critical_count
                FROM system_heartbeats h
                LEFT JOIN counts          c  ON c.system_id  = h.system_id
                LEFT JOIN critical_counts cc ON cc.system_id = h.system_id
                ORDER BY h.system_id
                LIMIT 1000
            """, endpoint="/systems")

            # Fallback to bounded events table (last 24 hours) if snapshots are completely empty/unavailable
            if not any(r.get("total_events", 0) > 0 for r in rows):
                rows = _exec_query("""
                    WITH counts AS (
                        SELECT system_id, COUNT(*) AS total_events
                        FROM events
                        WHERE ingested_at > NOW() - INTERVAL '24 hours'
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
                        h.os_version, h.ip_address, h.last_seen,
                        COALESCE(c.total_events, 0)    AS total_events,
                        COALESCE(cc.critical_count, 0) AS critical_count
                    FROM system_heartbeats h
                    LEFT JOIN counts          c  ON c.system_id  = h.system_id
                    LEFT JOIN critical_counts cc ON cc.system_id = h.system_id
                    ORDER BY h.system_id
                    LIMIT 1000
                """, endpoint="/systems(fallback)")

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
                    # Include ip_address from heartbeat row (was hardcoded empty string)
                    "ip_address":           row.get("ip_address") or "",
                    "total_events":         _i(row.get("total_events")),
                    # Expose data freshness: clients can show a staleness warning
                    "last_updated_at":      last_seen,
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
def get_alerts(limit: int = 200) -> List[Dict]:
    t0 = time.time()
    try:
        limit = _bounded_limit(limit)
        rows = _exec_query(pgsql.SQL("""
            SELECT
                id,
                system_id,
                COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                severity,
                rule_name,
                title,
                description,
                first_seen_at,
                last_seen_at,
                occurrence_count,
                acknowledged,
                acknowledged_at,
                escalated,
                escalated_at,
                assigned_to,
                metadata
            FROM alerts
            ORDER BY last_seen_at DESC
            LIMIT %s
        """), (limit,), endpoint="/alerts")

        if not rows:
            _backfill_native_alerts(limit)
            rows = _exec_query(pgsql.SQL("""
                SELECT
                    id,
                    system_id,
                    COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                    severity,
                    rule_name,
                    title,
                    description,
                    first_seen_at,
                    last_seen_at,
                    occurrence_count,
                    acknowledged,
                    acknowledged_at,
                    escalated,
                    escalated_at,
                    assigned_to,
                    metadata
                FROM alerts
                ORDER BY last_seen_at DESC
                LIMIT %s
            """), (limit,), endpoint="/alerts/backfill")

        alerts = [_format_alert_row(row, i) for i, row in enumerate(rows)]

        _log_req("/alerts", (time.time() - t0) * 1000, "ok", len(alerts))
        return alerts

    except Exception as exc:
        logger.error("/alerts error: %s", exc)
        _log_failure("/alerts", "endpoint", exc)
        _log_req("/alerts", (time.time() - t0) * 1000, "error")
        return []


@app.get("/alerts/recent")
def get_recent_alerts() -> List[Dict]:
    t0 = time.time()
    try:
        rows = _exec_query("""
            SELECT
                id,
                system_id,
                COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                severity,
                rule_name,
                title,
                description,
                first_seen_at,
                last_seen_at,
                occurrence_count,
                acknowledged,
                acknowledged_at,
                escalated,
                escalated_at,
                assigned_to,
                metadata
            FROM alerts
            WHERE acknowledged = FALSE
            ORDER BY last_seen_at DESC
            LIMIT 10
        """, endpoint="/alerts/recent")

        if not rows:
            _backfill_native_alerts(100)
            rows = _exec_query("""
                SELECT
                    id,
                    system_id,
                    COALESCE(NULLIF(hostname, ''), system_id) AS hostname,
                    severity,
                    rule_name,
                    title,
                    description,
                    first_seen_at,
                    last_seen_at,
                    occurrence_count,
                    acknowledged,
                    acknowledged_at,
                    escalated,
                    escalated_at,
                    assigned_to,
                    metadata
                FROM alerts
                WHERE acknowledged = FALSE
                ORDER BY last_seen_at DESC
                LIMIT 10
            """, endpoint="/alerts/recent/backfill")

        alerts = [_format_alert_row(row, i) for i, row in enumerate(rows)]

        _log_req("/alerts/recent", (time.time() - t0) * 1000, "ok", len(alerts))
        return alerts

    except Exception as exc:
        logger.error("/alerts/recent error: %s", exc)
        _log_failure("/alerts/recent", "endpoint", exc)
        return []


class AlertActionRequest(BaseModel):
    alert_id: str

    @property
    def record_id(self) -> Optional[int]:
        """Parse the numeric record ID from 'ALERT-<int>'. Returns None on malformed input."""
        try:
            raw = self.alert_id.replace("ALERT-", "", 1)
            if not raw.isdigit():
                return None
            return int(raw)
        except Exception:
            return None

@app.post("/alerts/acknowledge")
def acknowledge_alert(req: AlertActionRequest, request: Request) -> Dict:
    t0 = time.time()
    try:
        record_id = req.record_id
        if record_id is None:
            _log_failure("/alerts/acknowledge", "validation", f"malformed alert_id: {req.alert_id!r}")
            return {"success": False, "error": "Invalid alert_id format"}
        def _run_ack() -> bool:
            with _get_db() as conn:
                with conn.cursor() as cur:
                    user_id = getattr(request.state, "uid", "anonymous")
                    cur.execute(
                        """
                        UPDATE alerts
                        SET
                            acknowledged = TRUE,
                            acknowledged_at = NOW(),
                            suppressed_until = NOW() + (
                                COALESCE(
                                    NULLIF((SELECT cooldown_minutes FROM alert_rules WHERE id = alerts.rule_id), 0),
                                    %s
                                ) || ' minutes'
                            )::interval,
                            metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                        WHERE id = %s
                        """,
                        (
                            str(ALERT_ACK_COOLDOWN_MINUTES),
                            json.dumps({"acknowledged_by": user_id}),
                            record_id,
                        )
                    )
                    cur.execute(
                        """
                        UPDATE events
                        SET acknowledged = TRUE, acknowledged_at = NOW()
                        WHERE id = (SELECT source_event_id FROM alerts WHERE id = %s)
                        """,
                        (record_id,),
                    )
                    cur.execute(
                        "INSERT INTO audit_logs (user_id, action, resource_id) VALUES (%s, %s, %s)",
                        (user_id, "acknowledge_alert", str(record_id))
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
def escalate_alert(req: AlertActionRequest, request: Request) -> Dict:
    t0 = time.time()
    try:
        record_id = req.record_id
        if record_id is None:
            _log_failure("/alerts/escalate", "validation", f"malformed alert_id: {req.alert_id!r}")
            return {"success": False, "error": "Invalid alert_id format"}
        def _run_esc() -> bool:
            with _get_db() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        UPDATE alerts
                        SET
                            escalated = TRUE,
                            escalated_at = NOW(),
                            escalation_status = 'queued'
                        WHERE id = %s
                        RETURNING *
                        """,
                        (record_id,)
                    )
                    alert_row = dict(cur.fetchone() or {})
                    if alert_row.get("source_event_id") is not None:
                        cur.execute(
                            "UPDATE events SET escalated = TRUE, escalated_at = NOW() WHERE id = %s",
                            (alert_row.get("source_event_id"),),
                        )
                    user_id = getattr(request.state, "uid", "anonymous")
                    cur.execute(
                        "INSERT INTO audit_logs (user_id, action, resource_id) VALUES (%s, %s, %s)",
                        (user_id, "escalate_alert", str(record_id))
                    )
                    target_url = (alert_row.get("escalation_target") or ALERT_ESCALATION_WEBHOOK_URL or "").strip()
                    payload = {
                        "alert_id": f"ALERT-{alert_row.get('id', record_id)}",
                        "system_id": alert_row.get("system_id"),
                        "hostname": alert_row.get("hostname"),
                        "severity": alert_row.get("severity"),
                        "rule_name": alert_row.get("rule_name"),
                        "title": alert_row.get("title"),
                        "description": alert_row.get("description"),
                        "escalated_at": _iso(datetime.now(timezone.utc)),
                    }
                    delivered, delivery_status = _send_escalation_webhook(payload, target_url)
                    cur.execute(
                        """
                        UPDATE alerts
                        SET
                            escalation_status = %s,
                            escalation_target = COALESCE(NULLIF(%s, ''), escalation_target)
                        WHERE id = %s
                        """,
                        (
                            "delivered" if delivered else ("no_target" if not target_url else "failed"),
                            target_url,
                            record_id,
                        ),
                    )
                    cur.execute(
                        "INSERT INTO audit_logs (user_id, action, resource_id) VALUES (%s, %s, %s)",
                        (user_id, f"escalation_delivery:{delivery_status}", str(record_id))
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


class AlertRuleRequest(BaseModel):
    rule_name: str
    condition: str
    severity: str
    threshold: int
    cooldown_minutes: int = ALERT_ACK_COOLDOWN_MINUTES
    escalation_target: Optional[str] = None
    enabled: bool = True

@app.post("/alerts/rules")
def create_alert_rule(req: AlertRuleRequest, request: Request) -> Dict:
    t0 = time.time()
    try:
        def _run_rule_insert() -> Optional[int]:
            with _get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO alert_rules (
                            rule_name, condition, severity, threshold,
                            cooldown_minutes, escalation_target, enabled
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            req.rule_name,
                            req.condition,
                            req.severity,
                            req.threshold,
                            max(1, req.cooldown_minutes),
                            (req.escalation_target or "").strip() or None,
                            req.enabled,
                        )
                    )
                    rule_id = cur.fetchone()[0]
                    user_id = getattr(request.state, "uid", "anonymous")
                    cur.execute(
                        "INSERT INTO audit_logs (user_id, action, resource_id) VALUES (%s, %s, %s)",
                        (user_id, "create_alert_rule", req.rule_name)
                    )
                conn.commit()
            return rule_id
        result, ok = retry_with_backoff(_run_rule_insert, label="/alerts/rules")
        rule_id = int(result) if ok and result else None
        if rule_id is not None:
            _backfill_rule_alerts(rule_id)
        success = rule_id is not None
        _log_req("/alerts/rules", (time.time() - t0) * 1000, "ok" if success else "error")
        return {"success": success, "rule_id": rule_id}
    except Exception as exc:
        _log_failure("/alerts/rules", "endpoint", exc)
        return {"success": False}


class SystemRegisterRequest(BaseModel):
    hostname: str
    ip_address: str
    agent_key: str

@app.post("/systems/register")
def register_system(req: SystemRegisterRequest, request: Request) -> Dict:
    t0 = time.time()
    try:
        system_id = str(uuid.uuid4())
        def _run_sys_insert() -> bool:
            with _get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO systems (system_id, hostname, ip_address, agent_key) VALUES (%s, %s, %s, %s)",
                        (system_id, req.hostname, req.ip_address, req.agent_key)
                    )
                    user_id = getattr(request.state, "uid", "anonymous")
                    cur.execute(
                        "INSERT INTO audit_logs (user_id, action, resource_id) VALUES (%s, %s, %s)",
                        (user_id, "register_system", req.hostname)
                    )
                conn.commit()
            return True
        result, ok = retry_with_backoff(_run_sys_insert, label="/systems/register")
        success = bool(ok and result)
        _log_req("/systems/register", (time.time() - t0) * 1000, "ok" if success else "error")
        return {"success": success, "system_id": system_id if success else None}
    except Exception as exc:
        _log_failure("/systems/register", "endpoint", exc)
        return {"success": False}


class SystemCommandRequest(BaseModel):
    system_id: str
    command: str

@app.post("/systems/command")
def system_command(req: SystemCommandRequest, request: Request) -> Dict:
    t0 = time.time()
    try:
        # Validate command length to prevent excessive audit log entries
        if not req.command or len(req.command) > 500:
            return {"success": False, "output": "Invalid command: must be 1-500 characters"}
        def _run_audit() -> bool:
            with _get_db() as conn:
                with conn.cursor() as cur:
                    user_id = getattr(request.state, "uid", "anonymous")
                    # Truncate command in audit log to prevent log injection
                    safe_cmd = req.command[:200] if req.command else ""
                    cur.execute(
                        "INSERT INTO audit_logs (user_id, action, resource_id) VALUES (%s, %s, %s)",
                        (user_id, f"system_cmd: {safe_cmd}", req.system_id[:100])
                    )
                conn.commit()
            return True
        retry_with_backoff(_run_audit, label="/systems/command")

        # Mock execution response (no real shell execution)
        output = f"[SENTINEL-CMD] Command queued for node {req.system_id}.\n"
        output += f"[SENTINEL-CMD] Dispatched at {datetime.now(timezone.utc).isoformat()}."

        _log_req("/systems/command", (time.time() - t0) * 1000, "ok")
        return {"success": True, "output": output}
    except Exception as exc:
        _log_failure("/systems/command", "endpoint", exc)
        # Do not leak internal exception text to the client
        return {"success": False, "output": "Command dispatch failed. Check server logs."}

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


@app.get("/system-metrics")
def get_system_metrics() -> Dict:
    """
    Fleet-wide average resource metrics from the most recent heartbeat per system.

    Uses system_heartbeats (live data) rather than the events table (historical
    snapshots at event-fire time) so the numbers match what the OS actually reports.
    Falls back to the events table if no heartbeats exist yet.
    """
    t0 = time.time()
    try:
        def load_system_metrics() -> Dict:
            # Primary: latest heartbeat values per system (most accurate, reflects current state)
            row = _exec_one("""
                SELECT
                    ROUND(AVG(cpu_usage_percent)::numeric, 1)    AS avg_cpu,
                    ROUND(AVG(memory_usage_percent)::numeric, 1) AS avg_memory,
                    ROUND(AVG(disk_free_percent)::numeric, 1)    AS avg_disk
                FROM system_heartbeats
                WHERE last_seen > NOW() - INTERVAL '5 minutes'
            """, endpoint="/system-metrics")

            avg_cpu  = _f(row.get("avg_cpu"))
            avg_mem  = _f(row.get("avg_memory"))
            avg_disk = _f(row.get("avg_disk"))

            # Fallback: use last 1 hour of event readings if no fresh heartbeats
            if avg_cpu == 0.0 and avg_mem == 0.0:
                fallback = _exec_one("""
                    SELECT
                        ROUND(AVG(cpu_usage_percent)::numeric, 1)    AS avg_cpu,
                        ROUND(AVG(memory_usage_percent)::numeric, 1) AS avg_memory,
                        ROUND(AVG(disk_free_percent)::numeric, 1)    AS avg_disk
                    FROM events
                    WHERE ingested_at > NOW() - INTERVAL '1 hour'
                """, endpoint="/system-metrics_fallback")
                avg_cpu  = _f(fallback.get("avg_cpu"))
                avg_mem  = _f(fallback.get("avg_memory"))
                avg_disk = _f(fallback.get("avg_disk"))

            return {"avg_cpu": avg_cpu, "avg_memory": avg_mem, "avg_disk": avg_disk}

        result = _response_cache.get_or_set(
            _cache_key("/system-metrics"),
            API_CACHE_TTL_SECONDS,
            load_system_metrics,
        )
        _log_req("/system-metrics", (time.time() - t0) * 1000, "ok")
        return result

    except Exception as exc:
        _log_failure("/system-metrics", "endpoint", exc)
        return {"avg_cpu": 0.0, "avg_memory": 0.0, "avg_disk": 0.0}


@app.get("/metrics")
def get_metrics(
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    window_minutes: Optional[int] = None,
) -> List[Dict]:
    """Time-bucketed metric points. Falls back to feature_snapshots if no recent events.

    Adaptive bucket granularity:
      - ≤ 60 min window  → 5-minute buckets  (max 12 points)
      - ≤ 360 min window → 15-minute buckets (max 24 points)
      - > 360 min window → 1-hour buckets    (max 24 points for 24h)
    """
    t0 = time.time()
    try:
        def load_metrics() -> List[Dict]:
            params: List[Any] = []
            # Resolve bucket size based on window
            wm = window_minutes or 1440  # default 24h
            if wm <= 60:
                bucket_trunc = "5 minutes"
            elif wm <= 360:
                bucket_trunc = "15 minutes"
            else:
                bucket_trunc = "1 hour"

            if start_time and end_time:
                time_filter = pgsql.SQL("WHERE ingested_at >= %s AND ingested_at <= %s")
                params.extend([start_time, end_time])
            elif window_minutes and window_minutes > 0:
                time_filter = pgsql.SQL("WHERE ingested_at > NOW() - (%s * INTERVAL '1 minute')")
                params.append(window_minutes)
            else:
                time_filter = pgsql.SQL("WHERE ingested_at > NOW() - INTERVAL '24 hours'")

            query = pgsql.SQL("""
                SELECT
                    date_trunc({bucket}, ingested_at)                       AS bucket,
                    COUNT(*)                                                 AS event_count,
                    COUNT(*) FILTER (WHERE severity = 'CRITICAL')           AS critical_count,
                    COUNT(*) FILTER (WHERE severity = 'ERROR')              AS error_count,
                    COUNT(*) FILTER (WHERE severity = 'WARNING')            AS warning_count,
                    COUNT(*) FILTER (WHERE severity = 'INFO')               AS info_count,
                    ROUND(AVG(cpu_usage_percent)::numeric, 1)               AS avg_cpu,
                    ROUND(AVG(memory_usage_percent)::numeric, 1)            AS avg_memory,
                    ROUND(AVG(disk_free_percent)::numeric, 1)               AS avg_disk_free
                FROM events
                {time_filter}
                GROUP BY bucket
                ORDER BY bucket ASC
            """).format(
                bucket=pgsql.Literal(bucket_trunc),
                time_filter=time_filter,
            )

            rows = _exec_query(query, tuple(params) if params else None, endpoint="/metrics")

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
            _cache_key("/metrics", start_time=start_time, end_time=end_time, window_minutes=window_minutes),
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
            _cache_key("/dashboard-metrics", window_minutes=window_minutes),
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


# Note: The accurate /system-metrics endpoint backed by system_heartbeats
# is defined after /pipeline-health below.


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
        events_per_sec = float(f"{total_recent / span_sec:.1f}") if span_sec > 0 else 0.0

        # Compute EPS change vs. the prior 5-minute window for trend awareness
        prev_eps_row = _exec_one("""
            SELECT COUNT(*) AS total_prev,
                   EXTRACT(EPOCH FROM (MAX(ingested_at) - MIN(ingested_at))) AS span_prev
            FROM events WHERE ingested_at > NOW() - INTERVAL '10 minutes'
                          AND ingested_at <= NOW() - INTERVAL '5 minutes'
        """, endpoint="/pipeline-health/prev_eps")
        prev_total = _i(prev_eps_row.get("total_prev"))
        prev_span  = _f(prev_eps_row.get("span_prev"))
        prev_eps   = prev_total / prev_span if prev_span > 0 else 0.0
        eps_change_pct = 0
        if prev_eps > 0:
            eps_change_pct = int(round((events_per_sec - prev_eps) / prev_eps * 100))

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
        db_write_rate   = float(f"{_i(wr_row.get('writes_last_min')) / 60.0:.1f}")

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
            "eps_change_pct": eps_change_pct,
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
                SUM(total_events)                                    AS total_events,
                SUM(critical_count)                                  AS critical_events,
                SUM(error_count)                                     AS error_events,
                SUM(warning_count)                                   AS warning_events,
                SUM(info_count)                                      AS info_events,
                ROUND(AVG(cpu_usage_percent)::numeric, 2)            AS avg_cpu,
                ROUND(AVG(memory_usage_percent)::numeric, 2)         AS avg_memory,
                ROUND(AVG(disk_free_percent)::numeric, 2)            AS avg_disk_free,
                COUNT(DISTINCT system_id)                            AS total_systems
            FROM feature_snapshots
        """, endpoint="/metrics-export")

        # Fallback to heavily bounded query if snapshots are empty/broken
        if not row or row.get('total_events') is None:
            row = _exec_one("""
                SELECT
                    COUNT(*)                                             AS total_events,
                    COUNT(*) FILTER (WHERE severity = 'CRITICAL')        AS critical_events,
                    COUNT(*) FILTER (WHERE severity = 'ERROR')           AS error_events,
                    COUNT(*) FILTER (WHERE severity = 'WARNING')         AS warning_events,
                    COUNT(*) FILTER (WHERE severity = 'INFO')            AS info_events,
                    ROUND(AVG(cpu_usage_percent)::numeric, 2)            AS avg_cpu,
                    ROUND(AVG(memory_usage_percent)::numeric, 2)         AS avg_memory,
                    ROUND(AVG(disk_free_percent)::numeric, 2)            AS avg_disk_free,
                    COUNT(DISTINCT system_id)                            AS total_systems
                FROM events
                WHERE ingested_at > NOW() - INTERVAL '24 hours'
            """, endpoint="/metrics-export(fallback)")

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
        return {"status": "degraded"}

@app.get("/report/generate")
def generate_report() -> JSONResponse:
    t0 = time.time()
    try:
        metrics = get_dashboard_metrics()
        failures = get_system_failures(6)
        report_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dashboard_metrics": metrics,
            "system_failures": failures
        }
        _log_req("/report/generate", (time.time() - t0) * 1000, "ok")
        return JSONResponse(content=report_data)
    except Exception as exc:
        _log_failure("/report/generate", "endpoint", exc)
        # Do not leak internal exception details to clients
        return JSONResponse(content={"error": "Report generation failed. Check server logs."}, status_code=500)

@app.get("/ml/predictions")
def get_ml_predictions(limit: int = 100) -> JSONResponse:
    t0 = time.time()
    try:
        rows = _exec_query("""
            SELECT id, system_id, prediction_time, anomaly_score, failure_probability, predicted_fault, model_version
            FROM ml_predictions
            ORDER BY prediction_time DESC
            LIMIT %s
        """, (limit,), endpoint="/ml/predictions")
        
        result = []
        for r in rows:
            r['prediction_time'] = r['prediction_time'].isoformat() if r.get('prediction_time') else None
            r['anomaly_score'] = float(r['anomaly_score']) if r.get('anomaly_score') is not None else 0.0
            r['failure_probability'] = float(r['failure_probability']) if r.get('failure_probability') is not None else 0.0
            result.append(r)
            
        _log_req("/ml/predictions", (time.time() - t0) * 1000, "ok")
        return JSONResponse(content=result)
    except Exception as exc:
        _log_failure("/ml/predictions", "endpoint", exc)
        return JSONResponse(content={"error": str(exc)}, status_code=500)

@app.get("/ml/anomaly")
def get_anomaly_scores(limit: int = 1000) -> JSONResponse:
    t0 = time.time()
    try:
        rows = _exec_query("""
            SELECT system_id, anomaly_score, prediction_time
            FROM ml_predictions
            ORDER BY prediction_time DESC
            LIMIT %s
        """, (limit,), endpoint="/ml/anomaly")
        
        result = []
        for r in rows:
            r['prediction_time'] = r['prediction_time'].isoformat() if r.get('prediction_time') else None
            r['anomaly_score'] = float(r['anomaly_score']) if r.get('anomaly_score') is not None else 0.0
            result.append(r)
            
        _log_req("/ml/anomaly", (time.time() - t0) * 1000, "ok")
        return JSONResponse(content=result)
    except Exception as exc:
        _log_failure("/ml/anomaly", "endpoint", exc)
        return JSONResponse(content={"error": str(exc)}, status_code=500)

@app.get("/ml/failure-risk")
def get_failure_risk(limit: int = 1000) -> JSONResponse:
    t0 = time.time()
    try:
        rows = _exec_query("""
            SELECT system_id, failure_probability, predicted_fault, prediction_time
            FROM ml_predictions
            ORDER BY prediction_time DESC
            LIMIT %s
        """, (limit,), endpoint="/ml/failure-risk")
        
        result = []
        for r in rows:
            r['prediction_time'] = r['prediction_time'].isoformat() if r.get('prediction_time') else None
            r['failure_probability'] = float(r['failure_probability']) if r.get('failure_probability') is not None else 0.0
            result.append(r)
            
        _log_req("/ml/failure-risk", (time.time() - t0) * 1000, "ok")
        return JSONResponse(content=result)
    except Exception as exc:
        _log_failure("/ml/failure-risk", "endpoint", exc)
        return JSONResponse(content={"error": str(exc)}, status_code=500)


# ── /ml/anomalies ────────────────────────────────────────────────────────────
# Returns the latest anomaly prediction per system (from IsolationForest v2).
# Query params:
#   limit          — max rows returned (default 50)
#   only_anomalies — if true, filter to rows where is_anomaly = TRUE

@app.get("/ml/anomalies")
def get_ml_anomalies(limit: int = 50, only_anomalies: bool = False) -> JSONResponse:
    """
    Latest ML anomaly predictions per system.

    Returns anomaly_score (0-1 float), is_anomaly (bool), model_version,
    and prediction_time for each system.  When ``only_anomalies=true`` only
    rows flagged as anomalous by the Isolation Forest are returned.
    """
    t0 = time.time()
    try:
        if only_anomalies:
            sql = """
                SELECT DISTINCT ON (system_id)
                    system_id,
                    prediction_time,
                    anomaly_score,
                    is_anomaly,
                    failure_probability,
                    predicted_fault,
                    model_version,
                    cluster_id
                FROM ml_predictions
                WHERE is_anomaly = TRUE
                ORDER BY system_id, prediction_time DESC
                LIMIT %s
            """
        else:
            sql = """
                SELECT DISTINCT ON (system_id)
                    system_id,
                    prediction_time,
                    anomaly_score,
                    is_anomaly,
                    failure_probability,
                    predicted_fault,
                    model_version,
                    cluster_id
                FROM ml_predictions
                ORDER BY system_id, prediction_time DESC
                LIMIT %s
            """

        rows = _exec_query(sql, (limit,), endpoint="/ml/anomalies")

        result = []
        for r in rows:
            result.append({
                "system_id":           r.get("system_id", "unknown"),
                "prediction_time":     _iso(r.get("prediction_time")),
                "anomaly_score":       float(r["anomaly_score"]) if r.get("anomaly_score") is not None else 0.0,
                "is_anomaly":          bool(r.get("is_anomaly")) if r.get("is_anomaly") is not None else None,
                "failure_probability": float(r["failure_probability"]) if r.get("failure_probability") is not None else 0.0,
                "predicted_fault":     r.get("predicted_fault") or "NONE",
                "model_version":       r.get("model_version") or "unknown",
                "cluster_id":          r.get("cluster_id"),
            })

        _log_req("/ml/anomalies", (time.time() - t0) * 1000, "ok", len(result))
        return JSONResponse(content=result)
    except Exception as exc:
        _log_failure("/ml/anomalies", "endpoint", exc)
        return JSONResponse(content={"error": str(exc)}, status_code=500)


# ── /ml/clusters ─────────────────────────────────────────────────────────────
# Returns the latest KMeans cluster assignment per system.

@app.get("/ml/clusters")
def get_ml_clusters(limit: int = 50) -> JSONResponse:
    """
    Latest KMeans cluster assignments per system.

    Returns cluster_id (integer 0-2 by default), anomaly_score,
    and prediction_time.  Rows where cluster_id IS NULL were scored by
    the heuristic fallback (sklearn not available or insufficient data).
    """
    t0 = time.time()
    try:
        rows = _exec_query(
            """
            SELECT DISTINCT ON (system_id)
                system_id,
                prediction_time,
                cluster_id,
                anomaly_score,
                is_anomaly,
                model_version
            FROM ml_predictions
            WHERE cluster_id IS NOT NULL
            ORDER BY system_id, prediction_time DESC
            LIMIT %s
            """,
            (limit,),
            endpoint="/ml/clusters",
        )

        result = []
        for r in rows:
            result.append({
                "system_id":       r.get("system_id", "unknown"),
                "prediction_time": _iso(r.get("prediction_time")),
                "cluster_id":      r.get("cluster_id"),
                "anomaly_score":   float(r["anomaly_score"]) if r.get("anomaly_score") is not None else 0.0,
                "is_anomaly":      bool(r.get("is_anomaly")) if r.get("is_anomaly") is not None else None,
                "model_version":   r.get("model_version") or "unknown",
            })

        _log_req("/ml/clusters", (time.time() - t0) * 1000, "ok", len(result))
        return JSONResponse(content=result)
    except Exception as exc:
        _log_failure("/ml/clusters", "endpoint", exc)
        return JSONResponse(content={"error": str(exc)}, status_code=500)
