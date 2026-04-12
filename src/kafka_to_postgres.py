"""
SentinelCore - Kafka to PostgreSQL consumer.

This consumer preserves the existing pipeline contract while adding:
  - environment-driven Kafka and batch tuning
  - modular message processing helpers
  - Kafka lag and throughput observability
  - DB slowdown backoff instead of crash loops
  - retention cleanup hooks
  - optional partitioned-table preparation for future rollout
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from kafka import KafkaConsumer  # type: ignore
from kafka.errors import KafkaError  # type: ignore
try:
    from kafka.structs import OffsetAndMetadata  # type: ignore
except Exception:  # pragma: no cover - test stubs may not expose kafka.structs
    class OffsetAndMetadata(tuple):  # type: ignore
        """Fallback tuple-compatible OffsetAndMetadata for test environments."""

        def __new__(cls, offset: int, metadata: Any) -> "OffsetAndMetadata":
            return tuple.__new__(cls, (offset, metadata))
from psycopg2.extras import Json, execute_values
import psycopg2.sql as pgsql

from shared.resilience_constants import (
    CIRCUIT_BREAKER_RESET_SECS,
    CIRCUIT_BREAKER_THRESHOLD,
)
from shared.kafka_constants import (
    CONSUMER_DB_BACKOFF_SECS,
    CONSUMER_DB_SLOW_MS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_CLIENT_ID,
    KAFKA_GROUP_ID,
    KAFKA_LAG_LOG_INTERVAL_SECS,
    KAFKA_LAG_WARNING_THRESHOLD,
    KAFKA_MAX_POLL_RECORDS,
    KAFKA_MIN_TOPIC_PARTITIONS,
    KAFKA_POLL_TIMEOUT_MS,
    KAFKA_TOPIC,
    KAFKA_TOPIC_REPLICATION_FACTOR,
)
from shared.api_constants import (
    ALERT_ACK_COOLDOWN_MINUTES,
    ALERT_RULE_LOOKBACK_MINUTES,
)
from shared.db_constants import (
    DATA_RETENTION_DAYS,
    DB_INSERT_BATCH_SIZE,
    EVENT_PARTITIONING_ENABLED,
    EVENT_PARTITION_MONTHS_AHEAD,
    EVENT_PARTITION_MONTHS_BEHIND,
    RAW_XML_MAX_BYTES,
    RETENTION_CLEANUP_ENABLED,
    RETENTION_CLEANUP_INTERVAL_SECS,
    RETENTION_DELETE_BATCH_SIZE,
)
from shared.collector_constants import (
    COLLECTOR_SECRET,
)
from shared.ml_constants import (
    ML_PIPELINE_INTERVAL_SECS,
)
from sentinel_utils import (
    CircuitBreaker,
    clean_message,
    make_db_connection,
    retry_with_backoff,
    structured_log,
    timeout_wrapper,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka_to_postgres")

_db_cb = CircuitBreaker(
    threshold=CIRCUIT_BREAKER_THRESHOLD,
    reset_secs=CIRCUIT_BREAKER_RESET_SECS,
    label="PostgreSQL",
)
_kafka_cb = CircuitBreaker(
    threshold=CIRCUIT_BREAKER_THRESHOLD,
    reset_secs=CIRCUIT_BREAKER_RESET_SECS,
    label="Kafka",
)


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    """Convert values to float with a deterministic fallback."""
    try:
        return float(value) if value is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _truncate_text_bytes(raw_text: str, max_bytes: int) -> str:
    """Truncate text by encoded byte size while keeping UTF-8 valid."""
    encoded = (raw_text or "").encode("utf-8")
    if len(encoded) <= max_bytes:
        return raw_text or ""
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _shift_month(dt: datetime, month_delta: int) -> datetime:
    """Shift a timezone-aware datetime to another month boundary."""
    month_index = dt.month - 1 + month_delta
    year = dt.year + month_index // 12
    month = month_index % 12 + 1
    return dt.replace(year=year, month=month)


def _safe_text(value: Any, fallback: str = "") -> str:
    """Normalize arbitrary values into trimmed strings."""
    if value is None:
        return fallback
    return str(value).strip()


def _safe_int(value: Any, fallback: int = 0) -> int:
    """Convert values to int with a deterministic fallback."""
    try:
        return int(value) if value is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _parse_json_object(raw_value: Any) -> Dict[str, Any]:
    """Return a dict for JSON-like payloads."""
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = json.loads(raw_value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _build_native_alert_key(event_row: Dict[str, Any]) -> str:
    """Create a stable dedupe key for built-in severity-backed alerts."""
    key_parts = [
        "native",
        _safe_text(event_row.get("system_id"), "unknown"),
        _safe_text(event_row.get("severity"), "INFO"),
        _safe_text(event_row.get("fault_type"), "UNKNOWN"),
        _safe_text(event_row.get("fault_subtype"), "UNKNOWN"),
        _safe_text(event_row.get("provider_name"), "unknown"),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()


def _event_context_value(event_row: Dict[str, Any], field_name: str) -> Any:
    """Resolve rule field aliases against an inserted event row."""
    diagnostic_context = _parse_json_object(event_row.get("diagnostic_context"))
    field = field_name.strip().lower()
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
    field = aliases.get(field, field)
    if field in event_row:
        return event_row.get(field)
    return diagnostic_context.get(field)


def _parse_rule_condition(condition: str) -> Optional[Tuple[str, str, str]]:
    """Parse simple expressions such as `cpu > 90` or `fault_type = SECURITY_EVENT`."""
    match = re.match(r"^\s*([a-zA-Z0-9_\.]+)\s*(>=|<=|!=|=|==|>|<|contains)\s*(.+?)\s*$", condition or "")
    if not match:
        return None
    field_name, operator, raw_value = match.groups()
    return field_name.strip(), operator.strip().lower(), raw_value.strip().strip("'\"")


def _rule_matches_event(rule_row: Dict[str, Any], event_row: Dict[str, Any]) -> bool:
    """Evaluate a persisted alert rule against one event row."""
    parsed = _parse_rule_condition(_safe_text(rule_row.get("condition")))
    if not parsed:
        return False

    field_name, operator, expected_raw = parsed
    actual_value = _event_context_value(event_row, field_name)
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


def _build_rule_alert_key(rule_row: Dict[str, Any], event_row: Dict[str, Any]) -> str:
    """Create a stable dedupe key for rule-driven alerts."""
    key_parts = [
        "rule",
        str(rule_row.get("id", "0")),
        _safe_text(event_row.get("system_id"), "unknown"),
        _safe_text(event_row.get("severity"), "INFO"),
        _safe_text(event_row.get("fault_type"), "UNKNOWN"),
    ]
    return hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()


def _count_recent_rule_matches(cur: Any, rule_row: Dict[str, Any], event_row: Dict[str, Any]) -> int:
    """Count recent matching events for one rule and system."""
    event_time = event_row.get("ingested_at") or datetime.now(timezone.utc)
    cur.execute(
        """
        SELECT
            system_id,
            severity,
            fault_type,
            fault_subtype,
            provider_name,
            diagnostic_context,
            cpu_usage_percent,
            memory_usage_percent,
            disk_free_percent,
            normalized_message
        FROM events
        WHERE system_id = %s
          AND ingested_at >= %s - (%s || ' minutes')::interval
        """,
        (
            _safe_text(event_row.get("system_id"), "unknown"),
            event_time,
            str(ALERT_RULE_LOOKBACK_MINUTES),
        ),
    )
    match_count = 0
    for row in cur.fetchall():
        candidate = {
            "system_id": row[0],
            "severity": row[1],
            "fault_type": row[2],
            "fault_subtype": row[3],
            "provider_name": row[4],
            "diagnostic_context": row[5],
            "cpu_usage_percent": _safe_float(row[6]),
            "memory_usage_percent": _safe_float(row[7]),
            "disk_free_percent": _safe_float(row[8]),
            "normalized_message": _safe_text(row[9]),
        }
        if _rule_matches_event(rule_row, candidate):
            match_count += 1
    return match_count


def _upsert_alert(
    cur: Any,
    *,
    alert_key: str,
    source_event_id: int,
    system_id: str,
    hostname: str,
    severity: str,
    rule_name: str,
    title: str,
    description: str,
    source_type: str,
    rule_id: Optional[int],
    suppression_minutes: int,
    escalation_target: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Insert a new alert or roll a matching active alert forward."""
    cur.execute(
        """
        SELECT id, acknowledged, suppressed_until, occurrence_count
        FROM alerts
        WHERE alert_key = %s
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        (alert_key,),
    )
    existing = cur.fetchone()

    if existing:
        suppressed_until = existing[2]
        if suppressed_until is not None:
            # Guard: suppressed_until may be naive (no tzinfo) if stored without tz
            aware_suppressed = (
                suppressed_until if suppressed_until.tzinfo is not None
                else suppressed_until.replace(tzinfo=timezone.utc)
            )
            if aware_suppressed > datetime.now(timezone.utc):
            cur.execute(
                """
                UPDATE alerts
                SET
                    last_seen_at = NOW(),
                    source_event_id = %s,
                    occurrence_count = occurrence_count + 1
                WHERE id = %s
                """,
                (source_event_id, existing[0]),
            )
            return

        if not existing[1]:
            cur.execute(
                """
                UPDATE alerts
                SET
                    last_seen_at = NOW(),
                    source_event_id = %s,
                    occurrence_count = occurrence_count + 1,
                    title = %s,
                    description = %s,
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    escalation_target = COALESCE(NULLIF(%s, ''), escalation_target)
                WHERE id = %s
                """,
                (
                    source_event_id,
                    title,
                    description,
                    json.dumps(metadata or {}),
                    escalation_target or "",
                    existing[0],
                ),
            )
            return

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
            acknowledged,
            escalated,
            suppressed_until,
            escalation_target,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, FALSE, FALSE, NULL, %s, %s::jsonb)
        """,
        (
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
            escalation_target,
            json.dumps(metadata or {}),
        ),
    )


def generate_alerts_for_events(cur: Any, inserted_events: Sequence[Dict[str, Any]]) -> int:
    """Generate built-in and rule-driven alerts for newly inserted events."""
    if not inserted_events:
        return 0

    cur.execute(
        """
        SELECT
            id,
            rule_name,
            condition,
            severity,
            threshold,
            cooldown_minutes,
            escalation_target,
            enabled
        FROM alert_rules
        WHERE enabled = TRUE
        ORDER BY id ASC
        """
    )
    rules = [
        {
            "id": row[0],
            "rule_name": row[1],
            "condition": row[2],
            "severity": row[3],
            "threshold": row[4],
            "cooldown_minutes": row[5],
            "escalation_target": row[6],
            "enabled": row[7],
        }
        for row in cur.fetchall()
    ]

    alerts_created = 0
    for event_row in inserted_events:
        severity = _safe_text(event_row.get("severity"), "INFO").upper()
        system_id = _safe_text(event_row.get("system_id"), "unknown")
        hostname = _safe_text(event_row.get("hostname"), system_id)
        fault_type = _safe_text(event_row.get("fault_type"), "Unknown")
        description = _safe_text(event_row.get("parsed_message")) or _safe_text(event_row.get("event_message")) or fault_type
        source_event_id = _safe_int(event_row.get("id"))

        if severity in {"CRITICAL", "ERROR", "WARNING"}:
            _upsert_alert(
                cur,
                alert_key=_build_native_alert_key(event_row),
                source_event_id=source_event_id,
                system_id=system_id,
                hostname=hostname,
                severity=severity,
                rule_name=f"{fault_type} Detection",
                title=f"{severity}: {fault_type} on {hostname}",
                description=description,
                source_type="native",
                rule_id=None,
                suppression_minutes=ALERT_ACK_COOLDOWN_MINUTES,
                escalation_target=None,
                metadata={"provider_name": event_row.get("provider_name")},
            )
            alerts_created += 1

        for rule_row in rules:
            if not _rule_matches_event(rule_row, event_row):
                continue

            recent_count = _count_recent_rule_matches(cur, rule_row, event_row)
            threshold = max(1, _safe_int(rule_row.get("threshold"), 1))
            if recent_count < threshold:
                continue

            _upsert_alert(
                cur,
                alert_key=_build_rule_alert_key(rule_row, event_row),
                source_event_id=source_event_id,
                system_id=system_id,
                hostname=hostname,
                severity=_safe_text(rule_row.get("severity"), severity).upper(),
                rule_name=_safe_text(rule_row.get("rule_name"), "Custom Rule"),
                title=f"{_safe_text(rule_row.get('severity'), severity).upper()}: {_safe_text(rule_row.get('rule_name'), 'Custom Rule')} on {hostname}",
                description=f"Rule matched: {_safe_text(rule_row.get('condition'))}",
                source_type="rule",
                rule_id=_safe_int(rule_row.get("id")),
                suppression_minutes=max(1, _safe_int(rule_row.get("cooldown_minutes"), ALERT_ACK_COOLDOWN_MINUTES)),
                escalation_target=_safe_text(rule_row.get("escalation_target")) or None,
                metadata={"escalation_target": rule_row.get("escalation_target")},
            )
            alerts_created += 1

    return alerts_created


def _extract_events_from_payload(message: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return a normalized event list.

    Supports both the existing collector payload format:
      {"event": {...}, ...}
    and batched payloads:
      {"events": [{...}, ...], ...}
    """
    batched_events = message.get("events")
    if isinstance(batched_events, list):
        return [event for event in batched_events if isinstance(event, dict)]

    single_event = message.get("event")
    if isinstance(single_event, dict):
        return [single_event]

    return []


def _extract_resource_metric(
    system_info: Dict[str, Any],
    events: Sequence[Dict[str, Any]],
    key: str,
    fallback: float,
) -> float:
    """Prefer heartbeat metrics and fall back to the first event metric."""
    if key in system_info:
        return _safe_float(system_info[key], fallback)
    if events and key in events[0]:
        return _safe_float(events[0][key], fallback)
    return fallback


def _get_healthy_conn(existing: Optional[Any] = None) -> Any:
    """Return a live psycopg2 connection, reconnecting if needed."""
    if not _db_cb.allow():
        raise RuntimeError("[CB:PostgreSQL] Circuit OPEN - skipping DB operation")

    if existing is not None:
        try:
            with existing.cursor() as cur:
                cur.execute("SELECT 1")
            return existing
        except Exception as exc:
            logger.warning("DB connection unhealthy - reconnecting: %s", exc)
            structured_log(
                "kafka_to_postgres",
                {
                    "operation": "db_healthcheck",
                    "status": "failed",
                    "error": str(exc),
                },
                log=logger,
            )
            try:
                existing.close()
            except Exception:
                pass

    conn, ok = retry_with_backoff(make_db_connection, label="consumer_db_reconnect")
    if not ok or conn is None:
        _db_cb.record_failure()
        raise RuntimeError("Failed to connect to DB after retries")

    _db_cb.record_success()
    structured_log(
        "kafka_to_postgres",
        {
            "operation": "db_connect",
            "status": "ok",
            "connection_reused": existing is not None,
        },
        log=logger,
    )
    return conn


def _ensure_partition_shadow_tables(cur: Any) -> None:
    """
    Prepare a partitioned shadow table for future rollout.

    This is intentionally separate from the live `events` table so the
    existing schema and write path remain backward compatible.
    """
    if not EVENT_PARTITIONING_ENABLED:
        return

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events_partitioned (
            id                   BIGINT GENERATED ALWAYS AS IDENTITY,
            system_id            VARCHAR(100),
            hostname             VARCHAR(255),
            log_channel          VARCHAR(100),
            event_record_id      BIGINT,
            provider_name        VARCHAR(255),
            event_id             INTEGER,
            level                INTEGER,
            task                 INTEGER,
            opcode               INTEGER,
            keywords             VARCHAR(50),
            process_id           INTEGER,
            thread_id            INTEGER,
            severity             VARCHAR(20),
            fault_type           VARCHAR(50),
            diagnostic_context   JSONB,
            event_hash           VARCHAR(64),
            raw_xml              TEXT,
            cpu_usage_percent    NUMERIC(5,2),
            memory_usage_percent NUMERIC(5,2),
            disk_free_percent    NUMERIC(5,2),
            event_message        TEXT DEFAULT '',
            parsed_message       TEXT DEFAULT '',
            normalized_message   TEXT DEFAULT '',
            fault_subtype        VARCHAR(80) DEFAULT '',
            confidence_score     NUMERIC(3,2) DEFAULT 0.20,
            ingested_at          TIMESTAMP WITH TIME ZONE NOT NULL
        ) PARTITION BY RANGE (ingested_at);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_events_partitioned_system_ingested
        ON events_partitioned(system_id, ingested_at DESC);
        """
    )

    start_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_offsets = range(-EVENT_PARTITION_MONTHS_BEHIND, EVENT_PARTITION_MONTHS_AHEAD + 1)
    for month_offset in month_offsets:
        partition_start = _shift_month(start_month, month_offset)
        partition_end = _shift_month(partition_start, 1)
        partition_name = f"events_partitioned_{partition_start.strftime('%Y%m')}"
        query = pgsql.SQL("""
            CREATE TABLE IF NOT EXISTS {partition_table}
            PARTITION OF events_partitioned
            FOR VALUES FROM (%s) TO (%s);
        """).format(partition_table=pgsql.Identifier(partition_name))
        cur.execute(query, (partition_start, partition_end))


def setup_database(conn: Any) -> None:
    """Create or extend all required tables and indexes safely."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id                   SERIAL PRIMARY KEY,
                system_id            VARCHAR(100),
                hostname             VARCHAR(255),
                log_channel          VARCHAR(100),
                event_record_id      BIGINT,
                provider_name        VARCHAR(255),
                event_id             INTEGER,
                level                INTEGER,
                task                 INTEGER,
                opcode               INTEGER,
                keywords             VARCHAR(50),
                process_id           INTEGER,
                thread_id            INTEGER,
                severity             VARCHAR(20),
                fault_type           VARCHAR(50),
                diagnostic_context   JSONB,
                event_hash           VARCHAR(64) UNIQUE,
                raw_xml              TEXT,
                cpu_usage_percent    NUMERIC(5,2),
                memory_usage_percent NUMERIC(5,2),
                disk_free_percent    NUMERIC(5,2),
                ingested_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_events_system_id
                ON events(system_id);
            CREATE INDEX IF NOT EXISTS idx_events_ingested_at
                ON events(ingested_at DESC);
            CREATE INDEX IF NOT EXISTS idx_events_severity
                ON events(severity);
            CREATE INDEX IF NOT EXISTS idx_events_system_ingested
                ON events(system_id, ingested_at DESC);
            """
        )

        for column_name, column_type, default_value in [
            ("event_message", "TEXT", "''"),
            ("parsed_message", "TEXT", "''"),
            ("normalized_message", "TEXT", "''"),
            ("fault_subtype", "VARCHAR(80)", "''"),
            ("confidence_score", "NUMERIC(3,2)", "0.20"),
            ("acknowledged", "BOOLEAN", "FALSE"),
            ("acknowledged_at", "TIMESTAMP WITH TIME ZONE", "NULL"),
            ("escalated", "BOOLEAN", "FALSE"),
            ("escalated_at", "TIMESTAMP WITH TIME ZONE", "NULL"),
            ("assigned_to", "VARCHAR(100)", "NULL"),
        ]:
            query = pgsql.SQL("""
                ALTER TABLE events
                ADD COLUMN IF NOT EXISTS {col} {type} DEFAULT {default};
            """).format(
                col=pgsql.Identifier(column_name),
                type=pgsql.SQL(column_type),
                default=pgsql.SQL(default_value)
            )
            cur.execute(query)

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS system_heartbeats (
                system_id            VARCHAR(100) PRIMARY KEY,
                hostname             VARCHAR(255),
                cpu_usage_percent    NUMERIC(5,2),
                memory_usage_percent NUMERIC(5,2),
                disk_free_percent    NUMERIC(5,2),
                os_version           VARCHAR(255),
                agent_version        VARCHAR(50),
                ip_address           VARCHAR(50),
                uptime_seconds       BIGINT,
                last_seen            TIMESTAMP WITH TIME ZONE
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                id                   SERIAL PRIMARY KEY,
                system_id            VARCHAR(100) NOT NULL,
                snapshot_time        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                cpu_usage_percent    NUMERIC(5,2) NOT NULL DEFAULT 0,
                memory_usage_percent NUMERIC(5,2) NOT NULL DEFAULT 0,
                disk_free_percent    NUMERIC(5,2) NOT NULL DEFAULT 100,
                total_events         INTEGER NOT NULL DEFAULT 0,
                critical_count       INTEGER NOT NULL DEFAULT 0,
                error_count          INTEGER NOT NULL DEFAULT 0,
                warning_count        INTEGER NOT NULL DEFAULT 0,
                info_count           INTEGER NOT NULL DEFAULT 0,
                dominant_fault_type  VARCHAR(50) NOT NULL DEFAULT 'NONE',
                avg_confidence       NUMERIC(3,2) NOT NULL DEFAULT 0.20
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_system_time
                ON feature_snapshots(system_id, snapshot_time DESC);
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_predictions (
                id SERIAL PRIMARY KEY,
                system_id VARCHAR(100) NOT NULL,
                prediction_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                anomaly_score NUMERIC(4,3) DEFAULT 0.0,
                is_anomaly BOOLEAN DEFAULT NULL,
                failure_probability NUMERIC(4,3) DEFAULT 0.0,
                predicted_fault VARCHAR(100) DEFAULT 'NONE',
                cluster_id INTEGER DEFAULT NULL,
                model_version VARCHAR(50) DEFAULT 'v1'
            );
            CREATE INDEX IF NOT EXISTS idx_ml_predictions_system_time
                ON ml_predictions(system_id, prediction_time DESC);
            """
        )

        # Idempotent migration — add new columns if running against an existing DB
        for ml_col, ml_typedef in [
            ("is_anomaly", "BOOLEAN DEFAULT NULL"),
            ("cluster_id", "INTEGER DEFAULT NULL"),
        ]:
            cur.execute(
                f"ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS {ml_col} {ml_typedef};"
            )

        cur.execute(
            """
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
            """
        )

        for column_name, column_type, default_value in [
            ("cooldown_minutes", "INT", "30"),
            ("escalation_target", "TEXT", "NULL"),
            ("enabled", "BOOLEAN", "TRUE"),
            ("created_at", "TIMESTAMP WITH TIME ZONE", "CURRENT_TIMESTAMP"),
        ]:
            query = pgsql.SQL("""
                ALTER TABLE alert_rules
                ADD COLUMN IF NOT EXISTS {col} {type} DEFAULT {default};
            """).format(
                col=pgsql.Identifier(column_name),
                type=pgsql.SQL(column_type),
                default=pgsql.SQL(default_value),
            )
            cur.execute(query)

        cur.execute(
            """
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
            CREATE INDEX IF NOT EXISTS idx_alerts_last_seen
                ON alerts(last_seen_at DESC);
            CREATE INDEX IF NOT EXISTS idx_alerts_system_seen
                ON alerts(system_id, last_seen_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_active_key
                ON alerts(alert_key)
                WHERE acknowledged = FALSE;
            """
        )

        _ensure_partition_shadow_tables(cur)

    conn.commit()
    logger.info("Database schema verified and migrated.")
    structured_log(
        "kafka_to_postgres",
        {
            "operation": "setup_database",
            "status": "ok",
            "partition_shadow_enabled": EVENT_PARTITIONING_ENABLED,
        },
        log=logger,
    )


def update_heartbeat(cur: Any, msg: Dict[str, Any], heartbeat_time: Optional[datetime] = None) -> None:
    """Upsert the latest heartbeat row for a system."""
    events = _extract_events_from_payload(msg)
    system_info = msg.get("system_info") or {}
    heartbeat_time = heartbeat_time or datetime.now(timezone.utc)
    system_id = msg.get("system_id") or system_info.get("system_id") or "unknown"
    hostname = msg.get("hostname") or system_info.get("hostname") or "unknown"
    cpu = _extract_resource_metric(system_info, events, "cpu_usage_percent", 0.0)
    memory = _extract_resource_metric(system_info, events, "memory_usage_percent", 0.0)
    disk = _extract_resource_metric(system_info, events, "disk_free_percent", 100.0)

    cur.execute(
        """
        INSERT INTO system_heartbeats (
            system_id, hostname,
            cpu_usage_percent, memory_usage_percent, disk_free_percent,
            os_version, agent_version, ip_address,
            uptime_seconds, last_seen
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (system_id) DO UPDATE SET
            hostname             = EXCLUDED.hostname,
            cpu_usage_percent    = EXCLUDED.cpu_usage_percent,
            memory_usage_percent = EXCLUDED.memory_usage_percent,
            disk_free_percent    = EXCLUDED.disk_free_percent,
            os_version           = EXCLUDED.os_version,
            agent_version        = EXCLUDED.agent_version,
            ip_address           = EXCLUDED.ip_address,
            uptime_seconds       = EXCLUDED.uptime_seconds,
            last_seen            = EXCLUDED.last_seen;
        """,
        (
            system_id,
            hostname,
            cpu,
            memory,
            disk,
            system_info.get("os_version", msg.get("os_version", "Unknown")),
            system_info.get("agent_version", msg.get("collector_version", "Unknown")),
            system_info.get("ip_address", "Unknown"),
            system_info.get("uptime_seconds", msg.get("uptime_seconds", 0)),
            heartbeat_time,
        ),
    )


def extract_and_prepare_events(msg: Dict[str, Any]) -> List[Tuple[Any, ...]]:
    """Normalize the incoming payload and prepare rows for batch insert."""
    system_info = msg.get("system_info") or {}
    system_id = msg.get("system_id") or system_info.get("system_id") or "unknown"
    hostname = msg.get("hostname") or system_info.get("hostname") or "unknown"
    prepared_rows: List[Tuple[Any, ...]] = []

    for event in _extract_events_from_payload(msg):
        raw_message = event.get("event_message") or event.get("message") or ""
        raw_xml = _truncate_text_bytes(event.get("raw_xml") or "", RAW_XML_MAX_BYTES)
        parsed_message = clean_message(raw_message)
        normalized_message = parsed_message.lower()
        fault_subtype = event.get("fault_subtype") or event.get("fault_type") or "UNKNOWN"
        confidence_score = max(0.0, min(1.0, _safe_float(event.get("confidence_score"), 0.20)))

        prepared_rows.append(
            (
                system_id,
                hostname,
                event.get("log_channel"),
                event.get("event_record_id"),
                event.get("provider_name"),
                event.get("event_id"),
                event.get("level"),
                event.get("task"),
                event.get("opcode"),
                event.get("keywords"),
                event.get("process_id"),
                event.get("thread_id"),
                event.get("severity"),
                event.get("fault_type"),
                Json(event.get("diagnostic_context") or {}),
                event.get("event_hash"),
                raw_xml,
                _safe_float(
                    event.get("cpu_usage_percent"),
                    _extract_resource_metric(system_info, [event], "cpu_usage_percent", 0.0),
                ),
                _safe_float(
                    event.get("memory_usage_percent"),
                    _extract_resource_metric(system_info, [event], "memory_usage_percent", 0.0),
                ),
                _safe_float(
                    event.get("disk_free_percent"),
                    _extract_resource_metric(system_info, [event], "disk_free_percent", 100.0),
                ),
                raw_message,
                parsed_message,
                normalized_message,
                fault_subtype,
                confidence_score,
            )
        )

    return prepared_rows


def insert_event_batch(cur: Any, rows: Sequence[Tuple[Any, ...]]) -> List[Dict[str, Any]]:
    """Insert prepared event rows in idempotent batches and return inserted rows."""
    inserted_rows: List[Dict[str, Any]] = []
    for batch_start in range(0, len(rows), DB_INSERT_BATCH_SIZE):
        batch_rows = rows[batch_start : batch_start + DB_INSERT_BATCH_SIZE]
        execute_values(
            cur,
            """
            INSERT INTO events (
                system_id, hostname, log_channel, event_record_id,
                provider_name, event_id, level, task, opcode, keywords,
                process_id, thread_id, severity, fault_type,
                diagnostic_context, event_hash, raw_xml,
                cpu_usage_percent, memory_usage_percent, disk_free_percent,
                event_message, parsed_message, normalized_message,
                fault_subtype, confidence_score
            ) VALUES %s
            ON CONFLICT (event_hash) DO NOTHING
            RETURNING
                id, system_id, hostname, provider_name, severity, fault_type,
                diagnostic_context, ingested_at, event_record_id, event_hash,
                cpu_usage_percent, memory_usage_percent, disk_free_percent,
                event_message, parsed_message, normalized_message, fault_subtype
            """,
            batch_rows,
            page_size=DB_INSERT_BATCH_SIZE,
        )
        for row in cur.fetchall():
            inserted_rows.append(
                {
                    "id": row[0],
                    "system_id": row[1],
                    "hostname": row[2],
                    "provider_name": row[3],
                    "severity": row[4],
                    "fault_type": row[5],
                    "diagnostic_context": row[6],
                    "ingested_at": row[7],
                    "event_record_id": row[8],
                    "event_hash": row[9],
                    "cpu_usage_percent": _safe_float(row[10]),
                    "memory_usage_percent": _safe_float(row[11]),
                    "disk_free_percent": _safe_float(row[12]),
                    "event_message": _safe_text(row[13]),
                    "parsed_message": _safe_text(row[14]),
                    "normalized_message": _safe_text(row[15]),
                    "fault_subtype": _safe_text(row[16]),
                }
            )
    return inserted_rows


def process_message(conn: Any, msg: Dict[str, Any]) -> bool:
    """
    Process a single Kafka payload transactionally.

    The heartbeat upsert always runs, event inserts remain idempotent via
    `event_hash`, and the function preserves the legacy bool return contract.
    """
    system_id = msg.get("system_id") or (msg.get("system_info") or {}).get("system_id") or "unknown"
    rows = extract_and_prepare_events(msg)
    write_started_at = time.time()

    try:
        with conn.cursor() as cur:
            update_heartbeat(cur, msg)
            inserted_events: List[Dict[str, Any]] = []
            if rows:
                inserted_events = insert_event_batch(cur, rows)
                generate_alerts_for_events(cur, inserted_events)
        conn.commit()
        _db_cb.record_success()
        latency_ms = round((time.time() - write_started_at) * 1000, 2)
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "db_write",
                "system_id": system_id,
                "event_count": len(rows),
                "inserted_event_count": len(inserted_events),
                "db_write_latency_ms": latency_ms,
                "status": "ok",
            },
            log=logger,
        )
        return True

    except Exception as exc:
        logger.error("[process_message] Transaction failed for %s: %s", system_id, exc, exc_info=True)
        try:
            conn.rollback()
        except Exception as rollback_exc:
            logger.error("[process_message] Rollback failed: %s", rollback_exc)
            structured_log(
                "kafka_to_postgres",
                {
                    "operation": "db_rollback",
                    "system_id": system_id,
                    "status": "failed",
                    "error": str(rollback_exc),
                },
                log=logger,
            )
        _db_cb.record_failure()
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "process_message",
                "system_id": system_id,
                "event_count": len(rows),
                "status": "failed",
                "error": str(exc),
            },
            log=logger,
        )
        return False


def cleanup_expired_events(conn: Any) -> int:
    """Delete expired event rows in small batches to avoid large table locks."""
    if not RETENTION_CLEANUP_ENABLED:
        return 0

    deleted_rows = 0
    with conn.cursor() as cur:
        while True:
            cur.execute(
                """
                WITH expired_rows AS (
                    SELECT ctid
                    FROM events
                    WHERE ingested_at < NOW() - (%s || ' days')::interval
                    ORDER BY ingested_at
                    LIMIT %s
                )
                DELETE FROM events events_table
                USING expired_rows
                WHERE events_table.ctid = expired_rows.ctid;
                """,
                (str(DATA_RETENTION_DAYS), RETENTION_DELETE_BATCH_SIZE),
            )
            batch_deleted = cur.rowcount or 0
            deleted_rows += batch_deleted
            if batch_deleted < RETENTION_DELETE_BATCH_SIZE:
                break
    conn.commit()
    if deleted_rows:
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "retention_cleanup",
                "deleted_rows": deleted_rows,
                "retention_days": DATA_RETENTION_DAYS,
                "status": "ok",
            },
            log=logger,
        )
    return deleted_rows


def ensure_kafka_topic_partitioning() -> None:
    """Best-effort partition validation and expansion for the Kafka topic."""
    try:
        from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic  # type: ignore

        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id=f"{KAFKA_CONSUMER_CLIENT_ID}-admin",
        )
        try:
            topics = set(admin.list_topics())
            if KAFKA_TOPIC not in topics:
                admin.create_topics(
                    [
                        NewTopic(
                            name=KAFKA_TOPIC,
                            num_partitions=KAFKA_MIN_TOPIC_PARTITIONS,
                            replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR,
                        )
                    ]
                )
                structured_log(
                    "kafka_to_postgres",
                    {
                        "operation": "ensure_topic_partitioning",
                        "topic": KAFKA_TOPIC,
                        "partitions": KAFKA_MIN_TOPIC_PARTITIONS,
                        "replication_factor": KAFKA_TOPIC_REPLICATION_FACTOR,
                        "status": "created",
                    },
                    log=logger,
                )
                return

            metadata = admin.describe_topics([KAFKA_TOPIC])[0]
            existing_partitions = len(metadata.get("partitions", []))
            if existing_partitions < KAFKA_MIN_TOPIC_PARTITIONS:
                admin.create_partitions(
                    {KAFKA_TOPIC: NewPartitions(total_count=KAFKA_MIN_TOPIC_PARTITIONS)}
                )
                structured_log(
                    "kafka_to_postgres",
                    {
                        "operation": "ensure_topic_partitioning",
                        "topic": KAFKA_TOPIC,
                        "partitions_before": existing_partitions,
                        "partitions_after": KAFKA_MIN_TOPIC_PARTITIONS,
                        "status": "scaled",
                    },
                    log=logger,
                )
            else:
                structured_log(
                    "kafka_to_postgres",
                    {
                        "operation": "ensure_topic_partitioning",
                        "topic": KAFKA_TOPIC,
                        "partitions": existing_partitions,
                        "replication_factor": KAFKA_TOPIC_REPLICATION_FACTOR,
                        "status": "ok",
                    },
                    log=logger,
                )
        finally:
            admin.close()

        if KAFKA_TOPIC_REPLICATION_FACTOR <= 1:
            structured_log(
                "kafka_to_postgres",
                {
                    "operation": "ensure_topic_partitioning",
                    "topic": KAFKA_TOPIC,
                    "status": "warning",
                    "error": "single_replica_topic_configuration",
                    "replication_factor": KAFKA_TOPIC_REPLICATION_FACTOR,
                },
                log=logger,
            )
    except Exception as exc:
        _kafka_cb.record_failure()
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "ensure_topic_partitioning",
                "topic": KAFKA_TOPIC,
                "status": "failed",
                "error": str(exc),
            },
            log=logger,
        )


def commit_partition_offset(consumer: KafkaConsumer, topic_partition: Any, next_offset: int) -> bool:
    """Commit the next safe offset for one partition after DB persistence succeeds."""
    try:
        consumer.commit(offsets={topic_partition: OffsetAndMetadata(next_offset, None)})
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "commit_offset",
                "status": "ok",
                "topic": getattr(topic_partition, "topic", KAFKA_TOPIC),
                "partition": getattr(topic_partition, "partition", "unknown"),
                "next_offset": next_offset,
            },
            log=logger,
        )
        return True
    except Exception as exc:
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "commit_offset",
                "status": "failed",
                "topic": getattr(topic_partition, "topic", KAFKA_TOPIC),
                "partition": getattr(topic_partition, "partition", "unknown"),
                "next_offset": next_offset,
                "error": str(exc),
            },
            log=logger,
        )
        return False


def log_kafka_lag(consumer: KafkaConsumer) -> None:
    """Emit aggregate lag for all assigned partitions."""
    try:
        assigned_partitions = list(consumer.assignment())
        if not assigned_partitions:
            return

        end_offsets = consumer.end_offsets(assigned_partitions)
        total_lag = 0
        max_partition_lag = 0
        for partition in assigned_partitions:
            current_offset = consumer.position(partition)
            partition_lag = max(0, end_offsets.get(partition, 0) - current_offset)
            total_lag += partition_lag
            max_partition_lag = max(max_partition_lag, partition_lag)

        lag_status = "warning" if total_lag >= KAFKA_LAG_WARNING_THRESHOLD else "ok"
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "kafka_lag",
                "status": lag_status,
                "topic": KAFKA_TOPIC,
                "partition_count": len(assigned_partitions),
                "total_lag": total_lag,
                "max_partition_lag": max_partition_lag,
            },
            log=logger,
        )
    except Exception as exc:
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "kafka_lag",
                "status": "failed",
                "topic": KAFKA_TOPIC,
                "error": str(exc),
            },
            log=logger,
        )


# ---------------------------------------------------------------------------
# ML pipeline background thread
# ---------------------------------------------------------------------------

import threading as _threading


def _ml_pipeline_worker(stop_event: "_threading.Event") -> None:
    """
    Background daemon thread: runs feature_builder → ml_engine every
    ML_PIPELINE_INTERVAL_SECS seconds.  Completely non-blocking for the
    Kafka consumer loop — all DB I/O happens on its own connection.

    The thread is launched as a daemon so it exits automatically when the
    main process terminates.
    """
    # Deferred imports keep the module lightweight during testing
    try:
        import feature_builder as _fb
        import ml_engine as _ml
    except ImportError as _ie:
        logger.error("[ml_pipeline] Cannot import pipeline modules: %s — ML disabled.", _ie)
        return

    logger.info(
        "[ml_pipeline] Background worker started (interval=%ds).",
        ML_PIPELINE_INTERVAL_SECS,
    )

    # Stagger the first run by half the interval so the consumer has time to
    # ingest an initial batch before ML tries to score anything.
    stop_event.wait(ML_PIPELINE_INTERVAL_SECS // 2)

    ml_conn: Any = None
    ml_reconnect_delay = 5.0

    while not stop_event.is_set():
        cycle_start = time.time()
        try:
            # Ensure we have a healthy DB connection
            if ml_conn is None:
                ml_conn_result, ok = retry_with_backoff(
                    make_db_connection, label="ml_pipeline_db_connect"
                )
                if ok and ml_conn_result:
                    ml_conn = ml_conn_result
                    ml_reconnect_delay = 5.0
                else:
                    logger.error(
                        "[ml_pipeline] DB connect failed — retrying in %.0fs.",
                        ml_reconnect_delay,
                    )
                    stop_event.wait(ml_reconnect_delay)
                    ml_reconnect_delay = min(ml_reconnect_delay * 2, 60.0)
                    continue

            # ── feature_builder cycle ────────────────────────────────────
            try:
                _, systems_checked, snaps_written = _fb.run_cycle(ml_conn, cycle=0)
                structured_log(
                    "ml_pipeline",
                    {
                        "operation": "feature_builder_cycle",
                        "status": "ok",
                        "systems_checked": systems_checked,
                        "snapshots_written": snaps_written,
                    },
                    log=logger,
                )
            except Exception as fb_exc:
                logger.warning("[ml_pipeline] feature_builder cycle error: %s", fb_exc)
                structured_log(
                    "ml_pipeline",
                    {"operation": "feature_builder_cycle", "status": "failed", "error": str(fb_exc)},
                    log=logger,
                )

            # ── ml_engine cycle ──────────────────────────────────────────
            try:
                written = _ml.run_cycle(ml_conn)
                structured_log(
                    "ml_pipeline",
                    {
                        "operation": "ml_engine_cycle",
                        "status": "ok",
                        "predictions_written": written,
                    },
                    log=logger,
                )
            except Exception as ml_exc:
                logger.warning("[ml_pipeline] ml_engine cycle error: %s", ml_exc)
                structured_log(
                    "ml_pipeline",
                    {"operation": "ml_engine_cycle", "status": "failed", "error": str(ml_exc)},
                    log=logger,
                )

        except Exception as exc:
            logger.error("[ml_pipeline] Unexpected error: %s", exc, exc_info=True)
            try:
                if ml_conn:
                    ml_conn.close()
            except Exception:
                pass
            ml_conn = None

        # Sleep for the remainder of the interval (or immediately if overrun)
        elapsed = time.time() - cycle_start
        sleep_for = max(0.0, ML_PIPELINE_INTERVAL_SECS - elapsed)
        stop_event.wait(sleep_for)

    logger.info("[ml_pipeline] Background worker stopped.")
    try:
        if ml_conn:
            ml_conn.close()
    except Exception:
        pass


def run_consumer() -> None:
    """Run the Kafka consumer loop until interrupted."""
    shutdown_requested = False

    def handle_signal(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        logger.info("[graceful_shutdown] Signal %d received - stopping consumer", signum)
        shutdown_requested = True

    signal.signal(signal.SIGTERM, handle_signal)

    conn, ok = retry_with_backoff(make_db_connection, label="consumer_initial_db_connect")
    if not ok or conn is None:
        logger.critical("Cannot connect to PostgreSQL on startup - aborting")
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "startup_db_connect",
                "status": "failed",
            },
            log=logger,
        )
        return

    setup_database(conn)
    ensure_kafka_topic_partitioning()

    # ── Start the ML pipeline background thread ──────────────────────────────
    _ml_stop_event = _threading.Event()
    _ml_thread = _threading.Thread(
        target=_ml_pipeline_worker,
        args=(_ml_stop_event,),
        name="ml-pipeline",
        daemon=True,          # exits automatically when main process exits
    )
    _ml_thread.start()
    logger.info("[ml_pipeline] Background thread launched (interval=%ds).", ML_PIPELINE_INTERVAL_SECS)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        client_id=KAFKA_CONSUMER_CLIENT_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=KAFKA_POLL_TIMEOUT_MS,
        max_poll_records=KAFKA_MAX_POLL_RECORDS,
        value_deserializer=lambda raw_message: json.loads(raw_message.decode("utf-8")),
    )

    logger.info(
        "Listening on Kafka topic '%s' via %s (group=%s)",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_GROUP_ID,
    )

    processed_messages = 0
    failed_messages = 0
    processed_events = 0
    next_cleanup_at = time.time() + RETENTION_CLEANUP_INTERVAL_SECS
    next_lag_log_at = time.time() + KAFKA_LAG_LOG_INTERVAL_SECS

    try:
        while not shutdown_requested:
            try:
                records = consumer.poll(
                    timeout_ms=KAFKA_POLL_TIMEOUT_MS,
                    max_records=KAFKA_MAX_POLL_RECORDS,
                )
            except KafkaError as exc:
                _kafka_cb.record_failure()
                logger.error("[consumer.poll] Kafka error: %s - sleeping %.1fs", exc, CONSUMER_DB_BACKOFF_SECS)
                structured_log(
                    "kafka_to_postgres",
                    {
                        "operation": "consumer_poll",
                        "status": "failed",
                        "error": str(exc),
                    },
                    log=logger,
                )
                time.sleep(CONSUMER_DB_BACKOFF_SECS)
                continue
            except Exception as exc:
                logger.error("[consumer.poll] Unexpected poll error: %s", exc, exc_info=True)
                structured_log(
                    "kafka_to_postgres",
                    {
                        "operation": "consumer_poll",
                        "status": "failed",
                        "error": str(exc),
                    },
                    log=logger,
                )
                time.sleep(CONSUMER_DB_BACKOFF_SECS)
                continue

            if not records:
                if RETENTION_CLEANUP_ENABLED and time.time() >= next_cleanup_at:
                    try:
                        conn = _get_healthy_conn(conn)
                        def _do_clean():
                            return cleanup_expired_events(conn)
                        def _do_clean_timeout():
                            res, t_ok = timeout_wrapper(_do_clean, timeout_secs=60.0, label="cleanup")
                            if not t_ok: raise TimeoutError("Cleanup timed out")
                            return res
                        retry_with_backoff(_do_clean_timeout, max_attempts=3, label="cleanup_retry")
                    except Exception as exc:
                        structured_log(
                            "kafka_to_postgres",
                            {
                                "operation": "retention_cleanup",
                                "status": "failed",
                                "error": str(exc),
                            },
                            log=logger,
                        )
                    next_cleanup_at = time.time() + RETENTION_CLEANUP_INTERVAL_SECS
                continue

            batch_started_at = time.time()
            batch_messages = 0
            batch_events = 0
            batch_failures = 0

            for topic_partition, messages in records.items():
                last_committable_offset: Optional[int] = None
                partition_had_failure = False
                for message in messages:
                    if shutdown_requested:
                        break

                    payload = message.value
                    # Timing-safe comparison — prevents secret extraction via timing side-channels
                    submitted_key = payload.get("agent_key") or ""
                    if not isinstance(submitted_key, str):
                        submitted_key = ""
                    key_valid = hmac.compare_digest(
                        submitted_key.encode("utf-8"),
                        COLLECTOR_SECRET.encode("utf-8"),
                    )
                    if not key_valid:
                        logger.warning(
                            "Rejected message: Invalid or missing agent_key from %s",
                            (payload.get("system_info") or {}).get("system_id", "unknown"),
                        )
                        failed_messages += 1
                        batch_failures += 1
                        last_committable_offset = message.offset + 1
                        continue

                    system_id = payload.get("system_id") or (payload.get("system_info") or {}).get("system_id") or "unknown"
                    event_count = len(_extract_events_from_payload(payload))
                    message_started_at = time.time()

                    try:
                        conn = _get_healthy_conn(conn)
                    except Exception as exc:
                        failed_messages += 1
                        batch_failures += 1
                        partition_had_failure = True
                        structured_log(
                            "kafka_to_postgres",
                            {
                                "operation": "db_connect",
                                "status": "failed",
                                "system_id": system_id,
                                "event_count": event_count,
                                "error": str(exc),
                            },
                            log=logger,
                        )
                        time.sleep(CONSUMER_DB_BACKOFF_SECS)
                        break

                    def _run_process():
                        return process_message(conn, payload)
                    def _run_timeout():
                        res, t_ok = timeout_wrapper(_run_process, timeout_secs=15.0, label="process_message")
                        if not t_ok: raise TimeoutError("DB transaction timed out")
                        return res

                    res, r_ok = retry_with_backoff(_run_timeout, max_attempts=3, label=f"process_message_retry/{system_id}")
                    ok = bool(r_ok and res)
                    latency_ms = round((time.time() - message_started_at) * 1000, 2)
                    batch_messages += 1
                    batch_events += event_count

                    if ok:
                        processed_messages += 1
                        processed_events += event_count
                        last_committable_offset = message.offset + 1
                    else:
                        failed_messages += 1
                        batch_failures += 1
                        
                        # Fix: Drop-and-Proceed Dead Letter Queue (DLQ) Strategy
                        # If a message fails all retries, do NOT break the partition.
                        # Advance the offset anyway and log heavily so the ingestion pipeline isn't permanently stalled.
                        logger.error(f"[DLQ] Poison pill message dropped after retries exhausted from {system_id} at offset {message.offset}")
                        
                        dlq_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlq")
                        os.makedirs(dlq_dir, exist_ok=True)
                        dlq_path = os.path.join(dlq_dir, f"dlq_{system_id}_{message.offset}.json")
                        try:
                            with open(dlq_path, "w", encoding="utf-8") as f:
                                json.dump(payload, f, ensure_ascii=False, indent=2)
                        except Exception as dlq_exc:
                            logger.error(f"[DLQ] Failed to write DLQ file {dlq_path}: {dlq_exc}")

                        structured_log(
                            "kafka_to_postgres",
                            {
                                "operation": "dlq_message_dropped",
                                "status": "critical_failure",
                                "system_id": system_id,
                                "topic": getattr(topic_partition, "topic", KAFKA_TOPIC),
                                "partition": getattr(topic_partition, "partition", "unknown"),
                                "offset": message.offset,
                                "event_count": event_count,
                                "dlq_file": dlq_path,
                            },
                            log=logger,
                        )
                        # Advance past the bad message to unblock the partition stream
                        last_committable_offset = message.offset + 1

                    if latency_ms >= CONSUMER_DB_SLOW_MS:
                        logger.warning(
                            "DB write for %s was slow (%.2fms) - applying %.1fs backoff",
                            system_id,
                            latency_ms,
                            CONSUMER_DB_BACKOFF_SECS,
                        )
                        structured_log(
                            "kafka_to_postgres",
                            {
                                "operation": "consumer_backpressure",
                                "status": "warning",
                                "system_id": system_id,
                                "event_count": event_count,
                                "db_write_latency_ms": latency_ms,
                                "backoff_secs": CONSUMER_DB_BACKOFF_SECS,
                            },
                            log=logger,
                        )
                        time.sleep(CONSUMER_DB_BACKOFF_SECS)

                    if partition_had_failure:
                        break

                if last_committable_offset is not None:
                    commit_partition_offset(consumer, topic_partition, last_committable_offset)

                if partition_had_failure:
                    structured_log(
                        "kafka_to_postgres",
                        {
                            "operation": "partition_backoff",
                            "status": "warning",
                            "topic": getattr(topic_partition, "topic", KAFKA_TOPIC),
                            "partition": getattr(topic_partition, "partition", "unknown"),
                            "backoff_secs": CONSUMER_DB_BACKOFF_SECS,
                        },
                        log=logger,
                    )

            batch_duration = max(time.time() - batch_started_at, 0.001)
            batch_events_per_sec = round(batch_events / batch_duration, 2)
            structured_log(
                "kafka_to_postgres",
                {
                    "operation": "consume_batch",
                    "status": "ok" if batch_failures == 0 else "partial",
                    "messages": batch_messages,
                    "events": batch_events,
                    "failed_messages": batch_failures,
                    "batch_latency_ms": round(batch_duration * 1000, 2),
                    "events_per_sec": batch_events_per_sec,
                    "kafka_processing_latency_ms": round(batch_duration * 1000, 2),
                },
                log=logger,
            )

            now = time.time()
            if now >= next_lag_log_at:
                log_kafka_lag(consumer)
                next_lag_log_at = now + KAFKA_LAG_LOG_INTERVAL_SECS

            if RETENTION_CLEANUP_ENABLED and now >= next_cleanup_at:
                try:
                    conn = _get_healthy_conn(conn)
                    def _do_clean():
                        return cleanup_expired_events(conn)
                    def _do_clean_timeout():
                        res, t_ok = timeout_wrapper(_do_clean, timeout_secs=60.0, label="cleanup")
                        if not t_ok: raise TimeoutError("Cleanup timed out")
                        return res
                    retry_with_backoff(_do_clean_timeout, max_attempts=3, label="cleanup_retry")
                except Exception as exc:
                    structured_log(
                        "kafka_to_postgres",
                        {
                            "operation": "retention_cleanup",
                            "status": "failed",
                            "error": str(exc),
                        },
                        log=logger,
                    )
                next_cleanup_at = now + RETENTION_CLEANUP_INTERVAL_SECS

    except KeyboardInterrupt:
        logger.info("[graceful_shutdown] KeyboardInterrupt received")

    finally:
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "shutdown",
                "status": "ok",
                "processed_messages": processed_messages,
                "processed_events": processed_events,
                "failed_messages": failed_messages,
            },
            log=logger,
        )
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_consumer()
