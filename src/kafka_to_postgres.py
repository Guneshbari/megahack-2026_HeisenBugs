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

import json
import logging
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

from shared_constants import (
    CIRCUIT_BREAKER_RESET_SECS,
    CIRCUIT_BREAKER_THRESHOLD,
    CONSUMER_DB_BACKOFF_SECS,
    CONSUMER_DB_SLOW_MS,
    DATA_RETENTION_DAYS,
    DB_INSERT_BATCH_SIZE,
    EVENT_PARTITIONING_ENABLED,
    EVENT_PARTITION_MONTHS_AHEAD,
    EVENT_PARTITION_MONTHS_BEHIND,
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
    RAW_XML_MAX_BYTES,
    RETENTION_CLEANUP_ENABLED,
    RETENTION_CLEANUP_INTERVAL_SECS,
    RETENTION_DELETE_BATCH_SIZE,
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


def insert_event_batch(cur: Any, rows: Sequence[Tuple[Any, ...]]) -> int:
    """Insert prepared event rows in idempotent batches."""
    inserted_batches = 0
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
            """,
            batch_rows,
            page_size=DB_INSERT_BATCH_SIZE,
        )
        inserted_batches += 1
    return inserted_batches


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
            if rows:
                insert_event_batch(cur, rows)
        conn.commit()
        _db_cb.record_success()
        latency_ms = round((time.time() - write_started_at) * 1000, 2)
        structured_log(
            "kafka_to_postgres",
            {
                "operation": "db_write",
                "system_id": system_id,
                "event_count": len(rows),
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
                        partition_had_failure = True

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
