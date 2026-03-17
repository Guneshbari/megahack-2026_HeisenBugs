"""
SentinelCore — Kafka → PostgreSQL Consumer
Version: 2.0.0

Responsibilities:
  - Consume from Kafka topic 'sentinel-events' (poll-based, non-blocking)
  - Upsert system_heartbeats on EVERY message (even zero-event payloads)
  - Insert enriched events into the events table (idempotent via event_hash)
  - Maintain feature_snapshots and extended ML columns
  - Graceful shutdown via SIGTERM / KeyboardInterrupt
"""

import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from kafka import KafkaConsumer          # type: ignore
from kafka.errors import KafkaError      # type: ignore
import psycopg2
from psycopg2.extras import Json

from shared_constants import (
    DB_CONFIG,
    RETRY_MAX_ATTEMPTS,
    RETRY_BACKOFF_SECONDS,
    CIRCUIT_BREAKER_THRESHOLD,
    CIRCUIT_BREAKER_RESET_SECS,
    DB_QUERY_TIMEOUT_SECONDS,
)
from sentinel_utils import (
    retry_with_backoff,
    timeout_wrapper,
    CircuitBreaker,
    clean_message,
    make_db_connection,
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
logger = logging.getLogger("kafka_to_postgres")

# ============================================================================
# KAFKA CONFIG
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "sentinel-events"
KAFKA_GROUP_ID          = "postgres-ingester-group"
KAFKA_POLL_TIMEOUT_MS   = 5_000   # poll returns after this many ms if no messages

# ============================================================================
# DB STATE
# ============================================================================

_db_cb = CircuitBreaker(
    threshold=CIRCUIT_BREAKER_THRESHOLD,
    reset_secs=CIRCUIT_BREAKER_RESET_SECS,
    label="PostgreSQL",
)


def _get_healthy_conn(existing: Optional[Any] = None) -> Any:
    """
    Return a live psycopg2 connection.
    Pings an existing connection first; reconnects on failure.
    Raises RuntimeError if circuit is open or all retries exhausted.
    """
    if not _db_cb.allow():
        raise RuntimeError("[CB:PostgreSQL] Circuit OPEN — skipping DB operation")

    if existing is not None:
        try:
            with existing.cursor() as cur:
                cur.execute("SELECT 1")
            return existing
        except Exception:
            logger.warning("DB connection unhealthy — reconnecting")
            try:
                existing.close()
            except Exception:
                pass

    conn, ok = retry_with_backoff(make_db_connection, label="DB_reconnect")
    if not ok or conn is None:
        _db_cb.record_failure()
        raise RuntimeError("Failed to connect to DB after retries")

    _db_cb.record_success()
    return conn


# ============================================================================
# SCHEMA SETUP
# ============================================================================

def setup_database(conn: Any) -> None:
    """Create / extend all tables. Fully idempotent — safe on every startup."""
    with conn.cursor() as cur:

        # ── events ────────────────────────────────────────────────────────────
        cur.execute("""
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
        """)

        # ML-enrichment columns — ADD IF NOT EXISTS (backward compatible)
        for col, col_type, default in [
            ("event_message",      "TEXT",          "''"),
            ("parsed_message",     "TEXT",          "''"),
            ("normalized_message", "TEXT",          "''"),
            ("fault_subtype",      "VARCHAR(80)",   "''"),
            ("confidence_score",   "NUMERIC(3,2)",  "0.20"),
        ]:
            cur.execute(
                f"ALTER TABLE events ADD COLUMN IF NOT EXISTS {col} {col_type} DEFAULT {default};"
            )

        # ── system_heartbeats ─────────────────────────────────────────────────
        cur.execute("""
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
        """)

        # ── feature_snapshots (ML time-series) ───────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                id                   SERIAL PRIMARY KEY,
                system_id            VARCHAR(100)  NOT NULL,
                snapshot_time        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                cpu_usage_percent    NUMERIC(5,2)  NOT NULL DEFAULT 0,
                memory_usage_percent NUMERIC(5,2)  NOT NULL DEFAULT 0,
                disk_free_percent    NUMERIC(5,2)  NOT NULL DEFAULT 100,
                total_events         INTEGER       NOT NULL DEFAULT 0,
                critical_count       INTEGER       NOT NULL DEFAULT 0,
                error_count          INTEGER       NOT NULL DEFAULT 0,
                warning_count        INTEGER       NOT NULL DEFAULT 0,
                info_count           INTEGER       NOT NULL DEFAULT 0,
                dominant_fault_type  VARCHAR(50)   NOT NULL DEFAULT 'NONE',
                avg_confidence       NUMERIC(3,2)  NOT NULL DEFAULT 0.20
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_system_time
                ON feature_snapshots(system_id, snapshot_time DESC);
        """)

    conn.commit()
    logger.info("Database schema verified / migrated.")


# ============================================================================
# MESSAGE PROCESSING
# ============================================================================

def _safe_float(value: Any, fallback: float = 0.0) -> float:
    """Cast to float safely; return fallback on None / error."""
    try:
        return float(value) if value is not None else fallback
    except (TypeError, ValueError):
        return fallback


def process_message(conn: Any, msg: Dict[str, Any]) -> bool:
    """
    Process one Kafka payload inside a single transaction.

    Contract:
      - Heartbeat update ALWAYS runs (even when events list is empty)
      - Events inserted idempotently via ON CONFLICT (event_hash) DO NOTHING
      - Full rollback on any failure — connection left in clean state
      - Returns True on success, False on failure
    """
    system_id = msg.get("system_id") or "unknown"
    hostname  = msg.get("hostname")  or "unknown"
    sys_info  = msg.get("system_info") or {}
    events    = msg.get("events") or []

    # Derive resource metrics: prefer system_info, fall back to first event
    def _res(key: str, fallback: float = 0.0) -> float:
        if key in sys_info:
            return _safe_float(sys_info[key], fallback)
        if events and key in events[0]:
            return _safe_float(events[0][key], fallback)
        return fallback

    cpu  = _res("cpu_usage_percent",    0.0)
    mem  = _res("memory_usage_percent", 0.0)
    disk = _res("disk_free_percent",  100.0)

    try:
        with conn.cursor() as cur:

            # ── 1. Heartbeat — runs unconditionally ────────────────────────
            cur.execute("""
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
            """, (
                system_id,
                hostname,
                cpu, mem, disk,
                sys_info.get("os_version",    "Unknown"),
                sys_info.get("agent_version", "Unknown"),
                sys_info.get("ip_address",    "Unknown"),
                sys_info.get("uptime_seconds", 0),
                datetime.now(timezone.utc),
            ))

            if not events:
                logger.info(
                    "[process_message] Heartbeat-only update (no events) for system_id=%s",
                    system_id,
                )

            # ── 2. Events — idempotent inserts ─────────────────────────────
            for ev in events:
                raw_msg    = ev.get("event_message") or ""
                parsed     = clean_message(raw_msg)
                normalized = parsed.lower()

                fault_subtype    = ev.get("fault_subtype") or ev.get("fault_type") or "UNKNOWN"
                confidence_score = _safe_float(ev.get("confidence_score"), 0.20)
                # Clamp to valid range [0.0, 1.0]
                confidence_score = max(0.0, min(1.0, confidence_score))

                cur.execute("""
                    INSERT INTO events (
                        system_id, hostname, log_channel, event_record_id,
                        provider_name, event_id, level, task, opcode, keywords,
                        process_id, thread_id, severity, fault_type,
                        diagnostic_context, event_hash, raw_xml,
                        cpu_usage_percent, memory_usage_percent, disk_free_percent,
                        event_message, parsed_message, normalized_message,
                        fault_subtype, confidence_score
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (event_hash) DO NOTHING;
                """, (
                    system_id,
                    hostname,
                    ev.get("log_channel"),
                    ev.get("event_record_id"),
                    ev.get("provider_name"),
                    ev.get("event_id"),
                    ev.get("level"),
                    ev.get("task"),
                    ev.get("opcode"),
                    ev.get("keywords"),
                    ev.get("process_id"),
                    ev.get("thread_id"),
                    ev.get("severity"),
                    ev.get("fault_type"),
                    Json(ev.get("diagnostic_context") or {}),
                    ev.get("event_hash"),
                    ev.get("raw_xml"),
                    _safe_float(ev.get("cpu_usage_percent")),
                    _safe_float(ev.get("memory_usage_percent")),
                    _safe_float(ev.get("disk_free_percent"), 100.0),
                    raw_msg,
                    parsed,
                    normalized,
                    fault_subtype,
                    confidence_score,
                ))

        conn.commit()
        _db_cb.record_success()
        return True

    except Exception as exc:
        logger.error("[process_message] Transaction failed for %s: %s", system_id, exc)
        try:
            conn.rollback()
        except Exception as rb_exc:
            logger.error("[process_message] Rollback failed: %s", rb_exc)
        _db_cb.record_failure()
        return False


# ============================================================================
# CONSUMER LOOP
# ============================================================================

def run_consumer() -> None:
    _shutdown = False

    def _handle_signal(signum: int, frame: Any) -> None:
        nonlocal _shutdown
        logger.info("[graceful_shutdown] Signal %d received — stopping consumer", signum)
        _shutdown = True

    signal.signal(signal.SIGTERM, _handle_signal)

    # Initial DB connection with retry
    conn, ok = retry_with_backoff(make_db_connection, label="initial_DB_connect")
    if not ok or conn is None:
        logger.critical("Cannot connect to PostgreSQL on startup — aborting")
        return

    setup_database(conn)

    # poll()-based consumer — never blocks indefinitely
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=KAFKA_POLL_TIMEOUT_MS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info(
        "Listening on Kafka topic '%s' (poll timeout: %dms) ...",
        KAFKA_TOPIC, KAFKA_POLL_TIMEOUT_MS,
    )

    processed = 0
    failed    = 0

    try:
        while not _shutdown:
            # poll() is non-blocking — returns {} when no messages within timeout
            try:
                records = consumer.poll(
                    timeout_ms=KAFKA_POLL_TIMEOUT_MS, max_records=50
                )
            except KafkaError as ke:
                logger.error("[consumer.poll] Kafka error: %s — sleeping 5s", ke)
                time.sleep(5)
                continue
            except Exception as poll_exc:
                logger.error("[consumer.poll] Unexpected: %s — sleeping 5s", poll_exc)
                time.sleep(5)
                continue

            if not records:
                continue  # idle poll window — keep loop alive

            for _tp, messages in records.items():
                for message in messages:
                    if _shutdown:
                        break

                    t0       = time.time()
                    payload  = message.value
                    hostname = payload.get("hostname", "unknown")
                    sys_id   = payload.get("system_id", "unknown")

                    # Re-validate DB connection before each message
                    try:
                        conn = _get_healthy_conn(conn)
                    except RuntimeError as conn_exc:
                        logger.error(
                            "[consumer] DB unavailable: %s — skipping message", conn_exc
                        )
                        failed += 1
                        structured_log(
                            "kafka_to_postgres",
                            {"hostname": hostname, "system_id": sys_id,
                             "events": 0, "status": "db_unavailable", "latency_ms": 0},
                            log=logger,
                        )
                        continue

                    # Per-message isolation — one bad message never stops the loop
                    try:
                        ok = process_message(conn, payload)
                    except Exception as msg_exc:
                        logger.error("[consumer] process_message raised: %s", msg_exc)
                        ok = False

                    latency_ms   = (time.time() - t0) * 1000
                    events_count = len(payload.get("events") or [])
                    status       = "ok" if ok else "failed"

                    if ok:
                        processed += 1
                        logger.info(
                            "✓ %s | heartbeat updated | events ingested: %d",
                            hostname, events_count,
                        )
                    else:
                        failed += 1

                    structured_log(
                        "kafka_to_postgres",
                        {
                            "hostname":   hostname,
                            "system_id":  sys_id,
                            "events":     events_count,
                            "status":     status,
                            "latency_ms": round(latency_ms, 2),
                        },
                        log=logger,
                    )

    except KeyboardInterrupt:
        logger.info("[graceful_shutdown] KeyboardInterrupt received")

    finally:
        logger.info(
            "[graceful_shutdown] Consumer stopped. processed=%d failed=%d",
            processed, failed,
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