"""
SentinelCore - Feature Builder.

Generates feature snapshots from heartbeats and recent events while preserving:
  - a single aggregation query for all systems
  - backward-compatible helper APIs
  - one-system failure isolation
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2.extras

from shared_constants import (
    FEATURE_BUILDER_BATCH_TIMEOUT_SECS,
    FEATURE_BUILDER_HARD_TIMEOUT_SECS,
    FEATURE_BUILDER_INTERVAL_SECS,
    FEATURE_BUILDER_LOOKBACK_SECS,
    FEATURE_BUILDER_SYSTEM_BATCH_SIZE,
    FEATURE_BUILDER_THREAD_WORKERS,
)
from sentinel_utils import (
    CircuitBreaker,
    make_db_connection,
    retry_with_backoff,
    structured_log,
    timeout_wrapper,
)

FEATURE_BUILDER_VERSION = "2.0.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("feature_builder")

_db_cb = CircuitBreaker(label="feature_builder_postgres")


def _get_healthy_conn(existing: Optional[Any] = None) -> Any:
    """Return a live psycopg2 connection, reconnecting if needed."""
    if not _db_cb.allow():
        raise RuntimeError("[CB:feature_builder_postgres] Circuit OPEN - skipping DB")

    if existing is not None:
        try:
            with existing.cursor() as cur:
                cur.execute("SELECT 1")
            return existing
        except Exception as exc:
            logger.warning("Feature builder DB connection unhealthy - reconnecting: %s", exc)
            structured_log(
                "feature_builder",
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

    conn, ok = retry_with_backoff(make_db_connection, label="feature_builder_db_reconnect")
    if not ok or conn is None:
        _db_cb.record_failure()
        raise RuntimeError("Cannot reconnect to DB after retries")

    _db_cb.record_success()
    return conn


def fetch_active_systems(conn: Any) -> List[Dict[str, Any]]:
    """Return all rows from system_heartbeats in a stable order."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT system_id, hostname,
                   cpu_usage_percent, memory_usage_percent, disk_free_percent,
                   os_version, last_seen
            FROM system_heartbeats
            ORDER BY system_id
            """
        )
        return [dict(row) for row in cur.fetchall()]


def _safe_int(value: Any, fallback: int = 0) -> int:
    """Convert values to int with a deterministic fallback."""
    try:
        return int(value) if value is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    """Convert values to float with a deterministic fallback."""
    try:
        return float(value) if value is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _zero_stats() -> Dict[str, Any]:
    """Return the canonical zero-valued event stats payload."""
    return {
        "total_events": 0,
        "critical_count": 0,
        "error_count": 0,
        "warning_count": 0,
        "info_count": 0,
        "dominant_fault_type": "NONE",
        "avg_confidence": 0.20,
    }


def fetch_all_event_stats(conn: Any, system_ids: List[str], lookback_secs: int) -> Dict[str, Dict[str, Any]]:
    """Fetch aggregated stats for all systems with one query."""
    if not system_ids:
        return {}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                system_id,
                COUNT(*)                                                  AS total_events,
                COUNT(*) FILTER (WHERE severity = 'CRITICAL')             AS critical_count,
                COUNT(*) FILTER (WHERE severity = 'ERROR')                AS error_count,
                COUNT(*) FILTER (WHERE severity = 'WARNING')              AS warning_count,
                COUNT(*) FILTER (WHERE severity = 'INFO')                 AS info_count,
                MODE() WITHIN GROUP (ORDER BY fault_type)                 AS dominant_fault_type,
                ROUND(AVG(COALESCE(confidence_score, 0.20))::numeric, 2) AS avg_confidence
            FROM events
            WHERE system_id = ANY(%s)
              AND ingested_at > NOW() - (%s || ' seconds')::interval
            GROUP BY system_id
            """,
            (system_ids, str(lookback_secs)),
        )
        rows = cur.fetchall()

    result: Dict[str, Dict[str, Any]] = {system_id: _zero_stats() for system_id in system_ids}
    for row in rows:
        system_id = row["system_id"]
        result[system_id] = {
            "total_events": _safe_int(row["total_events"]),
            "critical_count": _safe_int(row["critical_count"]),
            "error_count": _safe_int(row["error_count"]),
            "warning_count": _safe_int(row["warning_count"]),
            "info_count": _safe_int(row["info_count"]),
            "dominant_fault_type": row.get("dominant_fault_type") or "NONE",
            "avg_confidence": _safe_float(row.get("avg_confidence"), 0.20),
        }
    return result


def fetch_event_stats(conn: Any, system_id: str, lookback_secs: int) -> Dict[str, Any]:
    """Backward-compatible single-system query helper."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                                                  AS total_events,
                COUNT(*) FILTER (WHERE severity = 'CRITICAL')             AS critical_count,
                COUNT(*) FILTER (WHERE severity = 'ERROR')                AS error_count,
                COUNT(*) FILTER (WHERE severity = 'WARNING')              AS warning_count,
                COUNT(*) FILTER (WHERE severity = 'INFO')                 AS info_count,
                MODE() WITHIN GROUP (ORDER BY fault_type)                 AS dominant_fault_type,
                ROUND(AVG(COALESCE(confidence_score, 0.20))::numeric, 2) AS avg_confidence
            FROM events
            WHERE system_id = %s
              AND ingested_at > NOW() - (%s || ' seconds')::interval
            """,
            (system_id, str(lookback_secs)),
        )
        row = cur.fetchone()

    if not row:
        return _zero_stats()

    return {
        "total_events": _safe_int(row.get("total_events")),
        "critical_count": _safe_int(row.get("critical_count")),
        "error_count": _safe_int(row.get("error_count")),
        "warning_count": _safe_int(row.get("warning_count")),
        "info_count": _safe_int(row.get("info_count")),
        "dominant_fault_type": row.get("dominant_fault_type") or "NONE",
        "avg_confidence": _safe_float(row.get("avg_confidence"), 0.20),
    }


def write_snapshot(conn: Any, system_id: str, heartbeat: Dict[str, Any], stats: Dict[str, Any]) -> bool:
    """Insert one feature snapshot row and keep the legacy bool contract."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO feature_snapshots (
                    system_id, snapshot_time,
                    cpu_usage_percent, memory_usage_percent, disk_free_percent,
                    total_events, critical_count, error_count, warning_count, info_count,
                    dominant_fault_type, avg_confidence
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    system_id,
                    datetime.now(timezone.utc),
                    _safe_float(heartbeat.get("cpu_usage_percent"), 0.0),
                    _safe_float(heartbeat.get("memory_usage_percent"), 0.0),
                    _safe_float(heartbeat.get("disk_free_percent"), 100.0),
                    _safe_int(stats.get("total_events")),
                    _safe_int(stats.get("critical_count")),
                    _safe_int(stats.get("error_count")),
                    _safe_int(stats.get("warning_count")),
                    _safe_int(stats.get("info_count")),
                    stats.get("dominant_fault_type") or "NONE",
                    _safe_float(stats.get("avg_confidence"), 0.20),
                ),
            )
        conn.commit()
        return True
    except Exception as exc:
        logger.error("[write_snapshot] %s: %s", system_id, exc)
        structured_log(
            "feature_builder",
            {
                "operation": "write_snapshot",
                "system_id": system_id,
                "status": "failed",
                "error": str(exc),
            },
            log=logger,
        )
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def _chunked(items: Sequence[Tuple[str, Dict[str, Any], Dict[str, Any]]], chunk_size: int) -> Iterable[Sequence[Tuple[str, Dict[str, Any], Dict[str, Any]]]]:
    """Yield fixed-size chunks from a sequence."""
    for start_index in range(0, len(items), chunk_size):
        yield items[start_index : start_index + chunk_size]


def _write_snapshot_batch(snapshot_batch: Sequence[Tuple[str, Dict[str, Any], Dict[str, Any]]]) -> Tuple[int, List[str]]:
    """Write a batch of snapshots using a dedicated worker connection."""
    if not snapshot_batch:
        return 0, []

    worker_conn = _get_healthy_conn(None)
    successes = 0
    failures: List[str] = []
    try:
        for system_id, heartbeat, stats in snapshot_batch:
            if write_snapshot(worker_conn, system_id, heartbeat, stats):
                successes += 1
            else:
                failures.append(system_id)
        return successes, failures
    finally:
        try:
            worker_conn.close()
        except Exception:
            pass


def run_cycle(conn: Any, cycle: int) -> Tuple[Any, int, int]:
    """
    Execute one feature-builder cycle.

    Returns ``(conn, systems_checked, snapshots_written)``.
    """
    conn = _get_healthy_conn(conn)
    systems = fetch_active_systems(conn)
    systems_checked = len(systems)

    if not systems:
        logger.info("[Cycle %d] No systems in heartbeats - skipping", cycle)
        return conn, 0, 0

    system_ids = [heartbeat["system_id"] for heartbeat in systems]
    all_stats = fetch_all_event_stats(conn, system_ids, FEATURE_BUILDER_LOOKBACK_SECS)
    snapshot_jobs = [
        (heartbeat["system_id"], heartbeat, all_stats.get(heartbeat["system_id"], _zero_stats()))
        for heartbeat in systems
    ]

    snapshots_written = 0
    worker_count = max(1, min(FEATURE_BUILDER_THREAD_WORKERS, len(snapshot_jobs)))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="feature-builder") as executor:
        futures = [
            executor.submit(_write_snapshot_batch, batch)
            for batch in _chunked(snapshot_jobs, max(1, FEATURE_BUILDER_SYSTEM_BATCH_SIZE))
        ]
        for future in futures:
            try:
                batch_successes, batch_failures = future.result(timeout=FEATURE_BUILDER_BATCH_TIMEOUT_SECS)
                snapshots_written += batch_successes
                for failed_system_id in batch_failures:
                    structured_log(
                        "feature_builder",
                        {
                            "operation": "write_snapshot_batch",
                            "system_id": failed_system_id,
                            "status": "failed",
                        },
                        log=logger,
                    )
            except FutureTimeoutError:
                logger.error("[Cycle %d] Snapshot batch exceeded %ss", cycle, FEATURE_BUILDER_BATCH_TIMEOUT_SECS)
                structured_log(
                    "feature_builder",
                    {
                        "operation": "write_snapshot_batch",
                        "cycle": cycle,
                        "status": "failed",
                        "error": f"batch timed out after {FEATURE_BUILDER_BATCH_TIMEOUT_SECS}s",
                    },
                    log=logger,
                )
            except Exception as exc:
                logger.error("[Cycle %d] Snapshot batch error: %s", cycle, exc, exc_info=True)
                structured_log(
                    "feature_builder",
                    {
                        "operation": "write_snapshot_batch",
                        "cycle": cycle,
                        "status": "failed",
                        "error": str(exc),
                    },
                    log=logger,
                )

    return conn, systems_checked, snapshots_written


def run_feature_builder(run_once: bool = False) -> None:
    """Run feature-builder cycles until stopped."""
    logger.info(
        "FeatureBuilder v%s | interval=%ss | lookback=%ss | workers=%s",
        FEATURE_BUILDER_VERSION,
        FEATURE_BUILDER_INTERVAL_SECS,
        FEATURE_BUILDER_LOOKBACK_SECS,
        FEATURE_BUILDER_THREAD_WORKERS,
    )

    shutdown_requested = False

    def handle_signal(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        logger.info("[graceful_shutdown] Signal %d received - stopping after current cycle", signum)
        shutdown_requested = True

    signal.signal(signal.SIGTERM, handle_signal)

    conn, ok = retry_with_backoff(make_db_connection, label="feature_builder_initial_connect")
    if not ok or conn is None:
        logger.critical("FeatureBuilder cannot connect to DB on startup - aborting")
        structured_log(
            "feature_builder",
            {
                "operation": "startup_db_connect",
                "status": "failed",
            },
            log=logger,
        )
        return

    cycle = 0

    while not shutdown_requested:
        cycle += 1
        cycle_started_at = time.time()
        cycle_status = "ok"
        systems_checked = 0
        snapshots_written = 0

        try:
            result, completed = timeout_wrapper(
                run_cycle,
                conn,
                cycle,
                timeout_secs=float(FEATURE_BUILDER_HARD_TIMEOUT_SECS),
                label=f"feature_builder_cycle/{cycle}",
            )
            if not completed or result is None:
                cycle_status = "timeout"
            else:
                conn, systems_checked, snapshots_written = result
        except KeyboardInterrupt:
            logger.info("[graceful_shutdown] KeyboardInterrupt")
            shutdown_requested = True
        except Exception as exc:
            logger.error("[Cycle %d] Unhandled error: %s", cycle, exc, exc_info=True)
            cycle_status = "failed"
            structured_log(
                "feature_builder",
                {
                    "operation": "run_cycle",
                    "cycle": cycle,
                    "status": "failed",
                    "error": str(exc),
                },
                log=logger,
            )
        finally:
            cycle_duration = time.time() - cycle_started_at
            if cycle_duration > FEATURE_BUILDER_INTERVAL_SECS:
                logger.warning(
                    "[Cycle %d] Duration %.2fs exceeded interval %ss",
                    cycle,
                    cycle_duration,
                    FEATURE_BUILDER_INTERVAL_SECS,
                )
                structured_log(
                    "feature_builder",
                    {
                        "operation": "cycle_watchdog",
                        "cycle": cycle,
                        "status": "warning",
                        "duration_s": round(cycle_duration, 3),
                        "interval_s": FEATURE_BUILDER_INTERVAL_SECS,
                    },
                    log=logger,
                )

            structured_log(
                "feature_builder",
                {
                    "cycle": cycle,
                    "duration_s": round(cycle_duration, 3),
                    "systems_checked": systems_checked,
                    "snapshots_written": snapshots_written,
                    "status": cycle_status,
                },
                log=logger,
            )

            if run_once:
                shutdown_requested = True
            elif not shutdown_requested:
                sleep_time = max(0.0, FEATURE_BUILDER_INTERVAL_SECS - cycle_duration)
                if sleep_time:
                    time.sleep(sleep_time)

    logger.info("[graceful_shutdown] FeatureBuilder stopped. Total cycles: %d", cycle)
    try:
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelCore Feature Builder")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single cycle then exit (useful for testing and CI)",
    )
    args = parser.parse_args()
    run_feature_builder(run_once=args.once)
