"""
SentinelCore — Feature Builder
Version: 1.0.0

Background worker that runs every 30 seconds.
Generates one feature_snapshot row per active system per cycle —
even when zero events occurred — ensuring continuous ML time-series data.

Usage:
    python feature_builder.py           # run forever
    python feature_builder.py --once    # single cycle, then exit (for testing)
"""

import argparse
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from shared_constants import DB_QUERY_TIMEOUT_SECONDS
from sentinel_utils import (
    retry_with_backoff,
    timeout_wrapper,
    CircuitBreaker,
    make_db_connection,
    structured_log,
)

# ============================================================================
# CONFIG
# ============================================================================

FEATURE_BUILDER_VERSION  = "1.0.0"
CYCLE_INTERVAL_SECS      = 30    # target cadence between cycles
SNAPSHOT_LOOKBACK_SECS   = 30    # events window aggregated per snapshot
CYCLE_HARD_TIMEOUT_SECS  = 25    # entire cycle body must finish within this

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("feature_builder")

# ============================================================================
# DB STATE
# ============================================================================

_db_cb = CircuitBreaker(label="FB_PostgreSQL")


def _get_healthy_conn(existing: Optional[Any] = None) -> Any:
    """Return a live psycopg2 connection, reconnecting if needed."""
    if not _db_cb.allow():
        raise RuntimeError("[CB:FB_PostgreSQL] Circuit OPEN — skipping DB")

    if existing is not None:
        try:
            with existing.cursor() as cur:
                cur.execute("SELECT 1")
            return existing
        except Exception:
            logger.warning("FB: DB connection lost — reconnecting")
            try:
                existing.close()
            except Exception:
                pass

    conn, ok = retry_with_backoff(make_db_connection, label="FB_reconnect")
    if not ok or conn is None:
        _db_cb.record_failure()
        raise RuntimeError("Cannot reconnect to DB after retries")

    _db_cb.record_success()
    return conn


# ============================================================================
# DATA QUERIES
# ============================================================================

def fetch_active_systems(conn: Any) -> List[Dict]:
    """Return all rows from system_heartbeats — always the canonical system list."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT system_id, hostname,
                   cpu_usage_percent, memory_usage_percent, disk_free_percent,
                   os_version, last_seen
            FROM system_heartbeats
            ORDER BY system_id
        """)
        return [dict(r) for r in cur.fetchall()]


def fetch_event_stats(conn: Any, system_id: str, lookback_secs: int) -> Dict:
    """
    Aggregate event counts for one system over the last N seconds.
    Always returns a fully-populated dict — never NULL values.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                COUNT(*)                                                         AS total_events,
                COUNT(*) FILTER (WHERE severity = 'CRITICAL')                   AS critical_count,
                COUNT(*) FILTER (WHERE severity = 'ERROR')                       AS error_count,
                COUNT(*) FILTER (WHERE severity = 'WARNING')                     AS warning_count,
                COUNT(*) FILTER (WHERE severity = 'INFO')                        AS info_count,
                MODE() WITHIN GROUP (ORDER BY fault_type)                        AS dominant_fault_type,
                ROUND(AVG(COALESCE(confidence_score, 0.20))::numeric, 2)         AS avg_confidence
            FROM events
            WHERE system_id = %s
              AND ingested_at > NOW() - (%s || ' seconds')::interval
        """, (system_id, str(lookback_secs)))

        row = cur.fetchone()

    # Defensive: return zeroed defaults on NULL or missing row
    if not row or row.get("total_events") is None:
        return {
            "total_events":       0,
            "critical_count":     0,
            "error_count":        0,
            "warning_count":      0,
            "info_count":         0,
            "dominant_fault_type": "NONE",
            "avg_confidence":     0.20,
        }

    def _i(v: Any) -> int:
        try:
            return int(v) if v is not None else 0
        except (TypeError, ValueError):
            return 0

    def _f(v: Any, fb: float = 0.0) -> float:
        try:
            return float(v) if v is not None else fb
        except (TypeError, ValueError):
            return fb

    return {
        "total_events":       _i(row["total_events"]),
        "critical_count":     _i(row["critical_count"]),
        "error_count":        _i(row["error_count"]),
        "warning_count":      _i(row["warning_count"]),
        "info_count":         _i(row["info_count"]),
        "dominant_fault_type": row.get("dominant_fault_type") or "NONE",
        "avg_confidence":     _f(row.get("avg_confidence"), 0.20),
    }


def write_snapshot(conn: Any, system_id: str, hb: Dict, stats: Dict) -> bool:
    """
    Insert one feature_snapshot row.
    Uses heartbeat resource metrics (always current) + aggregated event stats.
    Returns True on success, False on failure (with rollback).
    """
    def _f(v: Any, fb: float = 0.0) -> float:
        try:
            return float(v) if v is not None else fb
        except (TypeError, ValueError):
            return fb

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO feature_snapshots (
                    system_id, snapshot_time,
                    cpu_usage_percent, memory_usage_percent, disk_free_percent,
                    total_events, critical_count, error_count, warning_count, info_count,
                    dominant_fault_type, avg_confidence
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                system_id,
                datetime.now(timezone.utc),
                _f(hb.get("cpu_usage_percent"),    0.0),
                _f(hb.get("memory_usage_percent"), 0.0),
                _f(hb.get("disk_free_percent"),  100.0),
                stats["total_events"],
                stats["critical_count"],
                stats["error_count"],
                stats["warning_count"],
                stats["info_count"],
                stats["dominant_fault_type"],
                stats["avg_confidence"],
            ))
        conn.commit()
        return True

    except Exception as exc:
        logger.error("[write_snapshot] %s: %s", system_id, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


# ============================================================================
# SINGLE CYCLE
# ============================================================================

def run_cycle(conn: Any, cycle: int) -> tuple:
    """
    Execute one full feature-builder cycle.
    Returns (conn, systems_checked, snapshots_ok).
    """
    conn             = _get_healthy_conn(conn)
    systems          = fetch_active_systems(conn)
    systems_checked  = len(systems)
    snapshots_ok     = 0

    if not systems:
        logger.info("[Cycle %d] No systems in heartbeats — skipping", cycle)
        return conn, 0, 0

    for hb in systems:
        sid = hb["system_id"]
        try:
            stats = fetch_event_stats(conn, sid, SNAPSHOT_LOOKBACK_SECS)
            if write_snapshot(conn, sid, hb, stats):
                snapshots_ok += 1
                logger.debug(
                    "  ✓ %s | events=%d critical=%d cpu=%.1f%%",
                    sid, stats["total_events"], stats["critical_count"],
                    float(hb.get("cpu_usage_percent") or 0),
                )
            else:
                logger.warning("  ✗ %s snapshot write failed", sid)
        except Exception as sys_exc:
            # One system failure must NOT stop the rest of the loop
            logger.error("  [Cycle %d] %s error: %s", cycle, sid, sys_exc)
            continue

    return conn, systems_checked, snapshots_ok


# ============================================================================
# MAIN LOOP  (background_worker_pattern)
# ============================================================================

def run_feature_builder(run_once: bool = False) -> None:
    logger.info(
        "FeatureBuilder v%s | cycle=%ds | lookback=%ds",
        FEATURE_BUILDER_VERSION, CYCLE_INTERVAL_SECS, SNAPSHOT_LOOKBACK_SECS,
    )

    _shutdown = False

    def _handle_signal(signum: int, frame: Any) -> None:
        nonlocal _shutdown
        logger.info(
            "[graceful_shutdown] Signal %d received — stopping after current cycle", signum
        )
        _shutdown = True

    signal.signal(signal.SIGTERM, _handle_signal)

    # Initial connection
    conn, ok = retry_with_backoff(make_db_connection, label="FB_initial_connect")
    if not ok or conn is None:
        logger.critical("FeatureBuilder: cannot connect to DB on startup — aborting")
        return

    cycle = 0

    while not _shutdown:
        cycle       += 1
        t0           = time.time()
        cycle_status = "ok"
        systems_n    = 0
        snaps_n      = 0

        try:
            # Hard-cap the entire cycle body via timeout_wrapper
            result, timed_out_ok = timeout_wrapper(
                run_cycle,
                conn, cycle,
                timeout_secs=float(CYCLE_HARD_TIMEOUT_SECS),
                label=f"fb_cycle/{cycle}",
            )

            if not timed_out_ok or result is None:
                cycle_status = "timeout"
            else:
                conn, systems_n, snaps_n = result

        except KeyboardInterrupt:
            logger.info("[graceful_shutdown] KeyboardInterrupt")
            _shutdown = True

        except Exception as exc:
            logger.error("[Cycle %d] unhandled error: %s", cycle, exc, exc_info=True)
            cycle_status = "error"

        finally:
            duration = time.time() - t0
            structured_log(
                "feature_builder",
                {
                    "cycle":             cycle,
                    "duration_s":        round(duration, 3),
                    "systems_checked":   systems_n,
                    "snapshots_written": snaps_n,
                    "status":            cycle_status,
                },
                log=logger,
            )

            if run_once:
                _shutdown = True
            elif not _shutdown:
                sleep_time = max(0.0, CYCLE_INTERVAL_SECS - duration)
                if sleep_time:
                    time.sleep(sleep_time)

    # Graceful shutdown
    logger.info("[graceful_shutdown] FeatureBuilder stopped. Total cycles: %d", cycle)
    try:
        conn.close()
    except Exception:
        pass


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelCore Feature Builder")
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single cycle then exit (useful for testing / CI)",
    )
    args = parser.parse_args()
    run_feature_builder(run_once=args.once)