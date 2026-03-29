import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any

from sentinel_utils import make_db_connection, retry_with_backoff
import psycopg2.extras

MODEL_VERSION = 'v1'
CYCLE_INTERVAL = 30
_MAX_RECONNECT_SLEEP = 60  # seconds — caps exponential backoff

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('ml_engine')

def fetch_latest_snapshots(conn: Any) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT DISTINCT ON (system_id) *
            FROM feature_snapshots
            ORDER BY system_id, snapshot_time DESC
        """)
        return [dict(r) for r in cur.fetchall()]

def simple_anomaly_score(snapshot: Dict[str, Any]) -> float:
    score = 0.0
    if snapshot.get('cpu_usage_percent', 0) > 85:
        score += 0.3
    if snapshot.get('memory_usage_percent', 0) > 90:
        score += 0.3
    if snapshot.get('critical_count', 0) > 2:
        score += 0.4
    return min(score, 1.0)

def simple_failure_probability(snapshot: Dict[str, Any]) -> float:
    return min(
        (snapshot.get('critical_count', 0) * 0.2) +
        (snapshot.get('error_count', 0) * 0.1),
        1.0
    )

def predict_fault(snapshot: Dict[str, Any]) -> str:
    if snapshot.get('critical_count', 0) > 3:
        return 'SYSTEM_FAILURE'
    if snapshot.get('error_count', 0) > 5:
        return 'SERVICE_DEGRADATION'
    return 'NONE'

def write_prediction(conn: Any, system_id: str, anomaly: float, failure: float, fault: str) -> bool:
    """Insert one ML prediction row. Returns True on success, False on failure."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ml_predictions (
                    system_id, prediction_time,
                    anomaly_score, failure_probability,
                    predicted_fault, model_version
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                system_id,
                datetime.now(timezone.utc),
                # Clamp scores to [0.0, 1.0] — guard against NaN/Inf from future models
                max(0.0, min(1.0, float(anomaly))),
                max(0.0, min(1.0, float(failure))),
                fault or 'NONE',
                MODEL_VERSION
            ))
        conn.commit()
        return True
    except Exception as exc:
        logger.error('[write_prediction] %s: %s', system_id, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False

def run_cycle(conn: Any) -> None:
    """Run one ML prediction cycle over the latest feature snapshots."""
    snapshots = fetch_latest_snapshots(conn)
    if not snapshots:
        return

    written = 0
    for snap in snapshots:
        system_id = snap.get('system_id') or 'unknown'
        anomaly = simple_anomaly_score(snap)
        failure = simple_failure_probability(snap)
        fault   = predict_fault(snap)
        if write_prediction(conn, system_id, anomaly, failure, fault):
            written += 1

    logger.info('Generated ML predictions for %d/%d systems.', written, len(snapshots))

def run_ml_engine() -> None:
    """Run the ML engine in an infinite loop with reconnect on DB failure."""
    logger.info('Starting ML Engine worker...')
    conn, ok = retry_with_backoff(make_db_connection, label='ml_engine_connect')
    if not ok or not conn:
        logger.error('DB connection failed on startup — aborting ML engine')
        return

    reconnect_delay = 5.0  # seconds, doubles on each failure, capped at _MAX_RECONNECT_SLEEP

    while True:
        try:
            if conn is None:
                raise RuntimeError('No active DB connection')
            run_cycle(conn)
            reconnect_delay = 5.0  # reset on success
        except Exception as exc:
            logger.error('ML engine cycle error: %s', exc)
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            logger.info('ML engine reconnecting in %.0fs...', reconnect_delay)
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, _MAX_RECONNECT_SLEEP)
            conn, ok = retry_with_backoff(make_db_connection, label='ml_engine_reconnect')
            if not ok or not conn:
                logger.error('ML engine DB reconnect failed — will retry on next cycle')
                conn = None

        time.sleep(CYCLE_INTERVAL)

if __name__ == '__main__':
    run_ml_engine()
