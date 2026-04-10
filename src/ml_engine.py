"""
SentinelCore - ML Engine v2.

Runs an Isolation Forest (+ optional KMeans) on the latest feature snapshots
and writes anomaly scores / cluster IDs into ml_predictions.

Configuration is driven entirely by shared.ml_constants — no hard-coded
parameters live in this file.

Design constraints:
  - Single-threaded sklearn (n_jobs=1) to avoid fighting the Kafka consumer
    for CPU.
  - Sliding window of ML_BATCH_SIZE rows — never does a full table scan.
  - Falls back to lightweight heuristic scoring if fewer than
    ML_MIN_ROWS_FOR_MODEL rows are available (e.g. fresh install).
  - Synthetic data generation available in dev mode via
    ML_SYNTHETIC_FALLBACK_ENABLED constant.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras

from shared.db_constants import get_db_config
from shared.ml_constants import (
    FEATURE_COLUMNS,
    ISOLATION_FOREST_CONFIG,
    KMEANS_CONFIG,
    KMEANS_ENABLED,
    ML_BATCH_SIZE,
    ML_MIN_ROWS_FOR_MODEL,
    ML_PIPELINE_INTERVAL_SECS,
    ML_SYNTHETIC_FALLBACK_ENABLED,
)
from sentinel_utils import make_db_connection, retry_with_backoff, structured_log

# ---------------------------------------------------------------------------
# Lazy sklearn imports — lets the module load even if scikit-learn is absent;
# the actual calls will raise a clear ImportError only when ML is attempted.
# ---------------------------------------------------------------------------
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    _SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SKLEARN_AVAILABLE = False

MODEL_VERSION = "v2-isof"
_MAX_RECONNECT_SLEEP = 60  # seconds — caps exponential back-off

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml_engine")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _make_conn() -> Any:
    """Open a fresh psycopg2 connection using get_db_config()."""
    import psycopg2
    return psycopg2.connect(**get_db_config())


def _ensure_columns(conn: Any) -> None:
    """
    Idempotently add the new ml_predictions columns introduced in v2.
    Safe to call every startup — uses `ADD COLUMN IF NOT EXISTS`.
    """
    with conn.cursor() as cur:
        for col, typedef in [
            ("is_anomaly", "BOOLEAN DEFAULT NULL"),
            ("cluster_id", "INTEGER DEFAULT NULL"),
        ]:
            cur.execute(
                f"ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS {col} {typedef};"
            )
    conn.commit()


def fetch_latest_snapshots(conn: Any, limit: int = ML_BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Fetch the *limit* most-recent rows from feature_snapshots regardless of
    system_id.  Using a global recency window (not DISTINCT ON system_id)
    gives Isolation Forest a proper mini-batch to learn from.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, system_id, snapshot_time,
                   cpu_usage_percent, memory_usage_percent, disk_free_percent,
                   error_count, critical_count
            FROM feature_snapshots
            ORDER BY snapshot_time DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def _inject_synthetic_snapshots(conn: Any) -> int:
    """
    Insert 120 synthetic rows (≈10 % anomalous) for dev/testing purposes.
    Only runs when ML_SYNTHETIC_FALLBACK_ENABLED is True.
    Returns the number of rows inserted.
    """
    import random
    random.seed(42)

    system_ids = ["dev-sys-001", "dev-sys-002", "dev-sys-003"]
    rows: List[Tuple[Any, ...]] = []
    now = datetime.now(timezone.utc)

    for i in range(120):
        sid = system_ids[i % len(system_ids)]
        is_anomalous = (i % 10 == 0)          # ~10 % anomalies
        cpu   = random.uniform(88, 99) if is_anomalous else random.uniform(10, 70)
        mem   = random.uniform(88, 99) if is_anomalous else random.uniform(20, 75)
        disk  = random.uniform(5, 15)  if is_anomalous else random.uniform(30, 90)
        err   = random.randint(5, 10)  if is_anomalous else random.randint(0, 3)
        crit  = random.randint(3, 5)   if is_anomalous else random.randint(0, 1)
        rows.append((sid, now, round(cpu, 2), round(mem, 2), round(disk, 2), err, crit))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO feature_snapshots (
                system_id, snapshot_time,
                cpu_usage_percent, memory_usage_percent, disk_free_percent,
                error_count, critical_count
            ) VALUES %s
            """,
            rows,
        )
    conn.commit()
    logger.info("[ml_engine] Inserted %d synthetic snapshot rows (dev mode).", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Heuristic fallback (no sklearn needed)
# ---------------------------------------------------------------------------

def _heuristic_score(snap: Dict[str, Any]) -> Tuple[float, bool, str]:
    """Simple rule-based scorer used when sklearn is unavailable or data is scarce."""
    score = 0.0
    if snap.get("cpu_usage_percent", 0) > 85:
        score += 0.3
    if snap.get("memory_usage_percent", 0) > 90:
        score += 0.3
    if snap.get("critical_count", 0) > 2:
        score += 0.4
    score = min(score, 1.0)

    is_anomaly = score >= 0.5
    fault: str
    if snap.get("critical_count", 0) > 3:
        fault = "SYSTEM_FAILURE"
    elif snap.get("error_count", 0) > 5:
        fault = "SERVICE_DEGRADATION"
    else:
        fault = "NONE"
    return score, is_anomaly, fault


def _failure_probability(snap: Dict[str, Any]) -> float:
    return min(
        snap.get("critical_count", 0) * 0.2 + snap.get("error_count", 0) * 0.1,
        1.0,
    )


# ---------------------------------------------------------------------------
# sklearn pipeline
# ---------------------------------------------------------------------------

def _run_sklearn_pipeline(
    snapshots: List[Dict[str, Any]],
) -> Dict[int, Tuple[float, bool, Optional[int]]]:
    """
    Run IsolationForest (+ optional KMeans) on *snapshots*.

    Returns a dict keyed by snapshot row `id`:
        {row_id: (anomaly_score, is_anomaly, cluster_id_or_None)}
    """
    import numpy as np  # guaranteed to be present when sklearn is

    # Build feature matrix — fill missing values with 0 for robustness
    X = np.array(
        [[float(s.get(col) or 0) for col in FEATURE_COLUMNS] for s in snapshots],
        dtype=float,
    )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Isolation Forest
    iso = IsolationForest(**ISOLATION_FOREST_CONFIG)
    iso.fit(X_scaled)
    # decision_function returns higher = more normal; we invert to get anomaly score
    raw_scores = iso.decision_function(X_scaled)          # shape (n,)
    labels = iso.predict(X_scaled)                        # +1 normal, -1 anomaly

    # Normalise scores to [0, 1] where 1 = most anomalous
    min_s, max_s = raw_scores.min(), raw_scores.max()
    score_range = max_s - min_s if max_s != min_s else 1.0
    anomaly_scores = 1.0 - (raw_scores - min_s) / score_range  # invert & normalise

    # Optional KMeans
    cluster_ids: Optional[List[int]] = None
    if KMEANS_ENABLED:
        n_clusters = min(KMEANS_CONFIG["n_clusters"], len(snapshots))
        if n_clusters >= 2:
            km = KMeans(
                n_clusters=n_clusters,
                n_init=KMEANS_CONFIG["n_init"],
                random_state=KMEANS_CONFIG["random_state"],
            )
            cluster_ids = km.fit_predict(X_scaled).tolist()

    results: Dict[int, Tuple[float, bool, Optional[int]]] = {}
    for i, snap in enumerate(snapshots):
        row_id = snap["id"]
        a_score = float(anomaly_scores[i])
        is_anom = bool(labels[i] == -1)
        c_id = cluster_ids[i] if cluster_ids is not None else None
        results[row_id] = (a_score, is_anom, c_id)

    return results


# ---------------------------------------------------------------------------
# Write predictions
# ---------------------------------------------------------------------------

def write_prediction(
    conn: Any,
    *,
    system_id: str,
    anomaly_score: float,
    is_anomaly: bool,
    failure_probability: float,
    predicted_fault: str,
    cluster_id: Optional[int] = None,
) -> bool:
    """Insert one ML prediction row.  Returns True on success."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ml_predictions (
                    system_id, prediction_time,
                    anomaly_score, is_anomaly,
                    failure_probability, predicted_fault,
                    cluster_id, model_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    system_id,
                    datetime.now(timezone.utc),
                    max(0.0, min(1.0, float(anomaly_score))),
                    is_anomaly,
                    max(0.0, min(1.0, float(failure_probability))),
                    predicted_fault or "NONE",
                    cluster_id,
                    MODEL_VERSION,
                ),
            )
        conn.commit()
        return True
    except Exception as exc:
        logger.error("[write_prediction] %s: %s", system_id, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False


# ---------------------------------------------------------------------------
# Public API — called by the background thread in kafka_to_postgres
# ---------------------------------------------------------------------------

def run_cycle(conn: Any) -> int:
    """
    Execute one ML prediction cycle.

    1. Fetch the latest ML_BATCH_SIZE rows from feature_snapshots.
    2. If sklearn is available and data is sufficient → IsolationForest + KMeans.
    3. Otherwise → heuristic fallback.
    4. Write one ml_predictions row per unique system_id in the batch.

    Returns the number of predictions written.
    """
    snapshots = fetch_latest_snapshots(conn, ML_BATCH_SIZE)

    # Dev synthetic fallback
    if not snapshots and ML_SYNTHETIC_FALLBACK_ENABLED:
        logger.info("[ml_engine] No snapshots found — injecting synthetic data (dev mode).")
        _inject_synthetic_snapshots(conn)
        snapshots = fetch_latest_snapshots(conn, ML_BATCH_SIZE)

    if not snapshots:
        logger.info("[ml_engine] No feature snapshots available — skipping cycle.")
        return 0

    use_sklearn = _SKLEARN_AVAILABLE and len(snapshots) >= ML_MIN_ROWS_FOR_MODEL
    sklearn_results: Dict[int, Tuple[float, bool, Optional[int]]] = {}

    if use_sklearn:
        try:
            sklearn_results = _run_sklearn_pipeline(snapshots)
        except Exception as exc:
            logger.warning("[ml_engine] sklearn pipeline failed, falling back to heuristic: %s", exc)
            structured_log(
                "ml_engine",
                {"operation": "sklearn_pipeline", "status": "fallback", "error": str(exc)},
                log=logger,
            )
            sklearn_results = {}
    else:
        mode = "heuristic_fallback" if not use_sklearn else "sklearn"
        logger.info(
            "[ml_engine] Using %s (sklearn=%s, rows=%d, min=%d).",
            mode, _SKLEARN_AVAILABLE, len(snapshots), ML_MIN_ROWS_FOR_MODEL,
        )

    # Deduplicate: keep only the most-recent snapshot per system_id
    latest_by_system: Dict[str, Dict[str, Any]] = {}
    for snap in snapshots:
        sid = snap.get("system_id") or "unknown"
        prev = latest_by_system.get(sid)
        if prev is None or snap["snapshot_time"] > prev["snapshot_time"]:
            latest_by_system[sid] = snap

    written = 0
    for system_id, snap in latest_by_system.items():
        row_id = snap["id"]
        failure_prob = _failure_probability(snap)

        if sklearn_results and row_id in sklearn_results:
            anomaly_score, is_anom, cluster_id = sklearn_results[row_id]
            fault = "SYSTEM_FAILURE" if is_anom and snap.get("critical_count", 0) > 3 \
                    else ("SERVICE_DEGRADATION" if is_anom else "NONE")
        else:
            anomaly_score, is_anom, fault = _heuristic_score(snap)
            cluster_id = None

        if write_prediction(
            conn,
            system_id=system_id,
            anomaly_score=anomaly_score,
            is_anomaly=is_anom,
            failure_probability=failure_prob,
            predicted_fault=fault,
            cluster_id=cluster_id,
        ):
            written += 1

    if use_sklearn and sklearn_results:
        anomaly_count = sum(1 for _, is_a, _ in sklearn_results.values() if is_a)
        structured_log(
            "ml_engine",
            {
                "operation": "run_cycle",
                "status": "ok",
                "model": MODEL_VERSION,
                "snapshots_processed": len(snapshots),
                "systems_scored": len(latest_by_system),
                "predictions_written": written,
                "anomalies_detected": anomaly_count,
                "kmeans_enabled": KMEANS_ENABLED,
            },
            log=logger,
        )
    else:
        structured_log(
            "ml_engine",
            {
                "operation": "run_cycle",
                "status": "ok",
                "model": "heuristic",
                "systems_scored": len(latest_by_system),
                "predictions_written": written,
            },
            log=logger,
        )

    return written


def run_ml_engine() -> None:
    """
    Run the ML engine in an infinite loop with reconnect on DB failure.
    Intended for standalone execution (`python ml_engine.py`).
    The kafka_to_postgres background thread calls run_cycle() directly.
    """
    logger.info("Starting ML Engine standalone worker (interval=%ds)...", ML_PIPELINE_INTERVAL_SECS)

    conn, ok = retry_with_backoff(make_db_connection, label="ml_engine_connect")
    if not ok or not conn:
        logger.error("DB connection failed on startup — aborting ML engine.")
        return

    try:
        _ensure_columns(conn)
    except Exception as exc:
        logger.warning("[ml_engine] Column migration skipped: %s", exc)

    reconnect_delay = 5.0

    while True:
        try:
            if conn is None:
                raise RuntimeError("No active DB connection")
            run_cycle(conn)
            reconnect_delay = 5.0
        except Exception as exc:
            logger.error("ML engine cycle error: %s", exc)
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            logger.info("ML engine reconnecting in %.0fs...", reconnect_delay)
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, _MAX_RECONNECT_SLEEP)
            conn, ok = retry_with_backoff(make_db_connection, label="ml_engine_reconnect")
            if not ok or not conn:
                logger.error("ML engine DB reconnect failed — will retry on next cycle.")
                conn = None

        time.sleep(ML_PIPELINE_INTERVAL_SECS)


if __name__ == "__main__":
    run_ml_engine()
