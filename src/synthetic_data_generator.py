"""
SentinelCore - Synthetic Data Generator.

Populates feature_snapshots with realistic simulated telemetry so the ML
pipeline (ml_engine.py) can be tested without a live Kafka feed.

Usage:
    python src/synthetic_data_generator.py [--rows 200] [--systems 3]

Constraints:
  - Does NOT modify collector.py, kafka_to_postgres.py, or any pipeline code
  - Uses get_db_config() for DB connection (matches all other services)
  - Uses psycopg2 only — no heavyweight dependencies
  - ~10 % of rows are deliberately anomalous for meaningful ML output
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 is required.  Install it with: pip install psycopg2-binary")
    sys.exit(1)

# Use the shared config so this script respects the same env vars as every
# other service (SENTINEL_DB_HOST, SENTINEL_DB_NAME, etc.)
from shared.db_constants import get_db_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("synthetic_data_generator")


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------

def _generate_row(
    system_id: str,
    snapshot_time: datetime,
    rng: random.Random,
    force_anomaly: bool = False,
) -> Tuple:
    """Build one feature_snapshots tuple."""
    if force_anomaly:
        # High-stress anomalous profile
        cpu   = rng.uniform(88.0, 99.5)
        mem   = rng.uniform(88.0, 99.5)
        disk  = rng.uniform(2.0, 12.0)       # very low free space
        err   = rng.randint(5, 12)
        crit  = rng.randint(3, 6)
    else:
        # Normal operating profile
        cpu   = rng.uniform(5.0, 72.0)
        mem   = rng.uniform(15.0, 78.0)
        disk  = rng.uniform(25.0, 92.0)
        err   = rng.randint(0, 3)
        crit  = rng.randint(0, 1)

    # Derived stats
    warning_count = rng.randint(0, max(0, err * 2))
    info_count    = rng.randint(5, 50)
    total_events  = err + crit + warning_count + info_count
    avg_conf      = round(rng.uniform(0.55, 0.95), 2)
    fault_types   = ["NONE", "CPU_SPIKE", "MEMORY_PRESSURE", "DISK_FULL", "NETWORK_ERROR"]
    fault_type    = "NONE" if crit == 0 else rng.choice(fault_types[1:])

    return (
        system_id,
        snapshot_time,
        round(cpu, 2),
        round(mem, 2),
        round(disk, 2),
        total_events,
        crit,
        err,
        warning_count,
        info_count,
        fault_type,
        avg_conf,
    )


def generate_rows(
    system_ids: List[str],
    total_rows: int,
    anomaly_rate: float = 0.10,
    rng: random.Random | None = None,
) -> List[Tuple]:
    """
    Generate *total_rows* feature_snapshot tuples spread evenly across
    *system_ids*.  ~*anomaly_rate* fraction are anomalous.
    Timestamps spread over the last 6 hours so ML sees a time series.
    """
    rng = rng or random.Random(42)
    now = datetime.now(timezone.utc)
    rows: List[Tuple] = []

    for i in range(total_rows):
        system_id    = system_ids[i % len(system_ids)]
        # Spread evenly backwards in time — newest row first
        offset_secs  = (total_rows - i) * (6 * 3600 / total_rows)
        snapshot_time = now - timedelta(seconds=offset_secs)
        force_anomaly = (i % round(1 / anomaly_rate) == 0)
        rows.append(_generate_row(system_id, snapshot_time, rng, force_anomaly))

    return rows


# ---------------------------------------------------------------------------
# DB insert
# ---------------------------------------------------------------------------

INSERT_SQL = """
INSERT INTO feature_snapshots (
    system_id, snapshot_time,
    cpu_usage_percent, memory_usage_percent, disk_free_percent,
    total_events, critical_count, error_count, warning_count, info_count,
    dominant_fault_type, avg_confidence
) VALUES %s
"""


def ensure_table(conn: "psycopg2.extensions.connection") -> None:
    """Create feature_snapshots if it doesn't exist yet (dev convenience)."""
    with conn.cursor() as cur:
        cur.execute("""
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
        """)
    conn.commit()


def insert_rows(conn: "psycopg2.extensions.connection", rows: List[Tuple]) -> None:
    """Bulk-insert all rows in a single round-trip."""
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=200)
    conn.commit()


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(rows: List[Tuple]) -> None:
    anomalies = sum(1 for r in rows if r[6] >= 3 or r[2] >= 88)  # critical_count or cpu
    print(f"\n{'=' * 55}")
    print(f"  Synthetic Data Generator — Summary")
    print(f"{'=' * 55}")
    print(f"  Total rows inserted : {len(rows)}")
    print(f"  Anomalous rows      : {anomalies} ({anomalies / len(rows) * 100:.1f} %)")
    print(f"  Normal rows         : {len(rows) - anomalies}")
    print(f"{'=' * 55}")
    print(f"\n  Next steps:")
    print(f"    1. python src/ml_engine.py   # run a prediction cycle")
    print(f"    2. Check ml_predictions table for anomaly_score + is_anomaly")
    print(f"    3. GET /ml/anomalies?only_anomalies=true  via API")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Insert synthetic feature_snapshots rows for ML pipeline testing."
    )
    parser.add_argument(
        "--rows", type=int, default=200,
        help="Number of rows to generate (default: 200)",
    )
    parser.add_argument(
        "--systems", type=int, default=3,
        help="Number of synthetic system IDs (default: 3)",
    )
    parser.add_argument(
        "--anomaly-rate", type=float, default=0.10,
        help="Fraction of anomalous rows (default: 0.10 = 10 %%)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()

    system_ids = [f"synthetic-sys-{i + 1:03d}" for i in range(args.systems)]

    logger.info(
        "Generating %d rows across %d systems (anomaly_rate=%.0f %%, seed=%d)…",
        args.rows, args.systems, args.anomaly_rate * 100, args.seed,
    )

    rng = random.Random(args.seed)
    rows = generate_rows(system_ids, args.rows, args.anomaly_rate, rng)

    logger.info("Connecting to PostgreSQL via get_db_config()…")
    try:
        conn = psycopg2.connect(**get_db_config())
    except Exception as exc:
        logger.error("DB connection failed: %s", exc)
        logger.error(
            "Hint: set SENTINEL_DB_HOST, SENTINEL_DB_NAME, SENTINEL_DB_USER, "
            "SENTINEL_DB_PASSWORD environment variables or update .env"
        )
        sys.exit(1)

    try:
        ensure_table(conn)
        insert_rows(conn, rows)
        logger.info("Inserted %d rows into feature_snapshots.", len(rows))
        print_summary(rows)
    except Exception as exc:
        logger.error("Insert failed: %s", exc)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
