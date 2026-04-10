from shared.system_constants import env_bool, env_float, env_int

# ── Alert thresholds ────────────────────────────────────────────────────────
CPU_ALERT_THRESHOLD = env_int("SENTINEL_CPU_ALERT_THRESHOLD", 90)
MEMORY_ALERT_THRESHOLD = env_int("SENTINEL_MEMORY_ALERT_THRESHOLD", 90)
DISK_LOW_THRESHOLD = env_int("SENTINEL_DISK_LOW_THRESHOLD", 10)

# ── Feature builder scheduler ───────────────────────────────────────────────
FEATURE_BUILDER_INTERVAL_SECS = env_int("SENTINEL_FEATURE_BUILDER_INTERVAL_SECS", 30)
FEATURE_BUILDER_LOOKBACK_SECS = env_int("SENTINEL_FEATURE_BUILDER_LOOKBACK_SECS", 30)
FEATURE_BUILDER_HARD_TIMEOUT_SECS = env_int("SENTINEL_FEATURE_BUILDER_HARD_TIMEOUT_SECS", 25)
FEATURE_BUILDER_BATCH_TIMEOUT_SECS = env_int("SENTINEL_FEATURE_BUILDER_BATCH_TIMEOUT_SECS", 10)
FEATURE_BUILDER_THREAD_WORKERS = env_int("SENTINEL_FEATURE_BUILDER_THREADS", 4)
FEATURE_BUILDER_SYSTEM_BATCH_SIZE = env_int("SENTINEL_FEATURE_BUILDER_SYSTEM_BATCH_SIZE", 25)

# ── ML pipeline scheduler (runs inside kafka_to_postgres background thread) ─
ML_PIPELINE_INTERVAL_SECS = env_int("SENTINEL_ML_PIPELINE_INTERVAL_SECS", 60)

# ── Feature matrix ──────────────────────────────────────────────────────────
# Column names that must exist in feature_snapshots before ML can run.
# Disk is stored as *free* percent, so high values are healthy (not anomalous).
FEATURE_COLUMNS: list[str] = [
    "cpu_usage_percent",
    "memory_usage_percent",
    "disk_free_percent",
    "error_count",
    "critical_count",
]

# How many of the most-recent feature_snapshots rows to pull for training.
ML_BATCH_SIZE = env_int("SENTINEL_ML_BATCH_SIZE", 100)

# Require at least this many rows before running sklearn models.
# Falls back to heuristic scoring when fewer rows exist.
ML_MIN_ROWS_FOR_MODEL = env_int("SENTINEL_ML_MIN_ROWS", 10)

# ── Isolation Forest config ─────────────────────────────────────────────────
ISOLATION_FOREST_CONFIG: dict = {
    "n_estimators":  env_int("SENTINEL_IF_N_ESTIMATORS", 100),
    "contamination": env_float("SENTINEL_IF_CONTAMINATION", 0.05),
    "random_state":  env_int("SENTINEL_IF_RANDOM_STATE", 42),
    "n_jobs":        1,   # keep single-threaded — shared host with consumer
}

# ── KMeans config ───────────────────────────────────────────────────────────
KMEANS_ENABLED = env_bool("SENTINEL_KMEANS_ENABLED", True)
KMEANS_CONFIG: dict = {
    "n_clusters":   env_int("SENTINEL_KMEANS_N_CLUSTERS", 3),
    "n_init":       env_int("SENTINEL_KMEANS_N_INIT", 10),
    "random_state": env_int("SENTINEL_KMEANS_RANDOM_STATE", 42),
}

# ── Development / synthetic data ────────────────────────────────────────────
# When True and feature_snapshots is empty, ml_engine generates synthetic rows
# so the pipeline can be tested without a live Kafka feed.
ML_SYNTHETIC_FALLBACK_ENABLED = env_bool("SENTINEL_ML_SYNTHETIC_FALLBACK", False)
