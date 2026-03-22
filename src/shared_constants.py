"""Shared configuration and constants for SentinelCore services."""

from __future__ import annotations

import os


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable with a safe fallback."""
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    """Read a float environment variable with a safe fallback."""
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def env_bool(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def env_csv(name: str, default: str) -> list[str]:
    """Read a comma-separated environment variable into a list."""
    raw_value = os.getenv(name, default)
    return [item.strip() for item in raw_value.split(",") if item.strip()]


# ============================================================================
# SEVERITY LEVEL NAMES
# ============================================================================

LEVEL_NAMES = {1: "CRITICAL", 2: "ERROR", 3: "WARNING", 4: "INFO", 5: "VERBOSE"}

# ============================================================================
# RESOURCE ALERT THRESHOLDS
# ============================================================================

CPU_ALERT_THRESHOLD = env_int("SENTINEL_CPU_ALERT_THRESHOLD", 90)
MEMORY_ALERT_THRESHOLD = env_int("SENTINEL_MEMORY_ALERT_THRESHOLD", 90)
DISK_LOW_THRESHOLD = env_int("SENTINEL_DISK_LOW_THRESHOLD", 10)

# ============================================================================
# RELIABILITY & TIMEOUT CONSTANTS
# ============================================================================

RETRY_MAX_ATTEMPTS = env_int("SENTINEL_RETRY_MAX_ATTEMPTS", 3)
RETRY_BACKOFF_SECONDS = env_float("SENTINEL_RETRY_BACKOFF_SECONDS", 2.0)
DB_QUERY_TIMEOUT_SECONDS = env_int("SENTINEL_DB_QUERY_TIMEOUT_SECONDS", 5)
API_RESPONSE_TIMEOUT_SECONDS = env_int("SENTINEL_API_RESPONSE_TIMEOUT_SECONDS", 3)
CIRCUIT_BREAKER_THRESHOLD = env_int("SENTINEL_CIRCUIT_BREAKER_THRESHOLD", 5)
CIRCUIT_BREAKER_RESET_SECS = env_float("SENTINEL_CIRCUIT_BREAKER_RESET_SECS", 30.0)

# ============================================================================
# DATABASE AND INGESTION SCALING
# ============================================================================

DB_POOL_MIN_CONN = env_int("SENTINEL_DB_POOL_MIN", 2)
DB_POOL_MAX_CONN = env_int("SENTINEL_DB_POOL_MAX", 20)
DB_INSERT_BATCH_SIZE = env_int("SENTINEL_INSERT_BATCH_SIZE", 200)
DATA_RETENTION_DAYS = env_int("SENTINEL_RETENTION_DAYS", 30)
RAW_XML_MAX_BYTES = env_int("SENTINEL_RAW_XML_MAX_BYTES", 4096)

RETENTION_CLEANUP_ENABLED = env_bool("SENTINEL_RETENTION_CLEANUP_ENABLED", True)
RETENTION_CLEANUP_INTERVAL_SECS = env_int("SENTINEL_RETENTION_CLEANUP_INTERVAL_SECS", 900)
RETENTION_DELETE_BATCH_SIZE = env_int("SENTINEL_RETENTION_DELETE_BATCH_SIZE", 1000)

EVENT_PARTITIONING_ENABLED = env_bool("SENTINEL_EVENT_PARTITIONING_ENABLED", False)
EVENT_PARTITION_MONTHS_AHEAD = env_int("SENTINEL_EVENT_PARTITION_MONTHS_AHEAD", 2)
EVENT_PARTITION_MONTHS_BEHIND = env_int("SENTINEL_EVENT_PARTITION_MONTHS_BEHIND", 1)
EVENT_SHARD_KEY = os.getenv("SENTINEL_EVENT_SHARD_KEY", "system_id")

# ============================================================================
# KAFKA / CONSUMER CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = (
    os.getenv("SENTINEL_KAFKA_BROKERS")
    or os.getenv("SENTINEL_KAFKA_HOST")
    or os.getenv("KAFKA_BOOTSTRAP")
    or "kafka:9092"
)
KAFKA_TOPIC = os.getenv("SENTINEL_KAFKA_TOPIC", "sentinel-events")
KAFKA_GROUP_ID = os.getenv("SENTINEL_KAFKA_GROUP_ID", "postgres-ingester-group")
KAFKA_CONSUMER_CLIENT_ID = os.getenv("SENTINEL_KAFKA_CONSUMER_CLIENT_ID", "sentinel-consumer")
KAFKA_POLL_TIMEOUT_MS = env_int("SENTINEL_KAFKA_POLL_TIMEOUT_MS", 5000)
KAFKA_MAX_POLL_RECORDS = env_int("SENTINEL_KAFKA_MAX_POLL_RECORDS", 50)
KAFKA_MIN_TOPIC_PARTITIONS = env_int("SENTINEL_KAFKA_MIN_TOPIC_PARTITIONS", 3)
KAFKA_TOPIC_REPLICATION_FACTOR = env_int("SENTINEL_KAFKA_TOPIC_REPLICATION_FACTOR", 1)
KAFKA_LAG_LOG_INTERVAL_SECS = env_int("SENTINEL_KAFKA_LAG_LOG_INTERVAL_SECS", 30)
KAFKA_LAG_WARNING_THRESHOLD = env_int("SENTINEL_KAFKA_LAG_WARNING_THRESHOLD", 250)

CONSUMER_DB_SLOW_MS = env_int("SENTINEL_CONSUMER_DB_SLOW_MS", 1500)
CONSUMER_DB_BACKOFF_SECS = env_float("SENTINEL_CONSUMER_DB_BACKOFF_SECS", 2.0)

# ============================================================================
# FEATURE BUILDER CONFIGURATION
# ============================================================================

FEATURE_BUILDER_INTERVAL_SECS = env_int("SENTINEL_FEATURE_BUILDER_INTERVAL_SECS", 30)
FEATURE_BUILDER_LOOKBACK_SECS = env_int("SENTINEL_FEATURE_BUILDER_LOOKBACK_SECS", 30)
FEATURE_BUILDER_HARD_TIMEOUT_SECS = env_int("SENTINEL_FEATURE_BUILDER_HARD_TIMEOUT_SECS", 25)
FEATURE_BUILDER_BATCH_TIMEOUT_SECS = env_int("SENTINEL_FEATURE_BUILDER_BATCH_TIMEOUT_SECS", 10)
FEATURE_BUILDER_THREAD_WORKERS = env_int("SENTINEL_FEATURE_BUILDER_THREADS", 4)
FEATURE_BUILDER_SYSTEM_BATCH_SIZE = env_int("SENTINEL_FEATURE_BUILDER_SYSTEM_BATCH_SIZE", 25)

# ============================================================================
# API CONFIGURATION
# ============================================================================

API_CACHE_TTL_SECONDS = env_int("SENTINEL_API_CACHE_TTL_SECONDS", 5)
API_MAX_EVENTS_LIMIT = env_int("SENTINEL_API_MAX_EVENTS_LIMIT", 1000)
API_CORS_ALLOWED_ORIGINS = env_csv("SENTINEL_API_CORS_ALLOWED_ORIGINS", "http://localhost:5173")

# Firebase Admin SDK auth integration
# Set SENTINEL_FIREBASE_AUTH_ENABLED=true and SENTINEL_FIREBASE_SERVICE_ACCOUNT_PATH=/path/to/key.json
FIREBASE_AUTH_ENABLED = env_bool("SENTINEL_FIREBASE_AUTH_ENABLED", False)

# ============================================================================
# COLLECTOR CONFIGURATION
# ============================================================================

COLLECTOR_BASE_BATCH_SIZE = env_int("SENTINEL_COLLECTOR_BATCH_SIZE", 20)
COLLECTOR_MAX_BATCH_SIZE = env_int("SENTINEL_COLLECTOR_MAX_BATCH_SIZE", 100)
COLLECTOR_INTERVAL_SECONDS = env_int("SENTINEL_COLLECTION_INTERVAL_SECONDS", 30)
COLLECTOR_DYNAMIC_BATCHING_ENABLED = env_bool("SENTINEL_COLLECTOR_DYNAMIC_BATCHING_ENABLED", True)

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    "dbname": os.getenv("SENTINEL_DB_NAME", "sentinel_logs"),
    "user": os.getenv("SENTINEL_DB_USER", "sentinel_admin"),
    # No hardcoded default — empty string triggers startup warning and connection failure
    "password": os.getenv("SENTINEL_DB_PASSWORD", ""),
    "host": os.getenv("SENTINEL_DB_HOST", "postgres"),
    "port": env_int("SENTINEL_DB_PORT", 5432),
    "connect_timeout": DB_QUERY_TIMEOUT_SECONDS,
    "options": f"-c statement_timeout={DB_QUERY_TIMEOUT_SECONDS * 1000}",
}
