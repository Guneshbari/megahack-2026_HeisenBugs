"""
SentinelCore — Shared Constants
Single source of truth for all configuration values used across:
  collector, analyzer, api_server, kafka_to_postgres, feature_builder
"""

# ============================================================================
# SEVERITY LEVEL NAMES
# ============================================================================

LEVEL_NAMES = {1: 'CRITICAL', 2: 'ERROR', 3: 'WARNING', 4: 'INFO', 5: 'VERBOSE'}

# ============================================================================
# RESOURCE ALERT THRESHOLDS
# ============================================================================

CPU_ALERT_THRESHOLD    = 90   # percent
MEMORY_ALERT_THRESHOLD = 90   # percent
DISK_LOW_THRESHOLD     = 10   # percent free

# ============================================================================
# RELIABILITY & TIMEOUT CONSTANTS
# ============================================================================

RETRY_MAX_ATTEMPTS           = 3    # max retries for transient failures
RETRY_BACKOFF_SECONDS        = 2.0  # base backoff in seconds (doubles per attempt)
DB_QUERY_TIMEOUT_SECONDS     = 5    # hard limit on any single DB query
API_RESPONSE_TIMEOUT_SECONDS = 3    # hard limit on any API handler
CIRCUIT_BREAKER_THRESHOLD    = 5    # consecutive failures before circuit opens
CIRCUIT_BREAKER_RESET_SECS   = 30.0 # seconds before circuit half-opens

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    "dbname":          "sentinel_logs",
    "user":            "sentinel_admin",
    "password":        "changeme123",
    "host":            "localhost",
    "port":            5432,
    "connect_timeout": DB_QUERY_TIMEOUT_SECONDS,
    # Enforce per-statement timeout at Postgres level (value in ms)
    "options":         f"-c statement_timeout={DB_QUERY_TIMEOUT_SECONDS * 1000}",
}