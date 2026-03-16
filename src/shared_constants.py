"""
SentinelCore — Shared Constants
Centralizes values used by multiple modules (collector, analyzer, api_server, kafka_to_postgres).
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
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    "dbname": "sentinel_logs",
    "user": "sentinel_admin",
    "password": "changeme123",
    "host": "localhost",
    "port": 5432,
}
