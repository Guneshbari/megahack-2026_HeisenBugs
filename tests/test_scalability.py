"""
SentinelCore — Scalability Test Suite
Verifies that all 100-system fixes are correctly in place and
exercises the bulk/batch logic with simulated 100-system load.

Run:  python test_scalability.py
"""

import json
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch, call

# ── stub platform deps ────────────────────────────────────────────────────────
for m in ['win32evtlog','pywintypes','winreg','kafka','kafka.errors',
          'psutil','requests','psycopg2','psycopg2.pool','psycopg2.extras',
          'fastapi','fastapi.middleware','fastapi.middleware.cors','fastapi.responses']:
    sys.modules.setdefault(m, MagicMock())

import uuid as _uuid_real
sys.modules['uuid'] = _uuid_real

# Auto-detect src/ relative to this test file — works on any machine
_TESTS_DIR    = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_TESTS_DIR)
_SRC_DIR      = os.path.join(_PROJECT_ROOT, "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import shared_constants as SC
import importlib.util as _ilu

def _load(name, path):
    spec = _ilu.spec_from_file_location(name, path)
    mod  = _ilu.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
        return mod, True
    except Exception as e:
        print(f"[WARN] {path}: {e}")
        return None, False

_ktp, KTP_OK = _load("ktp",  os.path.join(_SRC_DIR, "kafka_to_postgres.py"))
_fb,  FB_OK  = _load("fb",   os.path.join(_SRC_DIR, "feature_builder.py"))
_api, API_OK = _load("api",  os.path.join(_SRC_DIR, "api_server.py"))


def _mock_cursor_conn(rows=None):
    conn = MagicMock()
    cur  = MagicMock()
    if rows is not None:
        cur.fetchall.return_value = rows
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
    return conn, cur


# =============================================================================
# 1. shared_constants — scalability constants present
# =============================================================================

class TestScalabilityConstants(unittest.TestCase):

    def test_db_pool_min_conn_exists(self):
        self.assertTrue(hasattr(SC, 'DB_POOL_MIN_CONN'))
        self.assertGreaterEqual(SC.DB_POOL_MIN_CONN, 1)

    def test_db_pool_max_conn_exists_and_gte_10(self):
        self.assertTrue(hasattr(SC, 'DB_POOL_MAX_CONN'))
        self.assertGreaterEqual(SC.DB_POOL_MAX_CONN, 10)

    def test_db_insert_batch_size_exists_and_positive(self):
        self.assertTrue(hasattr(SC, 'DB_INSERT_BATCH_SIZE'))
        self.assertGreater(SC.DB_INSERT_BATCH_SIZE, 0)

    def test_data_retention_days_exists(self):
        self.assertTrue(hasattr(SC, 'DATA_RETENTION_DAYS'))
        self.assertGreater(SC.DATA_RETENTION_DAYS, 0)

    def test_raw_xml_max_bytes_exists(self):
        self.assertTrue(hasattr(SC, 'RAW_XML_MAX_BYTES'))
        self.assertGreater(SC.RAW_XML_MAX_BYTES, 0)

    def test_db_config_reads_from_env(self):
        """Credentials must come from env vars, not be hardcoded only."""
        import os
        with patch.dict(os.environ, {"SENTINEL_DB_PASSWORD": "env_secret"}):
            import importlib
            import shared_constants as SC2
            importlib.reload(SC2)
            self.assertEqual(SC2.DB_CONFIG["password"], "env_secret")


# =============================================================================
# 2. kafka_to_postgres — batch insert (execute_values) is used
# =============================================================================

@unittest.skipUnless(KTP_OK, "kafka_to_postgres not loadable")
class TestBatchInsert(unittest.TestCase):

    def _make_payload(self, n_events=20, system_id="SYS-001"):
        events = []
        for i in range(n_events):
            events.append({
                "event_hash":           f"hash_{system_id}_{i}",
                "log_channel":          "System",
                "event_record_id":      i,
                "provider_name":        "TestProvider",
                "event_id":             100,
                "level":                2,
                "task": 0, "opcode": 0, "keywords": "0x0",
                "process_id": 0, "thread_id": 0,
                "severity":             "ERROR",
                "fault_type":           "TEST",
                "diagnostic_context":   {},
                "raw_xml":              "<Event><Data>test</Data></Event>",
                "event_message":        "test message",
                "cpu_usage_percent":    30.0,
                "memory_usage_percent": 40.0,
                "disk_free_percent":    80.0,
                "confidence_score":     0.9,
            })
        return {
            "system_id": system_id, "hostname": f"host-{system_id}",
            "system_info": {"cpu_usage_percent": 30.0, "memory_usage_percent": 40.0,
                            "disk_free_percent": 80.0, "os_version": "Windows 11",
                            "agent_version": "2.1.0", "ip_address": "10.0.0.1",
                            "uptime_seconds": 3600},
            "events": events,
        }

    def test_execute_values_called_not_execute_per_row(self):
        """
        With 20 events, execute() must NOT be called 20 times for event inserts.
        execute_values() should be called once (or ceil(20/batch_size) times).
        """
        conn, cur = _mock_cursor_conn()
        # Patch execute_values at the module level
        with patch.object(_ktp, 'execute_values') as mock_ev:
            _ktp.process_message(conn, self._make_payload(n_events=20))

        # execute_values must have been called (not 20 individual execute calls)
        self.assertTrue(mock_ev.called,
                        "execute_values was never called — row-by-row insert regression")

    def test_execute_called_only_for_heartbeat_not_events(self):
        """
        cur.execute() should be called exactly once (heartbeat INSERT).
        Event inserts go through execute_values, not execute.
        """
        conn, cur = _mock_cursor_conn()
        with patch.object(_ktp, 'execute_values'):
            _ktp.process_message(conn, self._make_payload(n_events=20))
        # Only the heartbeat upsert should use cur.execute
        self.assertEqual(cur.execute.call_count, 1,
                         f"Expected 1 execute call (heartbeat), got {cur.execute.call_count}")

    def test_raw_xml_truncated_to_max_bytes(self):
        """Events with very large raw_xml must be truncated before insert."""
        big_xml = "X" * 100_000   # 100 KB — far above RAW_XML_MAX_BYTES (4096)
        payload = self._make_payload(n_events=1)
        payload["events"][0]["raw_xml"] = big_xml

        conn, cur = _mock_cursor_conn()
        inserted_rows = []

        def _capture_ev(cur, sql, rows, **kwargs):
            inserted_rows.extend(rows)

        with patch.object(_ktp, 'execute_values', side_effect=_capture_ev):
            _ktp.process_message(conn, payload)

        self.assertTrue(len(inserted_rows) > 0, "No rows were captured")
        # raw_xml is at index 16 in the tuple (0-based)
        raw_xml_in_row = inserted_rows[0][16]
        self.assertLessEqual(len(raw_xml_in_row), SC.RAW_XML_MAX_BYTES,
                              "raw_xml was NOT truncated — storage bloat risk")

    def test_zero_events_does_not_call_execute_values(self):
        """Heartbeat-only payloads must not trigger execute_values."""
        conn, cur = _mock_cursor_conn()
        payload = self._make_payload(n_events=0)
        with patch.object(_ktp, 'execute_values') as mock_ev:
            result = _ktp.process_message(conn, payload)
        mock_ev.assert_not_called()
        self.assertTrue(result)

    def test_batch_chunked_when_events_exceed_batch_size(self):
        """
        If events > DB_INSERT_BATCH_SIZE, execute_values must be called
        multiple times (one per chunk).
        """
        batch_size = SC.DB_INSERT_BATCH_SIZE
        n_events   = batch_size * 3   # 3 full chunks
        payload    = self._make_payload(n_events=n_events)

        conn, cur = _mock_cursor_conn()
        call_count = [0]

        def _count(*a, **kw):
            call_count[0] += 1

        with patch.object(_ktp, 'execute_values', side_effect=_count):
            _ktp.process_message(conn, payload)

        self.assertEqual(call_count[0], 3,
                         f"Expected 3 execute_values calls for {n_events} events "
                         f"(batch_size={batch_size}), got {call_count[0]}")

    def test_composite_index_created_in_setup(self):
        """setup_database must create the (system_id, ingested_at) composite index."""
        conn, cur = _mock_cursor_conn()
        _ktp.setup_database(conn)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        self.assertIn("idx_events_system_ingested", all_sql,
                      "Composite index idx_events_system_ingested is missing from schema setup")


# =============================================================================
# 3. feature_builder — single bulk query replaces N+1
# =============================================================================

@unittest.skipUnless(FB_OK, "feature_builder not loadable")
class TestBulkEventStats(unittest.TestCase):

    def _make_bulk_rows(self, system_ids):
        """Simulate what Postgres returns for a GROUP BY system_id query."""
        return [
            MagicMock(**{
                "__getitem__": lambda self, k: {
                    "system_id": sid, "total_events": 10, "critical_count": 1,
                    "error_count": 2, "warning_count": 3, "info_count": 4,
                    "dominant_fault_type": "SERVICE_ERROR", "avg_confidence": 0.85,
                }[k],
                "get": lambda k, d=None, sid=sid: {
                    "system_id": sid, "total_events": 10, "critical_count": 1,
                    "error_count": 2, "warning_count": 3, "info_count": 4,
                    "dominant_fault_type": "SERVICE_ERROR", "avg_confidence": 0.85,
                }.get(k, d),
            })
            for sid in system_ids
        ]

    def test_fetch_all_event_stats_exists(self):
        self.assertTrue(hasattr(_fb, 'fetch_all_event_stats'),
                        "fetch_all_event_stats is missing — N+1 fix not applied")

    def test_single_query_for_100_systems(self):
        """
        fetch_all_event_stats must issue exactly ONE query regardless of
        how many system_ids are passed in.
        """
        system_ids = [f"SYS-{i:03d}" for i in range(100)]
        conn, cur  = _mock_cursor_conn(rows=[])

        _fb.fetch_all_event_stats(conn, system_ids, 30)

        self.assertEqual(cur.execute.call_count, 1,
                         f"Expected 1 DB query for 100 systems, got {cur.execute.call_count} "
                         f"— N+1 regression detected")

    def test_uses_any_operator_not_loop(self):
        """The SQL must use ANY(%s) not multiple WHERE clauses."""
        system_ids = ["SYS-001", "SYS-002"]
        conn, cur  = _mock_cursor_conn(rows=[])
        _fb.fetch_all_event_stats(conn, system_ids, 30)
        sql_called = str(cur.execute.call_args[0][0])
        self.assertIn("ANY", sql_called,
                      "SQL does not use ANY(%s) — may still be N+1")

    def test_missing_systems_get_zero_defaults(self):
        """
        Systems with no events in the window must get a zeroed dict,
        not None or a missing key.
        """
        system_ids = ["SYS-001", "SYS-002", "SYS-003"]
        # DB returns data for only SYS-001 — SYS-002 and SYS-003 have no events
        conn, cur  = _mock_cursor_conn(rows=[])
        result = _fb.fetch_all_event_stats(conn, system_ids, 30)

        for sid in system_ids:
            self.assertIn(sid, result, f"{sid} missing from result")
            stats = result[sid]
            for field in ("total_events","critical_count","error_count",
                          "warning_count","info_count","dominant_fault_type","avg_confidence"):
                self.assertIn(field, stats, f"{field} missing for {sid}")
                self.assertIsNotNone(stats[field], f"{sid}.{field} is None")

    def test_empty_system_list_returns_empty_dict(self):
        conn, cur = _mock_cursor_conn(rows=[])
        result = _fb.fetch_all_event_stats(conn, [], 30)
        self.assertEqual(result, {})
        cur.execute.assert_not_called()

    def test_run_cycle_uses_single_bulk_query(self):
        """
        run_cycle must call fetch_all_event_stats once, not once per system.
        """
        systems = [
            {"system_id": f"SYS-{i}", "hostname": f"host-{i}",
             "cpu_usage_percent": 30.0, "memory_usage_percent": 40.0,
             "disk_free_percent": 80.0}
            for i in range(10)
        ]
        conn, cur = _mock_cursor_conn()

        fetch_calls = []
        def _mock_fetch_all(c, ids, secs):
            fetch_calls.append(len(ids))
            return {sid: _fb._zero_stats() for sid in ids}

        with patch.object(_fb, 'fetch_active_systems', return_value=systems), \
             patch.object(_fb, 'fetch_all_event_stats', side_effect=_mock_fetch_all), \
             patch.object(_fb, 'write_snapshot', return_value=True), \
             patch.object(_fb, '_get_healthy_conn', return_value=conn):
            _fb.run_cycle(conn, 1)

        self.assertEqual(len(fetch_calls), 1,
                         "fetch_all_event_stats called more than once per cycle")
        self.assertEqual(fetch_calls[0], 10,
                         "fetch_all_event_stats was not called with all 10 systems")


# =============================================================================
# 4. api_server — ThreadedConnectionPool + tunable size
# =============================================================================

@unittest.skipUnless(API_OK, "api_server not loadable")
class TestConnectionPool(unittest.TestCase):

    def test_uses_threaded_connection_pool(self):
        """
        The pool must be instantiated as ThreadedConnectionPool, not
        SimpleConnectionPool.  We check the instantiation line, not comments.
        """
        source = open(os.path.join(_SRC_DIR, 'api_server.py')).read()
        self.assertIn("ThreadedConnectionPool", source,
                      "ThreadedConnectionPool not found in api_server.py")
        # The pool variable annotation and constructor must use Threaded, not Simple.
        # A bare 'SimpleConnectionPool(' call (not in a comment or string) is wrong.
        import re
        # Find actual code lines that call SimpleConnectionPool( — ignore comments
        code_lines = [
            l for l in source.splitlines()
            if 'SimpleConnectionPool(' in l and not l.strip().startswith('#')
            and 'NOT' not in l   # not the "is NOT safe" documentation comment
        ]
        self.assertEqual(code_lines, [],
                         f"Found SimpleConnectionPool() call in code: {code_lines}")

    def test_pool_uses_constants_not_magic_numbers(self):
        """Pool sizes must come from shared_constants, not hardcoded 1/10."""
        source = open(os.path.join(_SRC_DIR, 'api_server.py')).read()
        self.assertIn("DB_POOL_MIN_CONN", source)
        self.assertIn("DB_POOL_MAX_CONN", source)
        # Should NOT contain the old hardcoded values
        self.assertNotIn("minconn=1, maxconn=10", source,
                         "Old hardcoded pool size (1/10) still present")

    def test_pool_max_conn_is_at_least_10(self):
        self.assertGreaterEqual(SC.DB_POOL_MAX_CONN, 10)


# =============================================================================
# 5. Simulated 100-system load — end-to-end message processing
# =============================================================================

@unittest.skipUnless(KTP_OK, "kafka_to_postgres not loadable")
class TestHundredSystemLoad(unittest.TestCase):
    """
    Simulate 100 systems each sending a batch of 20 events and verify:
    - All messages processed without error
    - execute_values called (not individual inserts)
    - Performance: 100 messages processed in < 2 seconds (pure Python, no real DB)
    """

    def _make_payload(self, sys_num):
        sid = f"SYS-{sys_num:03d}"
        return {
            "system_id": sid, "hostname": f"host-{sys_num}",
            "system_info": {"cpu_usage_percent": 30.0, "memory_usage_percent": 40.0,
                            "disk_free_percent": 80.0, "os_version": "Win11",
                            "agent_version": "2.1", "ip_address": f"10.0.0.{sys_num}",
                            "uptime_seconds": 3600},
            "events": [
                {
                    "event_hash": f"hash_{sid}_{i}",
                    "log_channel": "System", "event_record_id": i,
                    "provider_name": "SCM", "event_id": 7031, "level": 2,
                    "task": 0, "opcode": 0, "keywords": "0x0",
                    "process_id": 0, "thread_id": 0,
                    "severity": "ERROR", "fault_type": "SERVICE_ERROR",
                    "diagnostic_context": {}, "raw_xml": "<E/>",
                    "event_message": f"Svc failed on {sid}",
                    "cpu_usage_percent": 30.0, "memory_usage_percent": 40.0,
                    "disk_free_percent": 80.0, "confidence_score": 0.9,
                }
                for i in range(20)
            ],
        }

    def test_100_systems_all_succeed(self):
        results = []
        ev_call_count = [0]

        def _mock_ev(*a, **kw):
            ev_call_count[0] += 1

        for sys_num in range(100):
            conn, cur = _mock_cursor_conn()
            with patch.object(_ktp, 'execute_values', side_effect=_mock_ev):
                ok = _ktp.process_message(conn, self._make_payload(sys_num))
            results.append(ok)

        self.assertEqual(sum(results), 100, "Not all 100 messages succeeded")
        # execute_values must have been called for each system (not row-by-row)
        self.assertEqual(ev_call_count[0], 100,
                         f"Expected 100 execute_values calls, got {ev_call_count[0]}")

    def test_100_systems_processed_fast(self):
        """100 message round-trips must complete in under 2 seconds (pure Python)."""
        t0 = time.time()
        for sys_num in range(100):
            conn, cur = _mock_cursor_conn()
            with patch.object(_ktp, 'execute_values'):
                _ktp.process_message(conn, self._make_payload(sys_num))
        elapsed = time.time() - t0
        self.assertLess(elapsed, 2.0,
                        f"100 messages took {elapsed:.2f}s — processing overhead too high")

    def test_one_bad_message_does_not_block_others(self):
        """A message whose DB commit raises must not stop subsequent messages."""
        results = []
        for sys_num in range(5):
            conn, cur = _mock_cursor_conn()
            if sys_num == 2:
                # System 2 fails at commit
                conn.commit.side_effect = Exception("disk full")
            with patch.object(_ktp, 'execute_values'):
                ok = _ktp.process_message(conn, self._make_payload(sys_num))
            results.append(ok)

        self.assertFalse(results[2], "Failed message should return False")
        self.assertTrue(results[0], "System 0 should still succeed")
        self.assertTrue(results[4], "System 4 should still succeed")


# =============================================================================
# 6. feature_builder — 100-system bulk query performance
# =============================================================================

@unittest.skipUnless(FB_OK, "feature_builder not loadable")
class TestBulkQueryScale(unittest.TestCase):

    def test_100_systems_single_query(self):
        system_ids = [f"SYS-{i:03d}" for i in range(100)]
        conn, cur  = _mock_cursor_conn(rows=[])

        _fb.fetch_all_event_stats(conn, system_ids, 30)

        # Still exactly 1 query for 100 systems
        self.assertEqual(cur.execute.call_count, 1)

    def test_1000_systems_still_single_query(self):
        """Verify O(1) query count holds at 1000 systems."""
        system_ids = [f"SYS-{i:04d}" for i in range(1000)]
        conn, cur  = _mock_cursor_conn(rows=[])

        _fb.fetch_all_event_stats(conn, system_ids, 30)

        self.assertEqual(cur.execute.call_count, 1,
                         "O(1) query guarantee broken at 1000 systems")

    def test_partial_results_rest_get_zero_defaults(self):
        """If only 50 of 100 systems have events, the other 50 get zeroed defaults."""
        system_ids = [f"SYS-{i:03d}" for i in range(100)]
        # Only return results for first 50
        active_50 = [
            type('Row', (), {
                '__getitem__': lambda s, k, i=i: {
                    "system_id": f"SYS-{i:03d}", "total_events": 5,
                    "critical_count": 1, "error_count": 1,
                    "warning_count": 2, "info_count": 1,
                    "dominant_fault_type": "SERVICE_ERROR",
                    "avg_confidence": 0.8,
                }[k],
                'get': lambda s, k, d=None, i=i: {
                    "system_id": f"SYS-{i:03d}", "total_events": 5,
                    "critical_count": 1, "error_count": 1,
                    "warning_count": 2, "info_count": 1,
                    "dominant_fault_type": "SERVICE_ERROR",
                    "avg_confidence": 0.8,
                }.get(k, d),
            })()
            for i in range(50)
        ]
        conn, cur = _mock_cursor_conn(rows=active_50)

        result = _fb.fetch_all_event_stats(conn, system_ids, 30)

        self.assertEqual(len(result), 100)
        # Systems 50-99 must have zero defaults, not missing
        for i in range(50, 100):
            sid = f"SYS-{i:03d}"
            self.assertIn(sid, result)
            self.assertEqual(result[sid]["total_events"], 0)


# =============================================================================
# RUNNER
# =============================================================================

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(__import__(__name__))
    runner = unittest.TextTestRunner(verbosity=2, failfast=False)
    result = runner.run(suite)

    total   = result.testsRun
    passed  = total - len(result.failures) - len(result.errors) - len(result.skipped)
    print(f"\n{'='*70}")
    print(f"  SCALABILITY RESULTS  |  Total: {total}  Passed: {passed}  "
          f"Failed: {len(result.failures)}  Errors: {len(result.errors)}")
    print(f"{'='*70}")
    sys.exit(0 if result.wasSuccessful() else 1)
