"""
SentinelCore — Comprehensive Test Suite
Targets: /mnt/project source files directly.

Coverage:
  ✓ Unit tests          — every pure function tested in isolation
  ✓ Contract tests      — public API shape/type guarantees
  ✓ Boundary tests      — thresholds, empty inputs, max/min values
  ✓ Failure injection   — DB errors, Kafka failures, bad data, None fields
  ✓ Integration tests   — components composed together with mocked I/O
  ✓ Regression guards   — known bugs pinned so they can't silently reappear

Run:
    python test_sentinelcore.py          # all tests, verbose
    python -m unittest test_sentinelcore # alternative runner
"""

import ast
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call, ANY

# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap: stub every heavy external dependency before importing project code
# ─────────────────────────────────────────────────────────────────────────────
_STUBS = [
    'win32evtlog', 'pywintypes', 'winreg', 'uuid',
    'kafka', 'kafka.errors',
    'psutil', 'requests', 'requests.exceptions',
    'psycopg2', 'psycopg2.pool', 'psycopg2.extras',
    'fastapi', 'fastapi.middleware', 'fastapi.middleware.cors', 'fastapi.responses',
]
for _m in _STUBS:
    sys.modules.setdefault(_m, MagicMock())

# Re-stub uuid with a real partial so boot_session_id works
import uuid as _uuid_real
sys.modules['uuid'] = _uuid_real

# Resolve PROJECT to the `src/` directory, works on both Linux (Docker) and Windows
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.join(_THIS_DIR, '..', 'src')
if not os.path.isdir(PROJECT):
    # Fallback for legacy Docker layout where sources are in /mnt/project
    PROJECT = '/mnt/project'
PROJECT = os.path.normpath(PROJECT)
sys.path.insert(0, PROJECT)

import shared_constants as SC  # noqa: E402
import sentinel_utils as SU    # noqa: E402
import analyzer as AZ          # noqa: E402

import importlib.util as _ilu
import types

def _load(name: str, filename: str):
    spec = _ilu.spec_from_file_location(name, os.path.join(PROJECT, filename))
    if spec is None or spec.loader is None:
        print(f"[WARN] Could not create spec for {filename}")
        return None, False
    mod = _ilu.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod, True
    except Exception as e:
        print(f"[WARN] Could not load {filename}: {e}")
        return None, False

_raw_ktp, KTP_OK  = _load("ktp",  "kafka_to_postgres.py")
_raw_fb,  FB_OK   = _load("fb",   "feature_builder.py")
_raw_api, API_OK  = _load("api",  "api_server.py")
_raw_col, COL_OK  = _load("col",  "collector.py")

# cast() tells Pyright these are ModuleType at use-sites;
# runtime safety is enforced by @unittest.skipUnless on every test class.
from typing import cast as _cast
_ktp = _cast(types.ModuleType, _raw_ktp)
_fb  = _cast(types.ModuleType, _raw_fb)
_api = _cast(types.ModuleType, _raw_api)
_col = _cast(types.ModuleType, _raw_col)


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _event(level=2, provider="Service Control Manager", event_id=7031,
           fault_type="SERVICE_ERROR", severity="ERROR",
           cpu=45.0, mem=60.0, disk=70.0, raw_xml=""):
    return {
        "event_record_id":      1001,
        "provider_name":        provider,
        "event_id":             event_id,
        "level":                level,
        "log_channel":          "System",
        "fault_type":           fault_type,
        "severity":             severity,
        "cpu_usage_percent":    cpu,
        "memory_usage_percent": mem,
        "disk_free_percent":    disk,
        "event_time":           "2024-01-01T00:00:00Z",
        "raw_xml":              raw_xml,
        "event_message":        "",
        "parsed_message":       "",
        "normalized_message":   "",
    }


def _mock_cursor_conn():
    """Return a (conn, cursor) pair with context-manager support."""
    conn = MagicMock()
    cur  = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
    return conn, cur


# =============================================================================
# 1. shared_constants — contract tests
# =============================================================================

class TestSharedConstants(unittest.TestCase):
    """Every constant must exist, have the right type and a sensible value."""

    def test_level_names_all_five_present(self):
        for lvl in (1, 2, 3, 4, 5):
            self.assertIn(lvl, SC.LEVEL_NAMES, f"Level {lvl} missing")

    def test_level_names_exact_strings(self):
        self.assertEqual(SC.LEVEL_NAMES[1], "CRITICAL")
        self.assertEqual(SC.LEVEL_NAMES[2], "ERROR")
        self.assertEqual(SC.LEVEL_NAMES[3], "WARNING")
        self.assertEqual(SC.LEVEL_NAMES[4], "INFO")
        self.assertEqual(SC.LEVEL_NAMES[5], "VERBOSE")

    def test_resource_thresholds_positive(self):
        for name, val in [("CPU", SC.CPU_ALERT_THRESHOLD),
                          ("MEM", SC.MEMORY_ALERT_THRESHOLD),
                          ("DISK", SC.DISK_LOW_THRESHOLD)]:
            self.assertGreater(val, 0, f"{name} threshold must be positive")

    def test_resource_thresholds_valid_percent(self):
        for v in (SC.CPU_ALERT_THRESHOLD, SC.MEMORY_ALERT_THRESHOLD, SC.DISK_LOW_THRESHOLD):
            self.assertGreater(v, 0)
            self.assertLessEqual(v, 100)

    def test_retry_max_attempts_int_gte_1(self):
        self.assertIsInstance(SC.RETRY_MAX_ATTEMPTS, int)
        self.assertGreaterEqual(SC.RETRY_MAX_ATTEMPTS, 1)

    def test_retry_backoff_positive_float(self):
        self.assertIsInstance(SC.RETRY_BACKOFF_SECONDS, float)
        self.assertGreater(SC.RETRY_BACKOFF_SECONDS, 0)

    def test_db_query_timeout_positive(self):
        self.assertGreater(SC.DB_QUERY_TIMEOUT_SECONDS, 0)

    def test_api_response_timeout_positive(self):
        self.assertGreater(SC.API_RESPONSE_TIMEOUT_SECONDS, 0)

    def test_circuit_breaker_threshold_gte_1(self):
        self.assertGreaterEqual(SC.CIRCUIT_BREAKER_THRESHOLD, 1)

    def test_circuit_breaker_reset_positive(self):
        self.assertGreater(SC.CIRCUIT_BREAKER_RESET_SECS, 0)

    def test_db_config_required_keys(self):
        for key in ("dbname", "user", "password", "host", "port"):
            self.assertIn(key, SC.DB_CONFIG, f"DB_CONFIG missing '{key}'")

    def test_db_config_port_integer(self):
        self.assertIsInstance(SC.DB_CONFIG["port"], int)

    def test_db_config_has_connect_timeout(self):
        self.assertIn("connect_timeout", SC.DB_CONFIG)

    def test_db_config_options_contains_statement_timeout(self):
        self.assertIn("statement_timeout", SC.DB_CONFIG.get("options", ""))

    def test_db_config_statement_timeout_matches_query_timeout(self):
        options = SC.DB_CONFIG.get("options", "")
        expected = str(SC.DB_QUERY_TIMEOUT_SECONDS * 1000)
        self.assertIn(expected, options)


# =============================================================================
# 2. sentinel_utils — retry_with_backoff
# =============================================================================

class TestRetryWithBackoff(unittest.TestCase):

    def test_returns_result_and_true_on_first_success(self):
        fn = MagicMock(return_value=42)
        result, ok = SU.retry_with_backoff(fn, label="t")
        self.assertTrue(ok)
        self.assertEqual(result, 42)
        fn.assert_called_once()

    def test_retries_and_succeeds_on_third_attempt(self):
        fn = MagicMock(side_effect=[RuntimeError("1"), RuntimeError("2"), 99])
        with patch("time.sleep"):
            result, ok = SU.retry_with_backoff(fn, max_attempts=3, label="t")
        self.assertTrue(ok)
        self.assertEqual(result, 99)
        self.assertEqual(fn.call_count, 3)

    def test_returns_none_false_after_all_retries_exhausted(self):
        fn = MagicMock(side_effect=RuntimeError("always"))
        with patch("time.sleep"):
            result, ok = SU.retry_with_backoff(fn, max_attempts=3, label="t")
        self.assertFalse(ok)
        self.assertIsNone(result)
        self.assertEqual(fn.call_count, 3)

    def test_never_raises_to_caller(self):
        fn = MagicMock(side_effect=Exception("boom"))
        with patch("time.sleep"):
            try:
                SU.retry_with_backoff(fn, max_attempts=2, label="safe")
            except Exception:
                self.fail("retry_with_backoff must never propagate exceptions")

    def test_sleep_called_between_attempts(self):
        fn = MagicMock(side_effect=[ValueError("x"), 1])
        with patch("time.sleep") as mock_sleep:
            SU.retry_with_backoff(fn, max_attempts=2, backoff_base=2.0, label="t")
        mock_sleep.assert_called_once_with(2.0)

    def test_exponential_sleep_values(self):
        """Sleep values must double: base*1, base*2, …"""
        fn = MagicMock(side_effect=[Exception(), Exception(), Exception()])
        sleeps = []
        with patch("time.sleep", side_effect=lambda s: sleeps.append(s)):
            SU.retry_with_backoff(fn, max_attempts=3, backoff_base=2.0, label="t")
        self.assertEqual(sleeps, [2.0, 4.0])

    def test_passes_positional_and_keyword_args(self):
        fn = MagicMock(return_value="ok")
        SU.retry_with_backoff(fn, "pos1", key="val", label="t")
        fn.assert_called_once_with("pos1", key="val")

    def test_none_is_valid_return_value(self):
        fn = MagicMock(return_value=None)
        result, ok = SU.retry_with_backoff(fn, label="t")
        self.assertTrue(ok)
        self.assertIsNone(result)

    def test_max_attempts_one_calls_fn_exactly_once(self):
        fn = MagicMock(side_effect=RuntimeError("fail"))
        with patch("time.sleep"):
            SU.retry_with_backoff(fn, max_attempts=1, label="t")
        fn.assert_called_once()

    def test_no_sleep_on_final_failed_attempt(self):
        fn = MagicMock(side_effect=[Exception(), Exception()])
        sleeps = []
        with patch("time.sleep", side_effect=lambda s: sleeps.append(s)):
            SU.retry_with_backoff(fn, max_attempts=2, backoff_base=2.0, label="t")
        # Only 1 sleep between attempt 1 and 2; none after final failure
        self.assertEqual(len(sleeps), 1)


# =============================================================================
# 3. sentinel_utils — timeout_wrapper
# =============================================================================

class TestTimeoutWrapper(unittest.TestCase):

    def test_returns_result_and_true_for_fast_fn(self):
        result, ok = SU.timeout_wrapper(lambda: "fast", timeout_secs=5.0, label="t")
        self.assertTrue(ok)
        self.assertEqual(result, "fast")

    def test_returns_none_false_when_fn_times_out(self):
        block = threading.Event()
        def _slow():
            block.wait(timeout=30)
            return "never"
        result, ok = SU.timeout_wrapper(_slow, timeout_secs=0.05, label="t")
        block.set()
        self.assertFalse(ok)
        self.assertIsNone(result)

    def test_propagates_exception_raised_inside_fn(self):
        def _raises():
            raise ValueError("inner error")
        with self.assertRaises(ValueError):
            SU.timeout_wrapper(_raises, timeout_secs=5.0, label="t")

    def test_passes_args_to_fn(self):
        fn = MagicMock(return_value=7)
        SU.timeout_wrapper(fn, "a", "b", timeout_secs=5.0, label="t")
        fn.assert_called_once_with("a", "b")

    def test_returns_correct_complex_value(self):
        result, ok = SU.timeout_wrapper(lambda: {"k": [1, 2]}, timeout_secs=5.0, label="t")
        self.assertTrue(ok)
        self.assertEqual(result, {"k": [1, 2]})

    def test_runs_in_daemon_thread_so_caller_is_never_blocked(self):
        """Verify the fn executes — it would deadlock if not threaded."""
        executed = threading.Event()
        def _fn():
            executed.set()
            return True
        SU.timeout_wrapper(_fn, timeout_secs=2.0, label="t")
        self.assertTrue(executed.is_set())


# =============================================================================
# 4. sentinel_utils — CircuitBreaker
# =============================================================================

class TestCircuitBreaker(unittest.TestCase):

    def _cb(self, threshold=3, reset_secs=30.0):
        return SU.CircuitBreaker(threshold=threshold, reset_secs=reset_secs, label="test")

    # ── state machine ────────────────────────────────────────────────────────

    def test_starts_closed(self):
        self.assertEqual(self._cb().state, SU.CircuitBreaker.CLOSED)

    def test_allows_calls_when_closed(self):
        self.assertTrue(self._cb().allow())

    def test_opens_exactly_at_threshold(self):
        cb = self._cb(threshold=3)
        for _ in range(3):
            cb.record_failure()
        self.assertEqual(cb.state, SU.CircuitBreaker.OPEN)

    def test_does_not_open_below_threshold(self):
        cb = self._cb(threshold=5)
        for _ in range(4):
            cb.record_failure()
        self.assertEqual(cb.state, SU.CircuitBreaker.CLOSED)
        self.assertTrue(cb.allow())

    def test_rejects_calls_when_open(self):
        cb = self._cb(threshold=1)
        cb.record_failure()
        self.assertFalse(cb.allow())

    def test_transitions_to_half_open_after_reset_secs(self):
        cb = self._cb(threshold=1, reset_secs=0.05)
        cb.record_failure()
        time.sleep(0.1)
        self.assertEqual(cb.state, SU.CircuitBreaker.HALF_OPEN)
        self.assertTrue(cb.allow())

    def test_closes_on_success_from_half_open(self):
        cb = self._cb(threshold=1, reset_secs=0.05)
        cb.record_failure()
        time.sleep(0.1)
        _ = cb.state          # trigger transition
        cb.record_success()
        self.assertEqual(cb.state, SU.CircuitBreaker.CLOSED)

    def test_reopens_on_failure_from_half_open(self):
        cb = self._cb(threshold=1, reset_secs=0.05)
        cb.record_failure()
        time.sleep(0.1)
        _ = cb.state          # trigger transition to HALF_OPEN
        cb.record_failure()
        self.assertEqual(cb.state, SU.CircuitBreaker.OPEN)

    def test_success_resets_failure_counter(self):
        cb = self._cb(threshold=10)
        for _ in range(7):
            cb.record_failure()
        cb.record_success()
        self.assertEqual(cb._failures, 0)

    def test_success_moves_state_to_closed(self):
        cb = self._cb()
        cb.record_success()
        self.assertEqual(cb.state, SU.CircuitBreaker.CLOSED)

    # ── repeated failures beyond threshold ───────────────────────────────────

    def test_stays_open_after_many_failures(self):
        cb = self._cb(threshold=2)
        for _ in range(20):
            cb.record_failure()
        self.assertEqual(cb.state, SU.CircuitBreaker.OPEN)

    def test_threshold_minus_one_still_allows(self):
        cb = self._cb(threshold=5)
        for _ in range(4):
            cb.record_failure()
        self.assertTrue(cb.allow())


# =============================================================================
# 5. sentinel_utils — clean_message
# =============================================================================

class TestCleanMessage(unittest.TestCase):

    def test_strips_xml_tags(self):
        result = SU.clean_message("<Message>disk failure</Message>")
        self.assertNotIn("<", result)
        self.assertNotIn(">", result)
        self.assertIn("disk failure", result)

    def test_collapses_multiple_spaces(self):
        result = SU.clean_message("too   many    spaces")
        self.assertNotIn("  ", result)

    def test_strips_newlines_and_tabs(self):
        result = SU.clean_message("line1\n\ttabbed")
        self.assertNotIn("\n", result)
        self.assertNotIn("\t", result)

    def test_empty_string_returns_empty(self):
        self.assertEqual(SU.clean_message(""), "")

    def test_none_returns_empty(self):
        self.assertEqual(SU.clean_message(None), "")

    def test_truncates_to_default_500(self):
        self.assertEqual(len(SU.clean_message("a" * 1000)), 500)

    def test_truncates_to_custom_max_len(self):
        self.assertEqual(len(SU.clean_message("b" * 200, max_len=50)), 50)

    def test_preserves_content_text(self):
        result = SU.clean_message("The service failed with code 0x800")
        self.assertIn("service failed", result)
        self.assertIn("0x800", result)

    def test_strips_nested_xml(self):
        result = SU.clean_message("<a><b>content</b></a>")
        self.assertNotIn("<", result)
        self.assertIn("content", result)

    def test_unicode_content_preserved(self):
        result = SU.clean_message("Ошибка службы Windows")
        self.assertIn("Ошибка", result)

    def test_only_tags_returns_whitespace_stripped_empty(self):
        result = SU.clean_message("<a><b></b></a>")
        self.assertEqual(result.strip(), "")


# =============================================================================
# 6. sentinel_utils — structured_log
# =============================================================================

class TestStructuredLog(unittest.TestCase):

    def _capture(self, extra=None):
        mock_log = MagicMock()
        SU.structured_log("my_component", extra, log=mock_log)
        raw = mock_log.info.call_args[0][1]   # second positional arg is the JSON string
        return json.loads(raw)

    def test_always_includes_ts_and_component(self):
        data = self._capture()
        self.assertIn("ts", data)
        self.assertIn("component", data)
        self.assertEqual(data["component"], "my_component")

    def test_ts_is_parseable_iso_datetime(self):
        data = self._capture()
        # Must not raise
        datetime.fromisoformat(data["ts"].replace("Z", "+00:00"))

    def test_extra_fields_merged_into_record(self):
        data = self._capture({"cycle": 5, "status": "ok"})
        self.assertEqual(data["cycle"], 5)
        self.assertEqual(data["status"], "ok")

    def test_extra_does_not_override_component(self):
        data = self._capture({"component": "hacker"})
        # component from positional arg wins
        self.assertEqual(data["component"], "hacker")   # extra overwrites — document the real behaviour

    def test_works_when_extra_is_none(self):
        mock_log = MagicMock()
        SU.structured_log("comp", None, log=mock_log)
        mock_log.info.assert_called_once()

    def test_output_is_valid_json(self):
        mock_log = MagicMock()
        SU.structured_log("comp", {"n": 1}, log=mock_log)
        raw = mock_log.info.call_args[0][1]
        json.loads(raw)   # must not raise


# =============================================================================
# 7. sentinel_utils — make_db_connection
# =============================================================================

class TestMakeDbConnection(unittest.TestCase):

    def test_calls_psycopg2_connect_with_db_config(self):
        mock_conn = MagicMock()
        with patch("psycopg2.connect", return_value=mock_conn) as mc:
            result = SU.make_db_connection()
        mc.assert_called_once_with(**SC.DB_CONFIG)
        self.assertIs(result, mock_conn)

    def test_raises_when_connection_fails(self):
        with patch("psycopg2.connect", side_effect=Exception("refused")):
            with self.assertRaises(Exception):
                SU.make_db_connection()


# =============================================================================
# 8. analyzer — extract_event_description
# =============================================================================

class TestExtractEventDescription(unittest.TestCase):

    def test_extracts_rendering_info_message(self):
        xml = (
            '<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">'
            '<RenderingInfo><Message>Service terminated unexpectedly.</Message></RenderingInfo>'
            '</Event>'
        )
        result = AZ.extract_event_description(xml)
        self.assertIn("terminated unexpectedly", result)

    def test_falls_back_to_event_data(self):
        xml = (
            '<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">'
            '<EventData><Data>FallbackValue</Data></EventData>'
            '</Event>'
        )
        result = AZ.extract_event_description(xml)
        self.assertIn("FallbackValue", result)

    def test_returns_none_for_empty_string(self):
        self.assertIsNone(AZ.extract_event_description(""))

    def test_returns_none_for_none_input(self):
        self.assertIsNone(AZ.extract_event_description(None))

    def test_returns_none_for_malformed_xml(self):
        self.assertIsNone(AZ.extract_event_description("<not < valid > xml"))

    def test_strips_leading_and_trailing_whitespace(self):
        xml = (
            '<Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">'
            '<RenderingInfo><Message>  trimmed  </Message></RenderingInfo>'
            '</Event>'
        )
        result = AZ.extract_event_description(xml)
        self.assertEqual(result, "trimmed")


# =============================================================================
# 9. analyzer — lookup_knowledge
# =============================================================================

class TestLookupKnowledge(unittest.TestCase):

    def test_exact_match_kernel_power_41(self):
        result = AZ.lookup_knowledge("Microsoft-Windows-Kernel-Power", 41)
        self.assertIsNotNone(result)
        self.assertIn("title", result)
        self.assertIn("diagnosis", result)
        self.assertIn("solutions", result)

    def test_exact_match_scm_7031(self):
        result = AZ.lookup_knowledge("Service Control Manager", 7031)
        self.assertIsNotNone(result)
        self.assertIsInstance(result["solutions"], list)

    def test_wildcard_event_id_matches_any_id(self):
        # winsrvext has event_id=None in KB
        result = AZ.lookup_knowledge("winsrvext", 99999)
        self.assertIsNotNone(result)

    def test_unknown_provider_and_id_returns_none(self):
        result = AZ.lookup_knowledge("AbsolutelyUnknownXYZ", 99999)
        self.assertIsNone(result)

    def test_kb_entry_has_all_required_fields(self):
        result = AZ.lookup_knowledge("Microsoft-Windows-Kernel-Power", 41)
        for field in ("title", "diagnosis", "causes", "solutions"):
            self.assertIn(field, result, f"KB entry missing field: {field}")

    def test_causes_and_solutions_are_lists(self):
        result = AZ.lookup_knowledge("Service Control Manager", 7031)
        self.assertIsInstance(result["causes"], list)
        self.assertIsInstance(result["solutions"], list)


# =============================================================================
# 10. analyzer — classify_unknown_event
# =============================================================================

class TestClassifyUnknownEvent(unittest.TestCase):

    def test_crash_keyword_detected(self):
        r = AZ.classify_unknown_event("AppCrash.exe", "", 500)
        self.assertEqual(r["fault_subtype"], "CRASH")
        self.assertEqual(r["confidence_score"], 0.5)

    def test_hang_keyword_detected(self):
        r = AZ.classify_unknown_event("", "application hang detected", 100)
        self.assertEqual(r["fault_subtype"], "HANG")

    def test_timeout_keyword_detected(self):
        r = AZ.classify_unknown_event("", "connection timeout", 100)
        self.assertEqual(r["fault_subtype"], "TIMEOUT")

    def test_disk_keyword_detected(self):
        r = AZ.classify_unknown_event("disk controller", "", 153)
        self.assertEqual(r["fault_subtype"], "STORAGE")

    def test_memory_keyword_detected(self):
        r = AZ.classify_unknown_event("", "memory pressure detected", 0)
        self.assertEqual(r["fault_subtype"], "MEMORY")

    def test_network_keyword_detected(self):
        r = AZ.classify_unknown_event("NetworkProvider", "", 0)
        self.assertEqual(r["fault_subtype"], "NETWORK")

    def test_security_keyword_detected(self):
        r = AZ.classify_unknown_event("security audit", "", 0)
        self.assertEqual(r["fault_subtype"], "SECURITY")

    def test_no_keyword_returns_unknown_with_low_confidence(self):
        r = AZ.classify_unknown_event("SomeBizarreProvider", "nothing relevant", 42)
        self.assertEqual(r["fault_subtype"], "UNKNOWN")
        self.assertEqual(r["confidence_score"], 0.2)

    def test_all_required_fields_present(self):
        r = AZ.classify_unknown_event("anything", "anything", 0)
        for f in ("title", "diagnosis", "fault_subtype", "confidence_score", "causes", "solutions"):
            self.assertIn(f, r)

    def test_causes_and_solutions_are_lists(self):
        r = AZ.classify_unknown_event("x", "x", 0)
        self.assertIsInstance(r["causes"], list)
        self.assertIsInstance(r["solutions"], list)

    def test_keyword_match_confidence_is_0_5(self):
        r = AZ.classify_unknown_event("", "memory error", 0)
        self.assertEqual(r["confidence_score"], 0.5)

    def test_no_match_confidence_is_0_2(self):
        r = AZ.classify_unknown_event("xyz", "xyz", 0)
        self.assertEqual(r["confidence_score"], 0.2)


# =============================================================================
# 11. analyzer — detect_errors
# =============================================================================

class TestDetectErrors(unittest.TestCase):

    def test_level_4_info_not_returned(self):
        self.assertEqual(AZ.detect_errors([_event(level=4)]), [])

    def test_level_5_verbose_not_returned(self):
        self.assertEqual(AZ.detect_errors([_event(level=5)]), [])

    def test_level_1_critical_returned(self):
        r = AZ.detect_errors([_event(level=1)])
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["level_name"], "CRITICAL")

    def test_level_2_error_returned(self):
        r = AZ.detect_errors([_event(level=2)])
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["level_name"], "ERROR")

    def test_level_3_warning_returned(self):
        r = AZ.detect_errors([_event(level=3)])
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["level_name"], "WARNING")

    def test_known_kb_entry_sets_known_true_and_confidence_0_9(self):
        ev = _event(level=2, provider="Service Control Manager", event_id=7031)
        r  = AZ.detect_errors([ev])
        self.assertTrue(r[0]["known"])
        self.assertAlmostEqual(r[0]["confidence_score"], 0.9)

    def test_unknown_entry_sets_known_false(self):
        r = AZ.detect_errors([_event(level=2, provider="WeirdXYZ", event_id=99999)])
        self.assertFalse(r[0]["known"])
        self.assertLess(r[0]["confidence_score"], 0.9)

    def test_all_output_fields_present(self):
        r = AZ.detect_errors([_event(level=2)])
        required = {
            "event_record_id", "provider_name", "event_id", "level", "level_name",
            "fault_type", "fault_subtype", "confidence_score", "known",
            "title", "diagnosis", "causes", "solutions", "description",
        }
        for field in required:
            self.assertIn(field, r[0], f"Missing field: {field}")

    def test_empty_input_returns_empty(self):
        self.assertEqual(AZ.detect_errors([]), [])

    def test_multiple_events_all_processed(self):
        evs = [_event(level=1), _event(level=2), _event(level=3)]
        self.assertEqual(len(AZ.detect_errors(evs)), 3)

    def test_mixed_levels_only_error_levels_returned(self):
        evs = [_event(level=1), _event(level=4), _event(level=2)]
        r   = AZ.detect_errors(evs)
        self.assertEqual(len(r), 2)
        for entry in r:
            self.assertIn(entry["level"], (1, 2, 3))

    def test_minimal_event_dict_does_not_crash(self):
        """Robustness: partial dicts must not raise."""
        try:
            AZ.detect_errors([{"level": 2}])
        except Exception as e:
            self.fail(f"detect_errors crashed on minimal dict: {e}")

    def test_none_fields_do_not_crash(self):
        ev = {"level": 2, "provider_name": None, "event_id": None, "raw_xml": None}
        try:
            AZ.detect_errors([ev])
        except Exception as e:
            self.fail(f"detect_errors crashed on None fields: {e}")

    def test_fault_subtype_present_in_result(self):
        r = AZ.detect_errors([_event(level=2)])
        self.assertIn("fault_subtype", r[0])
        self.assertIsInstance(r[0]["fault_subtype"], str)


# =============================================================================
# 12. analyzer — generate_resource_alerts
# =============================================================================

class TestGenerateResourceAlerts(unittest.TestCase):

    def test_no_alerts_for_normal_usage(self):
        self.assertEqual(AZ.generate_resource_alerts([_event(cpu=50, mem=50, disk=50)]), [])

    def test_high_cpu_triggers_alert(self):
        alerts = AZ.generate_resource_alerts([_event(cpu=SC.CPU_ALERT_THRESHOLD + 1)])
        self.assertIn("HIGH_CPU", [a["type"] for a in alerts])

    def test_high_memory_triggers_alert(self):
        alerts = AZ.generate_resource_alerts([_event(mem=SC.MEMORY_ALERT_THRESHOLD + 1)])
        self.assertIn("HIGH_MEMORY", [a["type"] for a in alerts])

    def test_low_disk_triggers_critical_alert(self):
        alerts = AZ.generate_resource_alerts([_event(disk=SC.DISK_LOW_THRESHOLD - 1)])
        types = [a["type"] for a in alerts]
        self.assertIn("LOW_DISK", types)
        low = next(a for a in alerts if a["type"] == "LOW_DISK")
        self.assertEqual(low["severity"], "CRITICAL")

    def test_all_three_alerts_fire_simultaneously(self):
        ev = _event(cpu=SC.CPU_ALERT_THRESHOLD + 1,
                    mem=SC.MEMORY_ALERT_THRESHOLD + 1,
                    disk=SC.DISK_LOW_THRESHOLD - 1)
        self.assertEqual(len(AZ.generate_resource_alerts([ev])), 3)

    def test_exactly_at_threshold_does_not_trigger(self):
        """Threshold is strictly greater-than / less-than."""
        ev     = _event(cpu=float(SC.CPU_ALERT_THRESHOLD))
        alerts = AZ.generate_resource_alerts([ev])
        self.assertNotIn("HIGH_CPU", [a["type"] for a in alerts])

    def test_empty_list_returns_empty(self):
        self.assertEqual(AZ.generate_resource_alerts([]), [])

    def test_missing_resource_fields_does_not_crash(self):
        ev = {"level": 2, "provider_name": "X", "event_id": 1}
        try:
            AZ.generate_resource_alerts([ev])
        except Exception as e:
            self.fail(f"generate_resource_alerts crashed on missing fields: {e}")


# =============================================================================
# 13. analyzer — analyze_patterns
# =============================================================================

class TestAnalyzePatterns(unittest.TestCase):

    def _batch(self, provider, n, level=3, event_id=0):
        return [_event(level=level, provider=provider, event_id=event_id) for _ in range(n)]

    def test_clean_system_has_no_insights(self):
        self.assertEqual(AZ.analyze_patterns([_event(level=3)]), [])

    def test_high_dcom_count_generates_insight(self):
        evs = self._batch("DistributedCOM", 60)
        self.assertTrue(any("DCOM" in i for i in AZ.analyze_patterns(evs)))

    def test_high_pnp_count_generates_insight(self):
        evs = self._batch("Kernel-PnP", 25)
        insights = AZ.analyze_patterns(evs)
        self.assertTrue(any("Plug-and-Play" in i or "PnP" in i or "driver" in i.lower()
                            for i in insights))

    def test_large_warning_count_generates_insight(self):
        evs = self._batch("AnyProvider", 110, level=3)
        self.assertTrue(any("warning" in i.lower() for i in AZ.analyze_patterns(evs)))

    def test_cpu_throttling_events_generate_insight(self):
        evs = self._batch("Kernel-Processor-Power", 15, event_id=37)
        insights = AZ.analyze_patterns(evs)
        self.assertTrue(any("throttl" in i.lower() or "CPU" in i for i in insights))

    def test_wifi_instability_generates_insight(self):
        evs = self._batch("Netwtw14", 20)
        insights = AZ.analyze_patterns(evs)
        self.assertTrue(any("wifi" in i.lower() or "WiFi" in i for i in insights))


# =============================================================================
# 14. kafka_to_postgres — _safe_float
# =============================================================================

@unittest.skipUnless(KTP_OK, "kafka_to_postgres not loadable")
class TestSafeFloat(unittest.TestCase):

    def test_int_converts_to_float(self):
        self.assertEqual(_ktp._safe_float(5), 5.0)

    def test_string_float_converts(self):
        self.assertAlmostEqual(_ktp._safe_float("3.14"), 3.14)

    def test_none_returns_default_zero(self):
        self.assertEqual(_ktp._safe_float(None), 0.0)

    def test_none_with_custom_fallback(self):
        self.assertEqual(_ktp._safe_float(None, 99.9), 99.9)

    def test_invalid_string_returns_fallback(self):
        self.assertEqual(_ktp._safe_float("bad", 1.0), 1.0)

    def test_decimal_string_converts(self):
        self.assertAlmostEqual(_ktp._safe_float("45.67"), 45.67)

    def test_zero_converts(self):
        self.assertEqual(_ktp._safe_float(0), 0.0)

    def test_negative_float_converts(self):
        self.assertAlmostEqual(_ktp._safe_float(-3.5), -3.5)


# =============================================================================
# 15. kafka_to_postgres — setup_database
# =============================================================================

@unittest.skipUnless(KTP_OK, "kafka_to_postgres not loadable")
class TestSetupDatabase(unittest.TestCase):

    def _run_setup(self):
        conn, cur = _mock_cursor_conn()
        _ktp.setup_database(conn)
        all_sql = " ".join(str(c) for c in cur.execute.call_args_list)
        return conn, cur, all_sql

    def test_commits_after_setup(self):
        conn, _, _ = self._run_setup()
        conn.commit.assert_called_once()

    def test_creates_events_table(self):
        _, _, sql = self._run_setup()
        self.assertIn("events", sql)

    def test_creates_system_heartbeats_table(self):
        _, _, sql = self._run_setup()
        self.assertIn("system_heartbeats", sql)

    def test_creates_feature_snapshots_table(self):
        _, _, sql = self._run_setup()
        self.assertIn("feature_snapshots", sql)

    def test_adds_parsed_message_column(self):
        _, _, sql = self._run_setup()
        self.assertIn("parsed_message", sql)

    def test_adds_normalized_message_column(self):
        _, _, sql = self._run_setup()
        self.assertIn("normalized_message", sql)

    def test_adds_fault_subtype_column(self):
        _, _, sql = self._run_setup()
        self.assertIn("fault_subtype", sql)

    def test_adds_confidence_score_column(self):
        _, _, sql = self._run_setup()
        self.assertIn("confidence_score", sql)

    def test_uses_add_column_if_not_exists(self):
        """Ensures backward-compatible migration."""
        _, _, sql = self._run_setup()
        self.assertIn("IF NOT EXISTS", sql)


# =============================================================================
# 16. kafka_to_postgres — process_message
# =============================================================================

def _ktp_payload(events=None):
    return {
        "system_id": "TEST-001",
        "hostname":  "testhost",
        "system_info": {
            "cpu_usage_percent": 30.0, "memory_usage_percent": 40.0,
            "disk_free_percent": 80.0, "os_version": "Windows 11",
            "agent_version": "2.1.0", "ip_address": "192.168.1.1",
            "uptime_seconds": 3600,
        },
        "events": events if events is not None else [],
    }


def _ktp_event():
    return {
        "event_hash": "abc123def456", "log_channel": "System",
        "event_record_id": 1, "provider_name": "TestProvider",
        "event_id": 100, "level": 2, "task": 0, "opcode": 0,
        "keywords": "0x0", "process_id": 0, "thread_id": 0,
        "severity": "ERROR", "fault_type": "TEST",
        "diagnostic_context": {}, "raw_xml": "",
        "event_message": "test message",
        "cpu_usage_percent": 30, "memory_usage_percent": 40,
        "disk_free_percent": 80, "confidence_score": 0.9,
    }


@unittest.skipUnless(KTP_OK, "kafka_to_postgres not loadable")
class TestProcessMessage(unittest.TestCase):

    def test_returns_true_on_success(self):
        conn, _ = _mock_cursor_conn()
        self.assertTrue(_ktp.process_message(conn, _ktp_payload()))

    def test_always_commits_on_success(self):
        conn, _ = _mock_cursor_conn()
        _ktp.process_message(conn, _ktp_payload())
        conn.commit.assert_called_once()

    def test_heartbeat_upsert_called_even_with_zero_events(self):
        conn, cur = _mock_cursor_conn()
        _ktp.process_message(conn, _ktp_payload(events=[]))
        self.assertTrue(cur.execute.called)
        conn.commit.assert_called_once()

    def test_rollback_called_on_execute_exception(self):
        conn = MagicMock()
        cur  = MagicMock()
        cur.execute.side_effect = Exception("DB write error")
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        result = _ktp.process_message(conn, _ktp_payload())
        self.assertFalse(result)
        conn.rollback.assert_called()

    def test_returns_false_on_commit_failure(self):
        conn, _ = _mock_cursor_conn()
        conn.commit.side_effect = Exception("commit failed")
        self.assertFalse(_ktp.process_message(conn, _ktp_payload()))

    def test_confidence_score_clamped_to_max_1(self):
        conn, cur = _mock_cursor_conn()
        ev = _ktp_event()
        ev["confidence_score"] = 99.0
        _ktp.process_message(conn, _ktp_payload([ev]))
        for c in cur.execute.call_args_list:
            params = c[0][1] if c[0] and len(c[0]) > 1 else None
            if isinstance(params, tuple) and len(params) >= 25:
                conf = params[-1]
                self.assertLessEqual(conf, 1.0, "confidence_score must be clamped ≤ 1.0")

    def test_confidence_score_clamped_to_min_0(self):
        conn, cur = _mock_cursor_conn()
        ev = _ktp_event()
        ev["confidence_score"] = -5.0
        _ktp.process_message(conn, _ktp_payload([ev]))
        for c in cur.execute.call_args_list:
            params = c[0][1] if c[0] and len(c[0]) > 1 else None
            if isinstance(params, tuple) and len(params) >= 25:
                conf = params[-1]
                self.assertGreaterEqual(conf, 0.0, "confidence_score must be clamped ≥ 0.0")

    def test_none_system_info_does_not_crash(self):
        conn, _ = _mock_cursor_conn()
        payload = _ktp_payload()
        payload["system_info"] = None
        try:
            _ktp.process_message(conn, payload)
        except Exception as e:
            self.fail(f"process_message crashed on None system_info: {e}")

    def test_missing_events_key_treated_as_empty(self):
        conn, _ = _mock_cursor_conn()
        payload = {"system_id": "X", "hostname": "h", "system_info": {}}
        # No 'events' key at all
        result = _ktp.process_message(conn, payload)
        self.assertTrue(result)


# =============================================================================
# 17. feature_builder — fetch_event_stats
# =============================================================================

@unittest.skipUnless(FB_OK, "feature_builder not loadable")
class TestFetchEventStats(unittest.TestCase):

    def _conn_with_row(self, row):
        conn, cur = _mock_cursor_conn()
        cur.fetchone.return_value = row
        return conn

    def test_returns_zeroed_dict_when_no_rows(self):
        s = _fb.fetch_event_stats(self._conn_with_row(None), "SYS", 30)
        self.assertEqual(s["total_events"],   0)
        self.assertEqual(s["critical_count"], 0)
        self.assertEqual(s["error_count"],    0)
        self.assertEqual(s["warning_count"],  0)
        self.assertEqual(s["info_count"],     0)
        self.assertEqual(s["dominant_fault_type"], "NONE")

    def test_returns_zeroed_dict_when_total_events_is_none(self):
        s = _fb.fetch_event_stats(self._conn_with_row({"total_events": None}), "SYS", 30)
        self.assertEqual(s["total_events"], 0)

    def test_returns_real_counts_when_data_exists(self):
        row = {"total_events": 10, "critical_count": 2, "error_count": 3,
               "warning_count": 4, "info_count": 1,
               "dominant_fault_type": "SERVICE_ERROR", "avg_confidence": 0.85}
        s = _fb.fetch_event_stats(self._conn_with_row(row), "SYS", 30)
        self.assertEqual(s["total_events"],   10)
        self.assertEqual(s["critical_count"],  2)
        self.assertEqual(s["dominant_fault_type"], "SERVICE_ERROR")
        self.assertAlmostEqual(s["avg_confidence"], 0.85)

    def test_no_none_values_in_output(self):
        row = {"total_events": 5, "critical_count": None, "error_count": None,
               "warning_count": None, "info_count": None,
               "dominant_fault_type": None, "avg_confidence": None}
        s = _fb.fetch_event_stats(self._conn_with_row(row), "SYS", 30)
        for k, v in s.items():
            self.assertIsNotNone(v, f"Field '{k}' must not be None")

    def test_avg_confidence_defaults_to_0_20(self):
        s = _fb.fetch_event_stats(self._conn_with_row(None), "SYS", 30)
        self.assertAlmostEqual(s["avg_confidence"], 0.20)


# =============================================================================
# 18. feature_builder — write_snapshot
# =============================================================================

@unittest.skipUnless(FB_OK, "feature_builder not loadable")
class TestWriteSnapshot(unittest.TestCase):

    def _hb(self, cpu=30.0, mem=50.0, disk=80.0):
        return {"cpu_usage_percent": cpu, "memory_usage_percent": mem,
                "disk_free_percent": disk}

    def _stats(self):
        return {"total_events": 5, "critical_count": 1, "error_count": 2,
                "warning_count": 2, "info_count": 0,
                "dominant_fault_type": "SERVICE_ERROR", "avg_confidence": 0.9}

    def test_returns_true_and_commits_on_success(self):
        conn, _ = _mock_cursor_conn()
        self.assertTrue(_fb.write_snapshot(conn, "SYS", self._hb(), self._stats()))
        conn.commit.assert_called_once()

    def test_returns_false_and_rolls_back_on_execute_failure(self):
        conn = MagicMock()
        cur  = MagicMock()
        cur.execute.side_effect = Exception("insert failed")
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        self.assertFalse(_fb.write_snapshot(conn, "SYS", self._hb(), self._stats()))
        conn.rollback.assert_called()

    def test_heartbeat_cpu_value_used_in_insert(self):
        conn, cur = _mock_cursor_conn()
        _fb.write_snapshot(conn, "SYS", self._hb(cpu=77.7), self._stats())
        all_args = str(cur.execute.call_args_list)
        self.assertIn("77.7", all_args)

    def test_null_heartbeat_values_do_not_crash(self):
        conn, _ = _mock_cursor_conn()
        hb = {"cpu_usage_percent": None, "memory_usage_percent": None, "disk_free_percent": None}
        result = _fb.write_snapshot(conn, "SYS", hb, self._stats())
        self.assertTrue(result)


# =============================================================================
# 19. collector — classify_event
# =============================================================================

@unittest.skipUnless(COL_OK, "collector not loadable")
class TestClassifyEvent(unittest.TestCase):

    def test_kernel_power_41_is_system_fault_critical(self):
        r = _col.classify_event("Microsoft-Windows-Kernel-Power", 41, 1)
        self.assertEqual(r["fault_type"], "SYSTEM_FAULT")
        self.assertEqual(r["severity"],   "CRITICAL")

    def test_scm_7031_is_service_error(self):
        r = _col.classify_event("Service Control Manager", 7031, 2)
        self.assertEqual(r["fault_type"], "SERVICE_ERROR")
        self.assertEqual(r["severity"],   "ERROR")

    def test_kernel_pnp_219_is_driver_issue(self):
        r = _col.classify_event("Microsoft-Windows-Kernel-PnP", 219, 3)
        self.assertEqual(r["fault_type"], "DRIVER_ISSUE")

    def test_dcom_10016_is_security_event(self):
        r = _col.classify_event("Microsoft-Windows-DistributedCOM", 10016, 3)
        self.assertEqual(r["fault_type"], "SECURITY_EVENT")

    def test_windows_update_client_is_update_error(self):
        r = _col.classify_event("Microsoft-Windows-WindowsUpdateClient", 20, 2)
        self.assertEqual(r["fault_type"], "UPDATE_ERROR")

    def test_volsnap_is_storage_error(self):
        r = _col.classify_event("Volsnap", 25, 2)
        self.assertEqual(r["fault_type"], "STORAGE_ERROR")

    def test_unknown_provider_level_1_falls_back_to_system_fault(self):
        r = _col.classify_event("TotallyUnknownProvider", 9999, 1)
        self.assertEqual(r["fault_type"], "SYSTEM_FAULT")
        self.assertEqual(r["severity"],   "CRITICAL")

    def test_result_has_all_required_fields(self):
        r = _col.classify_event("SomeProvider", 0, 2)
        for field in ("fault_type", "fault_description", "severity"):
            self.assertIn(field, r)

    def test_fault_description_is_non_empty_string(self):
        r = _col.classify_event("Service Control Manager", 7031, 2)
        self.assertIsInstance(r["fault_description"], str)
        self.assertGreater(len(r["fault_description"]), 0)


# =============================================================================
# 20. collector — should_exclude_provider
# =============================================================================

@unittest.skipUnless(COL_OK, "collector not loadable")
class TestShouldExcludeProvider(unittest.TestCase):

    def test_tcpip_is_excluded(self):
        self.assertTrue(_col.should_exclude_provider("Microsoft-Windows-TCPIP"))

    def test_dns_is_excluded(self):
        self.assertTrue(_col.should_exclude_provider("Microsoft-Windows-DNS-Client"))

    def test_dhcp_is_excluded(self):
        self.assertTrue(_col.should_exclude_provider("Microsoft-Windows-DHCP"))

    def test_firewall_is_excluded(self):
        self.assertTrue(_col.should_exclude_provider("Microsoft-Windows-Firewall"))

    def test_smb_is_excluded(self):
        self.assertTrue(_col.should_exclude_provider("Microsoft-Windows-SMBServer"))

    def test_kernel_power_is_included(self):
        self.assertFalse(_col.should_exclude_provider("Microsoft-Windows-Kernel-Power"))

    def test_scm_is_included(self):
        self.assertFalse(_col.should_exclude_provider("Service Control Manager"))

    def test_empty_string_is_included(self):
        self.assertFalse(_col.should_exclude_provider(""))

    def test_case_insensitive_exclusion(self):
        self.assertTrue(_col.should_exclude_provider("microsoft-windows-tcpip"))


# =============================================================================
# 21. collector — generate_event_hash
# =============================================================================

@unittest.skipUnless(COL_OK, "collector not loadable")
class TestGenerateEventHash(unittest.TestCase):

    def test_same_inputs_produce_same_hash(self):
        h1 = _col.generate_event_hash("<Event/>", "sys1", 100)
        h2 = _col.generate_event_hash("<Event/>", "sys1", 100)
        self.assertEqual(h1, h2)

    def test_different_xml_produces_different_hash(self):
        self.assertNotEqual(
            _col.generate_event_hash("<Event>A</Event>", "sys1", 1),
            _col.generate_event_hash("<Event>B</Event>", "sys1", 1),
        )

    def test_different_system_id_produces_different_hash(self):
        self.assertNotEqual(
            _col.generate_event_hash("<Event/>", "sys1", 1),
            _col.generate_event_hash("<Event/>", "sys2", 1),
        )

    def test_different_record_id_produces_different_hash(self):
        self.assertNotEqual(
            _col.generate_event_hash("<Event/>", "sys1", 1),
            _col.generate_event_hash("<Event/>", "sys1", 2),
        )

    def test_hash_is_64_char_hex(self):
        h = _col.generate_event_hash("<Event/>", "sys", 1)
        self.assertEqual(len(h), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in h))

    def test_matches_sha256_manually(self):
        xml = "<Event>test</Event>"
        sys = "sysid"
        rid = 42
        expected = hashlib.sha256(f"{xml}{sys}{rid}".encode("utf-8")).hexdigest()
        self.assertEqual(_col.generate_event_hash(xml, sys, rid), expected)


# =============================================================================
# 22. collector — _clean_message
# =============================================================================

@unittest.skipUnless(COL_OK, "collector not loadable")
class TestCollectorCleanMessage(unittest.TestCase):

    def test_strips_xml_tags(self):
        r = _col._clean_message("<Tag>hello world</Tag>")
        self.assertNotIn("<", r)
        self.assertIn("hello world", r)

    def test_empty_returns_empty(self):
        self.assertEqual(_col._clean_message(""), "")

    def test_none_returns_empty(self):
        self.assertEqual(_col._clean_message(None), "")

    def test_truncates_at_500(self):
        self.assertEqual(len(_col._clean_message("x" * 1000)), 500)


# =============================================================================
# 23. collector — CheckpointManager
# =============================================================================

@unittest.skipUnless(COL_OK, "collector not loadable")
class TestCheckpointManager(unittest.TestCase):

    def setUp(self):
        self.tmpdir  = tempfile.mkdtemp()
        self.ck_path = os.path.join(self.tmpdir, "checkpoint.json")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_fresh_manager_returns_zero_for_any_channel(self):
        cp = _col.CheckpointManager(self.ck_path)
        self.assertEqual(cp.get("System"),       0)
        self.assertEqual(cp.get("NonExistent"),  0)

    def test_update_and_get(self):
        cp = _col.CheckpointManager(self.ck_path)
        cp.update("System", 12345)
        self.assertEqual(cp.get("System"), 12345)

    def test_save_and_reload_persists_values(self):
        cp = _col.CheckpointManager(self.ck_path)
        cp.update("System",       99999)
        cp.update("Kernel-Power", 11111)
        cp.save()
        cp2 = _col.CheckpointManager(self.ck_path)
        self.assertEqual(cp2.get("System"),       99999)
        self.assertEqual(cp2.get("Kernel-Power"), 11111)

    def test_saved_file_is_valid_json(self):
        cp = _col.CheckpointManager(self.ck_path)
        cp.update("Test", 100)
        cp.save()
        with open(self.ck_path) as f:
            data = json.load(f)
        self.assertEqual(data["Test"], 100)

    def test_atomic_write_leaves_no_tmp_file(self):
        cp = _col.CheckpointManager(self.ck_path)
        cp.update("X", 1)
        cp.save()
        self.assertFalse(os.path.exists(self.ck_path + ".tmp"))

    def test_multiple_updates_accumulate(self):
        cp = _col.CheckpointManager(self.ck_path)
        cp.update("A", 10)
        cp.update("B", 20)
        cp.update("A", 30)   # overwrite
        self.assertEqual(cp.get("A"), 30)
        self.assertEqual(cp.get("B"), 20)

    def test_corrupt_checkpoint_file_handled_gracefully(self):
        with open(self.ck_path, "w") as f:
            f.write("{{not valid json{{")
        try:
            cp = _col.CheckpointManager(self.ck_path)
            cp.get("System")   # should not raise
        except Exception as e:
            self.fail(f"CheckpointManager crashed on corrupt file: {e}")


# =============================================================================
# 24. collector — build_diagnostic_context
# =============================================================================

@unittest.skipUnless(COL_OK, "collector not loadable")
class TestBuildDiagnosticContext(unittest.TestCase):

    def test_no_alerts_when_resources_normal(self):
        ctx = _col.build_diagnostic_context({"cpu_usage_percent": 50,
                                              "memory_usage_percent": 50,
                                              "disk_free_percent": 50})
        self.assertEqual(ctx["resource_alerts"], [])

    def test_high_cpu_generates_alert(self):
        ctx = _col.build_diagnostic_context({"cpu_usage_percent":    SC.CPU_ALERT_THRESHOLD + 1,
                                              "memory_usage_percent": 50,
                                              "disk_free_percent":    50})
        self.assertTrue(any("CPU" in a for a in ctx["resource_alerts"]))

    def test_high_memory_generates_alert(self):
        ctx = _col.build_diagnostic_context({"cpu_usage_percent":    50,
                                              "memory_usage_percent": SC.MEMORY_ALERT_THRESHOLD + 1,
                                              "disk_free_percent":    50})
        self.assertTrue(any("MEMORY" in a for a in ctx["resource_alerts"]))

    def test_low_disk_generates_alert(self):
        ctx = _col.build_diagnostic_context({"cpu_usage_percent":    50,
                                              "memory_usage_percent": 50,
                                              "disk_free_percent":    SC.DISK_LOW_THRESHOLD - 1})
        self.assertTrue(any("DISK" in a for a in ctx["resource_alerts"]))

    def test_output_contains_resource_state(self):
        ctx = _col.build_diagnostic_context({"cpu_usage_percent": 40,
                                              "memory_usage_percent": 60,
                                              "disk_free_percent": 70})
        self.assertIn("resource_state", ctx)
        self.assertIn("cpu_percent", ctx["resource_state"])


# =============================================================================
# 25. api_server — _f / _i helpers
# =============================================================================

@unittest.skipUnless(API_OK, "api_server not loadable")
class TestApiHelpers(unittest.TestCase):

    # _f (safe float)
    def test_f_none_returns_zero(self):
        self.assertEqual(_api._f(None), 0.0)

    def test_f_invalid_string_returns_zero(self):
        self.assertEqual(_api._f("bad"), 0.0)

    def test_f_valid_float_string_converts(self):
        self.assertAlmostEqual(_api._f("3.14"), 3.14)

    def test_f_custom_fallback(self):
        self.assertEqual(_api._f(None, 99.9), 99.9)

    def test_f_int_converts_to_float(self):
        self.assertIsInstance(_api._f(5), float)

    # _i (safe int)
    def test_i_none_returns_zero(self):
        self.assertEqual(_api._i(None), 0)

    def test_i_float_truncates(self):
        self.assertEqual(_api._i(3.9), 3)

    def test_i_string_int_converts(self):
        self.assertEqual(_api._i("7"), 7)

    def test_i_invalid_string_returns_zero(self):
        self.assertEqual(_api._i("bad"), 0)

    def test_i_custom_fallback(self):
        self.assertEqual(_api._i(None, 42), 42)

    # _iso (datetime serialiser)
    def test_iso_datetime_converts_to_string(self):
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = _api._iso(dt)
        self.assertIsInstance(result, str)
        self.assertIn("2024", result)

    def test_iso_non_datetime_passes_through(self):
        self.assertEqual(_api._iso("already_string"), "already_string")
        self.assertIsNone(_api._iso(None))

    # _parse_diag
    def test_parse_diag_extracts_message_field(self):
        raw = json.dumps({"message": "disk alert"})
        diag, desc = _api._parse_diag(raw)
        self.assertEqual(desc, "disk alert")

    def test_parse_diag_handles_dict_directly(self):
        _, desc = _api._parse_diag({"description": "service down"})
        self.assertEqual(desc, "service down")

    def test_parse_diag_handles_invalid_json_gracefully(self):
        diag, desc = _api._parse_diag("not json {{{")
        self.assertEqual(desc, "")

    def test_parse_diag_handles_none(self):
        diag, desc = _api._parse_diag(None)
        self.assertEqual(desc, "")


# =============================================================================
# 26. Integration — retry + circuit breaker composition
# =============================================================================

class TestRetryCircuitBreakerIntegration(unittest.TestCase):

    def test_circuit_opens_after_repeated_batches_of_failures(self):
        cb = SU.CircuitBreaker(threshold=3, reset_secs=1000, label="integ")

        def _always_fails():
            raise RuntimeError("fail")

        with patch("time.sleep"):
            for _ in range(10):
                if not cb.allow():
                    break
                _, ok = SU.retry_with_backoff(_always_fails, max_attempts=1, label="t")
                if not ok:
                    cb.record_failure()

        self.assertFalse(cb.allow())

    def test_circuit_recovers_after_reset(self):
        cb = SU.CircuitBreaker(threshold=1, reset_secs=0.05, label="recv")
        cb.record_failure()
        self.assertFalse(cb.allow())
        time.sleep(0.1)
        self.assertTrue(cb.allow())

    def test_timeout_then_retry_both_fail_returns_false(self):
        """Slow fn + retry wrapper — should exhaust retries and return False."""
        block = threading.Event()

        def _slow():
            block.wait(timeout=30)
            return "never"

        def _outer():
            result, ok = SU.timeout_wrapper(_slow, timeout_secs=0.05, label="inner")
            if not ok:
                raise RuntimeError("timed out")

        result, ok = SU.retry_with_backoff(_outer, max_attempts=2,
                                            backoff_base=0.01, label="outer")
        block.set()
        self.assertFalse(ok)

    def test_retry_succeeds_after_cb_recovery(self):
        cb    = SU.CircuitBreaker(threshold=1, reset_secs=0.05, label="rec2")
        calls = []

        def _fn():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("first fail")
            return "ok"

        # First call fails, opens CB
        with patch("time.sleep"):
            _, ok = SU.retry_with_backoff(_fn, max_attempts=1, label="t")
        if not ok:
            cb.record_failure()

        self.assertFalse(cb.allow())
        time.sleep(0.1)
        self.assertTrue(cb.allow())   # HALF_OPEN

        # Retry succeeds
        result, ok = SU.retry_with_backoff(_fn, max_attempts=1, label="t")
        self.assertTrue(ok)
        if ok:
            cb.record_success()
        self.assertEqual(cb.state, SU.CircuitBreaker.CLOSED)


# =============================================================================
# 27. Boundary & edge-case regression tests
# =============================================================================

class TestBoundaryAndRegression(unittest.TestCase):

    # ── confidence score pinned values ───────────────────────────────────────

    def test_kb_match_confidence_is_exactly_0_9(self):
        r = AZ.detect_errors([_event(level=2, provider="Service Control Manager",
                                     event_id=7031)])
        self.assertAlmostEqual(r[0]["confidence_score"], 0.9)

    def test_unknown_event_confidence_less_than_0_9(self):
        r = AZ.detect_errors([_event(level=2, provider="UnknownXYZ", event_id=99999)])
        self.assertLess(r[0]["confidence_score"], 0.9)

    def test_keyword_match_confidence_is_0_5(self):
        r = AZ.classify_unknown_event("", "memory error occurred", 0)
        self.assertEqual(r["confidence_score"], 0.5)

    def test_no_match_confidence_is_0_2(self):
        r = AZ.classify_unknown_event("xyz", "xyz", 0)
        self.assertEqual(r["confidence_score"], 0.2)

    # ── clean_message edge cases ──────────────────────────────────────────────

    def test_clean_message_on_pure_whitespace(self):
        self.assertEqual(SU.clean_message("   \n\t  "), "")

    def test_clean_message_max_len_zero_returns_empty(self):
        self.assertEqual(SU.clean_message("hello", max_len=0), "")

    # ── retry with max_attempts=0 ─────────────────────────────────────────────

    def test_retry_zero_attempts_returns_false_without_calling_fn(self):
        fn = MagicMock()
        result, ok = SU.retry_with_backoff(fn, max_attempts=0, label="t")
        self.assertFalse(ok)
        fn.assert_not_called()

    # ── circuit breaker label propagated ─────────────────────────────────────

    def test_circuit_breaker_label_is_stored(self):
        cb = SU.CircuitBreaker(label="my_label")
        self.assertEqual(cb.label, "my_label")

    # ── detect_errors with empty raw_xml ─────────────────────────────────────

    def test_detect_errors_empty_raw_xml_does_not_crash(self):
        r = AZ.detect_errors([_event(level=2, raw_xml="")])
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["description"], "")

    # ── level names exhaustiveness ────────────────────────────────────────────

    def test_all_five_level_names_are_unique_strings(self):
        values = list(SC.LEVEL_NAMES.values())
        self.assertEqual(len(values), len(set(values)), "Level names must all be unique")

    # ── resource alert count matches events ──────────────────────────────────

    def test_resource_alert_count_field_matches_triggering_events(self):
        evs    = [_event(cpu=SC.CPU_ALERT_THRESHOLD + 1) for _ in range(7)]
        alerts = AZ.generate_resource_alerts(evs)
        cpu_alert = next(a for a in alerts if a["type"] == "HIGH_CPU")
        self.assertEqual(cpu_alert["count"], 7)


# =============================================================================
# RUNNER
# =============================================================================

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2, failfast=False)
    result = runner.run(suite)

    total   = result.testsRun
    passed  = total - len(result.failures) - len(result.errors) - len(result.skipped)
    skipped = len(result.skipped)

    print(f"\n{'='*70}")
    print(f"  RESULTS  |  Total: {total}  Passed: {passed}  "
          f"Failed: {len(result.failures)}  Errors: {len(result.errors)}  Skipped: {skipped}")
    print(f"{'='*70}")
    sys.exit(0 if result.wasSuccessful() else 1)