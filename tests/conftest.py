"""Pytest bootstrap for SentinelCore tests."""

import importlib
import os
import sys

import pytest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_TESTS_DIR)
_SRC_DIR = os.path.join(_PROJECT_ROOT, "src")

if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)


@pytest.fixture(autouse=True)
def restore_module_config_state():
    """
    Reset env-backed shared modules after each test.

    Pytest runs the full suite in one process, so tests that reload
    ``shared_constants`` under temporary env overrides can leak state into
    later tests unless we restore the shared modules on teardown.
    """
    yield

    for module_name in ("shared_constants", "utils.db", "sentinel_utils"):
        module = sys.modules.get(module_name)
        if module is not None:
            importlib.reload(module)
