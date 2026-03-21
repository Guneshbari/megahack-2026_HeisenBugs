"""
tests/conftest.py
Place this file in: C:\ProgramData\megahack-2026_HeisenBugs\tests\conftest.py

pytest loads this automatically before any test file.
It adds src/ to sys.path so all tests can import shared_constants,
sentinel_utils, and all other project modules regardless of where
pytest is invoked from.
"""
import sys
import os

# Resolve src/ relative to this conftest.py file — works on any machine
_TESTS_DIR   = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_TESTS_DIR)
_SRC_DIR     = os.path.join(_PROJECT_ROOT, "src")

if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)