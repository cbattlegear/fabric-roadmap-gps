"""Shared test fixtures and import-path setup.

Tests live in ``tests/`` but need to import top-level modules from the
project root (e.g. ``lib.db_retry``, ``lib.release_item``,
``weekly_email_job``). Insert the project root onto ``sys.path`` once at
collection time so individual test files can use plain absolute imports.

Also forces ``CURRENT_ENVIRONMENT=development`` before any test module is
imported so production-only env-var checks (ACS resource ID, HTTPS base
URL, App Insights connection string) don't prevent ``import server`` /
``import weekly_email_job`` in tests.
"""

import os
import sys

os.environ.setdefault("CURRENT_ENVIRONMENT", "development")

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
