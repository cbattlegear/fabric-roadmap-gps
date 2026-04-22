"""Tests for the quick-wins extracted helpers.

Covers:
- ``server._clamp_int`` (L4)
- ``server._parse_bool`` (L5)
- ``db.db_sqlserver._rows_to_dicts`` (L6)
- ``server.verify_email_page`` GET branch logging on context lookup
  failure (M16 regression)
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from server import _clamp_int, _parse_bool
from db.db_sqlserver import _rows_to_dicts


# ---------------------------------------------------------------------------
# _clamp_int (L4)
# ---------------------------------------------------------------------------


class TestClampInt:
    def test_value_within_range_unchanged(self):
        assert _clamp_int(10, 1, 25) == 10

    def test_value_below_lo_clamped_up(self):
        assert _clamp_int(-5, 1, 25) == 1
        assert _clamp_int(0, 1, 90) == 1

    def test_value_above_hi_clamped_down(self):
        assert _clamp_int(99, 1, 25) == 25
        assert _clamp_int(1000, 1, 90) == 90

    def test_value_equal_to_bounds_unchanged(self):
        assert _clamp_int(1, 1, 25) == 1
        assert _clamp_int(25, 1, 25) == 25

    def test_single_point_range(self):
        assert _clamp_int(0, 5, 5) == 5
        assert _clamp_int(10, 5, 5) == 5
        assert _clamp_int(5, 5, 5) == 5


# ---------------------------------------------------------------------------
# _parse_bool (L5)
# ---------------------------------------------------------------------------


class TestParseBool:
    @pytest.mark.parametrize("val", ["1", "true", "TRUE", "True", "yes", "YES", "on", "ON"])
    def test_truthy_strings(self, val):
        assert _parse_bool(val) is True

    @pytest.mark.parametrize("val", ["0", "false", "FALSE", "no", "NO", "off", "OFF"])
    def test_falsy_strings(self, val):
        assert _parse_bool(val, default=True) is False

    def test_none_returns_default(self):
        assert _parse_bool(None) is False
        assert _parse_bool(None, default=True) is True

    def test_unrecognized_returns_default(self):
        assert _parse_bool("maybe") is False
        assert _parse_bool("maybe", default=True) is True
        assert _parse_bool("") is False

    def test_native_bool_passthrough(self):
        assert _parse_bool(True) is True
        assert _parse_bool(False) is False

    def test_whitespace_tolerated(self):
        assert _parse_bool("  true  ") is True
        assert _parse_bool(" 0 ", default=True) is False


class TestApiChangelogIncludeInactiveSemantics:
    """Regression: refactoring `include_inactive` parsing must preserve the
    historical semantics where missing => True but any unrecognized non-empty
    value (e.g. "maybe") => False.
    """

    @pytest.mark.parametrize(
        "qs,expected",
        [
            ("", True),                       # omitted -> True
            ("?include_inactive=true", True),
            ("?include_inactive=1", True),
            ("?include_inactive=yes", True),
            ("?include_inactive=false", False),
            ("?include_inactive=0", False),
            ("?include_inactive=no", False),
            ("?include_inactive=maybe", False),  # unknown -> False (historical)
            ("?include_inactive=", False),       # empty -> False (historical)
        ],
    )
    def test_include_inactive_parsing(self, qs, expected):
        import server

        captured = {}

        def _fake_changelog(_engine, *, days, include_inactive, product_name, release_type, release_status):
            captured["include_inactive"] = include_inactive
            return []

        client = server.app.test_client()
        with patch.object(server, "get_engine", return_value=object()), \
             patch.object(server, "get_changelog_with_changes", side_effect=_fake_changelog):
            resp = client.get(f"/api/changelog{qs}")

        assert resp.status_code == 200
        assert captured["include_inactive"] is expected


# ---------------------------------------------------------------------------
# _rows_to_dicts (L6)
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Minimal pyodbc cursor stand-in for testing."""

    def __init__(self, columns, rows):
        self.description = (
            [(c, None, None, None, None, None, None) for c in columns]
            if columns is not None
            else None
        )
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class TestRowsToDicts:
    def test_typical_result_set(self):
        cur = _FakeCursor(
            ["id", "name", "active"],
            [(1, "alpha", True), (2, "beta", False)],
        )
        assert _rows_to_dicts(cur) == [
            {"id": 1, "name": "alpha", "active": True},
            {"id": 2, "name": "beta", "active": False},
        ]

    def test_empty_rows(self):
        cur = _FakeCursor(["id", "name"], [])
        assert _rows_to_dicts(cur) == []

    def test_no_description_returns_empty(self):
        # cursor.description is None when there is no result set (e.g. after
        # a DML statement). Helper must not blow up.
        cur = _FakeCursor(None, [])
        assert _rows_to_dicts(cur) == []

    def test_single_column(self):
        cur = _FakeCursor(["count"], [(42,)])
        assert _rows_to_dicts(cur) == [{"count": 42}]


# ---------------------------------------------------------------------------
# verify_email_page GET branch logs on context lookup failure (M16)
# ---------------------------------------------------------------------------


class TestVerifyEmailContextLookupLogging:
    def test_get_logs_warning_when_context_lookup_raises(self, caplog):
        import server

        # Use Flask's test client so the endpoint executes its real code path.
        client = server.app.test_client()

        def _boom(_engine, _token):
            raise RuntimeError("simulated context failure")

        with patch.object(server, "get_engine", return_value=object()), \
             patch.object(server, "get_verify_email_context", side_effect=_boom), \
             caplog.at_level(logging.WARNING, logger=server.otelLogger.name):
            resp = client.get("/verify-email?token=any-token")

        # The page must still render (user-facing fallback preserved).
        assert resp.status_code == 200
        # And the warning must have been emitted (no silent swallow).
        warnings = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "verify-email context lookup failed" in r.getMessage()
        ]
        assert warnings, (
            "Expected a WARNING log line from verify_email_page when "
            "get_verify_email_context raises, but none was captured. "
            f"Captured: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
        )
