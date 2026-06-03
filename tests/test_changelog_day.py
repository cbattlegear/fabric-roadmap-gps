"""Tests for direct daily changelog linking.

Covers:
- ``GET /changelog/<date>`` page route (valid date renders, invalid 404s,
  and the route does not touch the database).
- ``GET /api/changelog?date=...`` exact-date mode wiring: a valid date is
  parsed and forwarded as ``on_date`` (with ``days`` ignored), and an invalid
  date is a 400.
- ``server._parse_iso_date`` strict parsing.

All tests run with no live SQL Server: the DB layer is mocked.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

import server
from server import _parse_iso_date


# ---------------------------------------------------------------------------
# _parse_iso_date
# ---------------------------------------------------------------------------


class TestParseIsoDate:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("2026-06-03", date(2026, 6, 3)),
            ("2000-01-01", date(2000, 1, 1)),
        ],
    )
    def test_valid_dates(self, value, expected):
        assert _parse_iso_date(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "",
            "not-a-date",
            "2026-13-40",   # out-of-range month/day
            "2026-02-30",   # impossible calendar day
            "2026-6-3",     # not zero-padded -> rejected by strict format
            "06-03-2026",   # wrong order
            "2026/06/03",   # wrong separator
            "2026-06-03T00:00:00",  # extra time component
        ],
    )
    def test_invalid_dates_return_none(self, value):
        assert _parse_iso_date(value) is None


# ---------------------------------------------------------------------------
# /changelog/<date_str> page route
# ---------------------------------------------------------------------------


class TestChangelogDayPage:
    def test_valid_date_renders_with_display_date(self):
        client = server.app.test_client()
        # The page is a client-rendered shell; it must NOT hit the database.
        with patch.object(server, "get_engine") as mock_engine:
            resp = client.get("/changelog/2026-06-03")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        # Server-rendered, human-readable date for link previews / SEO.
        assert "June 3, 2026" in body
        # The ISO date is handed to the client script via a data attribute.
        assert 'data-date="2026-06-03"' in body
        mock_engine.assert_not_called()

    @pytest.mark.parametrize(
        "bad",
        ["not-a-date", "2026-13-40", "2026-02-30", "2026-6-3", "2026"],
    )
    def test_invalid_date_returns_404(self, bad):
        client = server.app.test_client()
        resp = client.get(f"/changelog/{bad}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /api/changelog?date=... exact-date mode
# ---------------------------------------------------------------------------


class TestApiChangelogDateMode:
    def test_valid_date_forwards_on_date_and_ignores_days(self):
        captured = {}

        def _fake_changelog(_engine, **kwargs):
            captured.update(kwargs)
            return []

        client = server.app.test_client()
        with patch.object(server, "get_engine", return_value=object()), \
             patch.object(server, "get_changelog_with_changes", side_effect=_fake_changelog):
            resp = client.get("/api/changelog?date=2026-06-03&days=90")

        assert resp.status_code == 200
        assert captured["on_date"] == date(2026, 6, 3)

    def test_no_date_does_not_pass_on_date(self):
        captured = {}

        def _fake_changelog(_engine, **kwargs):
            captured.update(kwargs)
            return []

        client = server.app.test_client()
        with patch.object(server, "get_engine", return_value=object()), \
             patch.object(server, "get_changelog_with_changes", side_effect=_fake_changelog):
            resp = client.get("/api/changelog")

        assert resp.status_code == 200
        # Default (window) callers must not receive on_date, preserving the
        # original query plan / signature.
        assert "on_date" not in captured

    @pytest.mark.parametrize("bad", ["bad", "2026-13-40", "2026-6-3", "2026/06/03"])
    def test_invalid_date_returns_400(self, bad):
        client = server.app.test_client()
        # Must reject before touching the DB.
        with patch.object(server, "get_engine") as mock_engine, \
             patch.object(server, "get_changelog_with_changes") as mock_db:
            resp = client.get(f"/api/changelog?date={bad}")
        assert resp.status_code == 400
        mock_db.assert_not_called()
        mock_engine.assert_not_called()

    def test_date_mode_groups_single_day(self):
        rows = [
            {
                "release_item_id": "id-1",
                "feature_name": "Feature One",
                "product_name": "Power BI",
                "release_type": "Public Preview",
                "release_status": "Planned",
                "release_date": None,
                "last_modified": date(2026, 6, 3),
                "active": True,
                "changed_columns": ["Added to roadmap"],
            }
        ]

        client = server.app.test_client()
        with patch.object(server, "get_engine", return_value=object()), \
             patch.object(server, "get_changelog_with_changes", return_value=rows):
            resp = client.get("/api/changelog?date=2026-06-03")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["total_items"] == 1
        assert len(data["days"]) == 1
        assert data["days"][0]["date"] == "2026-06-03"
        assert data["days"][0]["count"] == 1
