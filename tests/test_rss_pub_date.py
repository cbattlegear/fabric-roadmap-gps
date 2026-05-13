"""Tests for ``scrape_fabric_blog._parse_rss_pub_date``.

The original implementation called ``datetime.strptime`` with
``%a, %d %b %Y %H:%M:%S %z`` which accepts numeric offsets like
``"+0000"`` but rejects named zones like ``"GMT"`` / ``"UTC"`` /
``"EST"``. RSS feeds in the wild publish both. The helper now uses
``email.utils.parsedate_to_datetime`` which handles the full RFC 2822
grammar.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from scrape_fabric_blog import _parse_rss_pub_date


class TestParseRssPubDate:
    def test_named_zone_gmt(self):
        # The exact case that was producing "Could not parse date" warnings.
        dt = _parse_rss_pub_date("Tue, 12 May 2026 17:30:00 GMT")
        assert dt == datetime(2026, 5, 12, 17, 30, 0)
        assert dt.tzinfo is None

    def test_named_zone_utc(self):
        dt = _parse_rss_pub_date("Mon, 16 Dec 2024 14:30:00 UTC")
        assert dt == datetime(2024, 12, 16, 14, 30, 0)

    def test_numeric_offset_zero(self):
        dt = _parse_rss_pub_date("Mon, 16 Dec 2024 14:30:00 +0000")
        assert dt == datetime(2024, 12, 16, 14, 30, 0)

    def test_numeric_offset_nonzero_strips_tz_keeping_wall_time(self):
        # Matches the original implementation's behavior: tzinfo is stripped
        # without converting to UTC. (Out of scope to change this here; the
        # SQL Server column is naive and feeds in practice publish GMT/UTC.)
        dt = _parse_rss_pub_date("Mon, 16 Dec 2024 14:30:00 +0500")
        assert dt.tzinfo is None
        assert dt == datetime(2024, 12, 16, 14, 30, 0)

    def test_named_zone_est_strips_tz_keeping_wall_time(self):
        dt = _parse_rss_pub_date("Mon, 16 Dec 2024 14:30:00 EST")
        assert dt == datetime(2024, 12, 16, 14, 30, 0)

    @pytest.mark.parametrize("value", [None, "", "not a date"])
    def test_invalid_returns_none(self, value):
        assert _parse_rss_pub_date(value) is None
