"""Tests for ``lib.quarter_date`` (post-2026 ``ReleaseDate`` source format).

The Fabric roadmap source JSON now publishes ``ReleaseDate`` as a
quarter token (e.g. ``"Q4 2024"``) instead of a calendar date. These
tests pin the parsing semantics, the inclusive quarter bounds used for
the "stickiness" check in ``save_releases``, and the display formatter.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from lib.quarter_date import (
    date_to_quarter_string,
    format_as_quarter,
    parse_release_date_value,
    quarter_bounds,
    quarter_end,
)


class TestParseReleaseDateValue:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("Q1 2024", date(2024, 3, 31)),
            ("Q2 2024", date(2024, 6, 30)),
            ("Q3 2024", date(2024, 9, 30)),
            ("Q4 2024", date(2024, 12, 31)),
            ("q4 2025", date(2025, 12, 31)),
            ("Q1-2026", date(2026, 3, 31)),
            ("  Q2 2026  ", date(2026, 6, 30)),
        ],
    )
    def test_quarter_tokens_resolve_to_quarter_end(self, value, expected):
        assert parse_release_date_value(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("03/15/2026", date(2026, 3, 15)),
            ("2026-03-15", date(2026, 3, 15)),
            ("2026-03-15T10:30:00", date(2026, 3, 15)),
        ],
    )
    def test_legacy_date_strings_still_parse(self, value, expected):
        assert parse_release_date_value(value) == expected

    def test_date_instance_passes_through(self):
        d = date(2026, 3, 15)
        assert parse_release_date_value(d) is d

    def test_datetime_truncated_to_date(self):
        dt = datetime(2026, 3, 15, 10, 30, 0)
        assert parse_release_date_value(dt) == date(2026, 3, 15)

    @pytest.mark.parametrize("value", [None, "", "not a date", "Q5 2024", "Q0 2024"])
    def test_invalid_returns_none(self, value):
        assert parse_release_date_value(value) is None


class TestQuarterBounds:
    def test_q1_bounds(self):
        assert quarter_bounds("Q1 2024") == (date(2024, 1, 1), date(2024, 3, 31))

    def test_q4_bounds(self):
        assert quarter_bounds("Q4 2024") == (date(2024, 10, 1), date(2024, 12, 31))

    def test_non_quarter_returns_none(self):
        assert quarter_bounds("2024-06-15") is None
        assert quarter_bounds(None) is None
        assert quarter_bounds(date(2024, 6, 15)) is None

    def test_quarter_end_helper(self):
        assert quarter_end("Q2 2025") == date(2025, 6, 30)
        assert quarter_end("not a quarter") is None


class TestDateToQuarterString:
    @pytest.mark.parametrize(
        "d,expected",
        [
            (date(2024, 1, 1), "Q1 2024"),
            (date(2024, 3, 31), "Q1 2024"),
            (date(2024, 4, 1), "Q2 2024"),
            (date(2024, 6, 15), "Q2 2024"),
            (date(2024, 9, 30), "Q3 2024"),
            (date(2024, 10, 1), "Q4 2024"),
            (date(2024, 12, 31), "Q4 2024"),
        ],
    )
    def test_month_to_quarter_mapping(self, d, expected):
        assert date_to_quarter_string(d) == expected


class TestFormatAsQuarter:
    def test_quarter_token_normalized(self):
        assert format_as_quarter("Q4 2024") == "Q4 2024"
        assert format_as_quarter("q1-2025") == "Q1 2025"

    def test_date_object(self):
        assert format_as_quarter(date(2024, 6, 15)) == "Q2 2024"

    def test_iso_string(self):
        assert format_as_quarter("2024-06-15") == "Q2 2024"

    def test_legacy_string(self):
        assert format_as_quarter("06/15/2024") == "Q2 2024"

    def test_none_and_empty(self):
        assert format_as_quarter(None) is None
        assert format_as_quarter("") is None

    def test_unparseable_returns_none(self):
        assert format_as_quarter("not a date") is None
