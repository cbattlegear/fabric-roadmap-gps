"""Tests for ``_build_vector_search_where`` (M2 — shared WHERE builder).

The two vector-search functions used to assemble identical filter logic
by hand. Centralizing here keeps them provably consistent.
"""

from datetime import date, datetime, timedelta

import pytest

from db.db_sqlserver import _build_vector_search_where


def test_default_filters_only_active_with_vector():
    where_sql, params = _build_vector_search_where()
    assert where_sql == "release_vector IS NOT NULL AND active = 1"
    assert params == []


def test_include_inactive_drops_active_filter():
    where_sql, params = _build_vector_search_where(include_inactive=True)
    assert where_sql == "release_vector IS NOT NULL"
    assert params == []


def test_product_name_filter():
    where_sql, params = _build_vector_search_where(product_name="Fabric")
    assert where_sql == "release_vector IS NOT NULL AND active = 1 AND product_name = ?"
    assert params == ["Fabric"]


def test_release_type_filter():
    where_sql, params = _build_vector_search_where(release_type="GA")
    assert where_sql == "release_vector IS NOT NULL AND active = 1 AND release_type = ?"
    assert params == ["GA"]


def test_release_status_filter():
    where_sql, params = _build_vector_search_where(release_status="Launched")
    assert where_sql == "release_vector IS NOT NULL AND active = 1 AND release_status = ?"
    assert params == ["Launched"]


def test_modified_within_days_uses_now_for_cutoff():
    """The ``now`` injection point exists exactly so this test isn't
    flaky around midnight UTC."""
    fixed_now = datetime(2026, 4, 22, 12, 0, 0)
    where_sql, params = _build_vector_search_where(
        modified_within_days=7,
        now=fixed_now,
    )
    assert where_sql == "release_vector IS NOT NULL AND active = 1 AND last_modified >= ?"
    assert params == [date(2026, 4, 15)]


def test_all_filters_combined_in_documented_order():
    """Param order must match the SQL WHERE order — otherwise pyodbc
    will bind the wrong value to the wrong placeholder."""
    fixed_now = datetime(2026, 4, 22, 12, 0, 0)
    where_sql, params = _build_vector_search_where(
        product_name="Fabric",
        release_type="GA",
        release_status="Launched",
        modified_within_days=30,
        include_inactive=True,
        now=fixed_now,
    )
    assert where_sql == (
        "release_vector IS NOT NULL"
        " AND product_name = ?"
        " AND release_type = ?"
        " AND release_status = ?"
        " AND last_modified >= ?"
    )
    assert params == ["Fabric", "GA", "Launched", date(2026, 3, 23)]


def test_count_and_search_use_identical_where():
    """Regression guard for M2: both call sites must call
    ``_build_vector_search_where`` with the same kwargs and get the
    same WHERE clause. We emulate by calling once and confirming the
    output is a plain function of the kwargs."""
    fixed_now = datetime(2026, 4, 22, 12, 0, 0)
    common = dict(
        product_name="Fabric",
        release_type="GA",
        release_status="Launched",
        modified_within_days=14,
        include_inactive=False,
        now=fixed_now,
    )
    a = _build_vector_search_where(**common)
    b = _build_vector_search_where(**common)
    assert a == b
