"""Unit tests for the ``/api/releases`` pure helpers in ``lib.releases_api``.

Covers parameter parsing/clamping, row formatting (regular + vector +
NULLs + date serialization), and pagination link/meta math.
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from lib.releases_api import (
    DEFAULT_PAGE_SIZE,
    DEFAULT_SORT,
    MAX_PAGE_SIZE,
    ReleasesQuery,
    _build_pagination_links,
    _build_pagination_meta,
    _format_release_row,
    _parse_releases_query,
)


# ---------------------------------------------------------------------------
# _parse_releases_query
# ---------------------------------------------------------------------------


def test_parse_defaults_for_empty_args():
    q = _parse_releases_query({})
    assert q == ReleasesQuery()
    assert q.page == 1
    assert q.page_size == DEFAULT_PAGE_SIZE
    assert q.sort == DEFAULT_SORT
    assert q.include_inactive is False
    assert q.modified_within_days is None
    assert q.q is None
    assert q.release_item_id is None


def test_parse_clamps_page_size_above_max():
    q = _parse_releases_query({"page_size": "9999"})
    assert q.page_size == MAX_PAGE_SIZE


def test_parse_clamps_page_size_below_one():
    # page_size=0 falls back to default (matches original ``or 50`` semantics)
    q = _parse_releases_query({"page_size": "0"})
    assert q.page_size == DEFAULT_PAGE_SIZE
    # negative values get clamped up to 1
    q2 = _parse_releases_query({"page_size": "-5"})
    assert q2.page_size == 1


def test_parse_clamps_page_below_one():
    q = _parse_releases_query({"page": "0"})
    assert q.page == 1
    q2 = _parse_releases_query({"page": "-3"})
    assert q2.page == 1


def test_parse_invalid_int_falls_back_to_defaults():
    q = _parse_releases_query({"page": "abc", "page_size": "xyz"})
    assert q.page == 1
    assert q.page_size == DEFAULT_PAGE_SIZE


def test_parse_invalid_sort_falls_back_to_default():
    q = _parse_releases_query({"sort": "bogus"})
    assert q.sort == DEFAULT_SORT


def test_parse_valid_sort_preserved():
    q = _parse_releases_query({"sort": "release_date"})
    assert q.sort == "release_date"


def test_parse_modified_within_days_clamped_high():
    q = _parse_releases_query({"modified_within_days": "365"})
    assert q.modified_within_days == 30


def test_parse_modified_within_days_clamped_low():
    q = _parse_releases_query({"modified_within_days": "0"})
    assert q.modified_within_days == 1


def test_parse_modified_within_days_invalid_is_none():
    q = _parse_releases_query({"modified_within_days": "not-a-number"})
    assert q.modified_within_days is None


def test_parse_include_inactive_truthy_values():
    for v in ("1", "true", "True", "yes", "YES"):
        assert _parse_releases_query({"include_inactive": v}).include_inactive is True


def test_parse_include_inactive_falsy_values():
    for v in ("0", "false", "no", "", "anything"):
        assert _parse_releases_query({"include_inactive": v}).include_inactive is False


def test_parse_preserves_filter_strings():
    q = _parse_releases_query({
        "product_name": "Power BI",
        "release_type": "GA",
        "release_status": "Launched",
        "q": "bookmarks",
        "release_item_id": "abc-123",
    })
    assert q.product_name == "Power BI"
    assert q.release_type == "GA"
    assert q.release_status == "Launched"
    assert q.q == "bookmarks"
    assert q.release_item_id == "abc-123"


def test_parse_offset_property():
    q = _parse_releases_query({"page": "3", "page_size": "20"})
    assert q.offset == 40


def test_parse_offset_first_page_is_zero():
    q = _parse_releases_query({"page": "1", "page_size": "50"})
    assert q.offset == 0


# ---------------------------------------------------------------------------
# _format_release_row
# ---------------------------------------------------------------------------


def _make_orm_row(**overrides):
    base = dict(
        release_item_id="r1",
        feature_name="Feature One",
        release_date=date(2024, 6, 15),
        release_type="GA",
        release_status="Launched",
        product_id="p1",
        product_name="Power BI",
        feature_description="desc",
        blog_title="Blog",
        blog_url="https://example.com/blog",
        last_modified=datetime(2024, 6, 16, 12, 30, 45),
        active=True,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_format_orm_row_regular():
    row = _make_orm_row()
    out = _format_release_row(row)
    assert out["release_item_id"] == "r1"
    assert out["feature_name"] == "Feature One"
    assert out["release_date"] == "Q2 2024"
    assert out["last_modified"] == "2024-06-16T12:30:45"
    assert out["active"] is True
    assert "distance" not in out


def test_format_dict_row_vector_includes_distance_rounded():
    row = {
        "release_item_id": "r2",
        "feature_name": "F2",
        "release_date": date(2025, 1, 1),
        "release_type": "Preview",
        "release_status": "Planned",
        "product_id": "p2",
        "product_name": "Fabric",
        "feature_description": "d",
        "blog_title": None,
        "blog_url": None,
        "last_modified": datetime(2025, 1, 2, 9, 0, 0),
        "active": True,
        "distance": 0.123456789,
    }
    out = _format_release_row(row)
    assert out["distance"] == 0.1235
    assert out["release_date"] == "Q1 2025"
    assert out["last_modified"] == "2025-01-02T09:00:00"


def test_format_handles_null_dates_and_fields():
    row = _make_orm_row(release_date=None, last_modified=None, blog_title=None, blog_url=None)
    out = _format_release_row(row)
    assert out["release_date"] is None
    assert out["last_modified"] is None
    assert out["blog_title"] is None
    assert out["blog_url"] is None


def test_format_explicit_distance_kwarg_overrides_row():
    row = {"release_item_id": "x", "distance": 0.5}
    out = _format_release_row(row, distance=0.4444444)
    assert out["distance"] == 0.4444


def test_format_distance_kwarg_zero_is_kept():
    row = {"release_item_id": "x"}
    out = _format_release_row(row, distance=0.0)
    assert out["distance"] == 0.0


def test_format_release_date_uses_quarter_format():
    row = _make_orm_row(release_date=date(2024, 12, 31))
    assert _format_release_row(row)["release_date"] == "Q4 2024"


def test_format_dict_row_missing_keys_yields_none():
    out = _format_release_row({})
    for k in (
        "release_item_id", "feature_name", "release_date", "release_type",
        "release_status", "product_id", "product_name", "feature_description",
        "blog_title", "blog_url", "last_modified", "active",
    ):
        assert out[k] is None
    assert "distance" not in out


# ---------------------------------------------------------------------------
# _build_pagination_meta
# ---------------------------------------------------------------------------


def test_pagination_meta_first_page_with_more():
    q = ReleasesQuery(page=1, page_size=10)
    meta = _build_pagination_meta(q, total=25)
    assert meta == {
        "page": 1, "page_size": 10, "total_items": 25, "total_pages": 3,
        "has_next": True, "has_prev": False, "next_page": 2, "prev_page": None,
    }


def test_pagination_meta_last_page():
    q = ReleasesQuery(page=3, page_size=10)
    meta = _build_pagination_meta(q, total=25)
    assert meta["has_next"] is False
    assert meta["has_prev"] is True
    assert meta["next_page"] is None
    assert meta["prev_page"] == 2
    assert meta["total_pages"] == 3


def test_pagination_meta_empty_total_yields_one_page():
    q = ReleasesQuery(page=1, page_size=10)
    meta = _build_pagination_meta(q, total=0)
    assert meta["total_pages"] == 1
    assert meta["total_items"] == 0
    assert meta["has_next"] is False
    assert meta["has_prev"] is False


def test_pagination_meta_exact_page_boundary():
    q = ReleasesQuery(page=2, page_size=10)
    meta = _build_pagination_meta(q, total=20)
    assert meta["total_pages"] == 2
    assert meta["has_next"] is False


def test_pagination_meta_middle_page():
    q = ReleasesQuery(page=2, page_size=10)
    meta = _build_pagination_meta(q, total=35)
    assert meta["total_pages"] == 4
    assert meta["has_next"] is True
    assert meta["has_prev"] is True


# ---------------------------------------------------------------------------
# _build_pagination_links
# ---------------------------------------------------------------------------


def test_links_first_page_has_no_prev():
    q = ReleasesQuery(page=1, page_size=50)
    links = _build_pagination_links(q, total=120)
    assert links["prev"] is None
    assert links["next"] is not None
    assert links["self"].startswith("/api/releases?")
    assert "page=1" in links["self"]


def test_links_last_page_has_no_next():
    q = ReleasesQuery(page=3, page_size=50)
    links = _build_pagination_links(q, total=120)
    assert links["next"] is None
    assert links["prev"] is not None
    assert "page=3" in links["self"]
    assert "page=3" in links["last"]


def test_links_single_page_no_next_prev():
    q = ReleasesQuery(page=1, page_size=50)
    links = _build_pagination_links(q, total=10)
    assert links["next"] is None
    assert links["prev"] is None
    assert links["first"] == links["last"] == links["self"]


def test_links_empty_results_yields_one_page():
    q = ReleasesQuery(page=1, page_size=50)
    links = _build_pagination_links(q, total=0)
    assert links["next"] is None
    assert links["prev"] is None
    assert "page=1" in links["last"]


def test_links_omit_default_sort():
    q = ReleasesQuery(page=1, page_size=50, sort="last_modified")
    links = _build_pagination_links(q, total=10)
    assert "sort=" not in links["self"]


def test_links_include_non_default_sort():
    q = ReleasesQuery(page=1, page_size=50, sort="release_date")
    links = _build_pagination_links(q, total=10)
    assert "sort=release_date" in links["self"]


def test_links_omit_none_and_empty_filters():
    q = ReleasesQuery(page=1, page_size=50, product_name="", release_type=None)
    links = _build_pagination_links(q, total=10)
    assert "product_name=" not in links["self"]
    assert "release_type=" not in links["self"]


def test_links_include_filter_params_url_encoded():
    q = ReleasesQuery(
        page=2, page_size=25, product_name="Power BI",
        release_type="GA", q="hello world", modified_within_days=7,
    )
    links = _build_pagination_links(q, total=200)
    self_link = links["self"]
    assert "product_name=Power+BI" in self_link
    assert "release_type=GA" in self_link
    assert "q=hello+world" in self_link
    assert "modified_within_days=7" in self_link
    assert "page_size=25" in self_link
    assert "page=2" in self_link


def test_links_middle_page_has_both_next_and_prev():
    q = ReleasesQuery(page=2, page_size=10)
    links = _build_pagination_links(q, total=30)
    assert "page=3" in links["next"]
    assert "page=1" in links["prev"]
    assert "page=3" in links["last"]
