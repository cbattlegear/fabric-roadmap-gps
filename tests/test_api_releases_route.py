"""Integration tests for the ``/api/releases`` route.

Verifies the refactored route still produces the same JSON envelope shape,
status codes, and Link header behavior. DB calls are patched with
in-memory fakes so no SQL Server / network is required.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from lib.description_format import render_description_html


@pytest.fixture
def server_module(monkeypatch):
    """Import server.py with environment configured for offline test use."""
    monkeypatch.setenv("CURRENT_ENVIRONMENT", "development")
    monkeypatch.setenv("SQLSERVER_CONN", "sqlite:///:memory:")
    import server  # noqa: WPS433 — late import after env setup
    return server


@pytest.fixture
def client(server_module):
    server_module.app.config.update(TESTING=True)
    with server_module.app.test_client() as c:
        yield c


def _row(**overrides):
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


def test_api_releases_list_envelope_shape(client, server_module):
    rows = [_row(release_item_id=f"r{i}") for i in range(3)]
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "count_recently_modified_releases", return_value=120), \
         patch.object(server_module, "get_recently_modified_releases", return_value=rows):
        resp = client.get("/api/releases?page=2&page_size=10")

    assert resp.status_code == 200
    assert resp.mimetype == "application/json"
    body = json.loads(resp.data)
    assert set(body.keys()) == {"data", "pagination", "links"}
    assert len(body["data"]) == 3
    assert body["pagination"] == {
        "page": 2, "page_size": 10, "total_items": 120, "total_pages": 12,
        "has_next": True, "has_prev": True, "next_page": 3, "prev_page": 1,
    }
    assert "/api/releases?" in body["links"]["self"]
    assert "page=2" in body["links"]["self"]
    assert body["links"]["next"] is not None
    assert body["links"]["prev"] is not None
    # Row shape — distance must be absent for regular (non-vector) rows
    assert "distance" not in body["data"][0]
    assert body["data"][0]["release_date"] == "Q2 2024"

    # Cache + Link headers
    assert "Cache-Control" in resp.headers
    link_hdr = resp.headers.get("Link", "")
    assert 'rel="next"' in link_hdr
    assert 'rel="prev"' in link_hdr


def test_api_releases_single_item_path(client, server_module):
    row = _row(release_item_id="abc-123")
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "get_release_item_by_id", return_value=row):
        resp = client.get("/api/releases?release_item_id=abc-123")

    assert resp.status_code == 200
    body = json.loads(resp.data)
    # Single-item path: NO envelope, just the row dict
    assert "data" not in body
    assert "pagination" not in body
    assert body["release_item_id"] == "abc-123"
    assert body["release_date"] == "Q2 2024"


def test_api_and_release_page_share_read_only_description_formatting(client, server_module):
    source = (
        "# Header## ControlsThis setting is **off by default**."
        '<script>alert(1)</script>'
    )
    row = _row(release_item_id="formatted", feature_description=source, row_hash="unchanged")
    original = vars(row).copy()
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "get_release_item_by_id", return_value=row), \
         patch.object(server_module, "fetch_history_rows", return_value=[]):
        api = client.get("/api/releases?release_item_id=formatted").get_json()
        page = client.get("/release/formatted")
    assert api["feature_description"] == source
    assert api["feature_description_html"] == render_description_html(source)
    description = BeautifulSoup(page.data, "html.parser").select_one(".change-description")
    assert str(description.h2) == "<h2>Controls</h2>"
    assert description.strong.get_text() == "off by default"
    assert description.script is None
    assert vars(row) == original


def test_list_api_also_supplies_sanitized_description_html(client, server_module):
    row = _row(feature_description='**Details** <img src=x onerror=alert(1)>')
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "count_recently_modified_releases", return_value=1), \
         patch.object(server_module, "get_recently_modified_releases", return_value=[row]):
        item = client.get("/api/releases").get_json()["data"][0]
    assert item["feature_description"] == row.feature_description
    assert item["feature_description_html"] == "<p><strong>Details</strong> </p>"


@pytest.mark.parametrize("heading", ["**Exact title**", "<h2>Exact title</h2>"])
def test_api_and_detail_deduplicate_only_the_matching_description_heading(client, server_module, heading):
    source = heading + "\n\n**Overview**Enable **safe** formatting."
    row = _row(release_item_id="formatted", feature_name="Exact title", feature_description=source)
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "get_release_item_by_id", return_value=row), \
         patch.object(server_module, "fetch_history_rows", return_value=[]):
        api = client.get("/api/releases?release_item_id=formatted").get_json()
        page = BeautifulSoup(client.get("/release/formatted").data, "html.parser")
    expected = render_description_html(source, feature_name=row.feature_name)
    assert api["feature_description"] == row.feature_description == source
    assert api["feature_description_html"] == expected
    assert page.select_one(".change-description").decode_contents().strip() == expected
    assert "Exact title" not in BeautifulSoup(expected, "html.parser").get_text()


def test_homepage_versions_changed_immutable_assets(client):
    page = BeautifulSoup(client.get("/").data, "html.parser")
    assert page.select_one('script[src="/static/app.js?v=description-boundaries-v4"][defer]')
    assert page.select_one('script[src="/static/blog-feedback.js?v=article-activation-v2"][defer]')
    assert page.select_one('link[href="/static/styles.css?v=section-navigation-v1"]')
    assert not page.select_one('script[src="/static/app.js"]')
    assert not page.select_one('script[src="/static/markdown-it.min.js"]')


def test_homepage_places_filters_directly_after_compact_hero(client):
    response = client.get("/")
    assert response.status_code == 200
    page = BeautifulSoup(response.data, "html.parser")
    main = page.select_one("main")
    hero = main.select_one(".hero-section.hero-home")
    assert hero is not None
    assert hero.h1.get_text(strip=True) == "Microsoft Fabric Roadmap Tracker"
    assert hero.select_one(".hero-subtitle").get_text(strip=True) == (
        "Track features, release dates, and recent changes."
    )
    assert [link.get_text(strip=True) for link in hero.select("a")] == ["Get email updates"]
    assert hero.a["href"] == "/subscribe"
    assert hero.select_one(".home-hero-copy") is not None
    assert [child.name for child in main.children if child.name] == ["section", "div"]
    content = hero.find_next_sibling("div")
    assert content.select_one(":scope > .recent-changes-section") is not None
    assert main.select(".card-grid, .card, form, input[type=email]") == []
    assert not main.select('a[href="/rss"], a[href="/rss.xml"], a[href="/api/releases"]')
    assert [node.get("id") for node in main.select(
        ".filters-container, #changes-grid, #load-more-btn, #infinite-scroll-sentinel"
    )] == [None, "changes-grid", "load-more-btn", "infinite-scroll-sentinel"]
    for control in (
        "product-filter", "type-filter", "status-filter", "days-filter",
        "sort-select", "search-input", "include-inactive", "clear-filters",
        "views-toggle", "saved-views-panel",
    ):
        assert main.select_one(f"#{control}") is not None
    assert [link["href"] for link in page.select("footer a")] == [
        "/rss", "/endpoints", "/subscribe", "/privacy",
    ]


def test_homepage_cta_focus_stays_inside_clipped_container(client):
    css = client.get("/static/styles.css").get_data(as_text=True)
    focus_rule = css.split(".hero-home .home-email-cta:focus-visible {", 1)[1].split("}", 1)[0]
    assert "outline: 2px solid var(--fabric-primary)" in focus_rule
    assert "outline-offset: -4px" in focus_rule


@pytest.mark.parametrize("path", ["/changelog", "/subscribe", "/endpoints"])
def test_compact_homepage_hero_does_not_replace_other_page_heroes(client, server_module, path):
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "get_distinct_values", return_value=[]):
        response = client.get(path)
    assert response.status_code == 200
    page = BeautifulSoup(response.data, "html.parser")
    assert page.select_one(".hero-section") is not None
    assert not page.select(".hero-home, .home-hero-content, .home-email-cta")
    if path == "/subscribe":
        assert page.select_one('input[type="email"]') is not None
        assert page.select_one('input[name="cadence"][value="daily"]') is not None
        assert page.select_one('input[name="cadence"][value="weekly"]') is not None
        assert page.select_one("#manage") is not None
    if path == "/endpoints":
        assert "/api/releases" in response.get_data(as_text=True)


def test_api_releases_single_item_not_found_returns_404(client, server_module):
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "get_release_item_by_id", return_value=None):
        resp = client.get("/api/releases?release_item_id=nope")

    assert resp.status_code == 404
    body = json.loads(resp.data)
    assert body == {"error": "release_item_id not found"}


def test_api_releases_first_page_no_link_header_when_total_fits(client, server_module):
    rows = [_row()]
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "count_recently_modified_releases", return_value=1), \
         patch.object(server_module, "get_recently_modified_releases", return_value=rows):
        resp = client.get("/api/releases")

    assert resp.status_code == 200
    body = json.loads(resp.data)
    assert body["links"]["next"] is None
    assert body["links"]["prev"] is None
    assert "Link" not in resp.headers


def test_api_releases_vector_search_path_includes_distance(client, server_module):
    vector_row = {
        "release_item_id": "vr1",
        "feature_name": "VFeat",
        "release_date": date(2025, 1, 1),
        "release_type": "Preview",
        "release_status": "Planned",
        "product_id": "p9",
        "product_name": "Fabric",
        "feature_description": "vd",
        "blog_title": None,
        "blog_url": None,
        "last_modified": datetime(2025, 1, 2, 9, 0, 0),
        "active": True,
        "distance": 0.123456789,
    }
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "embeddings_available", return_value=True), \
         patch.object(server_module, "get_embedding", return_value=[0.0] * 1536), \
         patch.object(server_module, "count_vector_search_releases", return_value=1), \
         patch.object(server_module, "vector_search_releases", return_value=[vector_row]):
        resp = client.get("/api/releases?q=test")

    assert resp.status_code == 200
    body = json.loads(resp.data)
    assert body["data"][0]["distance"] == 0.1235
    assert body["data"][0]["release_date"] == "Q1 2025"


def test_api_releases_q_skipped_when_embeddings_unavailable(client, server_module):
    """When embeddings aren't available, q falls back to plain text search
    via ``get_recently_modified_releases`` (q is passed through as-is)."""
    rows = [_row()]
    captured = {}

    def fake_get(engine, **kwargs):
        captured.update(kwargs)
        return rows

    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "embeddings_available", return_value=False), \
         patch.object(server_module, "count_recently_modified_releases", return_value=1), \
         patch.object(server_module, "get_recently_modified_releases", side_effect=fake_get):
        resp = client.get("/api/releases?q=hello")

    assert resp.status_code == 200
    assert captured.get("q") == "hello"
