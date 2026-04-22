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
    assert body["data"][0]["release_date"] == "2024-06-15"

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
    assert body["release_date"] == "2024-06-15"


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
    assert body["data"][0]["release_date"] == "2025-01-01"


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
