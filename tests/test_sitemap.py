"""Tests for sitemap rendering — XML escape (M11) and structure.

Importing ``server`` requires ``CURRENT_ENVIRONMENT=development`` (which
``conftest.py`` sets) so the production-only env-var checks don't trip.
"""

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch
import xml.etree.ElementTree as ET

import pytest

import server as server_module
from server import _render_sitemap_xml, _STATIC_SITEMAP_PAGES


def _row(release_item_id, last_modified=None):
    return SimpleNamespace(release_item_id=release_item_id, last_modified=last_modified)


class TestRenderSitemapXml:
    def test_returns_well_formed_xml(self):
        body = _render_sitemap_xml("https://example.com", [_row("guid-1", date(2026, 4, 22))])
        # ElementTree raises on malformed XML.
        ET.fromstring(body)

    def test_includes_static_pages(self):
        body = _render_sitemap_xml("https://example.com", [])
        for p in _STATIC_SITEMAP_PAGES:
            assert f"https://example.com{p['loc']}" in body

    def test_includes_release_pages(self):
        rows = [_row("guid-aaa", date(2026, 4, 1)), _row("guid-bbb", date(2026, 4, 2))]
        body = _render_sitemap_xml("https://example.com", rows)
        assert "https://example.com/release/guid-aaa" in body
        assert "https://example.com/release/guid-bbb" in body
        assert "<lastmod>2026-04-01</lastmod>" in body
        assert "<lastmod>2026-04-02</lastmod>" in body

    def test_release_id_with_xml_special_chars_is_escaped(self):
        # Defense-in-depth: today release_item_id is a Fabric API GUID, so
        # this can't happen in practice. But the previous code wrote the ID
        # straight into XML, which would corrupt the document if a future
        # ID format ever included &, <, >, or ".
        evil_id = 'a&b<c>d"e'
        body = _render_sitemap_xml("https://example.com", [_row(evil_id)])

        # Body must still parse as XML.
        root = ET.fromstring(body)

        # Find the offending <loc>; it should contain the literal evil ID
        # *after* parsing (i.e. the raw bytes are escaped).
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        locs = [el.text for el in root.findall(".//sm:loc", ns)]
        assert f"https://example.com/release/{evil_id}" in locs

        # And the raw body must NOT contain the unescaped < / > / & sequence
        # that would have broken the XML.
        assert "<c>" not in body
        assert 'a&b' not in body  # would be a&amp;b after escape

    def test_base_url_with_xml_special_chars_is_escaped(self):
        # Equally defensive — BASE_URL is operator-controlled, but if it
        # ever held an ampersand (e.g. from a misconfigured query string)
        # the XML would have broken silently before this fix.
        body = _render_sitemap_xml("https://example.com/?a=1&b=2", [])
        ET.fromstring(body)  # must still parse
        assert "&amp;" in body

    def test_row_with_no_last_modified_omits_lastmod_tag(self):
        body = _render_sitemap_xml("https://example.com", [_row("guid", last_modified=None)])
        # The release URL must still be present, but no lastmod element for it.
        assert "https://example.com/release/guid" in body
        # Static pages don't emit lastmod either, so a count of 0 is correct.
        assert "<lastmod>" not in body

    def test_empty_release_list_still_includes_static_pages(self):
        body = _render_sitemap_xml("https://example.com", [])
        root = ET.fromstring(body)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = root.findall(".//sm:url", ns)
        assert len(urls) == len(_STATIC_SITEMAP_PAGES)

    def test_static_pages_have_priority_and_changefreq(self):
        body = _render_sitemap_xml("https://example.com", [])
        root = ET.fromstring(body)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = root.findall(".//sm:url", ns)
        for url in urls:
            assert url.find("sm:loc", ns) is not None
            assert url.find("sm:changefreq", ns) is not None
            assert url.find("sm:priority", ns) is not None


# ---------------------------------------------------------------------------
# Route-level tests for /sitemap.xml — M11 (escape) + M12 (Cache-Control TTL)
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    server_module.app.config.update(TESTING=True)
    with server_module.app.test_client() as c:
        yield c


def test_sitemap_route_sets_one_hour_cache_control(client):
    """M12: /sitemap.xml advertises a 1-hour TTL so Front Door + bots cache."""
    rows = [SimpleNamespace(release_item_id="guid-1", last_modified=date(2026, 4, 22))]
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "get_active_releases_for_sitemap", return_value=rows):
        resp = client.get("/sitemap.xml")

    assert resp.status_code == 200
    cache_control = resp.headers.get("Cache-Control", "")
    assert "public" in cache_control
    assert "max-age=3600" in cache_control


def test_sitemap_route_escapes_release_item_id(client):
    """M11: defense-in-depth — XML-special chars in IDs round-trip safely."""
    evil_id = 'a&b<c>d"e'
    rows = [SimpleNamespace(release_item_id=evil_id, last_modified=date(2026, 4, 22))]
    with patch.object(server_module, "get_engine", return_value=object()), \
         patch.object(server_module, "get_active_releases_for_sitemap", return_value=rows):
        resp = client.get("/sitemap.xml")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)

    # Body must parse as XML and contain the evil ID after parsing.
    root = ET.fromstring(body)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = [el.text for el in root.findall(".//sm:loc", ns)]
    assert any(loc and loc.endswith(f"/release/{evil_id}") for loc in locs)

    # Raw bytes must be escaped — no unescaped <c> or & sequence.
    assert "<c>" not in body
