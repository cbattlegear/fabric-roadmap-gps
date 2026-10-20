"""Connection-first documentation follows real public contracts, without DB reads."""

from datetime import date
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup
import pytest

import server
from lib.releases_api import (
    ReleasesQuery, _build_pagination_meta, _format_release_row, _parse_releases_query,
)


@pytest.fixture
def page():
    with patch.object(server, "get_engine", side_effect=AssertionError("Docs must not query SQL")), \
         patch.object(server, "EMAIL_BASE_URL", "https://gps.example/"):
        response = server.app.test_client().get("/endpoints")
    assert response.status_code == 200
    return BeautifulSoup(response.data, "html.parser")


def test_connection_starters_and_reference_order(page):
    assert page.select_one(".hero-subtitle").get_text(strip=True) == "Access Fabric roadmap data."
    assert [section["id"] for section in page.select(".api-docs > section")] == [
        "rss", "rest", "mcp", "releases", "insights", "history", "changelog",
        "filter-options", "email", "caching",
    ]
    for nav in page.select(".api-docs nav"):
        for link in nav.select("a"):
            assert page.select_one(link["href"])
    assert page.select_one(".header-nav [aria-current]")["href"] == "/endpoints"
    assert page.select_one('a[href="/subscribe#manage"]')
    assert page.select_one(".hero-home") is None


def test_exact_copy_targets_and_accessible_scrollers(page):
    expected = {
        "rss-url": "https://gps.example/rss",
        "rest-url": "https://gps.example/api/releases?page_size=10",
        "mcp-url": "https://gps.example/mcp",
        "mcp-command": "claude mcp add --transport http fabric-gps https://gps.example/mcp",
    }
    assert len(page.select("[data-copy-target]")) == 4
    for button in page.select("[data-copy-target]"):
        target = button["data-copy-target"]
        assert page.select_one(f"#{target}").get_text() == expected[target]
        assert button["aria-label"].startswith("Copy ")
        status = page.select_one("#" + button["aria-describedby"])
        assert status["role"] == "status"
        recovery = button.find_parent(attrs={"data-copy-block": True}).select_one("[data-copy-recovery]")
        assert recovery.has_attr("hidden")
        assert recovery.select_one("input[readonly]")
        assert recovery.label["for"] == recovery.input["id"]
    for pre in page.select(".api-docs pre"):
        assert pre["tabindex"] == "0" and pre["role"] == "region" and pre["aria-label"]


def test_examples_use_valid_queries_and_no_executable_placeholder_links(page):
    for link in page.select(".api-docs a[href]"):
        href = link["href"]
        assert "YOUR-ID" not in href and "RELEASE_ID" not in href and "TOKEN" not in href
        query = parse_qs(urlsplit(href).query)
        assert "q" not in query  # No documentation click accidentally generates embeddings.
        if "release_status" in query:
            assert query["release_status"] == ["Shipped"]
        if "release_type" in query:
            assert query["release_type"] == ["General availability"]
        if href.startswith("/api/releases?"):
            args = {key: value[0] for key, value in query.items()}
            parsed = _parse_releases_query(args)
            assert parsed.page >= 1 and 1 <= parsed.page_size <= 200
    assert len(page.select("#rss details a[href^='/rss?']")) == 2
    assert len(page.select("#releases a[href^='/api/releases?']")) == 2


def test_json_examples_are_parseable_and_match_real_response_shapes(page):
    examples = {node["data-example"]: json.loads(node.get_text()) for node in page.select("[data-example]")}
    assert set(examples) == {"releases", "history", "changelog", "filters", "subscription"}
    release = examples["releases"]["data"][0]
    row = dict(release, release_date=date(2026, 12, 31), last_modified=date(2026, 10, 19))
    assert _format_release_row(SimpleNamespace(**row)) == release
    assert examples["releases"]["pagination"] == _build_pagination_meta(ReleasesQuery(page_size=1), 1)
    for entry in examples["history"]:
        assert set(entry) == {"changed_columns", "last_modified"}
        assert isinstance(entry["changed_columns"], list)
    changelog = examples["changelog"]
    assert changelog["total_items"] == sum(day["count"] for day in changelog["days"])
    assert all(day["count"] == len(day["items"]) for day in changelog["days"])
    assert examples["filters"]["release_statuses"] == ["Planned", "Shipped"]
    assert examples["subscription"]["types"] == ["General availability"]
    assert examples["subscription"]["cadence"] in ("daily", "weekly")


def test_cache_documentation_uses_implementation_values_and_explicit_exceptions():
    with patch.object(server, "_FRONT_END_TTL", 7200), patch.object(server, "_STALE_TTL", 180):
        response = server.app.test_client().get("/endpoints")
    page = BeautifulSoup(response.data, "html.parser")
    policy = page.select_one("[data-cache-policy]").get_text()
    assert policy == response.headers["Cache-Control"]
    assert "max-age=7200" in policy and "stale-while-revalidate=180" in policy
    text = page.select_one("#caching").get_text(" ", strip=True)
    assert "120 minutes fresh and 3 minutes" in text
    assert "/api/version" in text and "private/no-store" in text
    assert "without the stale window" in text
    assert "All JSON" not in text and "14400" not in text


@pytest.mark.parametrize("discovery", [[], RuntimeError("unavailable")])
def test_docs_are_available_without_live_or_populated_filter_options(discovery):
    with patch.object(server, "get_distinct_values", side_effect=discovery if isinstance(discovery, Exception) else None,
                      return_value=discovery) as lookup:
        response = server.app.test_client().get("/endpoints")
    assert response.status_code == 200
    lookup.assert_not_called()
    assert b"/api/filter-options" in response.data
    assert b"/api/releases?page_size=10" in response.data


def test_endpoint_configuration_is_escaped_not_injected():
    base = 'https://gps.example/"<script>bad()</script>'
    with patch.object(server, "EMAIL_BASE_URL", base):
        page = BeautifulSoup(server.app.test_client().get("/endpoints").data, "html.parser")
    assert page.select_one("#mcp-url").get_text() == base + "/mcp"
    assert not page.select("#mcp script, #mcp [onclick]")


def test_filter_options_endpoint_preserves_empty_arrays():
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_distinct_values", return_value=[]):
        response = server.app.test_client().get("/api/filter-options")
    assert response.get_json() == {"product_names": [], "release_types": [], "release_statuses": []}


def test_scrolling_is_scoped_and_commands_stay_unwrapped():
    css = (Path(__file__).resolve().parents[1] / "static" / "endpoints.css").read_text()
    assert ".api-docs pre {" in css and "overflow-x: auto" in css and "white-space: pre" in css
    assert ".api-docs pre code" in css and "word-break: normal" in css
    assert ".api-docs :focus-visible" in css
    assert "grid-template-columns: minmax(0, 1fr)" in css
    assert "min-height: 44px" in css
    assert "\n.container " not in css  # Do not weaken shared containment.


def test_copy_interactions():
    node = shutil.which("node")
    assert node, "Install Node.js 22+ to run frontend behavior tests."
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, "--test", str(root / "tests" / "endpoints.test.cjs")],
        cwd=root, text=True, capture_output=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
