"""Offline coverage for release snapshot generation and About presentation."""

import io
import json
from datetime import datetime, timezone
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import pytest
from bs4 import BeautifulSoup

from lib import github_releases as notes


def release(tag="v4.4.1", published="2026-10-19T12:00:00Z", **overrides):
    result = {
        "tag_name": tag, "name": f"Release {tag}", "published_at": published,
        "html_url": f"{notes.RELEASES_URL}/tag/{tag}",
        "body": "## Fixed\n\n- **Better** updates.",
        "draft": False, "prerelease": False,
    }
    result.update(overrides)
    return result


def response(rows):
    return io.BytesIO(json.dumps(rows).encode())


@pytest.fixture(autouse=True)
def clear_snapshot_cache():
    notes._read_snapshot.cache_clear()
    yield
    notes._read_snapshot.cache_clear()


def write_snapshot(path, rows, refreshed="2026-10-19T13:00:00Z"):
    path.write_text(json.dumps({
        "schema_version": 1, "refreshed_at": refreshed, "releases": rows,
    }), encoding="utf-8")


def test_filter_sort_limit_and_inconsistent_tags():
    rows = [
        release("v99.0.0", "2026-10-01T00:00:00Z"),
        release("v.4.4.0", "2026-10-17T00:00:00Z"),
        release("v4.4.1"),
        release("draft", draft=True),
        release("preview", prerelease=True),
        release("v4.3.0", "2026-10-18T00:00:00Z"),
    ]
    assert [item["tag_name"] for item in notes.select_releases(rows)] == [
        "v4.4.1", "v4.3.0", "v.4.4.0",
    ]


def test_fallback_fields_and_utc_dates(tmp_path):
    path = tmp_path / "notes.json"
    write_snapshot(path, [release(name=None, body=None, published_at="2026-10-19T01:00:00+02:00")])
    context = notes.release_notes_context(path, now=datetime(2026, 10, 19, 14, tzinfo=timezone.utc))
    item = context["releases"][0]
    assert item["name"] == "v4.4.1"
    assert item["body"] == ""
    assert item["publication_date"] == "2026-10-18"
    assert context["refreshed_label"] == "2026-10-19 13:00 UTC"
    assert not context["stale"]


@pytest.mark.parametrize("override", [
    {"published_at": None}, {"published_at": "invalid"},
    {"published_at": "2026-10-19T00:00:00"},
    {"html_url": "javascript:alert(1)"},
    {"html_url": "https://evil.example/releases/tag/v1"},
    {"tag_name": ""}, {"draft": None}, {"body": ["invalid"]},
    {"name": 0}, {"body": 0},
])
def test_reject_invalid_metadata(override):
    with pytest.raises((ValueError, TypeError)):
        notes.select_releases([release(**override)])


def test_markdown_does_not_execute_html_or_unsafe_links():
    source = (
        '# Heading\n\n<script>alert(1)</script><img src=x onerror=alert(1)>\n\n'
        '[bad](javascript:alert(1)) [data](data:text/html,evil) '
        '[file](file:///etc/passwd) [relative](//evil.example) '
        '[mail](mailto:person@example.com) '
        '[good](https://example.com/path "A title")\n\n'
        '![tracking](https://evil.example/track.png)\n\n'
        '- **Fixed** something\n\n`<script>`\n'
    )
    html = notes.render_release_markdown(source)
    soup = BeautifulSoup(html, "html.parser")
    assert not soup.select("script,img,iframe,svg,style,h1,h2,h3")
    assert soup.h4.get_text() == "Heading"
    assert soup.strong.get_text() == "Fixed"
    assert [a["href"] for a in soup.select("a")] == ["https://example.com/path"]
    assert soup.code.get_text() == "<script>"
    assert "<script>" not in html


def test_successful_refresh_and_edits_and_removals_replace_snapshot(tmp_path):
    path = tmp_path / "notes.json"
    with patch.object(notes, "urlopen", return_value=response([release(), release("old")])) as get:
        notes.refresh_snapshot(path, token="test-secret")
    request = get.call_args.args[0]
    assert request.get_header("Authorization") == "Bearer test-secret"
    assert get.call_args.kwargs["timeout"] == 15
    assert "test-secret" not in path.read_text()
    with patch.object(notes, "urlopen", return_value=response([release(body="Edited")])):
        notes.refresh_snapshot(path)
    snapshot = json.loads(path.read_text())
    assert len(snapshot["releases"]) == 1
    assert snapshot["releases"][0]["body"] == "Edited"
    with patch.object(notes, "urlopen", return_value=response([])):
        notes.refresh_snapshot(path)
    assert json.loads(path.read_text())["releases"] == []


def test_pagination_does_not_stop_before_stable_releases(tmp_path):
    first = [release(str(i), draft=True) for i in range(100)]
    with patch.object(notes, "urlopen", side_effect=[response(first), response([release()])]) as get:
        path = tmp_path / "notes.json"
        notes.refresh_snapshot(path)
    assert get.call_count == 2
    assert get.call_args.args[0].full_url.endswith("page=2")
    assert len(json.loads(path.read_text())["releases"]) == 1


@pytest.mark.parametrize("error", [
    TimeoutError(), URLError("unavailable"),
    HTTPError("https://api.github.com", 403, "rate limit", {}, None),
    HTTPError("https://api.github.com", 429, "rate limit", {}, None),
    HTTPError("https://api.github.com", 500, "server error", {}, None),
])
def test_failure_is_explicit_and_preserves_last_good_snapshot(tmp_path, error):
    path = tmp_path / "notes.json"
    path.write_text("last known good", encoding="utf-8")
    with patch.object(notes, "urlopen", side_effect=error), pytest.raises(RuntimeError, match="snapshot not replaced"):
        notes.refresh_snapshot(path)
    assert path.read_text() == "last known good"
    assert list(tmp_path.iterdir()) == [path]


def test_first_run_failure_does_not_write_success_shaped_snapshot(tmp_path):
    path = tmp_path / "notes.json"
    with patch.object(notes, "urlopen", side_effect=TimeoutError()), pytest.raises(RuntimeError):
        notes.refresh_snapshot(path)
    assert not path.exists()


@pytest.mark.parametrize("payload", ['{"error":"unexpected"}', '[{"draft":false}]', "not json"])
def test_invalid_response_preserves_snapshot(tmp_path, payload):
    path = tmp_path / "notes.json"
    path.write_text("last known good", encoding="utf-8")
    with patch.object(notes, "urlopen", return_value=io.BytesIO(payload.encode())), pytest.raises(ValueError):
        notes.refresh_snapshot(path)
    assert path.read_text() == "last known good"


def test_unavailable_and_invalid_snapshots_log_errors(tmp_path, caplog):
    path = tmp_path / "missing.json"
    assert not notes.release_notes_context(path)["available"]
    assert "snapshot is unavailable" in caplog.text
    notes._read_snapshot.cache_clear()
    path.write_text('{"schema_version":1,"refreshed_at":null}', encoding="utf-8")
    assert not notes.release_notes_context(path)["available"]


def test_stale_snapshot_retains_last_sync_time_and_does_not_make_network_calls(tmp_path):
    path = tmp_path / "notes.json"
    write_snapshot(path, [release()])
    with patch.object(notes, "urlopen", side_effect=AssertionError("No runtime network")):
        context = notes.release_notes_context(path, now=datetime(2026, 10, 22, tzinfo=timezone.utc))
    assert context["stale"]
    assert context["refreshed_label"] == "2026-10-19 13:00 UTC"
    assert len(context["releases"]) == 1


def test_stale_threshold_is_exactly_48_hours(tmp_path):
    path = tmp_path / "notes.json"
    write_snapshot(path, [])
    threshold = datetime(2026, 10, 21, 13, tzinfo=timezone.utc)
    assert not notes.release_notes_context(path, now=threshold)["stale"]
    assert notes.release_notes_context(path, now=threshold.replace(microsecond=1))["stale"]


@pytest.fixture
def server_module(monkeypatch):
    monkeypatch.setenv("CURRENT_ENVIRONMENT", "development")
    monkeypatch.setenv("SQLSERVER_CONN", "sqlite:///:memory:")
    import server
    server.app.config.update(TESTING=True)
    return server


@pytest.mark.parametrize("state", ["fresh", "stale", "empty", "unavailable", "no-body"])
def test_about_renders_bottom_section_and_explicit_states(server_module, tmp_path, state):
    path = tmp_path / "notes.json"
    if state != "unavailable":
        rows = [] if state == "empty" else [release(body="" if state == "no-body" else "**Fixed** safely.")]
        write_snapshot(path, rows)
    now = datetime(2026, 10, 22 if state == "stale" else 19, 14, tzinfo=timezone.utc)
    with patch.object(server_module, "release_notes_context", side_effect=lambda: notes.release_notes_context(path, now=now)), \
         patch.object(notes, "urlopen", side_effect=AssertionError("No runtime network")):
        page = server_module.app.test_client().get("/about")
    assert page.status_code == 200
    assert page.headers["Cache-Control"] == "no-cache"
    soup = BeautifulSoup(page.data, "html.parser")
    section = soup.select_one(".app-release-notes")
    assert section.parent.select(":scope > section")[-1] == section
    assert soup.select_one(".coming-soon-section").find_next_sibling() == section
    assert section.h2.get_text() == "What's New in Fabric GPS"
    assert section.select_one(f'a[href="{notes.RELEASES_URL}"]')
    assert soup.select_one('a[href="/changelog"]')
    text = section.get_text()
    if state == "unavailable":
        assert "temporarily unavailable" in text
    elif state == "empty":
        assert "No stable releases" in text
    else:
        assert section.h3.a["href"] == release()["html_url"]
        assert "2026-10-19 UTC" in text
        if state == "no-body":
            assert "No release notes were provided" in text
        else:
            assert section.strong.get_text() == "Fixed"
    assert ("may be out of date" in text) == (state == "stale")


def test_build_workflow_refreshes_before_docker_and_includes_release_lifecycle():
    workflow = (notes.SNAPSHOT_PATH.parent / ".github" / "workflows" / "docker-publish.yml").read_text()
    assert "types: [published, edited, unpublished, deleted]" in workflow
    assert workflow.index("run: python -m lib.github_releases") < workflow.index("name: Build and push Docker image")
    assert "contents: read" in workflow
    assert "github.event_name == 'release' && github.event.repository.default_branch" in workflow
    assert "value=latest" in workflow
    assert "cron:" in workflow and "workflow_dispatch:" in workflow
    dockerfile = (notes.SNAPSHOT_PATH.parent / "Dockerfile").read_text()
    assert "COPY . /app/" in dockerfile
    assert "release_notes.json" not in (notes.SNAPSHOT_PATH.parent / ".dockerignore").read_text()
