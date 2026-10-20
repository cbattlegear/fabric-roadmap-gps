"""Build-time GitHub release snapshots and offline About-page presentation."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


REPOSITORY = "cbattlegear/fabric-roadmap-gps"
RELEASES_URL = f"https://github.com/{REPOSITORY}/releases"
SNAPSHOT_PATH = Path(__file__).resolve().parent.parent / "release_notes.json"
DISPLAY_LIMIT = 3
logger = logging.getLogger(__name__)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Release timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def _web_url(value: str) -> bool:
    if any(ord(char) <= 32 or ord(char) == 127 for char in value):
        return False
    try:
        parsed = urlsplit(value)
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.hostname)
    except ValueError:
        return False


def select_releases(releases: list[dict]) -> list[dict]:
    """Validate stable published releases, ordering by publication, not tag."""
    selected = []
    for release in releases:
        if not isinstance(release, dict):
            raise ValueError("Invalid release object")
        if release.get("draft") is True or release.get("prerelease") is True:
            continue
        if release.get("draft") is not False or release.get("prerelease") is not False:
            raise ValueError("Missing release visibility")
        tag = release.get("tag_name")
        published = release.get("published_at")
        url = release.get("html_url")
        name = release.get("name")
        body = release.get("body")
        if name is None:
            name = tag
        if body is None:
            body = ""
        if not all(isinstance(value, str) for value in (tag, published, url, name, body)):
            raise ValueError("Invalid release fields")
        if not tag.strip() or not _web_url(url) or not url.startswith(RELEASES_URL + "/tag/"):
            raise ValueError("Invalid release tag or source URL")
        selected.append({
            "tag_name": tag,
            "name": name.strip() or tag,
            "published_at": _utc(published).isoformat(),
            "html_url": url,
            "body": body,
            "draft": False,
            "prerelease": False,
        })
    return sorted(selected, key=lambda release: _utc(release["published_at"]), reverse=True)[:DISPLAY_LIMIT]


def refresh_snapshot(path: Path = SNAPSHOT_PATH, *, token: str | None = None) -> None:
    """Replace the snapshot atomically; failures leave last-known-good data intact."""
    releases = []
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "Fabric-GPS-release-notes",
               "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    for page in range(1, 101):
        request = Request(
            f"https://api.github.com/repos/{REPOSITORY}/releases?per_page=100&page={page}",
            headers=headers,
        )
        try:
            with urlopen(request, timeout=15) as response:
                batch = json.load(response)
        except HTTPError as exc:
            raise RuntimeError(f"GitHub release refresh failed (HTTP {exc.code}); snapshot not replaced") from None
        except (URLError, TimeoutError) as exc:
            raise RuntimeError("GitHub release refresh timed out or could not connect; snapshot not replaced") from exc
        if not isinstance(batch, list):
            raise ValueError("GitHub releases response must be a list")
        releases.extend(batch)
        if len(batch) < 100:
            break
    else:
        raise RuntimeError("GitHub releases exceeded the pagination limit; snapshot not replaced")
    snapshot = {
        "schema_version": 1,
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "releases": select_releases(releases),
    }
    temporary = None
    try:
        with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(snapshot, handle, ensure_ascii=True)
            handle.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


@lru_cache(maxsize=1)
def _read_snapshot(path: Path) -> dict | None:
    try:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(snapshot, dict) or snapshot.get("schema_version") != 1:
            raise ValueError("Unsupported release snapshot")
        _utc(snapshot["refreshed_at"])
        if not isinstance(snapshot["releases"], list):
            raise ValueError("Invalid snapshot releases")
        snapshot["releases"] = select_releases(snapshot["releases"])
        return snapshot
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        logger.exception("Application release notes snapshot is unavailable: %s", path)
        return None


def render_release_markdown(source: str) -> str:
    """Disable raw HTML, remote images, and non-web links in release notes."""
    from bs4 import BeautifulSoup
    from markdown_it import MarkdownIt

    markdown = MarkdownIt("commonmark", {"html": False})
    soup = BeautifulSoup(markdown.render(source), "html.parser")
    for image in soup.find_all("img"):
        image.replace_with(image.get("alt", ""))
    for link in soup.find_all("a"):
        if not _web_url(link.get("href", "")):
            link.unwrap()
        else:
            link["rel"] = "noopener noreferrer"
    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        heading.name = f"h{min(int(heading.name[1]) + 3, 6)}"
    return str(soup)


def release_notes_context(path: Path = SNAPSHOT_PATH, *, now: datetime | None = None) -> dict:
    snapshot = _read_snapshot(path)
    context = {"available": snapshot is not None, "releases": [], "url": RELEASES_URL}
    if snapshot is None:
        return context
    refreshed = _utc(snapshot["refreshed_at"])
    context.update(
        refreshed_at=refreshed.isoformat(),
        refreshed_label=refreshed.strftime("%Y-%m-%d %H:%M UTC"),
        stale=(now or datetime.now(timezone.utc)) - refreshed > timedelta(hours=48),
        releases=[{
            **release,
            "publication_date": _utc(release["published_at"]).strftime("%Y-%m-%d"),
            "notes_html": render_release_markdown(release["body"]),
        } for release in snapshot["releases"]],
    )
    return context


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    refresh_snapshot(token=os.environ.get("GITHUB_TOKEN"))
    logger.info("Application release notes snapshot refreshed successfully")
