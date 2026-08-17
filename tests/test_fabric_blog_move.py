"""Tests for the Fabric blog move to community.fabric.microsoft.com.

Covers:
- scraper RSS/board URLs point straight at the community site
- RSS ``<description>`` HTML stripping
- canonical (post-redirect) URL resolution in ``scrape_from_rss``
- retirement of the legacy full-load mode
- ``migrate_blog_urls`` redirect resolution and row-migration branches

No live SQL Server, network, or secrets: ``requests`` and ``pyodbc`` are
mocked throughout.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import pytest
import requests


OLD_URL = "https://blog.fabric.microsoft.com/en-us/blog/sample-post/"
FINAL_URL = "https://community.fabric.microsoft.com/blog/fbc_fabricupdatesblogs/sample-post/123"
FEED_URL = "https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/sample-post/ba-p/123"

COMMUNITY_FEED = """<?xml version="1.0"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Fabric Updates Blog</title>
    <item>
      <title>Sample Post</title>
      <link>https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/sample-post/ba-p/123</link>
      <description><![CDATA[<P>Hello &amp; welcome to the <B>community</B> site.</P>]]></description>
      <pubDate>Tue, 01 Jan 2025 00:00:00 GMT</pubDate>
      <dc:creator>someuser</dc:creator>
    </item>
  </channel>
</rss>
"""


@pytest.fixture
def scraper_module():
    os.environ.setdefault("SQLSERVER_CONN", "Driver={Test};Server=test;")
    import scrape_fabric_blog
    return scrape_fabric_blog


@pytest.fixture
def scraper(scraper_module):
    from lib.rate_limit import SlidingWindowLimiter
    s = scraper_module.FabricBlogScraper(
        rate_limiter=SlidingWindowLimiter(max_calls=1000, window_seconds=60)
    )
    s.session = MagicMock()
    return s


@pytest.fixture
def migrate_module():
    os.environ.setdefault("SQLSERVER_CONN", "Driver={Test};Server=test;")
    import migrate_blog_urls
    return migrate_blog_urls


# ---------------------------------------------------------------------------
# Scraper: community site wiring
# ---------------------------------------------------------------------------

def test_community_urls_are_the_defaults(scraper):
    assert scraper.rss_url == (
        "https://community.fabric.microsoft.com/t5/s/rss/board"
        "?board.id=fbc_fabricupdatesblogs"
    )
    assert scraper.base_url == (
        "https://community.fabric.microsoft.com"
        "/t5/Fabric-Updates-Blog/bg-p/fbc_fabricupdatesblogs"
    )


def test_full_load_is_retired(scraper):
    with pytest.raises(RuntimeError, match="no longer supported"):
        scraper.scrape_all_pages(1, 5)


def test_strip_html_to_text(scraper_module):
    strip = scraper_module._strip_html_to_text
    assert strip("<P>Hello &amp; welcome</P>") == "Hello & welcome"
    assert strip("  a  b \n c ") == "a b c"
    assert strip("") == ""
    assert strip(None) == ""


def test_extract_articles_from_rss_strips_html_description(scraper):
    articles = scraper.extract_articles_from_rss(ET.fromstring(COMMUNITY_FEED))

    assert len(articles) == 1
    article = articles[0]
    assert article["title"] == "Sample Post"
    assert article["url"] == FEED_URL
    assert article["summary"] == "Hello & welcome to the community site."
    assert article["author"] == "someuser"
    assert article["post_date"] is not None


def test_scrape_from_rss_canonicalizes_new_article_url(scraper):
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(COMMUNITY_FEED))
    scraper.article_exists = MagicMock(return_value=False)
    scraper.fetch_article_details = MagicMock(return_value={
        "categories": None,
        "author": None,
        "final_url": FINAL_URL,
    })
    scraper.upsert_article_with_retry = MagicMock()

    scraper.scrape_from_rss()

    article = scraper.upsert_article_with_retry.call_args[0][0]
    assert article["url"] == FINAL_URL


def test_scrape_from_rss_existing_article_not_fetched(scraper):
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(COMMUNITY_FEED))
    scraper.article_exists = MagicMock(return_value=True)
    scraper.fetch_article_details = MagicMock()
    scraper.upsert_article_with_retry = MagicMock()

    scraper.scrape_from_rss()

    scraper.fetch_article_details.assert_not_called()
    article = scraper.upsert_article_with_retry.call_args[0][0]
    assert article["url"] == FEED_URL


def test_scrape_from_rss_fetch_failure_keeps_feed_url(scraper):
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(COMMUNITY_FEED))
    scraper.article_exists = MagicMock(return_value=False)
    scraper.fetch_article_details = MagicMock(return_value=None)
    scraper.upsert_article_with_retry = MagicMock()

    scraper.scrape_from_rss()

    article = scraper.upsert_article_with_retry.call_args[0][0]
    assert article["url"] == FEED_URL


# ---------------------------------------------------------------------------
# migrate_blog_urls
# ---------------------------------------------------------------------------

def _resp(status: int, url: str = FINAL_URL, headers: dict | None = None) -> MagicMock:
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.url = url
    r.headers = headers or {}
    return r


def test_fetch_final_url_returns_redirected_url(migrate_module):
    session = MagicMock()
    session.get.return_value = _resp(200)

    result = migrate_module.fetch_final_url(
        session, OLD_URL, timeout=5
    )

    assert result == FINAL_URL
    session.get.assert_called_once_with(OLD_URL, timeout=5)


def test_fetch_final_url_404_returns_none(migrate_module):
    session = MagicMock()
    session.get.return_value = _resp(404)

    assert migrate_module.fetch_final_url(session, OLD_URL, timeout=5) is None


def test_fetch_final_url_429_retries_then_succeeds(migrate_module, monkeypatch):
    monkeypatch.setattr(migrate_module.time, "sleep", lambda *_args: None)
    session = MagicMock()
    session.get.side_effect = [
        _resp(429, headers={"Retry-After": "0"}),
        _resp(200),
    ]

    result = migrate_module.fetch_final_url(
        session, OLD_URL, timeout=5, backoff_base=0.0, backoff_max=0.0
    )

    assert result == FINAL_URL
    assert session.get.call_count == 2


def _migration(migrate_module, *, dry_run: bool = False) -> "migrate_module.BlogUrlMigration":
    return migrate_module.BlogUrlMigration(
        "Driver={Test};Server=test;",
        dry_run=dry_run,
        session=MagicMock(),
    )


def test_migrate_row_updates_in_place_when_no_survivor(migrate_module):
    migration = _migration(migrate_module)
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None  # no survivor row
    mock_cursor.rowcount = 2
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(migrate_module.pyodbc, "connect", return_value=mock_conn):
        result = migration._migrate_row(7, OLD_URL, FINAL_URL, has_vector=True)

    assert result == "updated"
    executed = [call.args for call in mock_cursor.execute.call_args_list]
    # Release re-point happens, and the row URL is rewritten in place.
    assert ("UPDATE release_items SET blog_url = ? WHERE blog_url = ?", (FINAL_URL, OLD_URL)) in executed
    assert any(sql.startswith("UPDATE fabric_blog_posts SET url") for sql, _ in executed)
    assert not any("DELETE" in sql for sql, _ in executed)
    mock_conn.commit.assert_called()


def test_migrate_row_deletes_duplicate_and_carries_vector(migrate_module):
    migration = _migration(migrate_module)
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (99, True)  # survivor id=99, vector IS NULL
    mock_cursor.rowcount = 1
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(migrate_module.pyodbc, "connect", return_value=mock_conn):
        result = migration._migrate_row(7, OLD_URL, FINAL_URL, has_vector=True)

    assert result == "deleted"
    executed = [call.args for call in mock_cursor.execute.call_args_list]
    # Survivor has no vector and the old row does → vector is carried over
    # before the old row is deleted.
    assert any(sql.startswith("UPDATE fabric_blog_posts SET blog_vector") for sql, _ in executed)
    assert ("DELETE FROM fabric_blog_posts WHERE id = ?", (7,)) in executed
    assert not any(sql.startswith("UPDATE fabric_blog_posts SET url") for sql, _ in executed)
    mock_conn.commit.assert_called()


def test_migrate_row_dry_run_performs_no_writes(migrate_module):
    migration = _migration(migrate_module, dry_run=True)
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        (99, True),  # survivor check
        (0,),        # release count
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(migrate_module.pyodbc, "connect", return_value=mock_conn):
        result = migration._migrate_row(7, OLD_URL, FINAL_URL, has_vector=True)

    assert result == "deleted"
    executed = [call.args for call in mock_cursor.execute.call_args_list]
    for sql, _params in executed:
        assert not sql.strip().upper().startswith("UPDATE")
        assert not sql.strip().upper().startswith("DELETE")


def test_run_skips_redirect_landing_outside_community(migrate_module):
    migration = _migration(migrate_module)
    migration._fetch_pending_rows = MagicMock(return_value=[(7, OLD_URL, True)])
    migration._fetch_orphaned_release_urls = MagicMock(return_value=[])
    migration._migrate_row = MagicMock(return_value="updated")

    with patch.object(migrate_module, "fetch_final_url", return_value="https://example.com/elsewhere"):
        stats = migration.run()

    assert stats["pending_rows"] == 1
    assert stats["skipped"] == 1
    assert stats["updated"] == 0
    assert stats["deleted"] == 0
    migration._migrate_row.assert_not_called()


def test_run_migrates_rows_and_orphaned_release_urls(migrate_module):
    migration = _migration(migrate_module)
    migration._fetch_pending_rows = MagicMock(return_value=[(7, OLD_URL, True)])
    migration._fetch_orphaned_release_urls = MagicMock(return_value=[OLD_URL + "orphan/"])
    migration._migrate_row = MagicMock(return_value="updated")
    migration._repoint_release_url = MagicMock(return_value=3)

    with patch.object(migrate_module, "fetch_final_url", return_value=FINAL_URL):
        stats = migration.run()

    assert stats["updated"] == 1
    assert stats["orphaned_releases"] == 3
    migration._repoint_release_url.assert_called_once_with(OLD_URL + "orphan/", FINAL_URL)
