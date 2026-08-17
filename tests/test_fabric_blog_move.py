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


def scraper_pyodbc():
    """The scraper module's ``pyodbc`` handle, for patching ``connect``."""
    import scrape_fabric_blog
    return scrape_fabric_blog.pyodbc


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


def test_extract_message_id_handles_both_url_forms():
    from lib.community_urls import extract_message_id, message_id_like_pattern

    assert extract_message_id(FEED_URL) == "123"
    assert extract_message_id(FINAL_URL) == "123"
    assert extract_message_id(FINAL_URL + "/") == "123"
    # Legacy URLs end with a slug, so they have no id to match on.
    assert extract_message_id(OLD_URL) is None
    assert extract_message_id(None) is None
    assert message_id_like_pattern("123") == "%/123"


def test_scrape_from_rss_stores_feed_permalink_for_new_article(scraper):
    """A new article is stored under the feed permalink.

    The URL must not depend on resolving a redirect: the community site's bot
    protection can reject page requests, and a failed fetch previously left a
    row whose URL could never be corrected.
    """
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(COMMUNITY_FEED))
    scraper.find_article_url_by_message_id = MagicMock(return_value=None)
    scraper.article_exists = MagicMock(return_value=False)
    scraper.fetch_article_details = MagicMock(return_value={
        "categories": "Data Factory",
        "author": "Real Name",
    })
    scraper.upsert_article_with_retry = MagicMock()

    scraper.scrape_from_rss()

    article = scraper.upsert_article_with_retry.call_args[0][0]
    assert article["url"] == FEED_URL
    # The feed carries no categories, so the page fills that gap...
    assert article["categories"] == "Data Factory"
    # ...but it does supply an author, which is left alone.
    assert article["author"] == "someuser"


def test_scrape_from_rss_reuses_stored_url_for_migrated_row(scraper):
    """An article already stored under its canonical URL is not duplicated.

    The feed serves the ``/ba-p/<id>`` permalink while a migrated row holds
    the canonical ``/<id>`` URL. Matching on the message id must find that row
    and reuse its URL so the MERGE updates it instead of inserting a second
    row for the same post.
    """
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(COMMUNITY_FEED))
    scraper.find_article_url_by_message_id = MagicMock(return_value=FINAL_URL)
    scraper.article_exists = MagicMock()
    scraper.fetch_article_details = MagicMock()
    scraper.upsert_article_with_retry = MagicMock()

    scraper.scrape_from_rss()

    scraper.find_article_url_by_message_id.assert_called_once_with("123")
    # Known article → no page fetch, and no exact-URL probe is needed.
    scraper.fetch_article_details.assert_not_called()
    scraper.article_exists.assert_not_called()
    article = scraper.upsert_article_with_retry.call_args[0][0]
    assert article["url"] == FINAL_URL


def test_scrape_from_rss_existing_article_not_fetched(scraper):
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(COMMUNITY_FEED))
    scraper.find_article_url_by_message_id = MagicMock(return_value=FEED_URL)
    scraper.fetch_article_details = MagicMock()
    scraper.upsert_article_with_retry = MagicMock()

    scraper.scrape_from_rss()

    scraper.fetch_article_details.assert_not_called()
    article = scraper.upsert_article_with_retry.call_args[0][0]
    assert article["url"] == FEED_URL


def test_scrape_from_rss_stores_article_when_enrichment_fails(scraper):
    """Bot protection on the article page must not block ingestion."""
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(COMMUNITY_FEED))
    scraper.find_article_url_by_message_id = MagicMock(return_value=None)
    scraper.article_exists = MagicMock(return_value=False)
    scraper.fetch_article_details = MagicMock(return_value=None)
    scraper.upsert_article_with_retry = MagicMock()

    scraper.scrape_from_rss()

    scraper.upsert_article_with_retry.assert_called_once()
    article = scraper.upsert_article_with_retry.call_args[0][0]
    assert article["url"] == FEED_URL
    # Feed-sourced values survive even though the page could not be read.
    assert article["title"] == "Sample Post"
    assert article["author"] == "someuser"


def test_find_article_url_by_message_id_matches_url_suffix(scraper):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (FINAL_URL,)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(scraper_pyodbc(), "connect", return_value=mock_conn):
        result = scraper.find_article_url_by_message_id("123")

    assert result == FINAL_URL
    sql, params = mock_cursor.execute.call_args.args
    assert "SELECT TOP 1 url" in sql
    assert params == ("%/123",)


def test_find_article_url_by_message_id_returns_none_when_absent(scraper):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(scraper_pyodbc(), "connect", return_value=mock_conn):
        assert scraper.find_article_url_by_message_id("123") is None


# ---------------------------------------------------------------------------
# Scraper: article page enrichment
# ---------------------------------------------------------------------------

def test_extract_categories_prefers_article_tags(scraper_module):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(
        '<html><head>'
        '<meta property="article:tag" content="Data Factory">'
        '<meta property="article:tag" content="Real-Time Intelligence">'
        '<meta name="keywords" content="ignored">'
        '</head></html>',
        'html.parser',
    )
    assert scraper_module.FabricBlogScraper._extract_categories(soup) == (
        "Data Factory, Real-Time Intelligence"
    )


def test_extract_categories_falls_back_to_keywords_then_labels(scraper_module):
    from bs4 import BeautifulSoup

    extract = scraper_module.FabricBlogScraper._extract_categories

    keywords = BeautifulSoup(
        '<meta name="keywords" content="Power BI, Fabric ,, Power BI">', 'html.parser'
    )
    # Blank entries dropped, duplicates collapsed, order preserved.
    assert extract(keywords) == "Power BI, Fabric"

    labels = BeautifulSoup(
        '<div class="lia-message-labels">'
        '<a href="/t5/x/label-id/99">Announcements</a>'
        '</div>',
        'html.parser',
    )
    assert extract(labels) == "Announcements"

    assert extract(BeautifulSoup('<p>nothing here</p>', 'html.parser')) is None


def test_extract_author_prefers_metadata(scraper_module):
    from bs4 import BeautifulSoup

    extract = scraper_module.FabricBlogScraper._extract_author

    meta = BeautifulSoup('<meta name="author" content="Jane &amp; Co">', 'html.parser')
    assert extract(meta) == "Jane & Co"

    link = BeautifulSoup('<a class="lia-user-name-link">someuser</a>', 'html.parser')
    assert extract(link) == "someuser"

    assert extract(BeautifulSoup('<p>no author</p>', 'html.parser')) is None


def test_fetch_article_details_returns_none_on_bot_protection(scraper):
    scraper._rate_limited_get = MagicMock(
        side_effect=requests.HTTPError("403 Client Error: Forbidden")
    )

    assert scraper.fetch_article_details(FEED_URL) is None


def test_upsert_does_not_blank_out_stored_values(scraper):
    """The delta load must not erase details the feed doesn't carry.

    The community feed supplies no categories and no view count, so a plain
    ``SET categories = ?`` would wipe values gathered from the article page or
    preserved by the URL migration on the very next hourly run.
    """
    cursor = MagicMock()

    scraper.insert_or_update_article(cursor, {
        "title": "Sample Post",
        "url": FEED_URL,
        "categories": None,
        "post_date": None,
        "author": "someuser",
        "views": None,
        "summary": "Summary",
    })

    sql = cursor.execute.call_args.args[0]
    for column in ("categories", "post_date", "author", "views", "summary"):
        assert f"{column} = COALESCE(?, target.{column})" in sql, column
    # The title always reflects the feed.
    assert "title = ?," in sql


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


def _executed_sql(migrate_module, call_method) -> list:
    """Capture the SQL a migration method sends through a mocked pyodbc."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.rowcount = 0
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(migrate_module.pyodbc, "connect", return_value=mock_conn):
        call_method()

    return [call.args[0] for call in mock_cursor.execute.call_args_list]


def test_sql_has_no_boolean_predicate_in_select_list(migrate_module):
    """Guard against a T-SQL syntax error the mocked DB cannot surface.

    SQL Server has no boolean expression type in a select list, so
    ``SELECT (blog_vector IS NULL) AS ...`` fails with "Incorrect syntax near
    the keyword 'IS'". Null checks in a select list must go through CASE.
    """
    migration = _migration(migrate_module)
    statements = (
        _executed_sql(migrate_module, migration._fetch_pending_rows)
        + _executed_sql(migrate_module, migration._fetch_orphaned_release_urls)
        + _executed_sql(
            migrate_module,
            lambda: migration._migrate_row(7, OLD_URL, FINAL_URL, has_vector=True),
        )
    )
    assert statements, "expected the migration to issue SQL"

    for sql in statements:
        select_list = sql.split(" FROM ")[0]
        assert "IS NULL)" not in select_list, sql
        assert "IS NOT NULL)" not in select_list, sql

    pending_sql = _executed_sql(migrate_module, migration._fetch_pending_rows)[0]
    assert "CASE WHEN blog_vector IS NOT NULL THEN 1 ELSE 0 END" in pending_sql


def test_fetch_pending_rows_binds_limit_and_skips_community_urls(migrate_module):
    migration = migrate_module.BlogUrlMigration(
        "Driver={Test};Server=test;", limit=5, session=MagicMock()
    )
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(1, OLD_URL, 1), (2, OLD_URL + "b", 0)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(migrate_module.pyodbc, "connect", return_value=mock_conn):
        rows = migration._fetch_pending_rows()

    sql, params = mock_cursor.execute.call_args.args
    assert "SELECT TOP (?)" in sql
    assert params == (5, "https://community.fabric.microsoft.com%")
    # has_vector is normalized to a bool for the caller.
    assert rows == [(1, OLD_URL, True), (2, OLD_URL + "b", False)]


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
    # CASE WHEN ... THEN 1 ELSE 0 END → the driver returns 1/0, not a bool.
    mock_cursor.fetchone.return_value = (99, FEED_URL, 1)  # survivor id=99, vector IS NULL
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
    # Releases follow the surviving row's own URL, not a URL no row holds.
    assert ("UPDATE release_items SET blog_url = ? WHERE blog_url = ?", (FEED_URL, OLD_URL)) in executed
    mock_conn.commit.assert_called()


def test_migrate_row_finds_survivor_by_message_id(migrate_module):
    """The survivor lookup must match the other community URL form.

    The scraper stores the RSS permalink while the legacy redirect resolves to
    the canonical form. Comparing URL strings would miss the existing row and
    leave both forms behind as duplicates.
    """
    migration = _migration(migrate_module)
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.rowcount = 0
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(migrate_module.pyodbc, "connect", return_value=mock_conn):
        migration._migrate_row(7, OLD_URL, FINAL_URL, has_vector=False)

    sql, params = mock_cursor.execute.call_args_list[0].args
    assert "url LIKE ?" in sql
    # The row being migrated must not match itself.
    assert "id <> ?" in sql
    assert params == ("%/123", 7)


def test_migrate_row_carries_metadata_gaps_from_deleted_duplicate(migrate_module):
    """The legacy row's metadata must not be lost when it is deleted.

    A survivor created from the community RSS feed has no categories and no
    view count, so those gaps are filled from the legacy row. COALESCE puts
    the survivor's own value first, so fresher feed-sourced values win.
    """
    migration = _migration(migrate_module)
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (99, FINAL_URL, 0)  # survivor already has a vector
    mock_cursor.rowcount = 0
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(migrate_module.pyodbc, "connect", return_value=mock_conn):
        migration._migrate_row(7, OLD_URL, FINAL_URL, has_vector=False)

    carry = [
        (sql, params) for sql, params in
        [call.args for call in mock_cursor.execute.call_args_list]
        if sql.startswith("UPDATE target SET")
    ]
    assert len(carry) == 1, "expected exactly one metadata carry-over statement"
    sql, params = carry[0]
    for column in ("categories", "author", "views", "summary", "post_date"):
        assert f"{column} = COALESCE(target.{column}, src.{column})" in sql
    # (source row, target row) — carrying *from* the legacy row *into* the survivor.
    assert params == (7, 99)


def test_migrate_row_dry_run_performs_no_writes(migrate_module):
    migration = _migration(migrate_module, dry_run=True)
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        (99, FINAL_URL, 1),  # survivor check
        (0,),                # release count
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
