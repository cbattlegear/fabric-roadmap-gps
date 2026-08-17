"""Tests for full-article content sourced from the community API.

The community's rendered article pages sit behind bot protection (measured:
2 successes in 6 identical requests), and the RSS feed only publishes a short
teaser — empty for some posts. Content therefore comes from the Khoros
Community API instead. These tests cover that client, the scraper's use of it,
and the one-shot backfill for rows ingested before the switch.

No live SQL Server, network, or secrets: ``requests`` and ``pyodbc`` are
mocked throughout.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import pytest
import requests


MID = "5359124"
FEED_URL = f"https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/sample/ba-p/{MID}"
CANON_URL = f"https://community.fabric.microsoft.com/blog/fbc_fabricupdatesblogs/sample/{MID}"

FEED_XML = f"""<?xml version="1.0"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Fabric Updates Blog</title>
    <item>
      <title>Sample Post</title>
      <link>{FEED_URL}</link>
      <description><![CDATA[<P>Short teaser.</P>]]></description>
      <pubDate>Tue, 01 Jan 2025 00:00:00 GMT</pubDate>
      <dc:creator>someuser</dc:creator>
    </item>
  </channel>
</rss>
"""


def _response(payload, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = payload
    return resp


def _client(session):
    from lib.khoros_api import KhorosClient
    return KhorosClient(session=session, max_retries=1)


@pytest.fixture
def scraper_module():
    os.environ.setdefault("SQLSERVER_CONN", "Driver={Test};Server=test;")
    import scrape_fabric_blog
    return scrape_fabric_blog


@pytest.fixture
def scraper(scraper_module):
    from lib.rate_limit import SlidingWindowLimiter
    s = scraper_module.FabricBlogScraper(
        rate_limiter=SlidingWindowLimiter(max_calls=1000, window_seconds=60),
        khoros_client=MagicMock(),
    )
    s.session = MagicMock()
    return s


@pytest.fixture
def backfill_module():
    os.environ.setdefault("SQLSERVER_CONN", "Driver={Test};Server=test;")
    import backfill_blog_content
    return backfill_blog_content


# ---------------------------------------------------------------------------
# KhorosClient
# ---------------------------------------------------------------------------

def test_fetch_article_returns_full_body_and_metadata():
    session = MagicMock()
    session.get.return_value = _response({
        "status": "success",
        "data": {"items": [{
            "id": MID,
            "subject": "Sample Post",
            "body": "<P>Full body</P>",
            "teaser": "<P>Short teaser.</P>",
            "view_href": CANON_URL,
            "post_time": "2026-08-17T09:53:04.884-07:00",
            "author": {"login": "someuser"},
            "metrics": {"views": 576},
        }]},
    })

    article = _client(session).fetch_article(MID)

    assert article["body"] == "<P>Full body</P>"
    assert article["url"] == CANON_URL
    assert article["author"] == "someuser"
    assert article["views"] == 576


@pytest.mark.parametrize("bad_id", ["1' OR '1'='1", "abc", "", "12 OR 1=1", "5359124; DROP"])
def test_message_ids_are_restricted_to_digits(bad_id):
    """Ids are interpolated into LiQL, so anything non-numeric is rejected."""
    session = MagicMock()
    with pytest.raises(ValueError):
        _client(session).fetch_article(bad_id)
    with pytest.raises(ValueError):
        _client(session).fetch_labels(bad_id)
    session.get.assert_not_called()


def test_fetch_labels_returns_names_and_dedupes():
    session = MagicMock()
    session.get.return_value = _response({
        "status": "success",
        "data": {"items": [
            {"text": "Data Factory"},
            {"text": "announcements"},
            {"text": "Data Factory"},
        ]},
    })

    assert _client(session).fetch_labels(MID) == ["Data Factory", "announcements"]


def test_fetch_labels_distinguishes_empty_from_failure():
    """``[]`` means "no labels"; ``None`` means "we could not tell"."""
    empty = MagicMock()
    empty.get.return_value = _response({"status": "success", "data": {"items": []}})
    assert _client(empty).fetch_labels(MID) == []

    failed = MagicMock()
    failed.get.return_value = _response({"status": "error", "message": "nope"})
    assert _client(failed).fetch_labels(MID) is None


def test_api_errors_and_transport_failures_return_none():
    http_error = MagicMock()
    http_error.get.return_value = _response({}, status=503)
    assert _client(http_error).fetch_article(MID) is None

    boom = MagicMock()
    boom.get.side_effect = requests.RequestException("connection reset")
    assert _client(boom).fetch_article(MID) is None


# ---------------------------------------------------------------------------
# Scraper enrichment
# ---------------------------------------------------------------------------

def test_fetch_article_details_uses_body_and_labels(scraper):
    scraper.khoros.fetch_article.return_value = {
        "body": "<P>Hello &amp; welcome to the <B>community</B>.</P>",
        "author": "someuser",
        "views": 42,
    }
    scraper.khoros.fetch_labels.return_value = ["Data Factory", "Lakehouse"]

    details = scraper.fetch_article_details(MID)

    assert details["summary"] == "Hello & welcome to the community ."
    assert details["categories"] == "Data Factory, Lakehouse"
    assert details["views"] == 42


def test_label_failure_does_not_discard_the_body(scraper):
    """A labels error must not throw away content we already fetched."""
    scraper.khoros.fetch_article.return_value = {"body": "<P>Body</P>", "author": "u"}
    scraper.khoros.fetch_labels.return_value = None

    details = scraper.fetch_article_details(MID)

    assert details["summary"] == "Body"
    assert details["categories"] is None


def test_fetch_article_details_returns_none_when_api_unavailable(scraper):
    scraper.khoros.fetch_article.return_value = None
    assert scraper.fetch_article_details(MID) is None


def test_categories_are_truncated_to_column_width(scraper):
    scraper.khoros.fetch_article.return_value = {"body": "<P>Body</P>"}
    scraper.khoros.fetch_labels.return_value = [f"label-{i}" for i in range(200)]

    details = scraper.fetch_article_details(MID)

    assert len(details["categories"]) <= scraper.MAX_CATEGORIES_LEN


# ---------------------------------------------------------------------------
# Scraper ingestion loop
# ---------------------------------------------------------------------------

def _run_rss(scraper, stored_url=None):
    scraper.fetch_rss_feed = MagicMock(return_value=ET.fromstring(FEED_XML))
    scraper.find_article_url_by_message_id = MagicMock(return_value=stored_url)
    scraper.article_exists = MagicMock(return_value=False)
    scraper.upsert_article_with_retry = MagicMock()
    scraper.scrape_from_rss()
    return scraper.upsert_article_with_retry.call_args[0][0]


def test_known_articles_are_still_enriched(scraper):
    """Content is refreshed for articles we already have, so edits land."""
    scraper.fetch_article_details = MagicMock(return_value={
        "summary": "The full article body",
        "categories": "Data Factory",
        "author": "someuser",
        "views": 7,
    })

    article = _run_rss(scraper, stored_url=CANON_URL)

    scraper.fetch_article_details.assert_called_once_with(MID)
    assert article["summary"] == "The full article body"
    assert article["url"] == CANON_URL


def test_teaser_never_overwrites_a_stored_body(scraper):
    """If enrichment fails for a known article, keep what's in the database.

    The feed's teaser is a fraction of the real post, so sending it would
    downgrade a stored full body. ``None`` leaves the stored value in place.
    """
    scraper.fetch_article_details = MagicMock(return_value=None)

    article = _run_rss(scraper, stored_url=CANON_URL)

    assert article["summary"] is None


def test_new_article_keeps_teaser_when_enrichment_fails(scraper):
    """For an unknown article the teaser is better than nothing."""
    scraper.fetch_article_details = MagicMock(return_value=None)

    article = _run_rss(scraper, stored_url=None)

    assert article["summary"] == "Short teaser."


def test_upsert_casts_summary_before_comparing(scraper_module):
    """Guard the nvarchar(max)/ntext mismatch.

    pyodbc binds long strings as ``ntext``, which SQL Server refuses to
    compare against ``nvarchar(max)`` — the statement fails to prepare and no
    article is stored at all. Mocked cursors cannot catch this, so assert the
    CAST is present.
    """
    cursor = MagicMock()
    scraper_module.FabricBlogScraper.insert_or_update_article(
        MagicMock(), cursor,
        {
            'url': CANON_URL, 'title': 'T', 'categories': None, 'post_date': None,
            'author': None, 'views': None, 'summary': 'a full body',
        },
    )

    clears = [
        c.args[0] for c in cursor.execute.call_args_list
        if 'blog_vector = NULL' in c.args[0]
    ]
    assert clears, "expected the stale-embedding UPDATE"
    assert "CAST(? AS NVARCHAR(MAX))" in clears[0]
    assert "summary <> ?" not in clears[0]


def test_stale_embedding_is_cleared_only_when_summary_differs(scraper_module):
    cursor = MagicMock()
    scraper_module.FabricBlogScraper.insert_or_update_article(
        MagicMock(), cursor,
        {
            'url': CANON_URL, 'title': 'T', 'categories': None, 'post_date': None,
            'author': None, 'views': None, 'summary': 'a full body',
        },
    )

    clear_sql = next(
        c.args[0] for c in cursor.execute.call_args_list
        if 'blog_vector = NULL' in c.args[0]
    )
    # Only rows whose stored summary actually differs are invalidated.
    assert 'summary IS NULL OR summary <>' in clear_sql
    assert 'blog_vector IS NOT NULL' in clear_sql


def test_no_vector_clear_without_a_summary(scraper_module):
    """Never invalidate an embedding when we have no content to replace it."""
    cursor = MagicMock()
    scraper_module.FabricBlogScraper.insert_or_update_article(
        MagicMock(), cursor,
        {
            'url': CANON_URL, 'title': 'T', 'categories': None, 'post_date': None,
            'author': None, 'views': None, 'summary': None,
        },
    )

    assert not [
        c for c in cursor.execute.call_args_list
        if 'blog_vector = NULL' in c.args[0]
    ]


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def _backfill(backfill_module, rows, content, **kwargs):
    khoros = MagicMock()
    khoros.fetch_article.return_value = content
    khoros.fetch_labels.return_value = ["Data Factory"]

    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cursor

    job = backfill_module.BlogContentBackfill(
        "Driver={Test};", khoros_client=khoros, **kwargs
    )
    with patch.object(backfill_module.pyodbc, "connect", return_value=conn):
        stats = job.run()
    return stats, cursor


def test_backfill_replaces_teaser_and_clears_vector(backfill_module):
    stats, cursor = _backfill(
        backfill_module,
        rows=[(1, CANON_URL, "Short teaser.", None)],
        content={"body": "<P>The full article body</P>", "author": "u", "views": 9},
    )

    assert stats["updated"] == 1
    assert stats["vectors_cleared"] == 1
    writes = [c.args[0] for c in cursor.execute.call_args_list]
    assert any("blog_vector = NULL" in w for w in writes)
    assert any("summary = ?" in w for w in writes)


def test_backfill_skips_rows_that_already_match(backfill_module):
    """A re-run must be free: no writes, no cleared embeddings."""
    stats, cursor = _backfill(
        backfill_module,
        rows=[(1, CANON_URL, "The full article body", "Data Factory")],
        content={"body": "<P>The full article body</P>", "author": "u", "views": 9},
    )

    assert stats["unchanged"] == 1
    assert stats["updated"] == 0
    assert stats["vectors_cleared"] == 0
    assert not [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in c.args[0]
    ]


def test_backfill_dry_run_writes_nothing(backfill_module):
    stats, cursor = _backfill(
        backfill_module,
        rows=[(1, CANON_URL, "Short teaser.", None)],
        content={"body": "<P>The full article body</P>", "author": "u", "views": 9},
        dry_run=True,
    )

    assert stats["updated"] == 1
    assert not [c for c in cursor.execute.call_args_list if "UPDATE" in c.args[0]]


def test_backfill_skips_legacy_rows_without_a_message_id(backfill_module):
    stats, _ = _backfill(
        backfill_module,
        rows=[(1, "https://blog.fabric.microsoft.com/en-us/blog/old-post", "x", None)],
        content={"body": "<P>Body</P>"},
    )

    assert stats["skipped_no_id"] == 1
    assert stats["updated"] == 0


def test_backfill_reports_failure_when_api_returns_nothing(backfill_module):
    stats, cursor = _backfill(
        backfill_module,
        rows=[(1, CANON_URL, "Short teaser.", None)],
        content=None,
    )

    assert stats["failed"] == 1
    assert stats["updated"] == 0
    assert not [c for c in cursor.execute.call_args_list if "UPDATE" in c.args[0]]


def test_backfill_limit_is_applied_in_sql(backfill_module):
    _, cursor = _backfill(
        backfill_module,
        rows=[],
        content={"body": "<P>Body</P>"},
        limit=5,
    )

    select_sql = cursor.execute.call_args_list[0].args[0]
    assert "TOP 5" in select_sql
