"""Tests for resolving legacy blog URLs through the community title index.

The legacy domain returns HTTP 403 to automated clients, so redirect-following
resolves nothing; these cover the title-matching path that replaced it.
"""

from unittest.mock import MagicMock, patch

import pytest

from lib.community_index import CommunityBlogIndex, normalize_slug, slug_from_url


LEGACY = "https://blog.fabric.microsoft.com/en-US/blog/"
COMMUNITY = "https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/"


@pytest.fixture
def migrate_module():
    import os

    os.environ.setdefault("SQLSERVER_CONN", "Driver={Test};Server=test;")
    import migrate_blog_urls
    return migrate_blog_urls


def _post(mid, subject, url=None):
    return {
        "id": str(mid),
        "subject": subject,
        "url": url or f"{COMMUNITY}{subject.replace(' ', '-')}/ba-p/{mid}",
    }


def _index(posts):
    client = MagicMock()
    client.fetch_board_messages.return_value = posts
    return CommunityBlogIndex(client)


# ---------------------------------------------------------------- slugs


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Monitor your Eventstreams with workspace monitoring (preview)",
         "monitor-your-eventstreams-with-workspace-monitoring-preview"),
        ("SQL Audit Logs: More Signal, Less Noise", "sql-audit-logs-more-signal-less-noise"),
        ("  Trailing and leading  ", "trailing-and-leading"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_slug(text, expected):
    assert normalize_slug(text) == expected


def test_normalize_slug_folds_percent_encoded_non_breaking_hyphen():
    """A URL spelling U+2011 as %E2%80%91 must match the title's own hyphen.

    This is a real production case: the legacy URL percent-encodes a
    non-breaking hyphen, and without decoding it the slug degrades to
    `workspace-e2-80-91level` and matches nothing.
    """
    from_url = normalize_slug("Workspace%E2%80%91level-customer%E2%80%91managed-keys")
    from_title = normalize_slug("Workspace\u2011level customer\u2011managed keys")
    assert from_url == from_title == "workspace-level-customer-managed-keys"


def test_slug_from_url_handles_legacy_and_community_forms():
    legacy = f"{LEGACY}pipelines-are-evolving-beyond-etl"
    community = f"{COMMUNITY}Pipelines-are-evolving-beyond-ETL/ba-p/5359124"
    assert slug_from_url(legacy) == "pipelines-are-evolving-beyond-etl"
    # The community form must step back past `/ba-p/<id>` to the title segment.
    assert slug_from_url(community) == "pipelines-are-evolving-beyond-etl"


def test_slug_from_url_ignores_locale_and_empty_paths():
    assert slug_from_url("https://blog.fabric.microsoft.com/en-US/blog/") == ""
    assert slug_from_url("") == ""
    assert slug_from_url(None) == ""


# ---------------------------------------------------------------- resolve


def test_resolve_matches_legacy_url_slug_to_community_post():
    index = _index([_post(5359124, "Pipelines are evolving beyond ETL")])
    resolved = index.resolve(url=f"{LEGACY}pipelines-are-evolving-beyond-etl")
    assert resolved == f"{COMMUNITY}Pipelines-are-evolving-beyond-ETL/ba-p/5359124"


def test_resolve_falls_back_to_stored_title_when_url_slug_misses():
    index = _index([_post(1, "A totally different headline")])
    assert index.resolve(url=f"{LEGACY}slug-that-does-not-match") is None
    assert index.resolve(
        url=f"{LEGACY}slug-that-does-not-match",
        title="A totally different headline",
    ) == f"{COMMUNITY}A-totally-different-headline/ba-p/1"


def test_resolve_returns_none_for_unknown_post():
    index = _index([_post(1, "Something else")])
    assert index.resolve(url=f"{LEGACY}no-such-article", title="No such article") is None


def test_resolve_refuses_ambiguous_titles():
    """Two posts sharing a normalized title must not resolve to a coin flip."""
    index = _index([
        _post(111, "Fabric March 2026 Feature Summary"),
        _post(222, "Fabric March 2026 feature summary"),
    ])
    assert index.resolve(url=f"{LEGACY}fabric-march-2026-feature-summary") is None


def test_duplicate_id_is_not_treated_as_ambiguous():
    """The same post seen twice (paging overlap) is not a collision."""
    index = _index([_post(7, "Repeated post"), _post(7, "Repeated post")])
    assert index.resolve(title="Repeated post") == f"{COMMUNITY}Repeated-post/ba-p/7"


def test_index_is_built_once_and_reused():
    client = MagicMock()
    client.fetch_board_messages.return_value = [_post(1, "One")]
    index = CommunityBlogIndex(client)

    index.resolve(title="One")
    index.resolve(title="One")
    index.resolve(title="Two")

    client.fetch_board_messages.assert_called_once()


def test_unreadable_board_resolves_nothing_without_raising():
    client = MagicMock()
    client.fetch_board_messages.return_value = None
    index = CommunityBlogIndex(client)

    assert index.resolve(url=f"{LEGACY}anything") is None
    assert index.available is False


def test_posts_missing_subject_or_url_are_skipped():
    index = _index([
        {"id": "1", "subject": None, "url": f"{COMMUNITY}x/ba-p/1"},
        {"id": "2", "subject": "No url", "url": None},
        _post(3, "Good post"),
    ])
    assert index.resolve(title="Good post") == f"{COMMUNITY}Good-post/ba-p/3"
    assert index.resolve(title="No url") is None


# ---------------------------------------------- board enumeration paging


def test_fetch_board_messages_pages_until_short_page():
    from lib.khoros_api import KhorosClient

    client = KhorosClient()
    first = [{"id": str(i), "subject": f"s{i}", "view_href": f"u{i}"} for i in range(1000)]
    second = [{"id": "x", "subject": "last", "view_href": "ux"}]
    client._liql = MagicMock(side_effect=[first, second])

    messages = client.fetch_board_messages()

    assert len(messages) == 1001
    assert messages[-1] == {"id": "x", "subject": "last", "url": "ux"}
    offsets = [call.args[0] for call in client._liql.call_args_list]
    assert "OFFSET 0" in offsets[0]
    assert "OFFSET 1000" in offsets[1]
    # Comments must stay out of the index.
    assert "depth = 0" in offsets[0]


def test_fetch_board_messages_returns_none_when_first_page_fails():
    from lib.khoros_api import KhorosClient

    client = KhorosClient()
    client._liql = MagicMock(return_value=None)
    assert client.fetch_board_messages() is None


def test_fetch_board_messages_keeps_partial_results_on_later_failure():
    """A mid-run failure must not throw away pages already read."""
    from lib.khoros_api import KhorosClient

    client = KhorosClient()
    first = [{"id": str(i), "subject": f"s{i}", "view_href": f"u{i}"} for i in range(1000)]
    client._liql = MagicMock(side_effect=[first, None])

    messages = client.fetch_board_messages()

    assert messages is not None
    assert len(messages) == 1000


# ------------------------------------------------------ migration wiring


def test_migration_prefers_index_over_redirects(migrate_module):
    """The redirect path must not be touched when the index has an answer."""
    blog_index = MagicMock()
    blog_index.resolve.return_value = f"{COMMUNITY}Resolved/ba-p/42"

    migration = migrate_module.BlogUrlMigration(
        "Driver={Test};Server=test;", session=MagicMock(), blog_index=blog_index
    )

    with patch.object(migrate_module, "fetch_final_url") as redirect:
        resolved = migration._resolve_url(f"{LEGACY}resolved", "Resolved")

    assert resolved == f"{COMMUNITY}Resolved/ba-p/42"
    redirect.assert_not_called()
    assert migration.resolved_by == {"index": 1, "redirect": 0}


def test_migration_falls_back_to_redirect_when_index_misses(migrate_module):
    blog_index = MagicMock()
    blog_index.resolve.return_value = None

    migration = migrate_module.BlogUrlMigration(
        "Driver={Test};Server=test;", session=MagicMock(), blog_index=blog_index
    )

    with patch.object(migrate_module, "fetch_final_url", return_value=f"{COMMUNITY}Via-redirect/ba-p/9"):
        resolved = migration._resolve_url(f"{LEGACY}missing", "Missing")

    assert resolved == f"{COMMUNITY}Via-redirect/ba-p/9"
    assert migration.resolved_by == {"index": 0, "redirect": 1}
