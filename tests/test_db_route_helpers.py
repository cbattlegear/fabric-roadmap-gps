"""Tests for the new route-supporting helpers in ``db.db_sqlserver``.

Covers:
* ``get_latest_release_version`` — used by ``/api/version`` as a cache-buster
* ``get_active_releases_for_sitemap`` — used by ``/sitemap.xml``
* ``get_verify_email_context`` — used by the GET ``/verify-email`` page

We use an in-memory SQLite engine and re-create the real model tables so
we exercise the actual ORM mappings end-to-end without needing SQL Server
or pyodbc. The vector / SQL-Server-specific bits live only in raw SQL
strings, so ``Base.metadata.create_all`` works against SQLite.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine

from db.db_sqlserver import (
    Base,
    EmailSubscriptionModel,
    EmailVerificationModel,
    ReleaseItemModel,
    SitemapRelease,
    VerifyEmailContext,
    get_active_releases_for_sitemap,
    get_latest_release_version,
    get_verify_email_context,
)


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(eng)
    return eng


def _add_release(engine, *, release_item_id, last_modified, active=True,
                 feature_name="Feature", row_hash="x" * 64):
    from sqlalchemy.orm import Session
    with Session(engine) as s:
        s.add(ReleaseItemModel(
            release_item_id=release_item_id,
            feature_name=feature_name,
            row_hash=row_hash,
            last_modified=last_modified,
            active=active,
        ))
        s.commit()


# ---------------------------------------------------------------------------
# get_latest_release_version
# ---------------------------------------------------------------------------

def test_latest_version_empty_table_returns_empty_string(engine):
    assert get_latest_release_version(engine) == ""


def test_latest_version_returns_most_recent(engine):
    _add_release(engine, release_item_id="aaa", last_modified=date(2025, 1, 1))
    _add_release(engine, release_item_id="bbb", last_modified=date(2025, 6, 15))
    _add_release(engine, release_item_id="ccc", last_modified=date(2025, 3, 1))
    assert get_latest_release_version(engine) == "bbb"


def test_latest_version_breaks_ties_by_release_item_id_desc(engine):
    """When several rows share the same ``last_modified`` date the helper
    must return a deterministic value (sorted by id desc) rather than
    flapping between equally-recent rows on each call."""
    same_day = date(2025, 4, 22)
    _add_release(engine, release_item_id="aaa", last_modified=same_day)
    _add_release(engine, release_item_id="zzz", last_modified=same_day)
    _add_release(engine, release_item_id="mmm", last_modified=same_day)
    # Repeat a few times to assert determinism.
    results = {get_latest_release_version(engine) for _ in range(5)}
    assert results == {"zzz"}


# ---------------------------------------------------------------------------
# get_active_releases_for_sitemap
# ---------------------------------------------------------------------------

def test_sitemap_helper_filters_inactive(engine):
    _add_release(engine, release_item_id="active-1", last_modified=date(2025, 1, 1), active=True)
    _add_release(engine, release_item_id="active-2", last_modified=date(2025, 2, 1), active=True)
    _add_release(engine, release_item_id="archived", last_modified=date(2024, 1, 1), active=False)

    rows = get_active_releases_for_sitemap(engine)
    ids = sorted(r.release_item_id for r in rows)
    assert ids == ["active-1", "active-2"]


def test_sitemap_helper_returns_frozen_dataclass(engine):
    _add_release(engine, release_item_id="r1", last_modified=date(2025, 5, 5))
    rows = get_active_releases_for_sitemap(engine)
    assert len(rows) == 1
    row = rows[0]
    assert isinstance(row, SitemapRelease)
    assert row.release_item_id == "r1"
    assert row.last_modified == date(2025, 5, 5)
    # Frozen dataclass — must reject mutation.
    with pytest.raises(Exception):
        row.release_item_id = "r2"  # type: ignore[misc]


def test_sitemap_helper_empty_returns_empty_list(engine):
    assert get_active_releases_for_sitemap(engine) == []


# ---------------------------------------------------------------------------
# get_verify_email_context
# ---------------------------------------------------------------------------

def _add_verification(engine, *, token, email, pending_watch_release_id=None,
                      is_used=False):
    from sqlalchemy.orm import Session
    with Session(engine) as s:
        s.add(EmailVerificationModel(
            email=email,
            token=token,
            action_type="subscribe",
            expires_at=datetime.utcnow() + timedelta(days=1),
            is_used=is_used,
            pending_watch_release_id=pending_watch_release_id,
        ))
        s.commit()


def _add_subscription(engine, *, email, cadence="weekly", is_verified=True):
    from sqlalchemy.orm import Session
    with Session(engine) as s:
        s.add(EmailSubscriptionModel(
            email=email,
            is_verified=is_verified,
            email_cadence=cadence,
            unsubscribe_token="unsub-" + email,
        ))
        s.commit()


def test_verify_context_unknown_token_returns_none(engine):
    assert get_verify_email_context(engine, "no-such-token") is None


def test_verify_context_no_subscription_yet_defaults_to_weekly(engine):
    """A brand-new signup whose subscription row hasn't been created
    should still render display context with the default weekly cadence."""
    _add_verification(engine, token="tok-new", email="new@example.com")
    ctx = get_verify_email_context(engine, "tok-new")
    assert ctx == VerifyEmailContext(cadence="weekly", watch_feature_name=None)


def test_verify_context_uses_subscription_cadence(engine):
    _add_verification(engine, token="tok-daily", email="daily@example.com")
    _add_subscription(engine, email="daily@example.com", cadence="daily")
    ctx = get_verify_email_context(engine, "tok-daily")
    assert ctx is not None
    assert ctx.cadence == "daily"


def test_verify_context_resolves_watch_feature_name(engine):
    _add_release(engine, release_item_id="rel-watch", last_modified=date(2025, 1, 1),
                 feature_name="Direct Lake on OneLake")
    _add_verification(engine, token="tok-watch", email="watch@example.com",
                      pending_watch_release_id="rel-watch")
    ctx = get_verify_email_context(engine, "tok-watch")
    assert ctx is not None
    assert ctx.watch_feature_name == "Direct Lake on OneLake"


def test_verify_context_missing_release_leaves_watch_name_none(engine):
    """If pending_watch_release_id points to a release that no longer
    exists (deleted / archived hard-delete), context should still be
    returned — cadence is independent of the watch lookup."""
    _add_verification(engine, token="tok-stale", email="stale@example.com",
                      pending_watch_release_id="missing-release-id")
    _add_subscription(engine, email="stale@example.com", cadence="weekly")
    ctx = get_verify_email_context(engine, "tok-stale")
    assert ctx is not None
    assert ctx.watch_feature_name is None
    assert ctx.cadence == "weekly"


def test_verify_context_used_token_still_returns_display_context(engine):
    """The GET page is purely informational — even an already-used token
    should render context if the row still exists. The route uses the
    POST handler (not this helper) to enforce token validity."""
    _add_verification(engine, token="tok-used", email="used@example.com",
                      is_used=True)
    _add_subscription(engine, email="used@example.com", cadence="weekly")
    ctx = get_verify_email_context(engine, "tok-used")
    assert ctx is not None
    assert ctx.cadence == "weekly"


def test_verify_context_returns_frozen_dataclass(engine):
    _add_verification(engine, token="tok-frozen", email="frozen@example.com")
    ctx = get_verify_email_context(engine, "tok-frozen")
    assert ctx is not None
    with pytest.raises(Exception):
        ctx.cadence = "daily"  # type: ignore[misc]
