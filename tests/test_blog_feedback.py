"""Offline coverage for pair identity, persistence, retention, and HTTP policy."""

import hashlib
import importlib.util
import runpy
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import mssql
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.schema import CreateTable
from freezegun import freeze_time

from db import db_sqlserver as db
from lib.blog_feedback import blog_identity, valid_submission

URL = "https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/article/ba-p/12345"
CANONICAL_URL = "https://community.fabric.microsoft.com/blog/fbc_fabricupdatesblogs/article/12345"
OTHER_URL = "https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/other/ba-p/67890"
KEY = "a" * 64


@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    db.Base.metadata.create_all(engine)
    with db.session_scope(engine) as session, session.begin():
        session.add(db.ReleaseItemModel(
            release_item_id="r1", feature_name="Feature", row_hash="original",
            last_modified=date(2026, 10, 19), blog_title="Article", blog_url=URL,
            feature_description="Description", release_vector="[1,2]",
        ))
    yield engine
    engine.dispose()


def vote(engine, choice="helpful", url=URL, key=KEY):
    return db.record_blog_match_feedback(engine, "r1", url, choice, key)


def rows(engine):
    with db.session_scope(engine) as session:
        result = session.scalars(select(db.BlogMatchFeedbackModel)).all()
        session.expunge_all()
        return result


def test_community_identity_is_stable_but_host_and_article_must_match():
    assert blog_identity(URL) == blog_identity(CANONICAL_URL + "?source=rss#intro") == "community:12345"
    assert blog_identity(OTHER_URL) != blog_identity(URL)
    assert blog_identity(URL.replace("community.fabric.microsoft.com", "evil.example")) != blog_identity(URL)
    assert blog_identity("https://EXAMPLE.com:443/blog#intro") == "https://example.com/blog"
    assert blog_identity("https://example.com/blog?q=1") != blog_identity("https://example.com/blog?q=2")


@pytest.mark.parametrize("url", [
    None, "", [], "javascript:alert(1)", "data:text/html,hello", "//example.com",
    "https://", "https://[bad", "https://example.com:bad/blog", "https://user:password@example.com",
    "https://example.com/a\nb", "https://example.com/a b", "https://example.com/" + "x" * 1000,
    "https:example.com", "https://example.com\\bad",
    "https://example.com/\ud800",
])
def test_invalid_urls(url):
    assert blog_identity(url) is None
    assert not valid_submission("r1", url, "helpful", KEY)


def test_duplicate_and_changed_vote_are_one_row_and_do_not_mutate_release(engine):
    with freeze_time("2026-10-19 12:00:00"):
        assert vote(engine) == "recorded"
    with freeze_time("2026-10-19 13:00:00"):
        assert vote(engine, url=CANONICAL_URL) == "unchanged"
        assert rows(engine)[0].updated_at == datetime(2026, 10, 19, 12)
        assert vote(engine, "not_related") == "changed"
    feedback = rows(engine)
    assert len(feedback) == 1
    assert feedback[0].vote == "not_related"
    assert feedback[0].blog_url == URL
    assert feedback[0].created_at == datetime(2026, 10, 19, 12)
    assert feedback[0].updated_at == datetime(2026, 10, 19, 13)
    assert feedback[0].voter_key == hashlib.sha256(KEY.encode("ascii")).hexdigest()
    assert feedback[0].voter_key != KEY
    with db.session_scope(engine) as session:
        release = session.get(db.ReleaseItemModel, "r1")
        assert (release.blog_url, release.row_hash, release.release_vector) == (URL, "original", "[1,2]")


def test_other_browsers_count_separately(engine):
    vote(engine)
    vote(engine, key="b" * 64)
    assert len(rows(engine)) == 2


def test_retry_after_uncertain_commit_does_not_duplicate_vote(engine, monkeypatch):
    original_scope = db.session_scope
    attempts = []

    @contextmanager
    def uncertain_commit(engine):
        with original_scope(engine) as session:
            yield session
        attempts.append(True)
        if len(attempts) == 1:
            raise OperationalError("commit", {}, Exception("connection lost"), connection_invalidated=True)

    monkeypatch.setattr(db, "session_scope", uncertain_commit)
    monkeypatch.setattr("lib.db_retry.time.sleep", lambda _: None)
    assert vote(engine) == "unchanged"
    assert len(attempts) == 2
    assert len(rows(engine)) == 1


def test_rematch_disappearance_and_deleted_release_preserve_original_pair(engine):
    vote(engine)
    with db.session_scope(engine) as session, session.begin():
        session.get(db.ReleaseItemModel, "r1").blog_url = OTHER_URL
    with pytest.raises(db.FeedbackStaleMatch):
        vote(engine)
    assert vote(engine, "not_related", OTHER_URL) == "recorded"
    assert len(rows(engine)) == 2
    with db.session_scope(engine) as session, session.begin():
        release = session.get(db.ReleaseItemModel, "r1")
        release.blog_url = None
        release.blog_title = None
    with pytest.raises(db.FeedbackStaleMatch):
        vote(engine)
    with db.session_scope(engine) as session, session.begin():
        session.delete(session.get(db.ReleaseItemModel, "r1"))
    with pytest.raises(db.FeedbackReleaseNotFound):
        vote(engine)
    assert {row.blog_url for row in rows(engine)} == {URL, OTHER_URL}


def test_database_constraints_and_sql_server_lock_contract(engine):
    vote(engine)
    original = rows(engine)[0]
    with pytest.raises(IntegrityError):
        with db.session_scope(engine) as session, session.begin():
            session.add(db.BlogMatchFeedbackModel(
                release_item_id="r1", blog_key=original.blog_key, voter_key=original.voter_key,
                blog_url=URL, vote="helpful",
            ))
    with pytest.raises(IntegrityError):
        with db.session_scope(engine) as session, session.begin():
            session.add(db.BlogMatchFeedbackModel(
                release_item_id="r2", blog_key="c" * 64, voter_key="d" * 64,
                blog_url=URL, vote="invalid",
            ))
    ddl = str(CreateTable(db.BlogMatchFeedbackModel.__table__).compile(dialect=mssql.dialect()))
    assert "UNIQUE (release_item_id, blog_key, voter_key)" in ddl
    assert "CHECK (vote IN ('helpful', 'not_related'))" in ddl
    assert "FOREIGN KEY" not in ddl
    with patch.object(db, "session_scope") as scope:
        session = scope.return_value.__enter__.return_value
        session.execute.return_value.scalar_one_or_none.return_value = None
        with pytest.raises(db.FeedbackReleaseNotFound):
            vote(engine)
        query = session.execute.call_args.args[0]
        sql = str(query.compile(dialect=mssql.dialect()))
        assert "WITH (UPDLOCK, HOLDLOCK)" in sql


def test_invalid_helper_input_never_opens_a_session():
    with patch.object(db, "session_scope") as scope, pytest.raises(ValueError):
        vote(None, choice="invalid")
    scope.assert_not_called()


def test_retention_preserves_recent_and_boundary_rows(engine):
    with freeze_time("2026-10-19"):
        cutoff = datetime.utcnow() - timedelta(days=180)
        vote(engine)
        with db.session_scope(engine) as session, session.begin():
            row = session.scalar(select(db.BlogMatchFeedbackModel))
            row.updated_at = cutoff
        db.prune_blog_match_feedback(engine)
        assert len(rows(engine)) == 1
        with db.session_scope(engine) as session, session.begin():
            session.scalar(select(db.BlogMatchFeedbackModel)).updated_at = cutoff - timedelta(seconds=1)
        db.prune_blog_match_feedback(engine)
        assert rows(engine) == []


def test_refresh_prunes_before_any_network_access(monkeypatch):
    monkeypatch.setenv("CURRENT_ENVIRONMENT", "development")
    with patch.object(db, "make_engine", return_value="engine"), \
         patch.object(db, "init_db"), \
         patch.object(db, "prune_blog_match_feedback") as prune, \
         patch("requests.get", side_effect=RuntimeError("stop before network")):
        with pytest.raises(RuntimeError, match="stop before network"):
            runpy.run_module("get_current_releases", run_name="__main__")
    prune.assert_called_once_with("engine")


@pytest.fixture
def web(monkeypatch, engine):
    monkeypatch.setenv("CURRENT_ENVIRONMENT", "development")
    import server
    monkeypatch.setattr(server, "get_engine", lambda: engine)
    monkeypatch.setattr(server, "BASE_URL", "https://fabric-gps.example")
    monkeypatch.setattr(server, "EMAIL_BASE_URL", "https://fabric-gps.example")
    server.app.config.update(TESTING=True)
    server.limiter.reset()
    yield server, server.app.test_client()
    server.limiter.reset()


def post(client, **overrides):
    options = dict(
        json={"release_item_id": "r1", "blog_url": URL, "vote": "helpful"},
        headers={"Origin": "https://fabric-gps.example", "X-Feedback-Voter": KEY},
    )
    options.update(overrides)
    response = client.post("/api/blog-feedback", **options)
    assert response.headers["Cache-Control"] == "no-store"
    assert "Set-Cookie" not in response.headers
    return response


def test_endpoint_idempotency_and_changed_vote(web):
    _, client = web
    assert post(client).json == {"status": "recorded", "vote": "helpful"}
    assert post(client).json == {"status": "unchanged", "vote": "helpful"}
    assert post(client, json={"release_item_id": "r1", "blog_url": URL, "vote": "not_related"}).json == {
        "status": "changed", "vote": "not_related",
    }


@pytest.mark.parametrize("payload", [
    [], None, {}, {"release_item_id": "r1"},
    {"release_item_id": "r1", "blog_url": URL, "vote": "invalid"},
    {"release_item_id": "r1", "blog_url": URL, "vote": []},
    {"release_item_id": "r1", "blog_url": URL, "vote": "helpful", "extra": True},
    {"release_item_id": "r" * 37, "blog_url": URL, "vote": "helpful"},
    {"release_item_id": "\ud800", "blog_url": URL, "vote": "helpful"},
])
def test_endpoint_validation(web, payload):
    _, client = web
    # json=None is encoded explicitly to test the JSON null body.
    import json
    response = post(client, json=None, data=json.dumps(payload), content_type="application/json")
    assert response.status_code == 400


@pytest.mark.parametrize("origin", [
    "", "null", "https://evil.example", "https://fabric-gps.example.evil", "https://fabric-gps.example/path",
    "http://fabric-gps.example", "https://fabric-gps.example:444",
])
def test_endpoint_rejects_cross_origin_and_prefix_spoofing(web, origin):
    _, client = web
    assert post(client, headers={"Origin": origin, "X-Feedback-Voter": KEY}).status_code == 403


def test_endpoint_rejects_missing_voter_wrong_type_and_oversized_body(web):
    _, client = web
    assert post(client, headers={"Origin": "https://fabric-gps.example"}).status_code == 400
    assert post(client, json=None, data="vote=helpful", content_type="text/plain").status_code == 415
    assert post(client, json=None, data="{", content_type="application/json").status_code == 400
    assert post(client, json=None, data=" " * 4097, content_type="application/json").status_code == 413
    assert post(client, json=None, data="[" * 1500 + "]" * 1500, content_type="application/json").status_code == 400
    assert post(client, json=None, data=b"\xff", content_type="application/json").status_code == 400
    assert post(client, headers={
        "Origin": "https://fabric-gps.example", "X-Feedback-Voter": KEY, "Sec-Fetch-Site": "cross-site",
    }).status_code == 403


def test_endpoint_explicit_stale_missing_backend_errors(web, engine, monkeypatch):
    server, client = web
    assert post(client, json={"release_item_id": "r1", "blog_url": OTHER_URL, "vote": "helpful"}).status_code == 409
    assert post(client, json={"release_item_id": "missing", "blog_url": URL, "vote": "helpful"}).status_code == 404
    monkeypatch.setattr(server, "record_blog_match_feedback", Mock(side_effect=OperationalError("secret SQL", {}, Exception())))
    response = post(client)
    assert response.status_code == 503
    assert "secret" not in response.get_data(as_text=True)


def test_endpoint_rate_limit(web):
    _, client = web
    for _ in range(20):
        assert post(client).status_code == 200
    response = post(client)
    assert response.status_code == 429
    assert int(response.headers["Retry-After"]) > 0
    assert "wait" in response.json["error"]


def test_endpoint_hourly_limit_survives_minute_window_resets(web):
    _, client = web
    # MemoryStorage's cleanup thread uses real time (ignored by freezegun).
    with freeze_time(datetime.utcnow() + timedelta(days=1)) as clock:
        for _ in range(5):
            for _ in range(20):
                assert post(client).status_code == 200
            clock.tick(delta=timedelta(seconds=61))
        assert post(client).status_code == 429


def test_unknown_content_length_still_has_a_bounded_read(web):
    _, client = web
    response = post(client, json=None, data=" " * 4097, content_type="application/json", environ_overrides={
        "CONTENT_LENGTH": "", "wsgi.input_terminated": True,
    })
    assert response.status_code == 413


def test_no_store_also_applies_to_unsupported_methods(web):
    _, client = web
    response = client.get("/api/blog-feedback")
    assert response.status_code == 405
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("url,title,shown", [
    (URL, "Article", True), (None, "Article", False), (URL, None, False),
    ("javascript:alert(1)", "Article", False), ("https://[bad", "Article", False),
])
def test_release_controls_conditional_accessible_and_anonymous(web, engine, monkeypatch, url, title, shown):
    server, client = web
    with db.session_scope(engine) as session, session.begin():
        release = session.get(db.ReleaseItemModel, "r1")
        release.blog_url, release.blog_title = url, title
    monkeypatch.setattr(server, "fetch_history_rows", lambda *_: [])
    response = client.get("/release/r1", headers={"X-Feedback-Voter": KEY})
    assert response.status_code == 200
    assert "Set-Cookie" not in response.headers
    assert KEY not in response.get_data(as_text=True)
    page = BeautifulSoup(response.data, "html.parser")
    group = page.select_one(".blog-feedback")
    assert bool(group) == shown
    if shown:
        assert group["data-release-id"] == "r1"
        assert group["data-blog-url"] == URL
        assert group["role"] == "group"
        assert group.has_attr("hidden")
        assert len(group.select('button[type="button"][aria-pressed="false"]')) == 2
        assert group.select_one('[role="status"][aria-live="polite"]')
        assert not group.find_parent("a")
        article = page.select_one("a[data-feedback-article]")
        assert article["data-release-id"] == "r1"
        assert article["data-blog-url"] == URL
        assert article["target"] == "_blank"
        assert article["title"] == title
        assert article.select_one(".blog-article-title").get_text() == title


def test_release_article_title_is_escaped_and_privacy_navigation_stays_footer_only(web, engine, monkeypatch):
    server, client = web
    title = '<img src=x onerror="alert(1)"> A detailed article'
    with db.session_scope(engine) as session, session.begin():
        session.get(db.ReleaseItemModel, "r1").blog_title = title
    monkeypatch.setattr(server, "fetch_history_rows", lambda *_: [])
    page = BeautifulSoup(client.get("/release/r1").data, "html.parser")
    link = page.select_one("a[data-feedback-article]")
    assert link["title"] == title
    assert link.select_one(".blog-article-title").get_text() == title
    assert link.select_one("img") is None
    assert len(page.select('a[href="/privacy"]')) == 1
    assert page.select_one('a[href="/privacy"]').find_parent("footer")
    assert page.select_one('a[href="/about#blog-feedback-privacy"]') is None


def test_migration_matches_model_and_chain():
    path = Path(__file__).resolve().parents[1] / "migrations" / "versions" / "add_blog_match_feedback.py"
    spec = importlib.util.spec_from_file_location("feedback_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.down_revision == "add_unsub_token_history"
    with patch.object(module.op, "execute") as execute:
        module.upgrade()
    sql = "\n".join(call.args[0] for call in execute.call_args_list)
    assert "IF NOT EXISTS" in sql
    assert "UNIQUE (release_item_id, blog_key, voter_key)" in sql
    assert "CHECK (vote IN ('helpful', 'not_related'))" in sql
    for column in db.BlogMatchFeedbackModel.__table__.columns:
        assert column.name in sql
        if hasattr(column.type, "length"):
            assert f"VARCHAR({column.type.length})" in sql
    assert "ix_blog_match_feedback_updated_at" in sql
    with patch.object(module.op, "execute") as execute:
        module.downgrade()
    assert "DROP TABLE blog_match_feedback" in execute.call_args.args[0]
