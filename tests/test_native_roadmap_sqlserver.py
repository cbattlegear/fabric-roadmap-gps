"""Opt-in native checks using uniquely named, disposable fixture tables.

Requires INSIGHTS_TEST_SQLSERVER_CONN and NATIVE_TEST_DISPOSABLE=1. The caller
must verify copy identity/TLS first and suppress driver exception details.
No subscriptions, mail, embeddings, or production refresh steps are invoked.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import os
import re
import threading
import uuid

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError

from db import db_sqlserver as db


pytestmark = pytest.mark.skipif(
    not os.getenv("INSIGHTS_TEST_SQLSERVER_CONN") or os.getenv("NATIVE_TEST_DISPOSABLE") != "1",
    reason="Explicit disposable native database configuration required",
)


@pytest.fixture
def native_engine():
    engine = create_engine(
        URL.create("mssql+pyodbc", query={"odbc_connect": os.environ["INSIGHTS_TEST_SQLSERVER_CONN"]}),
        hide_parameters=True, connect_args={"timeout": 15},
    )
    prefix = "gps_validation_" + uuid.uuid4().hex
    releases, history, feedback = (f"dbo.{prefix}_{suffix}" for suffix in ("r", "h", "f"))
    engine.update_execution_options(validation_releases=releases, validation_history=history)
    created = []
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE {releases} (
                    release_item_id varchar(36) PRIMARY KEY,
                    feature_name nvarchar(400), release_date date,
                    release_type nvarchar(100), release_type_value int,
                    vso_item nvarchar(1000), release_status nvarchar(100),
                    release_status_value int, release_semester nvarchar(200),
                    product_id nvarchar(36), product_name nvarchar(200),
                    is_publish_externally bit, feature_description nvarchar(max),
                    row_hash varchar(64) NOT NULL, last_modified date NOT NULL,
                    blog_title nvarchar(500), blog_url nvarchar(1000),
                    release_vector nvarchar(max), active bit NOT NULL,
                    ValidFrom datetime2 GENERATED ALWAYS AS ROW START NOT NULL,
                    ValidTo datetime2 GENERATED ALWAYS AS ROW END NOT NULL,
                    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
                ) WITH (SYSTEM_VERSIONING=ON (HISTORY_TABLE={history}))
            """))
        created.append("releases")
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE {feedback} (
                    id varchar(36) PRIMARY KEY, release_item_id varchar(36) NOT NULL,
                    blog_key varchar(64) NOT NULL, blog_url varchar(1000) NOT NULL,
                    voter_key varchar(64) NOT NULL, vote varchar(11) NOT NULL,
                    created_at datetime NOT NULL, updated_at datetime NOT NULL,
                    UNIQUE (release_item_id, blog_key, voter_key),
                    CHECK (vote IN ('helpful', 'not_related'))
                )
            """))
        created.append("feedback")

        def substitute(_conn, _cursor, statement, parameters, _context, _many):
            statement = re.sub(r"\b(?:dbo\.)?release_items\b", releases, statement)
            statement = re.sub(r"\b(?:dbo\.)?blog_match_feedback\b", feedback, statement)
            return statement, parameters

        event.listen(engine, "before_cursor_execute", substitute, retval=True)
        try:
            yield engine
        finally:
            event.remove(engine, "before_cursor_execute", substitute)
    finally:
        with engine.begin() as conn:
            if "feedback" in created:
                conn.execute(text(f"DROP TABLE {feedback}"))
            if "releases" in created:
                conn.execute(text(f"ALTER TABLE {releases} SET (SYSTEM_VERSIONING=OFF)"))
                conn.execute(text(f"DROP TABLE {releases}"))
                conn.execute(text(f"DROP TABLE {history}"))
        engine.dispose()


def insert_release(engine, identifier="fixture-one"):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dbo.release_items (
                release_item_id, feature_name, feature_description, product_name,
                release_status, row_hash, last_modified, active, blog_title, blog_url
            ) VALUES (:id, 'Native fixture', 'Current', 'Fixture product', 'Planned',
                      'fixture-hash', CAST(GETUTCDATE() AS date), 1,
                      'Fixture article', 'https://example.test/article')
        """), {"id": identifier})


def test_native_feedback_constraints_idempotency_stale_pairs_and_concurrency(native_engine):
    engine = native_engine
    insert_release(engine)
    url, voter = "https://example.test/article", "a" * 64
    vote = lambda choice="helpful", key=voter: db.record_blog_match_feedback(
        engine, "fixture-one", url, choice, key
    )
    assert vote() == "recorded"
    assert vote() == "unchanged"
    assert vote("not_related") == "changed"
    barrier = threading.Barrier(4)

    def concurrent_vote(_):
        barrier.wait(timeout=10)
        return vote(key="b" * 64)

    with ThreadPoolExecutor(max_workers=4) as pool:
        statuses = list(pool.map(concurrent_vote, range(4)))
    assert statuses.count("recorded") == 1
    assert statuses.count("unchanged") == 3
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM blog_match_feedback")).scalar_one() == 2
        assert conn.execute(text("SELECT row_hash FROM release_items")).scalar_one() == "fixture-hash"
    for sql in (
        "INSERT INTO blog_match_feedback SELECT NEWID(),release_item_id,blog_key,blog_url,voter_key,vote,created_at,updated_at FROM blog_match_feedback",
        "UPDATE blog_match_feedback SET vote='invalid'",
    ):
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(sql))
    with engine.begin() as conn:
        conn.execute(text("UPDATE release_items SET blog_url='https://example.test/rematched'"))
    with pytest.raises(db.FeedbackStaleMatch):
        vote()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM release_items"))
    with pytest.raises(db.FeedbackReleaseNotFound):
        vote()
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM blog_match_feedback")).scalar_one() == 2


def test_native_changelog_boundaries_removals_restorations_and_same_day_edits(native_engine):
    engine = native_engine
    insert_release(engine)
    # Create deterministic past versions only in this uniquely named fixture.
    table = engine.get_execution_options()["validation_history"]
    assert re.fullmatch(r"dbo\.gps_validation_[0-9a-f]{32}_h", table)
    release_table = engine.get_execution_options()["validation_releases"]
    yesterday = datetime.utcnow().date() - timedelta(days=1)
    start = datetime.combine(yesterday, datetime.min.time())
    times = [start - timedelta(microseconds=1), start, start + timedelta(hours=12),
             start + timedelta(hours=13), start + timedelta(hours=14)]
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {release_table} SET (SYSTEM_VERSIONING=OFF)"))
        for index, at in enumerate(times):
            end = times[index + 1] if index + 1 < len(times) else conn.execute(
                text("SELECT ValidFrom FROM release_items")
            ).scalar_one()
            conn.execute(text(f"""
                INSERT INTO {table} (release_item_id,feature_name,feature_description,
                    product_name,release_status,row_hash,last_modified,active,ValidFrom,ValidTo)
                VALUES ('fixture-one','Native fixture',:description,'Fixture product',
                        'Planned','fixture-hash',:day,:active,:at,:until)
            """), {"description": f"Past {index}", "day": yesterday, "active": index != 3,
                   "at": at, "until": end})
        conn.execute(text(f"ALTER TABLE {release_table} SET (SYSTEM_VERSIONING=ON (HISTORY_TABLE={table}))"))
    rows = db.get_changelog_with_changes(engine, since=yesterday, through=yesterday)
    assert len(rows) == 4
    assert all(row["last_modified"] == yesterday for row in rows)
    changes = [change for row in rows for change in row["changed_columns"]]
    assert "Removed from Roadmap" in changes
    assert "Restored to Roadmap" in changes
    assert len(db.get_changelog_with_changes(
        engine, since=yesterday, through=yesterday, include_inactive=False
    )) == 3
    assert len(db.get_changelog_with_changes(
        engine, since=yesterday + timedelta(days=1), through=yesterday + timedelta(days=1)
    )) == 1


def test_native_mcp_protocol_queries_real_copy_without_private_fields():
    from flask import Flask
    from mcp_app import create_app
    from tests.test_mcp import http_client, protocol_client

    engine = create_engine(
        URL.create("mssql+pyodbc", query={"odbc_connect": os.environ["INSIGHTS_TEST_SQLSERVER_CONN"]}),
        hide_parameters=True, connect_args={"timeout": 15},
    )

    async def verify():
        app = create_app(Flask(__name__), lambda: engine, base_url="https://gps.example")
        async with http_client(app) as http, protocol_client(http) as session:
            result = await session.call_tool("search_releases", {"query": {"page_size": 2}})
            assert not result.is_error
            data = result.structured_content
            assert 0 < len(data["data"]) <= 2
            allowed = {
                "release_item_id", "feature_name", "release_date", "release_type",
                "release_status", "product_name", "feature_description", "blog_title",
                "blog_url", "last_modified", "active", "url", "truncated_fields",
            }
            assert set(data["data"][0]) <= allowed
            identifier = data["data"][0]["release_item_id"]
            for name, arguments in (
                ("get_release", {"release_item_id": identifier}),
                ("get_release_history", {"release_item_id": identifier, "limit": 2}),
                ("get_filter_options", {}),
            ):
                response = await session.call_tool(name, arguments)
                assert not response.is_error
                if name == "get_release":
                    assert set(response.structured_content) <= allowed
                if name == "get_release_history":
                    assert len(response.structured_content["data"]) <= 2
            invalid = await session.call_tool("search_releases", {"query": {"page_size": 26}})
            assert invalid.is_error
            second = await session.call_tool("search_releases", {"query": {"page_size": 2, "page": 2}})
            assert not second.is_error
            first_ids = {row["release_item_id"] for row in data["data"]}
            assert first_ids.isdisjoint(row["release_item_id"] for row in second.structured_content["data"])
    try:
        asyncio.run(verify())
    finally:
        engine.dispose()
