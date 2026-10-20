"""Legacy date API compatibility and automatic exact-time visit regressions."""

import shutil
import re
import sqlite3
import subprocess
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import server
from db.db_sqlserver import get_changelog_with_changes, get_changelog_visit_snapshot


@pytest.fixture
def temporal_changelog():
    """Execute the production query, adapting only SQL Server syntax/functions."""
    history = sqlite3.connect(":memory:")
    history.row_factory = sqlite3.Row
    history.execute("ATTACH DATABASE ':memory:' AS dbo")
    history.create_collation("Latin1_General_100_BIN2", lambda a, b: (a > b) - (a < b))
    history.create_function("CONCAT", -1, lambda *values: "".join(str(v) for v in values if v is not None))
    history.create_function("dateadd_days", 2, lambda days, value:
                            (date.fromisoformat(value[:10]) + timedelta(days=days)).isoformat() + value[10:])
    columns = (
        "release_item_id", "release_date", "release_type", "release_status",
        "feature_description", "feature_name", "product_name", "last_modified",
        "active", "release_semester", "vso_item", "row_hash", "ValidFrom", "ValidTo",
    )
    for table in ("release_items", "release_item_versions"):
        history.execute(f"CREATE TABLE dbo.{table} (" +
                        ", ".join(f"{column} TEXT COLLATE NOCASE" for column in columns) + ")")
    cutoff = "2026-10-20T00:00:00.0000000"
    engine = MagicMock()

    def execute(statement, params=None):
        sql = str(statement)
        result = MagicMock()
        if "DECLARE @cutoff" in sql:
            result.mappings.return_value.one.return_value = {
                "cutoff": cutoff, "window_start": "2026-07-22T00:00:00.0000000",
            }
            return result
        sql = sql.replace("dbo.release_items FOR SYSTEM_TIME ALL", "dbo.release_item_versions")
        sql = sql.replace("CAST(d.ValidFrom AS date)", "date(d.ValidFrom)")
        sql = sql.replace("CAST(GETUTCDATE() AS DATE)", "'2026-10-20'")
        sql = sql.replace("DATEADD(day, -:days, '2026-10-20')", "dateadd_days(-:days, '2026-10-20')")
        sql = sql.replace("CONVERT(datetime2(7), :event_before, 126)", ":event_before")
        sql = sql.replace("DATEADD(day, -90, :event_before)", "dateadd_days(-90, :event_before)")
        sql = sql.replace("DATEADD(day, 1, :through)", "dateadd_days(1, :through)")
        sql = sql.replace("CONVERT(varchar(33), d.ValidFrom, 126)", "d.ValidFrom")
        sql = re.sub(r"CONVERT\(varchar\(30\), (d\.\w+), 23\)", r"CAST(\1 AS TEXT)", sql)
        sql = sql.replace("nvarchar(max)", "TEXT")
        sql = sql.replace("STRING_AGG(v.ColName, ',') WITHIN GROUP (ORDER BY v.ColOrder)",
                          "group_concat(v.ColName, ',')")
        sql = sql.replace("ChangedColumns = CASE", "CASE")
        sql = sql.replace("END\n    FROM Diffs d", "END AS ChangedColumns\n    FROM Diffs d")
        if "TOP (10001)" in sql:
            sql = sql.replace("TOP (10001)", "") + " LIMIT 10001"
        bindings = {key: value.isoformat() if isinstance(value, date) else value
                    for key, value in (params or {}).items()}
        rows = [dict(row) for row in history.execute(sql, bindings)]
        for row in rows:
            row["active"] = row["active"] == "1"
            row["last_modified"] = date.fromisoformat(row["last_modified"])
        result.mappings.return_value.all.return_value = rows
        return result

    engine.connect.return_value.__enter__.return_value.execute.side_effect = execute

    def append(timestamp, **changes):
        values = dict(
            release_item_id="one", release_date="q4 2026", release_type="preview",
            release_status="planned", feature_description="original", feature_name="original",
            product_name="original", last_modified="2026-10-18", active=1,
            release_semester="semester", vso_item=None, row_hash="fixture",
            ValidFrom=timestamp, ValidTo="9999-12-31T23:59:59.9999999",
        )
        values.update(changes)
        history.execute("""
            UPDATE dbo.release_item_versions SET ValidTo = ?
            WHERE release_item_id = ? AND ValidTo = '9999-12-31T23:59:59.9999999'
        """, (timestamp, values["release_item_id"]))
        history.execute("DELETE FROM dbo.release_items WHERE release_item_id = ?", (values["release_item_id"],))
        for table in ("release_items", "release_item_versions"):
            history.execute(f"INSERT INTO dbo.{table} VALUES ({','.join('?' for _ in columns)})",
                            [values[column] for column in columns])

    yield engine, append, history
    history.close()


def test_event_day_permalink_matches_overview_edits_removals_and_utc_bounds(temporal_changelog):
    engine, append, _ = temporal_changelog
    append("2026-10-18T00:00:00.0000000")
    append("2026-10-18T23:59:59.9999999", release_item_id="removed")
    append("2026-10-19T00:00:00.0000000", feature_description="first edit", last_modified="2026-10-19")
    append("2026-10-19T01:00:00.0000000", feature_description="second edit", last_modified="2026-10-19")
    append("2026-10-19T02:00:00.0000000", feature_description="second edit",
           last_modified="2026-10-19", row_hash="pipeline only")
    append("2026-10-19T03:00:00.0000000", release_item_id="removed", active=0)
    append("2026-10-20T00:00:00.0000000", feature_description="next day", last_modified="2026-10-20")
    client = server.app.test_client()
    with patch.object(server, "get_engine", return_value=engine):
        overview = client.get("/api/changelog?visit=1").json
        daily = client.get("/api/changelog?event_date=2026-10-19&days=1",
                           headers={"If-None-Match": "*",
                                    "If-Modified-Since": "Mon, 19 Oct 2026 00:00:00 GMT"})
        legacy = client.get("/api/changelog?date=2026-10-19").json
    assert daily.status_code == 200
    assert daily.json["total_items"] == 3
    assert daily.headers["Cache-Control"] == "private, no-store"
    assert "ETag" not in daily.headers and "Last-Modified" not in daily.headers
    assert daily.json["mode"] == "events"
    assert daily.json["event_date"] == "2026-10-19"
    assert daily.json["total_events"] == 3
    assert daily.json["total_features"] == 2
    assert "cutoff" not in daily.json and "complete" not in daily.json
    day = next(day for day in overview["days"] if day["date"] == "2026-10-19")
    fields = lambda item: (item["release_item_id"], item["active"], tuple(item["changed_columns"]))
    assert sorted(map(fields, daily.json["days"][0]["items"])) == sorted(map(fields, day["items"]))
    assert day["count"] == 3
    assert sorted(change for item in day["items"] for change in item["changed_columns"]) == [
        "Description updated", "Description updated", "Removed from Roadmap",
    ]
    assert legacy["total_items"] == 1  # Existing date= selection is intentionally preserved.


@pytest.mark.parametrize("field,value,annotation", [
    ("release_date", "Q4 2026", "Release Date q4 2026 -> Q4 2026"),
    ("release_type", "PREVIEW", "Release Type preview -> PREVIEW"),
    ("release_status", "PLANNED", "Release Status planned -> PLANNED"),
    ("feature_description", "ORIGINAL", "Description updated"),
    ("feature_name", "ORIGINAL", "Name original -> ORIGINAL"),
    ("product_name", "ORIGINAL", "Workload original -> ORIGINAL"),
    ("release_semester", "SEMESTER", "Semester semester -> SEMESTER"),
])
def test_ci_columns_keep_case_only_events_and_matching_annotations(temporal_changelog, field, value, annotation):
    engine, append, history = temporal_changelog
    assert history.execute("SELECT 'original' = 'ORIGINAL' COLLATE NOCASE").fetchone()[0] == 1
    append("2026-01-01T00:00:00.0000000")
    append("2026-10-19T01:00:00.0000000", **{field: value})
    append("2026-10-19T02:00:00.0000000", row_hash="pipeline only", **{field: value})
    result = get_changelog_visit_snapshot(engine)
    assert result["complete"] is True
    assert len(result["items"]) == 1  # Retained predecessor is not an addition in this window.
    assert result["items"][0]["changed_columns"] == [annotation]
    assert result["items"][0]["observed_at"] == "2026-10-19T01:00:00.0000000Z"
    assert len(result["items"][0]["event_id"]) == 64


@freeze_time("2026-10-19 00:00:01+00:00")
@pytest.mark.parametrize("since", ["", "bad", "2026-10-9", "2026-10-20", "2026-07-20", "2026-02-30"])
def test_invalid_checkpoint_rejected_before_database(since):
    with patch.object(server, "get_engine") as engine:
        response = server.app.test_client().get("/api/changelog", query_string={"since": since})
    assert response.status_code == 400
    assert response.headers["Cache-Control"] == "private, no-store"
    engine.assert_not_called()


@freeze_time("2026-10-19 00:00:01+00:00")
@pytest.mark.parametrize("since", ["2026-10-19", "2026-07-21"])
def test_inclusive_utc_bounds_and_private_cache(since):
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_changelog_with_changes", return_value=[]) as query:
        response = server.app.test_client().get(
            f"/api/changelog?since={since}&days=1&product_name=Power+BI",
            headers={"If-Modified-Since": "Mon, 19 Oct 2026 00:00:00 GMT", "If-None-Match": "*"},
        )
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "private, no-store"
    assert "ETag" not in response.headers
    assert "Last-Modified" not in response.headers
    assert query.call_args.kwargs["since"] == date.fromisoformat(since)
    assert query.call_args.kwargs["through"] == date(2026, 10, 19)
    assert query.call_args.kwargs["include_inactive"] is True
    assert query.call_args.kwargs["product_name"] == "Power BI"
    assert response.json["complete"] is True
    assert response.json["total_events"] == response.json["total_features"] == 0


@freeze_time("2026-10-18 23:59:59+00:00")
def test_tomorrow_is_not_yet_a_valid_checkpoint():
    assert server.app.test_client().get("/api/changelog?since=2026-10-19").status_code == 400


def test_date_and_since_are_mutually_exclusive():
    with patch.object(server, "get_engine") as engine:
        response = server.app.test_client().get("/api/changelog?date=2026-10-19&since=2026-10-19")
    assert response.status_code == 400
    engine.assert_not_called()


@freeze_time("2026-10-19")
def test_repeated_events_are_not_collapsed_into_features():
    rows = [
        dict(release_item_id="one", feature_name="Feature", product_name="Power BI",
             release_date=None, last_modified=date(2026, 10, 19), active=active,
             changed_columns=[change])
        for active, change in [(True, "Added to roadmap"), (False, "Removed from Roadmap"),
                               (True, "Restored to Roadmap"), (True, "Description updated")]
    ]
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_changelog_with_changes", return_value=rows):
        response = server.app.test_client().get("/api/changelog?since=2026-10-19")
    assert response.status_code == 200
    assert response.json["total_events"] == 4
    assert response.json["total_features"] == 1
    assert response.json["days"][0]["count"] == 4
    assert response.json["days"][0]["items"][1]["active"] is False


@freeze_time("2026-10-19")
def test_backend_failure_is_not_a_complete_empty_response(monkeypatch):
    monkeypatch.setitem(server.app.config, "PROPAGATE_EXCEPTIONS", False)
    with patch.object(server, "get_engine", side_effect=RuntimeError("database unavailable")):
        response = server.app.test_client().get("/api/changelog?since=2026-10-19")
    assert response.status_code == 500
    assert response.headers["Cache-Control"] == "private, no-store"
    assert not response.is_json or not response.json.get("complete")


def test_temporal_query_preserves_every_tracked_transition_and_binds_filters():
    engine = MagicMock()
    execute = engine.connect.return_value.__enter__.return_value.execute
    execute.return_value.mappings.return_value.all.return_value = []
    product = "x'; DROP TABLE release_items; --"
    get_changelog_with_changes(
        engine, since=date(2026, 10, 18), through=date(2026, 10, 19),
        product_name=product, include_inactive=True,
    )
    statement, params = execute.call_args.args
    sql = str(statement)
    assert product not in sql
    assert params == {"product_name": product, "since": date(2026, 10, 18), "through": date(2026, 10, 19)}
    assert "FOR SYSTEM_TIME ALL" in sql
    assert "ValidFrom >= :since" in sql
    assert "ValidFrom < DATEADD(day, 1, :through)" in sql
    assert "CAST(d.ValidFrom AS date) AS last_modified" in sql
    assert "h.VersionNum AS ContentVer" in sql
    assert "MIN(h.VersionNum)" not in sql  # Later same-day refreshes are not discarded.
    assert "EXCEPT" in sql  # NULL-safe tracked-field comparison, not a row_hash comparison.
    predicate = sql.split("EXCEPT")[1].split("ORDER BY")[0]
    assert "p_active" in predicate
    assert "p_feature_description" in predicate
    assert "p_row_hash" not in predicate
    assert "blog_" not in predicate
    assert "AND active = 1" not in sql
    assert sql.rfind("AND product_name = :product_name") > sql.index("FROM Diffs d")


@freeze_time("2026-10-19 00:01:00+00:00")
def test_catch_up_reads_removal_despite_legacy_feed_suppression_and_later_edits():
    """Run the real writer and the query's actual CTEs/predicates without SQL Server."""
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session
    from db.db_sqlserver import Base, ReleaseItemModel, deactivate_missing_releases

    writer_engine = create_engine("sqlite://")
    Base.metadata.create_all(writer_engine)
    with Session(writer_engine) as session:
        session.add(ReleaseItemModel(
            release_item_id="one", feature_name="Feature", row_hash="x" * 64,
            active=True, last_modified=date(2026, 10, 1),
        ))
        session.commit()
    deactivate_missing_releases(writer_engine, set())
    with Session(writer_engine) as session:
        removed = session.scalar(select(ReleaseItemModel))
        assert removed.active is False
        assert removed.last_modified == date(2026, 10, 1)
        removal_day = removed.last_modified.isoformat()
    writer_engine.dispose()

    engine = MagicMock()
    execute = engine.connect.return_value.__enter__.return_value.execute
    execute.return_value.mappings.return_value.all.return_value = []
    get_changelog_with_changes(engine, since=date(2026, 10, 18), through=date(2026, 10, 19))
    statement, params = execute.call_args.args
    sql = str(statement)
    ctes = sql.split("\n    SELECT\n        d.release_item_id,", 1)[0]
    final_predicates = sql.split("    FROM Diffs d\n    INNER JOIN Selected l", 1)[1]
    # SQLite fixtures explicitly represent FOR SYSTEM_TIME ALL's version set.
    # Adapt only SQL Server's temporal syntax and UTC day/date-add expressions.
    query = (ctes + "\n SELECT d.active, d.ValidFrom, d.last_modified"
             " FROM Diffs d INNER JOIN Selected l" + final_predicates)
    query = query.replace(" FOR SYSTEM_TIME ALL", "")
    query = query.replace("DATEADD(day, 1, :through)", "date(:through, '+1 day')")
    query = query.replace("CAST(d.ValidFrom AS date)", "date(d.ValidFrom)")
    query = query.replace("nvarchar(max)", "TEXT")
    with sqlite3.connect(":memory:") as history:
        history.create_collation("Latin1_General_100_BIN2", lambda a, b: (a > b) - (a < b))
        history.execute("ATTACH DATABASE ':memory:' AS dbo")
        history.execute("""
            CREATE TABLE dbo.release_items (
                release_item_id TEXT, release_date TEXT, release_type TEXT,
                release_status TEXT, feature_description TEXT, feature_name TEXT,
                product_name TEXT, last_modified TEXT, active INTEGER,
                release_semester TEXT, vso_item TEXT, row_hash TEXT, ValidFrom TEXT
            )
        """)
        for timestamp, active, description, modified in [
            ("2026-10-01 00:00:00", 1, "original", "2026-10-01"),
            ("2026-10-19 00:01:00", 0, "original", removal_day),
            ("2026-10-19 00:02:00", 0, "original", removal_day),  # Blog/vector processing.
            ("2026-10-19 01:00:00", 1, "original", "2026-10-19"),
            ("2026-10-19 02:00:00", 1, "edited", "2026-10-19"),
            ("2026-10-20 00:00:00", 1, "later edit", "2026-10-20"),
        ]:
            history.execute("""
                INSERT INTO dbo.release_items
                (release_item_id, feature_name, feature_description, active, last_modified, ValidFrom)
                VALUES ('one', 'Feature', ?, ?, ?, ?)
            """, (description, active, modified, timestamp))
        rows = history.execute(query, {key: value.isoformat() for key, value in params.items()}).fetchall()
    assert rows == [
        (0, "2026-10-19 00:01:00", "2026-10-01"),
        (1, "2026-10-19 01:00:00", "2026-10-19"),
        (1, "2026-10-19 02:00:00", "2026-10-19"),
    ]


@pytest.mark.parametrize("kwargs", [
    {"since": date(2026, 10, 19)},
    {"since": date(2026, 10, 19), "through": date(2026, 10, 18)},
    {"since": date(2026, 10, 19), "through": date(2026, 10, 19), "on_date": date(2026, 10, 19)},
])
def test_db_rejects_incomplete_or_conflicting_catch_up_bounds(kwargs):
    engine = MagicMock()
    with pytest.raises(ValueError, match="since/through"):
        get_changelog_with_changes(engine, **kwargs)
    engine.connect.assert_not_called()


def test_changelog_shell_does_not_embed_personal_checkpoint():
    with patch.object(server, "get_engine") as engine:
        response = server.app.test_client().get("/changelog?since=1999-12-31")
    assert response.status_code == 200
    assert "1999-12-31" not in response.get_data(as_text=True)
    html = response.get_data(as_text=True)
    for removed in ("catch-up-toggle", "checkpoint-mark", "checkpoint-reset", "Pick up where you left off"):
        assert removed not in html
    assert "Changelog</h1>" in html
    engine.assert_not_called()


def test_browser_checkpoint_and_changelog_behaviors():
    node = shutil.which("node")
    assert node, "Node.js 22+ is required for browser-logic tests (also installed by CI)"
    result = subprocess.run(
        [node, "--test", str(Path(__file__).with_name("checkpoint.test.cjs"))],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_visit_snapshot_is_unfiltered_private_and_has_full_precision_metadata():
    cutoff = "2026-10-19T00:00:00.1234567Z"
    row = dict(release_item_id="one", feature_name="Feature", product_name="Other",
               release_date=None, last_modified=date(2026, 10, 18), active=False,
               changed_columns=["Removed from Roadmap"], event_id="a" * 64,
               observed_at="2026-10-18T23:59:59.9999999Z")
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_changelog_visit_snapshot", return_value={
             "cutoff": cutoff, "window_start": "2026-07-21T00:00:00.1234567Z",
             "complete": True, "items": [row],
         }) as query, patch.object(server, "get_changelog_with_changes") as legacy:
        response = server.app.test_client().get(
            "/api/changelog?visit=1&product_name=Power+BI&include_inactive=false&days=1",
            headers={"If-None-Match": "*", "If-Modified-Since": "Mon, 19 Oct 2026 00:00:00 GMT"},
        )
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "private, no-store"
    assert "ETag" not in response.headers and "Last-Modified" not in response.headers
    assert response.json["mode"] == "visit"
    assert response.json["cutoff"] == cutoff
    assert response.json["days"][0]["items"][0]["observed_at"].endswith("9999999Z")
    assert response.json["days"][0]["items"][0]["active"] is False
    assert response.json["complete"] is True
    assert query.call_args.kwargs == {}
    legacy.assert_not_called()


@pytest.mark.parametrize("query", ["visit=0", "visit=", "visit=1&since=2026-10-19", "visit=1&date=2026-10-19"])
def test_invalid_visit_combinations_rejected_privately_before_database(query):
    with patch.object(server, "get_engine") as engine:
        response = server.app.test_client().get("/api/changelog?" + query)
    assert response.status_code == 400
    assert response.headers["Cache-Control"] == "private, no-store"
    engine.assert_not_called()


def test_overflow_is_an_explicit_incomplete_error_not_a_capped_success():
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_changelog_visit_snapshot", return_value={"complete": False, "items": []}):
        response = server.app.test_client().get("/api/changelog?visit=1")
    assert response.status_code == 503
    assert response.json["complete"] is False
    assert "cutoff" not in response.json
    assert response.headers["Cache-Control"] == "private, no-store"


def test_visit_sql_binds_exclusive_precise_server_cutoff_caps_results_and_keeps_event_identity():
    engine = MagicMock()
    execute = engine.connect.return_value.__enter__.return_value.execute
    execute.return_value.mappings.return_value.all.return_value = [{
        "release_item_id": "one", "feature_name": "Feature", "product_name": "Product",
        "release_type": "Preview", "release_status": "Planned",
        "observed_at": "2026-10-19T00:00:00.0000001",
    }]
    cutoff = "2026-10-19T00:00:00.0000002"
    first = get_changelog_with_changes(engine, event_before=cutoff)
    statement, params = execute.call_args.args
    sql = str(statement)
    assert "TOP (10001)" in sql
    assert "DATEADD(day, -90, CONVERT(datetime2(7), :event_before, 126))" in sql
    assert "ValidFrom < CONVERT(datetime2(7), :event_before, 126)" in sql
    assert "ValidTo >= DATEADD(day, -90, CONVERT(datetime2(7), :event_before, 126))" in sql
    assert "EXCEPT" in sql
    assert "CAST(d.ValidFrom AS date) AS last_modified" in sql
    assert params == {"event_before": cutoff}
    assert first[0]["observed_at"] == "2026-10-19T00:00:00.0000001Z"
    assert len(first[0]["event_id"]) == 64
    assert get_changelog_with_changes(engine, event_before=cutoff) == first
    execute.return_value.mappings.return_value.all.return_value[0]["observed_at"] = "2026-10-19T00:00:00.0000002"
    assert get_changelog_with_changes(engine, event_before=cutoff)[0]["event_id"] != first[0]["event_id"]


@pytest.mark.parametrize("count,complete", [(10000, True), (10001, False)])
def test_server_snapshot_metadata_and_exact_limit(count, complete):
    engine = MagicMock()
    execute = engine.connect.return_value.__enter__.return_value.execute
    execute.return_value.mappings.return_value.one.return_value = {
        "cutoff": "2026-10-19T00:00:00.1234567", "window_start": "2026-07-21T00:00:00.1234567",
    }
    with patch("db.db_sqlserver.get_changelog_with_changes", return_value=[{}] * count) as query:
        result = get_changelog_visit_snapshot(engine)
    assert result["complete"] is complete
    assert result["cutoff"] == "2026-10-19T00:00:00.1234567Z"
    assert query.call_args.kwargs == {"event_before": "2026-10-19T00:00:00.1234567"}
    assert "DECLARE @cutoff datetime2(7) = SYSUTCDATETIME()" in str(execute.call_args.args[0])


def test_daily_permalink_never_runs_visit_tracking():
    response = server.app.test_client().get("/changelog/2026-10-19")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "/static/checkpoint.js" not in html
    assert "/static/changelog.js" not in html
