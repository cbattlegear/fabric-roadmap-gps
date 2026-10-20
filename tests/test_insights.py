"""No-network metric, temporal-diff SQL, route, and cache regression coverage."""

import copy
import json
import re
import sqlite3
from datetime import date, datetime
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import pytest
import pyodbc
from freezegun import freeze_time
from sqlalchemy.exc import SQLAlchemyError
from bs4 import BeautifulSoup

import server
from db.db_sqlserver import _RELEASE_FIELDS, _insights_sql, get_insights
from lib.insights import parse_insights_query, prepare_insights


TODAY = date(2026, 10, 19)


@pytest.fixture
def payload():
    return {
        "activity": [
            {"product_name": "Power BI", "kind": "edit", "events": 3, "features": 1},
            {"product_name": "Unknown product", "kind": "addition", "events": 1, "features": 1},
        ],
        "transitions": [{"day": "2026-10-18", "events": 2, "features": 1}],
        "target_changes": 1,
        "upcoming": [{"bucket": "2027-Q1", "features": 1}, {"bucket": "unknown", "features": 1}],
        "products": [{"name": "Power BI"}, {"name": "Unknown product"}],
        "coverage": {"earliest_version_in_query": None, "last_data_write": "2026-10-19T12:00:00"},
        "details": {"total": 51, "items": [{
            "release_item_id": "feature-1", "feature_name": "A feature <script>",
            "product_name": "Power BI", "observed_at": "2026-10-18T13:00:00",
            "kind": "edit", "previous_status": "Planned", "release_status": "Shipped",
            "previous_type": None, "release_type": "GA",
            "previous_target": "2026-12-31", "revised_target": "2027-03-31",
            "status_changed": 1, "target_changed": 1, "bucket": None,
        }]},
    }


def test_utc_calendar_window_and_bounds():
    query = parse_insights_query({}, TODAY)
    assert query.start == date(2026, 9, 20)
    assert query.end == date(2026, 10, 20)
    assert query.days == 30
    assert parse_insights_query({"days": "90"}, TODAY).start == date(2026, 7, 22)
    with freeze_time("2026-10-20 00:00:00"):
        assert parse_insights_query({}).end == date(2026, 10, 21)


@pytest.mark.parametrize("args", [
    {"days": "31"}, {"days": "bad"}, {"days": "0"}, {"page": "0"},
    {"page": "201"}, {"end": "2026-1-1"}, {"end": "2027-01-01"},
    {"end": "2020-01-01"}, {"metric": "forecast"}, {"product_name": "a" * 201},
    {"bucket": "2026-10-18"}, {"metric": "status", "bucket": "2026-10-20"},
    {"metric": "status", "bucket": "2026-09-19"},
    {"metric": "upcoming", "bucket": "2027-Q5"},
    {"metric": "upcoming", "bucket": "bad"},
])
def test_invalid_queries(args):
    with pytest.raises(ValueError):
        parse_insights_query(args, TODAY)


def test_links_preserve_exact_event_filters_and_pagination(payload):
    query = parse_insights_query({"days": "90", "product_name": "Power BI"}, TODAY)
    data = prepare_insights(payload, query)
    activity = parse_qs(urlsplit(data["activity"][0]["url"]).query)
    assert activity == {
        "days": ["90"], "end": ["2026-10-20"], "product_name": ["Power BI"],
        "metric": ["edit"], "page": ["1"],
    }
    transition = parse_qs(urlsplit(data["transitions"][0]["url"]).query)
    assert transition["metric"] == ["status"]
    assert transition["bucket"] == ["2026-10-18"]
    assert transition["product_name"] == ["Power BI"]
    upcoming = parse_qs(urlsplit(data["upcoming"][1]["url"]).query)
    assert upcoming["bucket"] == ["unknown"]
    assert upcoming["metric"] == ["upcoming"]
    assert data["coverage"]["complete"] is False
    assert data["details"]["next_url"] == query.url(page=2)
    assert data["details"]["previous_url"] is None
    assert data["details"]["capped"] is False
    assert data["visuals"]["events"] == 4
    assert data["visuals"]["status_events"] == 2
    assert data["visuals"]["target_percent"] == 25
    assert data["visuals"]["upcoming_features"] == 2
    assert len(data["visuals"]["status_points"].split()) == 90
    assert data["visuals"]["status_end"] == "Oct 19"
    assert data["visuals"]["products"][0]["events"] == 3
    assert data["visuals"]["products"][0]["width"] == 100
    data["details"]["total"] = 10001
    last_page = parse_insights_query({"page": "200"}, TODAY)
    prepare_insights(data, last_page)
    assert data["details"]["capped"] is True
    assert data["details"]["next_url"] is None


@pytest.fixture
def temporal_events():
    """Execute the production diff CTE in SQLite, adapting only dialect syntax.

    Native period filtering / JSON serialization / timeout wiring are separately
    covered by the opt-in SQL Server fixture and helper/route contract tests.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    fields = [spec.model_attr for spec in _RELEASE_FIELDS]
    conn.execute("CREATE TABLE temporal_rows (release_item_id TEXT, "
                 + ", ".join(f"{field} TEXT" for field in fields)
                 + ", active INTEGER, ValidFrom TEXT, ValidTo TEXT)")

    def add_chain(item_id, versions):
        for index, version in enumerate(versions):
            row = dict(
                release_item_id=item_id, feature_name="Feature", feature_description="Description",
                product_name="Power BI", active=1, release_status="Planned", release_type="Preview",
                release_date="2026-12-31",
            )
            row.update(version)
            row["ValidTo"] = versions[index + 1]["ValidFrom"] if index + 1 < len(versions) else "9999-12-31"
            conn.execute("INSERT INTO temporal_rows (" + ", ".join(row) + ") VALUES ("
                         + ", ".join("?" for _ in row) + ")", list(row.values()))

    def events(product=None):
        sql = str(_insights_sql()).split("), Upcoming AS (")[0] + ") SELECT * FROM Events ORDER BY ValidFrom"
        sql = sql.replace(
            "FROM dbo.release_items FOR SYSTEM_TIME FROM :baseline TO :end",
            "FROM temporal_rows WHERE ValidFrom < :end AND ValidTo > :baseline AND ValidFrom < ValidTo",
        )
        sql = sql.replace("nvarchar(max)", "TEXT").replace("Latin1_General_100_BIN2", "BINARY").replace("N''", "''")
        return [dict(row) for row in conn.execute(sql, {
            "baseline": "2026-09-19T23:59:59.999999", "start": "2026-09-20T00:00:00",
            "end": "2026-10-20T00:00:00", "product": product,
        })]

    yield add_chain, events
    conn.close()


def test_repeated_transitions_pipeline_exclusion_and_product_attribution(temporal_events):
    add, events = temporal_events
    add("one", [
        {"ValidFrom": "2026-09-01T00:00:00"},
        {"ValidFrom": "2026-10-18T01:00:00"},  # pipeline write: unchanged content
        {"ValidFrom": "2026-10-18T02:00:00", "feature_description": "Revised"},
        {"ValidFrom": "2026-10-18T03:00:00", "release_status": "Shipped"},
        {"ValidFrom": "2026-10-18T04:00:00", "release_status": "Planned"},
        {"ValidFrom": "2026-10-18T05:00:00", "active": 0},
        {"ValidFrom": "2026-10-18T06:00:00", "active": 1, "product_name": "Data Factory"},
    ])
    result = events()
    assert [row["kind"] for row in result] == ["edit", "edit", "edit", "removal", "restoration"]
    assert len({row["release_item_id"] for row in result}) == 1
    assert result[0]["status_changed"] == 0  # description-only edit
    assert result[1]["status_changed"] == 1
    assert result[2]["status_changed"] == 1
    assert events("Data Factory")[0]["kind"] == "restoration"
    assert events("Data Factory")[0]["p_product_name"] == "Power BI"
    assert len(events("Power BI")) == 4


def test_boundaries_missing_history_and_unknown_targets(temporal_events):
    add, events = temporal_events
    add("boundary", [
        {"ValidFrom": "2026-09-19T22:00:00"},
        {"ValidFrom": "2026-09-20T00:00:00", "release_date": None},
        {"ValidFrom": "2026-10-19T23:59:59", "release_date": "2027-03-31"},
        {"ValidFrom": "2026-10-20T00:00:00", "release_date": "2027-06-30"},
    ])
    add("missing", [{"ValidFrom": "2026-10-18T00:00:00", "product_name": None, "release_status": None}])
    result = events()
    assert len(result) == 3
    assert result[0]["target_changed"] == 1
    assert result[0]["p_release_date"] == "2026-12-31"
    assert result[1]["kind"] == "addition"
    assert result[1]["product"] == "Unknown product"
    assert result[1]["target_changed"] == result[1]["status_changed"] == 0
    assert result[2]["p_release_date"] is None
    assert result[2]["target_changed"] == 1


def test_hash_only_changes_and_normalized_empty_text_do_not_count(temporal_events):
    add, events = temporal_events
    add("one", [
        {"ValidFrom": "2026-09-01T00:00:00", "feature_description": None},
        {"ValidFrom": "2026-10-18T00:00:00", "feature_description": ""},
        {"ValidFrom": "2026-10-18T01:00:00", "feature_description": ""},
    ])
    assert events() == []


def test_other_tracked_fields_are_substantive(temporal_events):
    add, events = temporal_events
    add("one", [
        {"ValidFrom": "2026-09-01T00:00:00"},
        {"ValidFrom": "2026-10-18T00:00:00", "is_publish_externally": "0", "release_status_value": "1"},
        {"ValidFrom": "2026-10-18T01:00:00", "vso_item": "https://example.test/item"},
    ])
    assert len(events()) == 2
    assert events()[0]["status_changed"] == 1


def test_upcoming_current_quarters_unknowns_and_product_filters():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.create_function("YEAR", 1, lambda value: int(value[:4]))
    conn.create_function("DATEPART", 2, lambda part, value: (int(value[5:7]) - 1) // 3 + 1)
    conn.create_function("CONCAT", -1, lambda *parts: "".join(str(part) for part in parts))
    conn.execute("""CREATE TABLE current_releases (
        release_item_id TEXT, feature_name TEXT, product_name TEXT,
        release_status TEXT, release_type TEXT, release_date TEXT, active INTEGER)""")
    conn.executemany("INSERT INTO current_releases VALUES (?, ?, ?, ?, ?, ?, ?)", [
        ("current-quarter", "A", "Power BI", "Planned", "Preview", "2026-10-01", 1),
        ("future", "B", "Power BI", None, "Preview", "2027-03-31", 1),
        ("undated", "C", None, None, None, None, 1),
        ("old-target", "D", "Power BI", "Planned", None, "2026-09-30", 1),
        ("removed", "E", "Power BI", "Planned", None, "2027-03-31", 0),
        ("shipped", "F", "Power BI", "Shipped", None, "2027-03-31", 1),
    ])
    sql = "WITH Upcoming AS (" + str(_insights_sql()).split("), Upcoming AS (")[1].split("), Detail AS (")[0]
    sql += ") SELECT * FROM Upcoming ORDER BY release_item_id"
    sql = sql.replace("dbo.release_items", "current_releases").replace("DATEPART(quarter,", "DATEPART('quarter',")
    params = {"product": None, "quarter_start": "2026-10-01"}
    rows = [dict(row) for row in conn.execute(sql, params)]
    assert [(row["release_item_id"], row["bucket"]) for row in rows] == [
        ("current-quarter", "2026-Q4"), ("future", "2027-Q1"), ("undated", "unknown")]
    assert rows[2]["product_name"] == "Unknown product"
    assert len(list(conn.execute(sql, params | {"product": "Power BI"}))) == 2
    conn.close()


def test_query_is_parameterized_and_restores_driver_timeout(payload):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.invalidated = False
    driver = conn.connection.driver_connection
    driver.timeout = 42
    encoded = json.dumps(payload)
    conn.execute.return_value = [(encoded[:100],), (encoded[100:],)]
    query = parse_insights_query({"product_name": "x'; DROP TABLE x;--"}, TODAY)
    assert get_insights(engine, query) == payload
    sql, params = conn.execute.call_args.args
    assert query.product_name not in str(sql)
    assert params["product"] == query.product_name
    assert params["limit"] == 50
    assert params["baseline"] == datetime(2026, 9, 19, 23, 59, 59, 999999)
    assert driver.timeout == 42
    assert "MAXDOP 2" in str(sql)
    assert not re.search(r"LAG\((blog_|release_vector|row_hash|last_modified)", str(sql))
    conn.execute.side_effect = SQLAlchemyError("query failed")
    with pytest.raises(SQLAlchemyError):
        get_insights(engine, query)
    assert driver.timeout == 42


def test_disconnected_driver_cleanup_does_not_mask_database_error():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.invalidated = False

    class Driver:
        def __init__(self):
            self.closed = False
            self._timeout = 42

        @property
        def timeout(self):
            return self._timeout

        @timeout.setter
        def timeout(self, value):
            if self.closed:
                raise pyodbc.ProgrammingError("Attempt to use a closed connection")
            self._timeout = value

    driver = Driver()
    conn.connection.driver_connection = driver
    original = SQLAlchemyError("original disconnect")

    def disconnect(*_):
        driver.closed = True
        conn.invalidated = True
        raise original

    conn.execute.side_effect = disconnect
    with pytest.raises(SQLAlchemyError, match="original disconnect") as caught:
        get_insights(engine, parse_insights_query({}, TODAY))
    assert caught.value is original


def test_cleanup_error_is_logged_and_connection_discarded_without_masking_query_error():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.invalidated = False

    class Driver:
        _timeout = 42

        @property
        def timeout(self):
            return self._timeout

        @timeout.setter
        def timeout(self, value):
            if value == 42:
                raise pyodbc.ProgrammingError("closed during cleanup")
            self._timeout = value

    conn.connection.driver_connection = Driver()
    original = SQLAlchemyError("original query failure")
    conn.execute.side_effect = original
    with pytest.raises(SQLAlchemyError, match="original query failure") as caught:
        get_insights(engine, parse_insights_query({}, TODAY))
    assert caught.value is original
    conn.invalidate.assert_called_once()


def test_cleanup_failure_after_success_returns_database_error(payload):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.invalidated = False

    class Driver:
        _timeout = 42

        @property
        def timeout(self):
            return self._timeout

        @timeout.setter
        def timeout(self, value):
            if value == 42:
                raise pyodbc.ProgrammingError("closed during cleanup")
            self._timeout = value

    conn.connection.driver_connection = Driver()
    conn.execute.return_value = [(json.dumps(payload),)]
    with pytest.raises(SQLAlchemyError, match="Unable to restore insights connection timeout"):
        get_insights.__wrapped__(engine, parse_insights_query({}, TODAY))
    conn.invalidate.assert_called_once()


@freeze_time("2026-10-19 14:00:00")
def test_api_shape_filter_forwarding_and_html_tables(payload):
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_insights", side_effect=lambda *_: copy.deepcopy(payload)) as db:
        client = server.app.test_client()
        response = client.get("/api/insights?days=90&product_name=Power%20BI&metric=status&bucket=2026-10-18&page=2")
        assert response.status_code == 200
        data = response.get_json()
        assert data["filters"]["days"] == 90
        assert data["filters"]["page"] == 2
        assert data["coverage"]["complete"] is False
        assert data["activity"][0]["events"] == 3
        query = db.call_args.args[1]
        assert query.product_name == "Power BI"
        assert query.bucket == "2026-10-18"
        html = client.get("/insights").get_data(as_text=True)
        assert "<caption>" in html and 'scope="col"' in html
        assert "/release/feature-1" in html
        assert "A feature &lt;script&gt;" in html
        assert "Current" in html
        assert "Observed history, not a complete audit" not in html
        assert "Last stored data write" not in html
        assert "Missing predecessors" not in html
        assert "UTC window:" not in html
        assert 'class="insights-grid"' in html
        assert 'class="insights-donut"' in html
        assert 'class="insights-trend"' in html
        soup = BeautifulSoup(html, "html.parser")
        assert not soup.select_one("#evidence").has_attr("open")
        for table in soup.select("table"):
            assert table.find_parent("details") is not None
        assert 'aria-current="page"' in html
        assert 'name="end" value="2026-10-20"' in html


@freeze_time("2026-10-19")
def test_cache_varies_with_data_filters_and_window_rollover(payload):
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_insights", side_effect=lambda *_: copy.deepcopy(payload)):
        client = server.app.test_client()
        first = client.get("/api/insights")
        assert first.headers["Last-Modified"].startswith("Mon, 19 Oct 2026")
        assert "max-age=3600" in first.headers["Cache-Control"]
        etag = first.headers["ETag"]
        conditional = client.get("/api/insights", headers={"If-None-Match": etag})
        assert conditional.status_code == 304
        assert conditional.headers["ETag"] == etag
        assert "public" in conditional.headers["Cache-Control"]
        assert client.get("/api/insights?days=90").headers["ETag"] != etag
        assert client.get("/api/insights?page=2").headers["ETag"] != etag
        assert client.get("/api/insights?product_name=Power%20BI").headers["ETag"] != etag
        payload["activity"][0]["events"] += 1
        assert client.get("/api/insights").headers["ETag"] != etag
        payload["activity"][0]["events"] -= 1
        with freeze_time("2026-10-20"):
            assert client.get("/api/insights").headers["ETag"] != etag


@pytest.mark.parametrize("path", ["/api/insights?days=7", "/insights?metric=bogus"])
def test_bad_filters_rejected_before_db(path):
    with patch.object(server, "get_engine") as engine:
        response = server.app.test_client().get(path)
    assert response.status_code == 400
    assert response.headers["Cache-Control"] == "no-store"
    engine.assert_not_called()


@pytest.mark.parametrize("path", ["/api/insights", "/insights"])
def test_db_errors_visible_not_zero_filled(path):
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_insights", side_effect=SQLAlchemyError("offline")):
        response = server.app.test_client().get(path)
    assert response.status_code == 503
    assert response.headers["Cache-Control"] == "no-store"
    assert "temporarily unavailable" in response.get_data(as_text=True)


def test_empty_unknown_coverage_state(payload):
    payload.update(activity=[], transitions=[], upcoming=[], target_changes=0)
    payload["details"] = {"total": 0, "items": []}
    payload["coverage"]["last_data_write"] = None
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_insights", return_value=payload):
        response = server.app.test_client().get("/insights")
    assert response.status_code == 200
    assert "No observed roadmap activity" in response.get_data(as_text=True)
    assert 'class="insights-donut"' in response.get_data(as_text=True)
    assert payload["coverage"]["complete"] is False
    assert payload["visuals"]["target_percent"] == 0
    assert payload["visuals"]["products"] == []


@freeze_time("2026-10-19")
@pytest.mark.parametrize("suffix", ["?metric=target", "?page=2", "?metric=upcoming&bucket=2027-Q1"])
def test_explicit_drilldowns_expand_evidence_but_filtered_overview_does_not(payload, suffix):
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_insights", side_effect=lambda *_: copy.deepcopy(payload)):
        client = server.app.test_client()
        drilldown = BeautifulSoup(client.get("/insights" + suffix).get_data(as_text=True), "html.parser")
        assert drilldown.select_one("#evidence").has_attr("open")
        overview = BeautifulSoup(client.get("/insights?product_name=Power%20BI").get_data(as_text=True), "html.parser")
        assert not overview.select_one("#evidence").has_attr("open")


@freeze_time("2026-10-19")
@pytest.mark.parametrize("suffix", ["", "?metric=target"])
def test_release_window_copy_preserves_event_metric_and_drilldown(payload, suffix):
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_insights", side_effect=lambda *_: copy.deepcopy(payload)):
        client = server.app.test_client()
        html = client.get("/insights" + suffix).get_data(as_text=True)
        soup = BeautifulSoup(html, "html.parser")
        assert soup.select_one("#targets-title").text == "Release window changes"
        assert "Planned dates added, changed or removed" in html
        assert "of observed events changed a planned release date" in html
        assert "Explore release window changes" in html
        assert "release window change events" in soup.select_one(".insights-donut")["aria-label"]
        assert "Target changes" not in html
        assert "Explore target changes" not in html
        assert "changed a target" not in html
        assert parse_qs(urlsplit(soup.select_one(".insights-donut")["href"]).query)["metric"] == ["target"]
        if suffix:
            assert "release window changes" in soup.select_one("#evidence > summary").text
        data = client.get("/api/insights?metric=target").get_json()
        assert data["filters"]["metric"] == "target"
        assert data["target_changes"] == 1


@freeze_time("2026-10-19")
@pytest.mark.parametrize("suffix", ["", "?metric=upcoming&bucket=2027-Q1"])
def test_planned_releases_copy_preserves_quarter_groups_and_api_keys(payload, suffix):
    with patch.object(server, "get_engine", return_value=object()), \
         patch.object(server, "get_insights", side_effect=lambda *_: copy.deepcopy(payload)):
        client = server.app.test_client()
        html = client.get("/insights" + suffix).get_data(as_text=True)
        soup = BeautifulSoup(html, "html.parser")
        assert soup.select_one("#upcoming-title").text == "Planned releases per quarter"
        assert "View planned release data" in html
        assert "features with planned releases" in html
        assert "features with releases planned for 2027-Q1" in html
        assert "Upcoming targets" not in html
        assert "View target data" not in html
        quarter_link = soup.select_one(".insights-quarter-track")["href"]
        params = parse_qs(urlsplit(quarter_link).query)
        assert params["metric"] == ["upcoming"]
        assert params["bucket"] == ["2027-Q1"]
        if suffix:
            assert "Matching planned releases" in soup.select_one("#evidence > summary").text
        data = client.get("/api/insights?metric=upcoming&bucket=2027-Q1").get_json()
        assert data["filters"]["metric"] == "upcoming"
        assert data["filters"]["bucket"] == "2027-Q1"
        assert data["upcoming"][0]["features"] == 1


def test_shared_navigation_wraps_without_dashboard_stylesheet():
    client = server.app.test_client()
    css = client.get("/static/styles.css").get_data(as_text=True)
    nav_rule = re.search(r"\.header-nav\s*\{([^}]+)\}", css)
    assert nav_rule is not None
    assert "flex-wrap: wrap" in nav_rule.group(1)
    html = client.get("/changelog").get_data(as_text=True)
    assert "/static/styles.css?v=compact-views-v1" in html
    assert "/static/insights.css" not in html
    soup = BeautifulSoup(html, "html.parser")
    assert soup.select_one('.header-nav a[href="/insights"]') is not None
