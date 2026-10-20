"""Opt-in native temporal/JSON verification in a disposable non-production DB.

Set INSIGHTS_TEST_SQLSERVER_CONN explicitly; ordinary CI skips this test.
"""

import json
import os
import time
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from db.db_sqlserver import _insights_sql


@pytest.mark.skipif(not os.getenv("INSIGHTS_TEST_SQLSERVER_CONN"),
                    reason="Dedicated non-production SQL Server fixture not configured")
def test_native_temporal_aggregates_and_exact_evidence():
    engine = create_engine(URL.create(
        "mssql+pyodbc", query={"odbc_connect": os.environ["INSIGHTS_TEST_SQLSERVER_CONN"]}))
    name = "insights_test_" + uuid.uuid4().hex
    table, history = f"dbo.{name}", f"dbo.{name}_history"
    created = False
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE {table} (
                    release_item_id nvarchar(36) PRIMARY KEY,
                    feature_name nvarchar(400), feature_description nvarchar(max),
                    release_date date, release_type nvarchar(100), release_status nvarchar(100),
                    release_semester nvarchar(200), vso_item nvarchar(1000),
                    product_name nvarchar(200), release_type_value int, release_status_value int,
                    product_id nvarchar(36), is_publish_externally bit, active bit NOT NULL,
                    blog_url nvarchar(1000),
                    ValidFrom datetime2 GENERATED ALWAYS AS ROW START NOT NULL,
                    ValidTo datetime2 GENERATED ALWAYS AS ROW END NOT NULL,
                    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {history}))
            """))
        created = True
        statements = [
            f"INSERT INTO {table} (release_item_id, feature_name, product_name, active, release_status, release_date) "
            "VALUES ('one', 'One', 'Power BI', 1, 'Planned', '2099-03-31')",
            f"UPDATE {table} SET blog_url = 'https://example.test/blog'",
            f"UPDATE {table} SET feature_description = 'First edit'",
            f"UPDATE {table} SET feature_description = 'Second edit'",
            f"UPDATE {table} SET release_status = 'Shipped', release_date = NULL",
            f"UPDATE {table} SET active = 0",
            f"UPDATE {table} SET active = 1, product_name = 'Data Factory'",
        ]
        for statement in statements:
            time.sleep(.02)
            with engine.begin() as conn:
                conn.execute(text(statement))
        now = datetime.utcnow()
        params = {
            "baseline": now - timedelta(days=30, microseconds=1),
            "start": now - timedelta(days=30), "end": now + timedelta(days=1),
            "quarter_start": now.date().replace(day=1),
            "product": None, "metric": "all", "bucket": None, "offset": 0, "limit": 50,
        }
        query = text(str(_insights_sql()).replace("dbo.release_items", table))

        def fetch(**overrides):
            with engine.connect() as conn:
                return json.loads("".join(row[0] for row in conn.execute(query, params | overrides)))

        data = fetch()
        assert sum(row["events"] for row in data["activity"]) == 6
        assert data["details"]["total"] == len(data["details"]["items"]) == 6
        assert data["target_changes"] == 1
        assert sum(row["events"] for row in data["transitions"]) == 1
        assert fetch(metric="edit")["details"]["total"] == 3
        assert fetch(metric="status")["details"]["total"] == 1
        assert fetch(metric="target")["details"]["items"][0]["previous_target"] == "2099-03-31"
        assert fetch(product="Data Factory")["details"]["total"] == 1
        assert fetch(metric="removal")["details"]["total"] == 1
        assert fetch(metric="restoration")["details"]["total"] == 1
        assert fetch(metric="upcoming")["details"]["total"] == 0  # currently Shipped
    finally:
        if created:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table} SET (SYSTEM_VERSIONING = OFF)"))
                conn.execute(text(f"DROP TABLE {table}"))
                conn.execute(text(f"DROP TABLE {history}"))
        engine.dispose()
