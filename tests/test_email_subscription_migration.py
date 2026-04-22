"""Regression tests for the populated ``2504c17595e2`` migration.

The migration runs raw T-SQL against SQL Server, so we can't exercise its
side effects in a unit test (no SQL Server in CI). What we can — and
should — pin is that:

* The migration file is no longer a stub (catches an accidental revert
  to the no-op ``pass`` body that the M9 finding called out).
* It still creates both expected tables and uses ``IF NOT EXISTS`` guards
  for safety against environments where ``init_db()`` already produced
  the tables.
* The column shapes match the ``Base.metadata.create_all()`` contract at
  this revision (generic ``DATETIME`` columns, no SQL-side defaults), so
  Alembic-bootstrapped DBs and ``init_db()``-bootstrapped DBs converge.
* The migration declares the expected revision identifiers so the chain
  stays intact (``add_blog_related_columns`` lists this as its
  ``down_revision``).
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest


MIGRATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "migrations"
    / "versions"
    / "2504c17595e2_add_email_subscription_tables.py"
)


@pytest.fixture(scope="module")
def migration_module():
    spec = importlib.util.spec_from_file_location("_mig_2504c17595e2", MIGRATION_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def upgrade_sql(migration_module) -> str:
    """Capture every SQL string passed to ``op.execute`` during ``upgrade()``.

    We monkey-patch ``alembic.op.execute`` to a recorder and call the
    real ``upgrade()`` function, so the test breaks if anyone reverts the
    body to ``pass`` or removes a CREATE TABLE statement.
    """
    captured: list[str] = []

    from alembic import op as alembic_op

    original = alembic_op.execute
    alembic_op.execute = lambda sql, *a, **kw: captured.append(str(sql))  # type: ignore[assignment]
    try:
        migration_module.upgrade()
    finally:
        alembic_op.execute = original

    return "\n".join(captured)


@pytest.fixture(scope="module")
def downgrade_sql(migration_module) -> str:
    captured: list[str] = []

    from alembic import op as alembic_op

    original = alembic_op.execute
    alembic_op.execute = lambda sql, *a, **kw: captured.append(str(sql))  # type: ignore[assignment]
    try:
        migration_module.downgrade()
    finally:
        alembic_op.execute = original

    return "\n".join(captured)


# ---------------------------------------------------------------------------
# Revision metadata
# ---------------------------------------------------------------------------

def test_revision_identifiers(migration_module):
    """Pins the chain so the next migration's ``down_revision`` keeps working."""
    assert migration_module.revision == "2504c17595e2"
    assert migration_module.down_revision == "ab4002808504"


# ---------------------------------------------------------------------------
# upgrade()
# ---------------------------------------------------------------------------

def test_upgrade_is_no_longer_a_stub(upgrade_sql):
    """The whole point of M9 — guard against silently reverting to ``pass``."""
    assert upgrade_sql.strip(), "upgrade() emitted no SQL — stub regression?"
    assert "CREATE TABLE" in upgrade_sql.upper()


def test_upgrade_creates_email_subscriptions(upgrade_sql):
    assert re.search(r"CREATE\s+TABLE\s+email_subscriptions", upgrade_sql, re.IGNORECASE)


def test_upgrade_creates_email_verifications(upgrade_sql):
    assert re.search(r"CREATE\s+TABLE\s+email_verifications", upgrade_sql, re.IGNORECASE)


def test_upgrade_uses_if_not_exists_guards(upgrade_sql):
    """Idempotent against environments where init_db() already ran."""
    # Each CREATE TABLE statement must be guarded.
    create_table_count = len(re.findall(r"CREATE\s+TABLE", upgrade_sql, re.IGNORECASE))
    if_not_exists_table_count = len(
        re.findall(r"IF\s+NOT\s+EXISTS\s*\(\s*SELECT[^)]*FROM\s+sys\.tables", upgrade_sql, re.IGNORECASE)
    )
    assert create_table_count >= 2
    assert if_not_exists_table_count >= create_table_count, (
        "Every CREATE TABLE must be wrapped in an IF NOT EXISTS guard "
        "so the migration is safe against init_db()-bootstrapped DBs."
    )


def test_upgrade_creates_expected_indexes(upgrade_sql):
    """All ix_/uq_ names should match SQLAlchemy's naming_convention so
    the IF NOT EXISTS guards line up regardless of who created them."""
    expected_indexes = [
        "ix_email_subscriptions_email",
        "ix_email_subscriptions_verification_token",
        "ix_email_subscriptions_unsubscribe_token",
        "ix_email_verifications_email",
        "ix_email_verifications_token",
    ]
    for name in expected_indexes:
        assert name in upgrade_sql, f"missing index creation: {name}"


def test_upgrade_uses_datetime_not_datetime2(upgrade_sql):
    """At THIS revision the model used Column(DateTime), which SQLAlchemy
    maps to DATETIME on MSSQL. Using DATETIME2 here would diverge from
    init_db()-bootstrapped production DBs (later migrations only widen
    last_email_sent, not the other DATETIME columns)."""
    # No DATETIME2 columns at this revision — the only DATETIME2 in the
    # codebase is in *later* migrations. Allow occurrences in comments by
    # restricting to non-comment SQL.
    sql_upper = upgrade_sql.upper()
    assert "DATETIME2" not in sql_upper, (
        "upgrade() should use DATETIME (not DATETIME2) at this revision "
        "to match Base.metadata.create_all() output"
    )
    # Spot-check that the time columns are present as DATETIME.
    assert re.search(r"created_at\s+DATETIME\b", upgrade_sql, re.IGNORECASE)
    assert re.search(r"expires_at\s+DATETIME\b", upgrade_sql, re.IGNORECASE)


def test_upgrade_has_no_sql_side_defaults_for_bool_or_datetime(upgrade_sql):
    """At this revision the model uses Python-side ``default=`` only;
    no ``server_default=`` was set. So create_all() did not emit DEFAULTs
    either. Adding DEFAULTs here would diverge from init_db() DBs."""
    # No DEFAULT clauses anywhere in CREATE TABLE bodies for this
    # revision's columns.
    assert " DEFAULT " not in upgrade_sql.upper(), (
        "Bool/datetime columns at this revision had no server_default; "
        "the migration must not add DB-side DEFAULT clauses or it diverges "
        "from init_db()-bootstrapped DBs."
    )


def test_upgrade_excludes_columns_added_by_later_migrations(upgrade_sql):
    """These columns are added by later migrations and must NOT appear in
    this revision's CREATE TABLE — otherwise running the later migrations
    would fail because their IF NOT EXISTS guards skip the ALTER."""
    forbidden = [
        "email_cadence",
        "pending_watch_release_id",
        "bounce_count",
        "last_bounced_at",
        "unsubscribe_token_rotated_at",
    ]
    for col in forbidden:
        assert col not in upgrade_sql, (
            f"{col} is added by a later migration and must NOT appear in 2504c17595e2"
        )


def test_upgrade_uses_date_for_last_email_sent(upgrade_sql):
    """``last_email_sent`` was originally DATE; ``upgrade_last_email_sent_to_datetime``
    later widens it. Using DATETIME2 here would short-circuit that migration."""
    assert re.search(r"last_email_sent\s+DATE\b", upgrade_sql, re.IGNORECASE)


# ---------------------------------------------------------------------------
# downgrade()
# ---------------------------------------------------------------------------

def test_downgrade_drops_both_tables(downgrade_sql):
    assert re.search(r"DROP\s+TABLE\s+email_verifications", downgrade_sql, re.IGNORECASE)
    assert re.search(r"DROP\s+TABLE\s+email_subscriptions", downgrade_sql, re.IGNORECASE)


def test_downgrade_uses_if_exists_guards(downgrade_sql):
    """Idempotent against partial-state databases."""
    drop_count = len(re.findall(r"DROP\s+TABLE", downgrade_sql, re.IGNORECASE))
    guard_count = len(
        re.findall(r"IF\s+EXISTS\s*\(\s*SELECT[^)]*FROM\s+sys\.tables", downgrade_sql, re.IGNORECASE)
    )
    assert drop_count >= 2
    assert guard_count >= drop_count
