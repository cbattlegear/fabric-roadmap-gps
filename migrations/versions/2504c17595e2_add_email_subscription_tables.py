"""Add Email Subscription tables

Revision ID: 2504c17595e2
Revises: ab4002808504
Create Date: 2025-09-18 20:41:58.338100

    Originally generated as a no-op stub because the email-subscription tables
    were created on first boot via ``init_db()`` (``Base.metadata.create_all``).
    That worked for the existing production database but meant a fresh
    ``alembic upgrade head`` against an empty SQL Server would NOT create the
    tables, leaving downstream migrations (``add_cadence_and_watches``,
    ``add_pending_watch_col``, ``add_bounce_tracking``,
    ``add_unsubscribe_token_history``) to ALTER tables that didn't exist.

    This populated upgrade matches the schema **as ``Base.metadata.create_all()``
    produced it at this revision**, so DBs bootstrapped via Alembic and DBs
    bootstrapped via ``init_db()`` end up with the same column shape:

    * ``DATETIME`` (not ``DATETIME2``) for the time columns — that's what
      SQLAlchemy emits for a generic ``Column(DateTime, ...)`` on the MSSQL
      dialect.
    * No SQL-side ``DEFAULT`` clauses on the bool / datetime columns —
      defaults at this revision were Python-side only (``default=...``),
      not ``server_default=...``, so ``create_all()`` did not emit DB
      defaults either. (Later migrations such as ``add_cadence_and_watches``
      and ``add_bounce_tracking`` introduced server-side defaults for the
      columns they add.)
    * ``last_email_sent DATE`` here; later widened to ``DATETIME2`` by
      ``upgrade_last_email_sent_to_datetime``.
    * Columns added by later migrations (``email_cadence``,
      ``pending_watch_release_id``, ``bounce_count``, ``last_bounced_at``,
      ``unsubscribe_token_rotated_at``) are intentionally NOT created here.

    All statements use ``IF NOT EXISTS`` guards so the migration is safe to
    apply against environments where ``init_db()`` has already created the
    tables (the existing production case)."""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2504c17595e2'
down_revision: Union[str, Sequence[str], None] = 'ab4002808504'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create email_subscriptions and email_verifications tables.

    Idempotent — guarded with ``IF NOT EXISTS`` so this is safe against
    databases where ``init_db()`` already produced the tables. Index names
    match SQLAlchemy's ``naming_convention`` (``ix_%(column_0_label)s``)
    so guards line up whichever path created the table.
    """
    # email_subscriptions table — column shapes match Base.metadata.create_all()
    # output at this revision (generic DATETIME, no SQL-side defaults).
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'email_subscriptions')
        BEGIN
            CREATE TABLE email_subscriptions (
                id VARCHAR(36) NOT NULL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                verification_token VARCHAR(64) NULL,
                is_verified BIT NOT NULL,
                is_active BIT NOT NULL,
                created_at DATETIME NOT NULL,
                verified_at DATETIME NULL,
                last_email_sent DATE NULL,
                unsubscribe_token VARCHAR(64) NOT NULL,
                product_filter VARCHAR(200) NULL,
                release_type_filter VARCHAR(200) NULL,
                release_status_filter VARCHAR(200) NULL,
                CONSTRAINT uq_email_subscriptions_email UNIQUE (email)
            )
        END
    """)

    # Indexes on email_subscriptions
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'ix_email_subscriptions_email'
                       AND object_id = OBJECT_ID('email_subscriptions'))
        BEGIN
            CREATE INDEX ix_email_subscriptions_email
                ON email_subscriptions (email)
        END
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'ix_email_subscriptions_verification_token'
                       AND object_id = OBJECT_ID('email_subscriptions'))
        BEGIN
            CREATE INDEX ix_email_subscriptions_verification_token
                ON email_subscriptions (verification_token)
        END
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'ix_email_subscriptions_unsubscribe_token'
                       AND object_id = OBJECT_ID('email_subscriptions'))
        BEGIN
            CREATE INDEX ix_email_subscriptions_unsubscribe_token
                ON email_subscriptions (unsubscribe_token)
        END
    """)

    # email_verifications table — same DATETIME / no-server-default contract.
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'email_verifications')
        BEGIN
            CREATE TABLE email_verifications (
                id VARCHAR(36) NOT NULL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                token VARCHAR(64) NOT NULL,
                action_type VARCHAR(20) NOT NULL,
                created_at DATETIME NOT NULL,
                expires_at DATETIME NOT NULL,
                is_used BIT NOT NULL,
                used_at DATETIME NULL,
                CONSTRAINT uq_email_verifications_token UNIQUE (token)
            )
        END
    """)

    # Indexes on email_verifications
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'ix_email_verifications_email'
                       AND object_id = OBJECT_ID('email_verifications'))
        BEGIN
            CREATE INDEX ix_email_verifications_email
                ON email_verifications (email)
        END
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'ix_email_verifications_token'
                       AND object_id = OBJECT_ID('email_verifications'))
        BEGIN
            CREATE INDEX ix_email_verifications_token
                ON email_verifications (token)
        END
    """)


def downgrade() -> None:
    """Drop email_verifications and email_subscriptions tables.

    Note: later migrations may have added FKs that reference these tables
    (e.g. ``feature_watches.subscription_id``). Run their downgrades first
    via ``alembic downgrade <prior-revision>`` rather than calling this
    in isolation against a populated database.
    """
    op.execute("""
        IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'email_verifications')
        BEGIN
            DROP TABLE email_verifications
        END
    """)
    op.execute("""
        IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'email_subscriptions')
        BEGIN
            DROP TABLE email_subscriptions
        END
    """)
