"""add unsubscribe_token_history + rotated_at; support multi-token grace window

Replaces a single previous_unsubscribe_token slot with a history table so that
several recently-issued tokens can remain valid simultaneously during their
grace window. Also adds unsubscribe_token_rotated_at on email_subscriptions
to gate rotation frequency.

Revision ID: add_unsub_token_history
Revises: add_email_verif_rate_idx
Create Date: 2026-04-20 16:50:00.000000
"""
from alembic import op


revision = 'add_unsub_token_history'
down_revision = 'add_email_verif_rate_idx'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.columns
                       WHERE object_id = OBJECT_ID('email_subscriptions')
                         AND name = 'unsubscribe_token_rotated_at')
        BEGIN
            ALTER TABLE email_subscriptions
                ADD unsubscribe_token_rotated_at DATETIME NULL
        END
    """)

    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables
                       WHERE name = 'unsubscribe_token_history')
        BEGIN
            CREATE TABLE unsubscribe_token_history (
                id VARCHAR(36) NOT NULL PRIMARY KEY,
                subscription_id VARCHAR(36) NOT NULL,
                token VARCHAR(64) NOT NULL,
                expires_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL CONSTRAINT df_unsub_history_created
                    DEFAULT (SYSUTCDATETIME())
            )
        END
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'uq_unsub_token_history_token'
                         AND object_id = OBJECT_ID('unsubscribe_token_history'))
        BEGIN
            CREATE UNIQUE INDEX uq_unsub_token_history_token
                ON unsubscribe_token_history (token)
        END
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name = 'ix_unsub_token_history_sub_expires'
                         AND object_id = OBJECT_ID('unsubscribe_token_history'))
        BEGIN
            CREATE INDEX ix_unsub_token_history_sub_expires
                ON unsubscribe_token_history (subscription_id, expires_at)
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'ix_unsub_token_history_sub_expires'
                     AND object_id = OBJECT_ID('unsubscribe_token_history'))
        BEGIN
            DROP INDEX ix_unsub_token_history_sub_expires ON unsubscribe_token_history
        END
    """)
    op.execute("""
        IF EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'uq_unsub_token_history_token'
                     AND object_id = OBJECT_ID('unsubscribe_token_history'))
        BEGIN
            DROP INDEX uq_unsub_token_history_token ON unsubscribe_token_history
        END
    """)
    op.execute("""
        IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'unsubscribe_token_history')
        BEGIN
            DROP TABLE unsubscribe_token_history
        END
    """)
    op.execute("""
        IF EXISTS (SELECT 1 FROM sys.columns
                   WHERE object_id = OBJECT_ID('email_subscriptions')
                     AND name = 'unsubscribe_token_rotated_at')
        BEGIN
            ALTER TABLE email_subscriptions DROP COLUMN unsubscribe_token_rotated_at
        END
    """)
