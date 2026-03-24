"""add bounce tracking columns to email_subscriptions

Revision ID: add_bounce_tracking
Revises: add_email_cache
Create Date: 2026-03-24 20:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_bounce_tracking'
down_revision = 'add_email_cache'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('email_subscriptions')
                       AND name = 'bounce_count')
        BEGIN
            ALTER TABLE email_subscriptions ADD bounce_count INT NOT NULL DEFAULT 0
        END
    """)

    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('email_subscriptions')
                       AND name = 'last_bounced_at')
        BEGIN
            ALTER TABLE email_subscriptions ADD last_bounced_at DATETIME2 NULL
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('email_subscriptions')
                   AND name = 'last_bounced_at')
        BEGIN
            ALTER TABLE email_subscriptions DROP COLUMN last_bounced_at
        END
    """)

    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('email_subscriptions')
                   AND name = 'bounce_count')
        BEGIN
            ALTER TABLE email_subscriptions DROP COLUMN bounce_count
        END
    """)
