"""upgrade last_email_sent from DATE to DATETIME2

Revision ID: upgrade_last_email_sent
Revises: add_cadence_and_watches
Create Date: 2026-04-14 19:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'upgrade_last_email_sent'
down_revision = 'add_pending_watch_col'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('email_subscriptions')
                   AND name = 'last_email_sent'
                   AND system_type_id = (SELECT system_type_id FROM sys.types WHERE name = 'date'))
        BEGIN
            ALTER TABLE email_subscriptions ALTER COLUMN last_email_sent DATETIME2 NULL
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('email_subscriptions')
                   AND name = 'last_email_sent'
                   AND system_type_id = (SELECT system_type_id FROM sys.types WHERE name = 'datetime2'))
        BEGIN
            ALTER TABLE email_subscriptions ALTER COLUMN last_email_sent DATE NULL
        END
    """)
