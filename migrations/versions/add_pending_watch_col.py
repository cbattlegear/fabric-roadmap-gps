"""add pending_watch_release_id to email_verifications

Revision ID: add_pending_watch_col
Revises: add_cadence_and_watches
Create Date: 2026-04-13 21:25:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_pending_watch_col'
down_revision = 'add_cadence_and_watches'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('email_verifications')
                       AND name = 'pending_watch_release_id')
        BEGIN
            ALTER TABLE email_verifications ADD pending_watch_release_id NVARCHAR(36) NULL
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('email_verifications')
                   AND name = 'pending_watch_release_id')
        BEGIN
            ALTER TABLE email_verifications DROP COLUMN pending_watch_release_id
        END
    """)
