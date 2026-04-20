"""add composite index on email_verifications(email, created_at) for rate-limit query

Revision ID: add_email_verif_rate_idx
Revises: rebuild_email_content_cache
Create Date: 2026-04-20 14:20:00.000000
"""
from alembic import op


revision = 'add_email_verif_rate_idx'
down_revision = 'rebuild_email_content_cache'
branch_labels = None
depends_on = None


def upgrade():
    # Supports the per-email hourly verification quota query in
    # db.db_sqlserver._enforce_email_verification_quota, which filters on
    # (email == :email AND created_at >= :cutoff). The existing single-column
    # index on `email` would still scan all rows for a given address.
    op.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'ix_email_verifications_email_created_at'
              AND object_id = OBJECT_ID('email_verifications')
        )
        BEGIN
            CREATE INDEX ix_email_verifications_email_created_at
                ON email_verifications (email, created_at)
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'ix_email_verifications_email_created_at'
              AND object_id = OBJECT_ID('email_verifications')
        )
        BEGIN
            DROP INDEX ix_email_verifications_email_created_at ON email_verifications
        END
    """)
