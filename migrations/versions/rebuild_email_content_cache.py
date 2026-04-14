"""rebuild email_content_cache with per-date per-filter keying

Revision ID: rebuild_email_content_cache
Revises: upgrade_last_email_sent
Create Date: 2026-04-14 19:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'rebuild_email_content_cache'
down_revision = 'upgrade_last_email_sent'
branch_labels = None
depends_on = None


def upgrade():
    # Drop old singleton cache table and recreate with composite key
    op.execute("""
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'email_content_cache')
        BEGIN
            DROP TABLE email_content_cache
        END
    """)

    op.execute("""
        CREATE TABLE email_content_cache (
            id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            cache_date VARCHAR(10) NOT NULL,
            cache_key VARCHAR(255) NOT NULL,
            generated_at DATETIME2 NOT NULL,
            content_json NVARCHAR(MAX) NOT NULL,
            CONSTRAINT uq_email_cache_date_key UNIQUE (cache_date, cache_key)
        )
    """)

    op.execute("""
        CREATE INDEX ix_email_content_cache_date ON email_content_cache(cache_date)
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'email_content_cache')
        BEGIN
            DROP TABLE email_content_cache
        END
    """)

    op.execute("""
        CREATE TABLE [dbo].[email_content_cache](
            [id] INT NOT NULL DEFAULT 1,
            [generated_at] DATETIME2 NOT NULL,
            [content_json] NVARCHAR(MAX) NOT NULL,
        PRIMARY KEY CLUSTERED ([id] ASC),
        CONSTRAINT [ck_email_content_cache_singleton] CHECK ([id] = 1)
        )
    """)
