"""add email_content_cache table

Revision ID: add_email_cache
Revises: add_history_sproc
Create Date: 2026-03-24 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_email_cache'
down_revision = 'add_history_sproc'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'email_content_cache')
        BEGIN
            CREATE TABLE [dbo].[email_content_cache](
                [id] INT NOT NULL DEFAULT 1,
                [generated_at] DATETIME2 NOT NULL,
                [content_json] NVARCHAR(MAX) NOT NULL,
            PRIMARY KEY CLUSTERED ([id] ASC),
            CONSTRAINT [ck_email_content_cache_singleton] CHECK ([id] = 1)
            )
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'email_content_cache')
        BEGIN
            DROP TABLE email_content_cache
        END
    """)
