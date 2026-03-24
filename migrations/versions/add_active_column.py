"""add active column to release_items

Revision ID: add_active_col
Revises: add_blog_related_cols
Create Date: 2026-03-24 16:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_active_col'
down_revision = 'add_blog_related_cols'
branch_labels = None
depends_on = None


def upgrade():
    # Add active column (BIT NOT NULL DEFAULT 1) if it doesn't exist
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('release_items')
                       AND name = 'active')
        BEGIN
            ALTER TABLE release_items ADD active BIT NOT NULL DEFAULT 1
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('release_items')
                   AND name = 'active')
        BEGIN
            ALTER TABLE release_items DROP COLUMN active
        END
    """)
