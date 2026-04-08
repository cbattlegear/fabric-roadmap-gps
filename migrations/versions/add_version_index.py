"""add version lookup index on last_modified DESC

Revision ID: add_version_index
Revises: update_history_sproc_v2
Create Date: 2026-04-08 15:45:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'add_version_index'
down_revision = 'update_history_sproc_v2'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_release_items_version_DESC' AND object_id = OBJECT_ID('dbo.release_items'))
        BEGIN
            CREATE INDEX IX_release_items_version_DESC
                ON dbo.release_items (last_modified DESC)
                INCLUDE (release_item_id);
        END
    """)


def downgrade():
    op.execute("DROP INDEX IF EXISTS IX_release_items_version_DESC ON dbo.release_items;")
