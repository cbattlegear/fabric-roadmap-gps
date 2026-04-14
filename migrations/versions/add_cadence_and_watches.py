"""add email_cadence column and feature_watches table

Revision ID: add_cadence_and_watches
Revises: add_version_index
Create Date: 2026-04-13 19:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_cadence_and_watches'
down_revision = 'add_version_index'
branch_labels = None
depends_on = None


def upgrade():
    # Add email_cadence column to email_subscriptions
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('email_subscriptions')
                       AND name = 'email_cadence')
        BEGIN
            ALTER TABLE email_subscriptions ADD email_cadence VARCHAR(10) NOT NULL DEFAULT 'weekly'
        END
    """)

    # Create feature_watches table
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'feature_watches')
        BEGIN
            CREATE TABLE feature_watches (
                id VARCHAR(36) NOT NULL PRIMARY KEY,
                subscription_id VARCHAR(36) NOT NULL,
                release_item_id VARCHAR(36) NOT NULL,
                created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
                last_notified_hash VARCHAR(64) NULL,
                CONSTRAINT fk_feature_watches_subscription
                    FOREIGN KEY (subscription_id) REFERENCES email_subscriptions(id)
                    ON DELETE CASCADE,
                CONSTRAINT fk_feature_watches_release
                    FOREIGN KEY (release_item_id) REFERENCES release_items(release_item_id),
                CONSTRAINT uq_feature_watches_sub_release
                    UNIQUE (subscription_id, release_item_id)
            )
        END
    """)

    # Index on release_item_id for efficient lookups during email job
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes
                       WHERE name = 'ix_feature_watches_release_item_id'
                       AND object_id = OBJECT_ID('feature_watches'))
        BEGIN
            CREATE INDEX ix_feature_watches_release_item_id
                ON feature_watches(release_item_id)
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'feature_watches')
        BEGIN
            DROP TABLE feature_watches
        END
    """)

    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('email_subscriptions')
                   AND name = 'email_cadence')
        BEGIN
            ALTER TABLE email_subscriptions DROP COLUMN email_cadence
        END
    """)
