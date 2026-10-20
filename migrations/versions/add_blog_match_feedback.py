"""Anonymous feedback for exact release/blog pairs."""

from alembic import op

revision = "add_blog_match_feedback"
down_revision = "add_unsub_token_history"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'blog_match_feedback')
        BEGIN
            CREATE TABLE blog_match_feedback (
                id VARCHAR(36) NOT NULL CONSTRAINT pk_blog_match_feedback PRIMARY KEY,
                release_item_id VARCHAR(36) NOT NULL,
                blog_key VARCHAR(64) NOT NULL,
                blog_url VARCHAR(1000) NOT NULL,
                voter_key VARCHAR(64) NOT NULL,
                vote VARCHAR(11) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                CONSTRAINT uq_blog_match_feedback_pair_voter
                    UNIQUE (release_item_id, blog_key, voter_key),
                CONSTRAINT ck_blog_match_feedback_vote
                    CHECK (vote IN ('helpful', 'not_related'))
            )
        END
    """)
    op.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE object_id = OBJECT_ID('blog_match_feedback')
                         AND name = 'ix_blog_match_feedback_updated_at')
        BEGIN
            CREATE INDEX ix_blog_match_feedback_updated_at
                ON blog_match_feedback (updated_at)
        END
    """)


def downgrade():
    op.execute("""
        IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'blog_match_feedback')
            DROP TABLE blog_match_feedback
    """)
