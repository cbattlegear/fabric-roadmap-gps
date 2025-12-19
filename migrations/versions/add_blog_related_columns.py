"""add blog related columns to release_items and fabric_blog_posts

Revision ID: add_blog_related_cols
Revises: 2504c17595e2
Create Date: 2025-12-17 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_blog_related_cols'
down_revision = '2504c17595e2'
branch_labels = None
depends_on = None


def upgrade():
    # Create fabric_blog_posts table if it doesn't exist
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fabric_blog_posts')
        BEGIN
            CREATE TABLE [dbo].[fabric_blog_posts](
                [id] [int] IDENTITY(1,1) NOT NULL,
                [title] [nvarchar](500) NOT NULL,
                [url] [nvarchar](1000) NOT NULL,
                [categories] [nvarchar](500) NULL,
                [post_date] [date] NULL,
                [author] [nvarchar](200) NULL,
                [views] [int] NULL,
                [summary] [nvarchar](max) NULL,
                [scraped_at] [datetime2](7) NULL,
                [updated_at] [datetime2](7) NULL,
                [blog_vector] [vector](1536) NULL,
            PRIMARY KEY CLUSTERED 
            (
                [id] ASC
            )WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
            UNIQUE NONCLUSTERED 
            (
                [url] ASC
            )WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
                    
        END
    """)
    
    # Add blog_vector to fabric_blog_posts if table exists but column doesn't
    op.execute("""
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'fabric_blog_posts')
        AND NOT EXISTS (SELECT * FROM sys.columns 
                        WHERE object_id = OBJECT_ID('fabric_blog_posts') 
                        AND name = 'blog_vector')
        BEGIN
            ALTER TABLE fabric_blog_posts ADD [blog_vector] [vector](1536) NULL
        END
    """)
    
    # Add blog-related columns to release_items table one at a time
    # Check and add blog_title if it doesn't exist
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns 
                       WHERE object_id = OBJECT_ID('release_items') 
                       AND name = 'blog_title')
        BEGIN
            ALTER TABLE release_items ADD blog_title NVARCHAR(500) NULL
        END
    """)
    
    # Check and add blog_url if it doesn't exist
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns 
                       WHERE object_id = OBJECT_ID('release_items') 
                       AND name = 'blog_url')
        BEGIN
            ALTER TABLE release_items ADD blog_url NVARCHAR(1000) NULL
        END
    """)
    
    # Check and add release_vector if it doesn't exist
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns 
                       WHERE object_id = OBJECT_ID('release_items') 
                       AND name = 'release_vector')
        BEGIN
            ALTER TABLE release_items ADD [release_vector] [vector](1536) NULL
        END
    """)

    # Check and add vector_distance if it doesn't exist
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns 
                       WHERE object_id = OBJECT_ID('release_items') 
                       AND name = 'vector_distance')
        BEGIN
            ALTER TABLE release_items ADD [vector_distance] float
        END
    """)
    
    # Create index on blog_url for faster lookups
    op.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes 
                       WHERE object_id = OBJECT_ID('release_items') 
                       AND name = 'idx_blog_url')
        BEGIN
            CREATE INDEX idx_blog_url ON release_items(blog_url)
        END
    """)


def downgrade():
    # Drop release_items index first
    op.execute("""
        IF EXISTS (SELECT * FROM sys.indexes 
                   WHERE object_id = OBJECT_ID('release_items') 
                   AND name = 'idx_blog_url')
        BEGIN
            DROP INDEX idx_blog_url ON release_items
        END
    """)
    
    # Drop release_items columns one at a time (check if they exist first)
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns 
                   WHERE object_id = OBJECT_ID('release_items') 
                   AND name = 'release_vector')
        BEGIN
            ALTER TABLE release_items DROP COLUMN release_vector
        END
    """)
    
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns 
                   WHERE object_id = OBJECT_ID('release_items') 
                   AND name = 'blog_url')
        BEGIN
            ALTER TABLE release_items DROP COLUMN blog_url
        END
    """)
    
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns 
                   WHERE object_id = OBJECT_ID('release_items') 
                   AND name = 'blog_title')
        BEGIN
            ALTER TABLE release_items DROP COLUMN blog_title
        END
    """)
    
    # Drop blog_vector from fabric_blog_posts if it exists
    op.execute("""
        IF EXISTS (SELECT * FROM sys.columns 
                   WHERE object_id = OBJECT_ID('fabric_blog_posts') 
                   AND name = 'blog_vector')
        BEGIN
            ALTER TABLE fabric_blog_posts DROP COLUMN blog_vector
        END
    """)
    
    # Note: We don't drop the fabric_blog_posts table itself in downgrade
    # as it may contain data and is managed by the scraper script
