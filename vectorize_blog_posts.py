#!/usr/bin/env python3
"""
Blog Post Vectorization Script
Finds blog posts with null blog_vector, generates embeddings using Azure OpenAI,
and updates the database with the vector data.
"""

import os
import sys
import logging
from typing import List, Dict, Optional
import pyodbc
from openai import AzureOpenAI

from lib.batch_commit import BatchCommitter, get_batch_commit_size
from lib.db_retry import retry_on_transient_errors, is_transient_sql_azure_error
from lib.telemetry import init_telemetry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Wire Azure Monitor for the pipeline run (no-op in development).
init_telemetry("fabric-gps-vectorize")


class BlogVectorizer:
    """Generates and stores vector embeddings for blog posts"""
    
    def __init__(self):
        # Database connection from environment
        self.connection_string = os.getenv('SQLSERVER_CONN')
        if not self.connection_string:
            raise ValueError("SQLSERVER_CONN environment variable not set")
        
        # Azure OpenAI configuration
        self.azure_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        self.azure_api_key = os.getenv('AZURE_OPENAI_API_KEY')
        self.deployment_name = os.getenv('AZURE_OPENAI_EMBEDDING_DEPLOYMENT', 'text-embedding-3-small')
        
        if not self.azure_endpoint or not self.azure_api_key:
            raise ValueError(
                "AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY environment variables must be set"
            )
        
        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            azure_endpoint=self.azure_endpoint,
            api_key=self.azure_api_key,
            api_version="2024-02-01"
        )

        # Long-lived DB connection for the batch loop (issue #76).
        self.conn = None
        self.cursor = None

        logger.info("BlogVectorizer initialized successfully")

    def connect(self):
        """Establish (or re-establish) the long-lived batch connection."""
        if self.conn is None:
            self.conn = pyodbc.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")

    def close(self):
        """Close the long-lived batch connection (best effort)."""
        try:
            if self.cursor is not None:
                self.cursor.close()
        except Exception:  # noqa: BLE001 — best-effort cleanup
            pass
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:  # noqa: BLE001 — best-effort cleanup
            pass
        self.cursor = None
        self.conn = None

    def _drop_connection_silently(self):
        """Tear down the connection so the next attempt reconnects."""
        self.close()
    
    @retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
    def get_posts_without_vectors(self) -> List[Dict]:
        """
        Fetch blog posts that have null blog_vector

        Returns:
            List of dictionaries containing post data
        """
        try:
            self.connect()

            query = """
            SELECT id, title, categories, summary, url
            FROM fabric_blog_posts
            WHERE blog_vector IS NULL
            ORDER BY post_date DESC
            """

            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            posts = []
            for row in rows:
                posts.append({
                    'id': row[0],
                    'title': row[1] or '',
                    'categories': row[2] or '',
                    'summary': row[3] or '',
                    'url': row[4] or ''
                })

            logger.info(f"Found {len(posts)} posts without vectors")
            return posts
        except Exception as exc:
            if is_transient_sql_azure_error(exc):
                self._drop_connection_silently()
            raise
    
    def create_text_for_embedding(self, post: Dict) -> str:
        """
        Combine title, categories, and summary into a single text for embedding
        
        Args:
            post: Dictionary containing post data
            
        Returns:
            Combined text string
        """
        parts = []
        
        if post['title']:
            parts.append(f"Title: {post['title']}")
        
        if post['categories']:
            parts.append(f"Categories: {post['categories']}")
        
        if post['summary']:
            parts.append(f"Summary: {post['summary']}")
        
        combined_text = " | ".join(parts)
        
        # Trim if too long (Azure OpenAI has token limits)
        # text-embedding-3-small supports up to 8191 tokens
        if len(combined_text) > 32000:  # Rough character limit
            combined_text = combined_text[:32000]
            logger.warning(f"Trimmed text for post ID {post['id']} (too long)")
        
        return combined_text
    
    def generate_embedding(self, text: str):
        """
        Generate embedding vector using Azure OpenAI
        
        Args:
            text: Text to embed
            
        Returns:
            List of floats representing the embedding vector, or None if failed
        """
        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.deployment_name
            )
            
            embedding = response.data[0].embedding
            logger.debug(f"Generated embedding with {len(embedding)} dimensions")
            
            return response
        
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None
    
    @retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
    def update_post_vector(self, post_id: int, embedding) -> bool:
        """
        Execute (but do NOT commit) the blog_vector UPDATE for a specific post.

        The commit is deferred to the surrounding ``BatchCommitter`` so the
        pipeline pays one ``COMMIT`` round-trip per N items instead of one
        per row (issue #76). On a transient SQL error this drops the shared
        connection so the retry decorator's next attempt establishes a fresh
        one before re-running the statement.

        Args:
            post_id: Database ID of the post
            embedding: OpenAI embedding response object

        Returns:
            True on success. Raises after retry exhaustion; the caller is
            expected to catch and count as a failure.
        """
        try:
            self.connect()

            update_sql = """
            UPDATE fabric_blog_posts SET blog_vector = JSON_QUERY(CAST(? AS NVARCHAR(MAX)), '$.data[0].embedding') WHERE id = ?
            """

            self.cursor.execute(update_sql, (embedding.model_dump_json(), post_id))
            return True
        except Exception as exc:
            if is_transient_sql_azure_error(exc):
                self._drop_connection_silently()
            raise
    
    def vectorize_all_posts(self, batch_size: Optional[int] = None):
        """
        Main method to vectorize all posts without vectors

        Args:
            batch_size: Number of successful items to commit at once. Defaults
                to ``BATCH_COMMIT_SIZE`` env var (or 25).
        """
        try:
            # Get posts without vectors
            posts = self.get_posts_without_vectors()

            if not posts:
                logger.info("No posts found that need vectorization")
                return

            commit_size = batch_size if batch_size is not None else get_batch_commit_size()
            logger.info(
                f"Starting vectorization of {len(posts)} posts (commit every {commit_size})"
            )

            success_count = 0
            failure_count = 0

            self.connect()
            with BatchCommitter(self.conn, batch_size=commit_size) as committer:
                for i, post in enumerate(posts, 1):
                    try:
                        logger.info(f"Processing post {i}/{len(posts)}: {post['title']}")

                        # Create combined text
                        text = self.create_text_for_embedding(post)

                        if not text.strip():
                            logger.warning(f"Skipping post ID {post['id']} - no content to vectorize")
                            failure_count += 1
                            continue

                        # Generate embedding (API errors must NOT poison the SQL conn)
                        embedding = self.generate_embedding(text)

                        if not embedding:
                            logger.error(f"Failed to generate embedding for post ID {post['id']}")
                            failure_count += 1
                            continue

                        # Stage the UPDATE on the shared connection. Commit is
                        # deferred to the BatchCommitter (every N items).
                        try:
                            self.update_post_vector(post['id'], embedding)
                        except Exception as e:
                            # The retry decorator may have reconnected mid-batch;
                            # if so, refresh the committer's connection so the
                            # next commit targets the live one (and we drop the
                            # in-flight transaction's pending counter).
                            if committer.connection is not self.conn:
                                committer.replace_connection(self.conn)
                            logger.error(f"✗ Failed to update vector for post ID {post['id']}: {e}")
                            failure_count += 1
                            continue

                        # The retry path may have swapped the connection; keep
                        # the committer pointed at the live one.
                        if committer.connection is not self.conn:
                            committer.replace_connection(self.conn)

                        committer.mark_success()
                        success_count += 1
                        logger.info(f"✓ Successfully vectorized post ID {post['id']}")

                    except Exception as e:
                        logger.error(f"Error processing post ID {post['id']}: {e}")
                        failure_count += 1

            # Final summary
            logger.info("=" * 60)
            logger.info(f"Vectorization complete!")
            logger.info(f"Successfully vectorized: {success_count}")
            logger.info(f"Failed: {failure_count}")
            logger.info(f"Total processed: {len(posts)}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during vectorization: {e}")
            raise


def main():
    """Main entry point"""
    vectorizer = None
    try:
        vectorizer = BlogVectorizer()
        vectorizer.vectorize_all_posts()

    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)
    finally:
        if vectorizer is not None:
            vectorizer.close()


if __name__ == "__main__":
    main()
