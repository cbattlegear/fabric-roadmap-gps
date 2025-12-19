#!/usr/bin/env python3
"""
Release-Blog Matching Script
Vectorizes releases and finds the most related blog article for each release using vector similarity.
"""

import os
import sys
import logging
from typing import List, Dict, Optional, Tuple
import pyodbc
from openai import AzureOpenAI
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReleaseBlogMatcher:
    """Matches releases with related blog articles using vector embeddings"""
    
    def __init__(self):
        self.MAXIMUM_VECTOR_DISTANCE = 0.361

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
        
        # Initialize database connection (will be set when connecting)
        self.conn = None
        self.cursor = None
        
        logger.info("ReleaseBlogMatcher initialized successfully")
    
    def connect(self):
        """Establish database connection"""
        if self.conn is None:
            self.conn = pyodbc.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.conn = None
        self.cursor = None
        logger.info("Database connection closed")
    
    def commit(self):
        """Commit current transaction"""
        if self.conn:
            self.conn.commit()
    
    def get_releases_without_articles(self) -> List[Dict]:
        """
        Fetch releases that have null blog_url
        
        Returns:
            List of dictionaries containing release data
        """
        try:
            query = """
            SELECT release_item_id, feature_name, product_name, feature_description, release_vector
            FROM release_items
            WHERE blog_url IS NULL
            ORDER BY last_modified DESC
            """
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            releases = []
            for row in rows:
                releases.append({
                    'release_item_id': row[0],
                    'feature_name': row[1] or '',
                    'product_name': row[2] or '',
                    'feature_description': row[3] or '',
                    'release_vector': row[4] or ''
                })
            
            logger.info(f"Found {len(releases)} releases without blogs")
            return releases
        
        except Exception as e:
            logger.error(f"Error fetching releases without blogs: {e}")
            raise
    
    def create_text_for_embedding(self, release: Dict) -> str:
        """
        Combine feature_name, product_name, and feature_description into a single text for embedding
        
        Args:
            release: Dictionary containing release data
            
        Returns:
            Combined text string
        """
        parts = []
        
        if release['feature_name']:
            parts.append(f"Title: {release['feature_name']}")
        
        if release['product_name']:
            parts.append(f"Categories: {release['product_name']}")
        
        if release['feature_description']:
            parts.append(f"Summary: {release['feature_description']}")
        
        combined_text = " | ".join(parts)
        
        # Trim if too long
        if len(combined_text) > 32000:
            combined_text = combined_text[:32000]
            logger.warning(f"Trimmed text for release {release['release_item_id']} (too long)")
        
        return combined_text
    
    def generate_embedding(self, text: str):
        """
        Generate embedding vector using Azure OpenAI
        
        Args:
            text: Text to embed
            
        Returns:
            Response object with embedding, or None if failed
        """
        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.deployment_name
            )
            
            logger.debug(f"Generated embedding with {len(response.data[0].embedding)} dimensions")
            
            return response
        
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None
    
    def update_release_vector(self, release_item_id: str, embedding) -> bool:
        """
        Update the release_vector column for a specific release
        
        Args:
            release_item_id: Database ID of the release
            embedding: OpenAI embedding response object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            update_sql = """
            UPDATE release_items
            SET release_vector = JSON_QUERY(CAST(? AS NVARCHAR(MAX)), '$.data[0].embedding')
            WHERE release_item_id = ?
            """
            
            self.cursor.execute(update_sql, (embedding.model_dump_json(), release_item_id))
            self.commit()
            
            return True
        
        except Exception as e:
            logger.error(f"Error updating vector for release {release_item_id}: {e}")
            return False
    
    def find_most_related_blog(self, release_item_id) -> Optional[Tuple[str, str, float]]:
        """
        Find the most related blog article using vector similarity
        
        Args:
            release_item_id: ID of release_item to find blog
            
        Returns:
            Tuple of (blog_title, blog_url, distance) or None if no match found
        """
        try:
            query = """
            SELECT TOP 1 title, url, 
                   VECTOR_DISTANCE('cosine', (SELECT release_vector FROM release_items WHERE release_item_id = ?), blog_vector) as distance
            FROM fabric_blog_posts
            WHERE blog_vector IS NOT NULL
            ORDER BY distance ASC
            """

            self.cursor.execute(query, (release_item_id,))
            row = self.cursor.fetchone()
            
            if row:
                return (row[0], row[1], row[2])
            
            return None
        
        except Exception as e:
            logger.error(f"Error finding related blog: {e}")
            return None
    
    def update_release_blog_info(self, release_item_id: str, blog_title: str, blog_url: str, distance) -> bool:
        """
        Update the blog_title and blog_url columns for a specific release
        
        Args:
            release_item_id: Database ID of the release
            blog_title: Title of the related blog article
            blog_url: URL of the related blog article
            
        Returns:
            True if successful, False otherwise
        """
        try:
            update_sql = """
            UPDATE release_items
            SET blog_title = ?,
                blog_url = ?,
                vector_distance = ?
            WHERE release_item_id = ?
            """
            
            self.cursor.execute(update_sql, (blog_title, blog_url, distance, release_item_id))
            self.commit()
            
            return True
        
        except Exception as e:
            logger.error(f"Error updating blog info for release {release_item_id}: {e}")
            return False
    
    def vectorize_and_match_all_releases(self):
        """
        Main method to vectorize all releases and match them with blog articles
        """
        try:
            # Get releases without vectors
            releases = self.get_releases_without_articles()
            
            if not releases:
                logger.info("No releases found that need vectorization")
                return
            
            logger.info(f"Starting vectorization and matching of {len(releases)} releases")
            
            success_count = 0
            failure_count = 0
            matched_count = 0
            
            for i, release in enumerate(releases, 1):
                try:
                    logger.info(f"Processing release {i}/{len(releases)}: {release['feature_name']}")
                    if release['release_vector'] == '':
                        # Create combined text
                        text = self.create_text_for_embedding(release)
                        
                        if not text.strip():
                            logger.warning(f"Skipping release {release['release_item_id']} - no content to vectorize")
                            failure_count += 1
                            continue
                        
                        # Generate embedding
                        embedding = self.generate_embedding(text)
                        
                        if not embedding:
                            logger.error(f"Failed to generate embedding for release {release['release_item_id']}")
                            failure_count += 1
                            continue
                        
                        # Update release vector in database
                        if not self.update_release_vector(release['release_item_id'], embedding):
                            logger.error(f"Failed to update vector for release {release['release_item_id']}")
                            failure_count += 1
                            continue
                        
                        success_count += 1
                        logger.info(f"✓ Successfully vectorized release {release['release_item_id']}")
                    else: 
                        logger.info(f"✓ {release['release_item_id']} already vectorized!")
                    
                    # Find most related blog article
                    blog_match = self.find_most_related_blog(release['release_item_id'])
                    
                    if blog_match:
                        blog_title, blog_url, distance = blog_match
                        if distance <= self.MAXIMUM_VECTOR_DISTANCE:
                            logger.info(f"  → Found related blog: '{blog_title}' (distance: {distance:.4f})")
                            
                            # Update release with blog info
                            if self.update_release_blog_info(release['release_item_id'], blog_title, blog_url, distance):
                                matched_count += 1
                                logger.info(f"  ✓ Updated release with blog info")
                            else:
                                logger.error(f"  ✗ Failed to update release with blog info")
                        else:
                            logger.warning(f"  ⚠ Related article too disimilar")
                    else:
                        logger.warning(f"  ⚠ No related blog article found")
                
                except Exception as e:
                    logger.error(f"Error processing release {release['release_item_id']}: {e}")
                    failure_count += 1
            
            # Final summary
            logger.info("=" * 60)
            logger.info(f"Vectorization and matching complete!")
            logger.info(f"Successfully vectorized: {success_count}")
            logger.info(f"Matched with blog articles: {matched_count}")
            logger.info(f"Failed: {failure_count}")
            logger.info(f"Total processed: {len(releases)}")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"Fatal error during vectorization and matching: {e}")
            raise


def main():
    """Main entry point"""
    matcher = None
    try:
        matcher = ReleaseBlogMatcher()
        matcher.connect()
        matcher.vectorize_and_match_all_releases()
    
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)
    
    finally:
        if matcher:
            matcher.close()


if __name__ == "__main__":
    main()
