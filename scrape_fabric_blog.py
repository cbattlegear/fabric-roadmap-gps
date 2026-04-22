#!/usr/bin/env python3
"""
Fabric Blog Scraper
Scrapes blog posts from https://blog.fabric.microsoft.com and stores them in SQL Server
Supports two modes:
- Full load: Scrapes all pages from the blog
- Delta load: Checks RSS feed for new articles only
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import pyodbc
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
import html

from lib.db_retry import retry_on_transient_errors
from lib.telemetry import init_telemetry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Wire Azure Monitor for the pipeline run (no-op in development).
init_telemetry("fabric-gps-blog-scraper")


class FabricBlogScraper:
    """Scrapes Microsoft Fabric blog posts and stores them in SQL Server"""
    
    def __init__(self):
        self.base_url = "https://blog.fabric.microsoft.com/en-US/blog"
        self.rss_url = "https://blog.fabric.microsoft.com/en-us/blog/feed/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Database connection from environment
        self.connection_string = os.getenv('SQLSERVER_CONN')
        if not self.connection_string:
            raise ValueError("SQLSERVER_CONN environment variable not set")
    
    def fetch_page(self, page_num: int) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a single blog page
        
        Args:
            page_num: Page number to fetch (1-169)
            
        Returns:
            BeautifulSoup object or None if fetch fails
        """
        url = f"{self.base_url}?page={page_num}"
        try:
            logger.info(f"Fetching page {page_num}: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Add small delay to be respectful to the server
            time.sleep(1)
            
            return BeautifulSoup(response.content, 'html.parser')
        
        except requests.RequestException as e:
            logger.error(f"Failed to fetch page {page_num}: {e}")
            return None
    
    def extract_articles(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract article data from a parsed page
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of dictionaries containing article data
        """
        articles = []
        article_elements = soup.find_all('article', class_='post')
        
        for article in article_elements:
            try:
                # Extract title and URL
                title_elem = article.find('h2', class_='text-responsive-24px')
                if not title_elem or not title_elem.find('a'):
                    continue
                
                title_link = title_elem.find('a')
                title = html.unescape(title_link.get_text(strip=True))
                url = urljoin(self.base_url, title_link.get('href', '')).rstrip('/')
                
                # Extract categories
                categories = []
                category_links = article.find_all('a', class_='blog-post-tag')
                for cat_link in category_links:
                    categories.append(cat_link.get_text(strip=True))
                categories_str = ', '.join(categories) if categories else None
                
                # Extract post date and author
                metadata = article.find('div', class_='metadata')
                post_date = None
                author = None
                views = None
                
                if metadata:
                    # Find date (format: "November 24, 2025 by")
                    post_bio = metadata.find('div', class_='post-bio')
                    if post_bio:
                        bio_text = post_bio.get_text(strip=True)

                        # Extract date (before " by ")
                        if ' by' in bio_text:
                            date_str = bio_text.split(' by')[0].strip()
                            # Remove leading date text if present
                            for prefix in ['November', 'October', 'September', 'August', 
                                         'July', 'June', 'May', 'April', 'March', 
                                         'February', 'January', 'December']:
                                if date_str.startswith(prefix):
                                    try:
                                        post_date = datetime.strptime(date_str, '%B %d, %Y')
                                    except ValueError:
                                        logger.warning(f"Could not parse date: {date_str}")
                                    break
                        
                        # Extract author
                        author_link = post_bio.find('a')
                        if author_link:
                            author = author_link.get_text(strip=True)
                        
                        # Extract views
                        views_elem = post_bio.find('span', class_='postview')
                        if views_elem:
                            views_text = views_elem.get_text(strip=True)
                            # Extract number from "28 Views"
                            views_str = views_text.replace('Views', '').replace(',', '').strip()
                            try:
                                views = int(views_str)
                            except ValueError:
                                pass
                
                # Extract summary/description
                summary = None
                # Look for paragraph with summary text (usually first <p> after metadata)
                summary_elem = article.find('p')
                if summary_elem:
                    summary = html.unescape(summary_elem.get_text(strip=True))
                    # Remove "Continue reading" link text if present
                    if 'Continue reading' in summary:
                        summary = summary.split('Continue reading')[0].strip()
                
                articles.append({
                    'title': title,
                    'url': url[0:-7],
                    'categories': categories_str,
                    'post_date': post_date,
                    'author': author,
                    'views': views,
                    'summary': summary
                })
            except Exception as e:
                logger.error(f"Error extracting article data: {e}")
                continue
        
        return articles
    
    def create_table_if_not_exists(self, cursor):
        """Create the blog_posts table if it doesn't exist"""
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fabric_blog_posts')
        BEGIN
            CREATE TABLE fabric_blog_posts (
                id INT IDENTITY(1,1) PRIMARY KEY,
                title NVARCHAR(500) NOT NULL,
                url NVARCHAR(1000) NOT NULL UNIQUE,
                categories NVARCHAR(500),
                post_date DATE,
                author NVARCHAR(200),
                views INT,
                summary NVARCHAR(MAX),
                scraped_at DATETIME2 DEFAULT GETUTCDATE(),
                updated_at DATETIME2 DEFAULT GETUTCDATE(),
                blog_vector VECTOR(1536, float32) NULL
            );
            
            CREATE INDEX idx_post_date ON fabric_blog_posts(post_date DESC);
            CREATE INDEX idx_categories ON fabric_blog_posts(categories);
        END
        """
        cursor.execute(create_table_sql)
        logger.info("Ensured fabric_blog_posts table exists")
    
    def insert_or_update_article(self, cursor, article: Dict):
        """
        Insert article into database or update if URL already exists

        Args:
            cursor: Database cursor
            article: Dictionary containing article data
        """
        upsert_sql = """
        MERGE fabric_blog_posts AS target
        USING (SELECT ? AS url) AS source
        ON target.url = source.url
        WHEN MATCHED THEN
            UPDATE SET 
                title = ?,
                categories = ?,
                post_date = ?,
                author = ?,
                views = ?,
                summary = ?,
                updated_at = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT (title, url, categories, post_date, author, views, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        
        cursor.execute(upsert_sql, (
            article['url'].rstrip('/'),  # for USING clause
            article['title'],
            article['categories'],
            article['post_date'],
            article['author'],
            article['views'],
            article['summary'],
            # INSERT values
            article['title'],
            article['url'].rstrip('/'),
            article['categories'],
            article['post_date'],
            article['author'],
            article['views'],
            article['summary']
        ))

    @retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
    def upsert_article_with_retry(self, article: Dict):
        """Open a fresh connection, upsert the article, commit, and close.

        Per-article connection management is heavier than batching, but the
        MERGE upsert is idempotent on URL so retries after a transient failure
        are safe, and per-call commit means a mid-loop crash never loses a
        previously-processed article.
        """
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            self.insert_or_update_article(cursor, article)
            conn.commit()
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass

    @retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
    def article_exists(self, url: str) -> bool:
        """Check whether an article with this URL is already in the database."""
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM fabric_blog_posts WHERE url = ?",
                (url,),
            )
            return cursor.fetchone()[0] > 0
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass
    
    def scrape_all_pages(self, start_page: int = 1, end_page: int = 169):
        """
        Scrape all blog pages and store in database

        Args:
            start_page: First page to scrape (default: 1)
            end_page: Last page to scrape (default: 169)
        """
        try:
            total_articles = 0
            failed_pages = []

            for page_num in range(start_page, end_page + 1):
                logger.info(f"Processing page {page_num}/{end_page}")

                soup = self.fetch_page(page_num)
                if not soup:
                    failed_pages.append(page_num)
                    continue

                articles = self.extract_articles(soup)
                logger.info(f"Found {len(articles)} articles on page {page_num}")

                # Insert articles into database (per-article connection + retry).
                page_success = 0
                for article in articles:
                    try:
                        self.upsert_article_with_retry(article)
                        total_articles += 1
                        page_success += 1
                    except Exception as e:
                        logger.error(f"Failed to insert article '{article.get('title')}': {e}")

                logger.info(f"Committed {page_success}/{len(articles)} articles from page {page_num}")

            # Final summary
            logger.info("=" * 60)
            logger.info(f"Scraping complete!")
            logger.info(f"Total articles processed: {total_articles}")
            logger.info(f"Pages scraped: {end_page - start_page + 1 - len(failed_pages)}/{end_page - start_page + 1}")

            if failed_pages:
                logger.warning(f"Failed pages: {failed_pages}")

        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}")
            raise
    
    def fetch_rss_feed(self) -> Optional[ET.Element]:
        """
        Fetch and parse the RSS feed
        
        Returns:
            XML Element root or None if fetch fails
        """
        try:
            logger.info(f"Fetching RSS feed: {self.rss_url}")
            response = self.session.get(self.rss_url, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            return root
        
        except requests.RequestException as e:
            logger.error(f"Failed to fetch RSS feed: {e}")
            return None
        except ET.ParseError as e:
            logger.error(f"Failed to parse RSS feed XML: {e}")
            return None
    
    def extract_articles_from_rss(self, root: ET.Element) -> List[Dict]:
        """
        Extract article data from RSS feed
        
        Args:
            root: XML Element root of the RSS feed
            
        Returns:
            List of dictionaries containing article data
        """
        articles = []
        
        # RSS feeds typically have items under channel
        channel = root.find('channel')
        if not channel:
            logger.error("No channel found in RSS feed")
            return articles
        
        items = channel.findall('item')
        logger.info(f"Found {len(items)} items in RSS feed")
        
        for item in items:
            try:
                # Extract title
                title_elem = item.find('title')
                title = html.unescape(title_elem.text) if title_elem is not None and title_elem.text else None
                
                # Extract URL
                link_elem = item.find('link')
                url = link_elem.text.rstrip('/') if link_elem is not None and link_elem.text else None
                
                if not title or not url:
                    logger.warning("Skipping item with missing title or URL")
                    continue
                
                # Extract description/summary
                description_elem = item.find('description')
                summary = html.unescape(description_elem.text) if description_elem is not None and description_elem.text else None
                
                # Extract publish date
                pub_date_elem = item.find('pubDate')
                post_date = None
                if pub_date_elem is not None:
                    try:
                        # RSS pubDate format: "Mon, 16 Dec 2024 14:30:00 +0000"
                        date_str = pub_date_elem.text
                        post_date = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
                        # Remove timezone info for SQL Server
                        post_date = post_date.replace(tzinfo=None)
                    except ValueError as e:
                        logger.warning(f"Could not parse date '{pub_date_elem.text}': {e}")
                
                # Extract author (dc:creator or author tag)
                author = None
                creator_elem = item.find('{http://purl.org/dc/elements/1.1/}creator')
                if creator_elem is not None:
                    author = creator_elem.text
                else:
                    author_elem = item.find('author')
                    if author_elem is not None:
                        author = author_elem.text
                
                # Extract categories
                category_elems = item.findall('category')
                categories = [cat.text for cat in category_elems if cat.text]
                categories_str = ', '.join(categories) if categories else None
                
                articles.append({
                    'title': title,
                    'url': url,
                    'categories': categories_str,
                    'post_date': post_date,
                    'author': author,
                    'views': None,  # Not available in RSS feed
                    'summary': summary
                })
            
            except Exception as e:
                logger.error(f"Error extracting RSS item data: {e}")
                continue
        
        return articles
    
    def fetch_article_details(self, url: str) -> Dict:
        """
        Fetch individual article page to extract additional details
        
        Args:
            url: Article URL to fetch
            
        Returns:
            Dictionary containing categories and author, or None if fetch fails
        """
        try:
            logger.info(f"Fetching article details from: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            details = {
                'categories': None,
                'author': None
            }
            
            # Extract categories
            categories = []
            category_links = soup.find_all('a', class_='blog-post-tag')
            for cat_link in category_links:
                category_text = cat_link.get_text(strip=True)
                if category_text:
                    categories.append(category_text)
            details['categories'] = ', '.join(categories) if categories else None
            
            # Extract author - look in post-bio section
            post_bio = soup.find('div', class_='post-bio')
            if post_bio:
                # Find the author link (usually has pattern "post by [Author Name]")
                author_span = post_bio.find('span', class_='font-semibold')
                if author_span:
                    author_link = author_span.find('a')
                    if author_link:
                        details['author'] = html.unescape(author_link.get_text(strip=True))
            
            logger.info(f"Extracted details - Categories: {details['categories']}, Author: {details['author']}")
            
            # Add small delay to be respectful to the server
            time.sleep(1)
            
            return details
        
        except requests.RequestException as e:
            logger.error(f"Failed to fetch article details from {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error extracting article details from {url}: {e}")
            return None
    
    def scrape_from_rss(self):
        """
        Scrape articles from RSS feed and store in database (delta load)
        """
        try:
            # Fetch RSS feed
            root = self.fetch_rss_feed()
            if not root:
                logger.error("Failed to fetch RSS feed")
                return

            # Extract articles from RSS
            articles = self.extract_articles_from_rss(root)
            logger.info(f"Extracted {len(articles)} articles from RSS feed")

            new_count = 0
            updated_count = 0

            # Insert articles into database. Each article gets its own
            # connection + commit + retry, so a transient SQL error or a
            # mid-loop crash never loses already-processed articles.
            for article in articles:
                try:
                    trimmed_url = article['url'].rstrip('/')
                    exists = self.article_exists(trimmed_url)

                    # For new articles, fetch additional details from the article page
                    if not exists:
                        logger.info(f"New article found: {article['title']}")
                        article_details = self.fetch_article_details(article['url'])

                        if article_details:
                            # Update article with fetched details if not already present in RSS
                            if not article.get('categories') and article_details.get('categories'):
                                article['categories'] = article_details['categories']
                            if not article.get('author') and article_details.get('author'):
                                article['author'] = article_details['author']

                    self.upsert_article_with_retry(article)

                    if exists:
                        updated_count += 1
                    else:
                        new_count += 1

                except Exception as e:
                    logger.error(f"Failed to insert article '{article.get('title')}': {e}")

            # Final summary
            logger.info("=" * 60)
            logger.info(f"RSS feed scraping complete!")
            logger.info(f"New articles: {new_count}")
            logger.info(f"Updated articles: {updated_count}")
            logger.info(f"Total processed: {len(articles)}")

        except Exception as e:
            logger.error(f"Fatal error during RSS scraping: {e}")
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Fabric Blog Scraper - Scrape Microsoft Fabric blog posts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full load - scrape all pages (default)
  python scrape_fabric_blog.py
  
  # Full load - scrape specific page range
  python scrape_fabric_blog.py --start-page 1 --end-page 10
  
  # Delta load - check RSS feed for new articles
  python scrape_fabric_blog.py --rss
        """
    )
    
    parser.add_argument(
        '--rss',
        action='store_true',
        help='Delta load: Check RSS feed for new articles instead of full site scan'
    )
    
    parser.add_argument(
        '--start-page',
        type=int,
        default=1,
        help='Start page for full load (default: 1)'
    )
    
    parser.add_argument(
        '--end-page',
        type=int,
        default=169,
        help='End page for full load (default: 169)'
    )
    
    args = parser.parse_args()
    
    try:
        scraper = FabricBlogScraper()
        
        if args.rss:
            # Delta load: RSS feed only
            logger.info("Starting Fabric Blog scraper (RSS delta load)")
            scraper.scrape_from_rss()
        else:
            # Full load: scrape all pages
            logger.info(f"Starting Fabric Blog scraper (full load: pages {args.start_page}-{args.end_page})")
            scraper.scrape_all_pages(args.start_page, args.end_page)
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
