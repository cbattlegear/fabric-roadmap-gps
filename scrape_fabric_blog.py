#!/usr/bin/env python3
"""
Fabric Blog Scraper
Scrapes blog posts from the Microsoft Fabric Community "Fabric Updates Blog"
(https://community.fabric.microsoft.com) and stores them in SQL Server.

The blog moved from https://blog.fabric.microsoft.com to the community site;
old blog URLs (and the old RSS feed URL) redirect to the community equivalents.

Supports delta load: Checks the community RSS feed for new articles only.
The legacy full-load (page scan) mode targeted the old WordPress site and is
no longer supported.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import pyodbc
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
import html

from lib.db_retry import retry_on_transient_errors
from lib.rate_limit import SlidingWindowLimiter
from lib.telemetry import init_telemetry


# Default rate-limit and backoff knobs. All overridable via env vars so
# the refresh container can tune without a code change.
DEFAULT_MAX_REQUESTS_PER_MINUTE = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 16.0


def _build_default_limiter() -> SlidingWindowLimiter:
    """Construct the limiter from env-var overrides."""
    max_per_min = int(os.getenv(
        'BLOG_SCRAPER_MAX_REQUESTS_PER_MINUTE',
        str(DEFAULT_MAX_REQUESTS_PER_MINUTE),
    ))
    return SlidingWindowLimiter(max_calls=max_per_min, window_seconds=60.0)

def _parse_rss_pub_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse an RSS ``<pubDate>`` value into a naive ``datetime``.

    RSS pubDate uses the RFC 2822 grammar, which permits both numeric
    offsets (``"+0000"``) and named zones (``"GMT"``, ``"UTC"``,
    ``"EST"``, ...). ``datetime.strptime("%z")`` only handles the
    numeric form, so feeds that publish ``"... GMT"`` would warn and
    drop the date. ``email.utils.parsedate_to_datetime`` handles the
    full grammar.

    Returns ``None`` for empty / unparseable input.
    """
    if not date_str:
        return None
    try:
        dt = parsedate_to_datetime(date_str)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is not None:
        # SQL Server column is naive; strip tzinfo keeping wall-clock time
        # (preserves prior strptime behavior; feeds in practice publish GMT/UTC).
        dt = dt.replace(tzinfo=None)
    return dt


def _strip_html_to_text(html_text: Optional[str]) -> str:
    """Convert an HTML fragment (e.g. an RSS ``<description>``) to plain text.

    The community RSS feed wraps descriptions in full HTML markup (``<P>``,
    ``<SPAN>``, ...). Storing raw markup would pollute summaries and
    embeddings, so strip the tags, unescape entities and collapse runs of
    whitespace into single spaces.
    """
    if not html_text:
        return ''
    text = BeautifulSoup(html_text, 'html.parser').get_text(' ')
    return ' '.join(text.split())


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
    
    # The Fabric Updates Blog now lives on the Microsoft Fabric Community
    # site. The old blog.fabric.microsoft.com URLs (including the old RSS
    # feed path) redirect here, so we point straight at the community
    # equivalents to avoid the redirect hop.
    COMMUNITY_BOARD_URL = (
        "https://community.fabric.microsoft.com"
        "/t5/Fabric-Updates-Blog/bg-p/fbc_fabricupdatesblogs"
    )
    COMMUNITY_RSS_URL = (
        "https://community.fabric.microsoft.com/t5/s/rss/board"
        "?board.id=fbc_fabricupdatesblogs"
    )

    def __init__(self, rate_limiter: Optional[SlidingWindowLimiter] = None):
        self.base_url = self.COMMUNITY_BOARD_URL
        self.rss_url = self.COMMUNITY_RSS_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # In-process rate limiter shared across every outbound request
        # this scraper makes (page list, RSS feed, individual articles).
        self.rate_limiter = rate_limiter or _build_default_limiter()
        self.max_retries = int(os.getenv(
            'BLOG_SCRAPER_MAX_RETRIES', str(DEFAULT_MAX_RETRIES)
        ))
        self.backoff_base = float(os.getenv(
            'BLOG_SCRAPER_BACKOFF_BASE', str(DEFAULT_BACKOFF_BASE_SECONDS)
        ))
        self.backoff_max = float(os.getenv(
            'BLOG_SCRAPER_BACKOFF_MAX', str(DEFAULT_BACKOFF_MAX_SECONDS)
        ))

        # Database connection from environment
        self.connection_string = os.getenv('SQLSERVER_CONN')
        if not self.connection_string:
            raise ValueError("SQLSERVER_CONN environment variable not set")

    def _rate_limited_get(self, url: str, *, timeout: int = 30) -> requests.Response:
        """GET ``url`` through the rate limiter with 429 exponential backoff.

        Retries on HTTP 429 (and 503 — a similar back-off signal) with
        exponential delays capped at ``self.backoff_max``. Honors
        ``Retry-After`` if present. Other HTTPErrors fall through to the
        caller via ``raise_for_status``.
        """
        attempt = 0
        while True:
            self.rate_limiter.wait_for_capacity()
            response = self.session.get(url, timeout=timeout)

            if response.status_code not in (429, 503):
                # Only successful (or non-throttling-error) requests count
                # against the local quota — otherwise repeated 429s would
                # eat our budget and cascade into longer client-side waits
                # on top of the server's backoff signal.
                self.rate_limiter.record()
                response.raise_for_status()
                return response

            if attempt >= self.max_retries:
                logger.error(
                    "Giving up on %s after %d retries (status=%s)",
                    url, attempt, response.status_code,
                )
                self.rate_limiter.record()
                response.raise_for_status()
                return response

            backoff = min(
                self.backoff_max,
                self.backoff_base * (2 ** attempt),
            )
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    backoff = max(backoff, float(retry_after))
                except ValueError:
                    pass

            logger.warning(
                "HTTP %s on %s (attempt %d/%d) — sleeping %.1fs",
                response.status_code, url, attempt + 1,
                self.max_retries, backoff,
            )
            time.sleep(backoff)
            attempt += 1
    
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
            response = self._rate_limited_get(url)
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
        Legacy full-load mode — no longer supported.

        The blog moved from blog.fabric.microsoft.com (WordPress) to the
        Microsoft Fabric Community site, where the article list is rendered
        client-side and the old ``article.post`` selectors / ``?page=N``
        pagination no longer apply. The historical full load already ran
        against the old site; new articles arrive via the RSS delta load.
        """
        raise RuntimeError(
            "Full-load scraping is no longer supported: the Fabric blog moved "
            "to https://community.fabric.microsoft.com (client-rendered board). "
            "Historical posts were already loaded from the old site; use "
            "`python scrape_fabric_blog.py --rss` for the delta load."
        )

    def fetch_rss_feed(self) -> Optional[ET.Element]:
        """
        Fetch and parse the RSS feed
        
        Returns:
            XML Element root or None if fetch fails
        """
        try:
            logger.info(f"Fetching RSS feed: {self.rss_url}")
            response = self._rate_limited_get(self.rss_url)

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
                
                # Extract description/summary (strip HTML — the community
                # feed embeds full markup in <description>)
                description_elem = item.find('description')
                summary = _strip_html_to_text(description_elem.text) or None if description_elem is not None and description_elem.text else None
                
                # Extract publish date
                pub_date_elem = item.find('pubDate')
                post_date = None
                if pub_date_elem is not None and pub_date_elem.text:
                    date_str = pub_date_elem.text
                    post_date = _parse_rss_pub_date(date_str)
                    if post_date is None:
                        logger.warning(f"Could not parse date '{date_str}'")
                
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
            Dictionary containing categories, author, and ``final_url`` (the
            URL after redirects — the community site redirects feed links to
            the canonical article URL), or None if fetch fails
        """
        try:
            logger.info(f"Fetching article details from: {url}")
            response = self._rate_limited_get(url)

            soup = BeautifulSoup(response.content, 'html.parser')

            details = {
                'categories': None,
                'author': None,
                'final_url': response.url.rstrip('/') if getattr(response, 'url', None) else url
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

                    # For new articles, fetch additional details from the article page.
                    # The fetch also resolves the community site's redirect chain
                    # (feed link -> canonical article URL), which is what we store.
                    if not exists:
                        logger.info(f"New article found: {article['title']}")
                        article_details = self.fetch_article_details(article['url'])

                        if article_details:
                            # Canonicalize: store the final (post-redirect) URL so
                            # each article has exactly one row and links in the
                            # UI/API don't bounce through redirects.
                            final_url = (article_details.get('final_url') or trimmed_url).rstrip('/')
                            if final_url != trimmed_url:
                                article['url'] = final_url
                                exists = self.article_exists(final_url)

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
        description='Fabric Blog Scraper - Scrape Microsoft Fabric blog posts '
                    '(community.fabric.microsoft.com)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delta load - check RSS feed for new articles (supported mode)
  python scrape_fabric_blog.py --rss

The blog moved from blog.fabric.microsoft.com to the Microsoft Fabric
Community site. The RSS feed is the supported load mode; the old full
page-load (running without --rss) is no longer available.
        """
    )
    
    parser.add_argument(
        '--rss',
        action='store_true',
        help='Delta load: Check RSS feed for new articles (supported load mode)'
    )
    
    parser.add_argument(
        '--start-page',
        type=int,
        default=1,
        help='Start page (kept for compatibility; full load is no longer supported)'
    )
    
    parser.add_argument(
        '--end-page',
        type=int,
        default=169,
        help='End page (kept for compatibility; full load is no longer supported)'
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
