#!/usr/bin/env python3
"""Backfill full article content for blog posts already in the database.

Rows ingested before the community API was wired in hold only the RSS feed's
teaser as their summary — a few hundred characters, and empty for some posts —
which is a poor basis for an embedding. This one-shot job re-reads each post
from the community API and replaces that teaser with the full body, along with
the categories and view counts the feed never carried.

Any row whose summary actually changes has its ``blog_vector`` cleared so the
next ``vectorize_blog_posts.py`` run re-embeds it against the real content.
Rows that already match are left completely alone, so the job is safe to
re-run and cheap the second time.

Usage:
    python backfill_blog_content.py --dry-run          # report, change nothing
    python backfill_blog_content.py --limit 5          # try a handful first
    python backfill_blog_content.py                    # full run
"""

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional

import pyodbc

from lib.community_urls import extract_message_id
from lib.khoros_api import KhorosClient
from lib.rate_limit import SlidingWindowLimiter
from lib.telemetry import init_telemetry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

init_telemetry("fabric-gps-backfill-blog-content")

DEFAULT_MAX_REQUESTS_PER_MINUTE = 30

# Matches the scraper's column width for categories (nvarchar(500)).
MAX_CATEGORIES_LEN = 500


def _strip_html_to_text(html_text: Optional[str]) -> str:
    """Convert an HTML fragment to collapsed plain text."""
    if not html_text:
        return ''
    from bs4 import BeautifulSoup
    return ' '.join(BeautifulSoup(html_text, 'html.parser').get_text(' ').split())


class BlogContentBackfill:
    """Re-reads stored posts from the community API and stores full bodies."""

    def __init__(
        self,
        connection_string: str,
        dry_run: bool = False,
        limit: Optional[int] = None,
        khoros_client: Optional[KhorosClient] = None,
    ):
        self.connection_string = connection_string
        self.dry_run = dry_run
        self.limit = limit
        rate_limiter = SlidingWindowLimiter(
            max_calls=int(os.getenv(
                'BLOG_SCRAPER_MAX_REQUESTS_PER_MINUTE',
                str(DEFAULT_MAX_REQUESTS_PER_MINUTE),
            )),
            window_seconds=60.0,
        )
        self.khoros = khoros_client or KhorosClient(rate_limiter=rate_limiter)

    def fetch_candidates(self, cursor) -> List[Dict]:
        """Return stored posts that carry a community message id.

        Legacy rows whose URL has no message id can't be looked up in the API
        and are reported as skipped rather than failed.
        """
        top = f"TOP {int(self.limit)} " if self.limit else ""
        cursor.execute(
            f"SELECT {top}id, url, summary, categories FROM fabric_blog_posts ORDER BY id"
        )
        return [
            {'id': row[0], 'url': row[1], 'summary': row[2], 'categories': row[3]}
            for row in cursor.fetchall()
        ]

    def _fetch_content(self, message_id: str) -> Optional[Dict]:
        """Pull body, labels, author and views for one message."""
        article = self.khoros.fetch_article(message_id)
        if article is None:
            return None

        labels = self.khoros.fetch_labels(message_id)
        categories = ', '.join(labels)[:MAX_CATEGORIES_LEN] if labels else None

        return {
            'summary': _strip_html_to_text(article.get('body')) or None,
            'categories': categories,
            'author': article.get('author'),
            'views': article.get('views'),
        }

    def run(self) -> Dict[str, int]:
        stats = {
            'examined': 0, 'updated': 0, 'unchanged': 0,
            'skipped_no_id': 0, 'failed': 0, 'vectors_cleared': 0,
        }

        conn = pyodbc.connect(self.connection_string)
        try:
            cursor = conn.cursor()
            rows = self.fetch_candidates(cursor)
            logger.info(f"Examining {len(rows)} blog row(s)")

            for row in rows:
                stats['examined'] += 1
                message_id = extract_message_id(row['url'])
                if not message_id:
                    stats['skipped_no_id'] += 1
                    logger.info(f"[SKIP] row {row['id']}: no message id in {row['url']}")
                    continue

                try:
                    content = self._fetch_content(message_id)
                except Exception as exc:  # noqa: BLE001
                    stats['failed'] += 1
                    logger.error(f"[FAIL] row {row['id']} ({message_id}): {exc}")
                    continue

                if content is None:
                    stats['failed'] += 1
                    logger.warning(f"[FAIL] row {row['id']}: API returned no content")
                    continue

                new_summary = content['summary']
                if not new_summary:
                    stats['failed'] += 1
                    logger.warning(f"[FAIL] row {row['id']}: API returned an empty body")
                    continue

                # Leave rows that already hold the full body alone, so a
                # re-run costs nothing and doesn't churn updated_at.
                summary_changed = (row['summary'] or '') != new_summary
                needs_categories = bool(content['categories']) and not row['categories']
                if not summary_changed and not needs_categories:
                    stats['unchanged'] += 1
                    continue

                old_len = len(row['summary'] or '')
                if self.dry_run:
                    if summary_changed:
                        stats['vectors_cleared'] += 1
                    stats['updated'] += 1
                    logger.info(
                        f"[DRY-RUN] row {row['id']}: summary {old_len} -> "
                        f"{len(new_summary)} chars, categories="
                        f"{content['categories']!r}, views={content['views']}"
                    )
                    continue

                # Clear the embedding first, while `summary` still holds the
                # old value, and as a plain NULL assignment — the VECTOR type
                # has limited expression support.
                if summary_changed:
                    cursor.execute(
                        "UPDATE fabric_blog_posts SET blog_vector = NULL WHERE id = ?",
                        (row['id'],),
                    )
                    stats['vectors_cleared'] += 1

                cursor.execute(
                    "UPDATE fabric_blog_posts SET "
                    "summary = ?, "
                    "categories = COALESCE(?, categories), "
                    "author = COALESCE(?, author), "
                    "views = COALESCE(?, views), "
                    "updated_at = GETUTCDATE() "
                    "WHERE id = ?",
                    (
                        new_summary,
                        content['categories'],
                        content['author'],
                        content['views'],
                        row['id'],
                    ),
                )
                conn.commit()
                stats['updated'] += 1
                logger.info(
                    f"[UPDATE] row {row['id']}: summary {old_len} -> "
                    f"{len(new_summary)} chars, categories="
                    f"{content['categories']!r}, views={content['views']}"
                )

        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 — best-effort cleanup
                pass

        logger.info("=" * 60)
        logger.info("Backfill complete!" if not self.dry_run else "Dry run complete!")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        if stats['vectors_cleared'] and not self.dry_run:
            logger.info(
                f"Run vectorize_blog_posts.py to re-embed "
                f"{stats['vectors_cleared']} post(s)."
            )
        logger.info("=" * 60)
        return stats


def main():
    parser = argparse.ArgumentParser(
        description="Backfill full blog article content from the community API",
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Report what would change without writing anything',
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='Only examine the first N rows (useful for a trial run)',
    )
    args = parser.parse_args()

    connection_string = os.getenv('SQLSERVER_CONN')
    if not connection_string:
        logger.error("SQLSERVER_CONN environment variable not set")
        sys.exit(1)

    backfill = BlogContentBackfill(
        connection_string, dry_run=args.dry_run, limit=args.limit
    )
    stats = backfill.run()
    sys.exit(1 if stats['failed'] else 0)


if __name__ == '__main__':
    main()
