#!/usr/bin/env python3
"""
One-shot migration: rewrite legacy blog.fabric.microsoft.com URLs to their
canonical community.fabric.microsoft.com equivalents.

The Fabric Updates Blog moved to the Microsoft Fabric Community site. Old
blog URLs (and the old RSS feed URL) redirect to the community equivalents,
but rows already stored in ``fabric_blog_posts`` — and the ``blog_url``
values copied into ``release_items`` — still point at the old domain.

For every stored URL that is not already a community URL, this script:

1. fetches the old URL and follows the redirect chain to the canonical
   community article URL;
2. re-points ``release_items.blog_url`` from the old URL to the canonical
   URL;
3. if a ``fabric_blog_posts`` row already exists for the canonical URL (e.g.
   loaded via the new RSS feed): carries the old row's vector over to the
   surviving row (when the survivor has none) and DELETEs the old row;
   otherwise UPDATEs the old row's URL in place (preserving its vector,
   title, summary, ...);
4. as a second pass, re-points ``release_items.blog_url`` values that no
   longer correspond to any pending ``fabric_blog_posts`` row (orphaned
   references).

The migration is safe to re-run: rows already pointing at community URLs are
skipped, each row migrates in its own connection + commit, so an interrupted
run resumes where it left off.

Usage:
  python migrate_blog_urls.py --dry-run
  python migrate_blog_urls.py
  python migrate_blog_urls.py --limit 10
"""

import argparse
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import pyodbc
import requests

from lib.rate_limit import SlidingWindowLimiter
from lib.telemetry import init_telemetry

# Rows already pointing here are skipped; resolved redirect targets must
# land here for the migration to apply them.
COMMUNITY_URL_PREFIX = "https://community.fabric.microsoft.com"

# Same rate-limit knobs as the scraper so the community site sees one
# consistent budget regardless of which entry point is running.
DEFAULT_MAX_REQUESTS_PER_MINUTE = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 16.0

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Wire Azure Monitor for the migration run (no-op in development).
init_telemetry("fabric-gps-migrate-blog-urls")


def fetch_final_url(
    session: requests.Session,
    url: str,
    *,
    timeout: int = 30,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE_SECONDS,
    backoff_max: float = DEFAULT_BACKOFF_MAX_SECONDS,
    limiter: Optional[SlidingWindowLimiter] = None,
) -> Optional[str]:
    """Fetch ``url`` following redirects; return the final (canonical) URL.

    Retries on HTTP 429/503 with exponential backoff honoring ``Retry-After``.
    Returns ``None`` if the fetch ultimately fails (network error, other
    HTTP error, or the redirects don't produce a usable final URL).
    """
    attempt = 0
    while True:
        if limiter is not None:
            limiter.wait_for_capacity()
        try:
            response = session.get(url, timeout=timeout)
        except requests.RequestException as e:
            if attempt >= max_retries:
                logger.error(f"Giving up on {url} after {attempt} retries: {e}")
                return None
            logger.warning(f"Fetch error for {url} (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(min(backoff_max, backoff_base * (2 ** attempt)))
            attempt += 1
            continue

        if response.status_code not in (429, 503):
            if limiter is not None:
                limiter.record()
            if response.status_code >= 400:
                logger.error(f"HTTP {response.status_code} resolving {url} — skipping")
                return None
            final_url = (getattr(response, 'url', None) or url).rstrip('/')
            return final_url or None

        if attempt >= max_retries:
            logger.error(
                f"Giving up on {url} after {attempt} retries (status={response.status_code})"
            )
            return None
        backoff = min(backoff_max, backoff_base * (2 ** attempt))
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                backoff = max(backoff, float(retry_after))
            except ValueError:
                pass
        logger.warning(
            f"HTTP {response.status_code} on {url} (attempt {attempt + 1}/{max_retries}) — "
            f"sleeping {backoff:.1f}s"
        )
        time.sleep(backoff)
        attempt += 1


class BlogUrlMigration:
    """Rewrites old blog URLs stored in ``fabric_blog_posts`` and
    ``release_items`` to canonical community URLs."""

    def __init__(
        self,
        connection_string: str,
        *,
        dry_run: bool = False,
        limit: Optional[int] = None,
        session: Optional[requests.Session] = None,
        rate_limiter: Optional[SlidingWindowLimiter] = None,
    ):
        self.connection_string = connection_string
        self.dry_run = dry_run
        self.limit = limit
        self.session = session or requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.limiter = rate_limiter or SlidingWindowLimiter(
            max_calls=int(os.getenv(
                'BLOG_SCRAPER_MAX_REQUESTS_PER_MINUTE',
                str(DEFAULT_MAX_REQUESTS_PER_MINUTE),
            )),
            window_seconds=60.0,
        )

    # ------------------------------------------------------------------
    # Database helpers (each opens its own connection so a transient
    # Azure SQL failure only loses the current row, not the run)
    # ------------------------------------------------------------------

    def _fetch_pending_rows(self) -> List[Tuple[int, str, bool]]:
        """Old-domain rows still needing migration: (id, url, has_vector)."""
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            params: list = [f"{COMMUNITY_URL_PREFIX}%"]
            # NOTE: T-SQL has no boolean expression type, so the null check
            # must go through CASE — `(blog_vector IS NOT NULL)` in a select
            # list is a syntax error.
            if self.limit is not None:
                cursor.execute(
                    "SELECT TOP (?) id, url, "
                    "CASE WHEN blog_vector IS NOT NULL THEN 1 ELSE 0 END AS has_vector "
                    "FROM fabric_blog_posts "
                    "WHERE url NOT LIKE ? "
                    "ORDER BY id",
                    (self.limit, *params),
                )
            else:
                cursor.execute(
                    "SELECT id, url, "
                    "CASE WHEN blog_vector IS NOT NULL THEN 1 ELSE 0 END AS has_vector "
                    "FROM fabric_blog_posts "
                    "WHERE url NOT LIKE ? "
                    "ORDER BY id",
                    params,
                )
            return [(row[0], row[1], bool(row[2])) for row in cursor.fetchall()]
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def _fetch_orphaned_release_urls(self) -> List[str]:
        """Distinct old-domain ``release_items.blog_url`` values that no
        longer match a pending ``fabric_blog_posts`` row."""
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT blog_url FROM release_items "
                "WHERE blog_url IS NOT NULL "
                "AND blog_url NOT LIKE ? "
                "AND blog_url NOT IN (SELECT url FROM fabric_blog_posts) "
                "ORDER BY blog_url",
                (f"{COMMUNITY_URL_PREFIX}%",),
            )
            return [row[0] for row in cursor.fetchall() if row[0]]
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def _migrate_row(self, old_id: int, old_url: str, final_url: str, has_vector: bool) -> str:
        """Atomically migrate one blog row (own connection + commit).

        Returns one of ``'updated'``, ``'deleted'``, ``'skipped'``.
        """
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()

            # Does the canonical URL already have a row (e.g. from the new RSS feed)?
            cursor.execute(
                "SELECT id, CASE WHEN blog_vector IS NULL THEN 1 ELSE 0 END "
                "AS vector_is_null "
                "FROM fabric_blog_posts WHERE url = ?",
                (final_url,),
            )
            survivor = cursor.fetchone()

            # Re-point any releases that reference the old URL. Done before
            # the row change so releases never point at a URL that disappears.
            if self.dry_run:
                cursor.execute(
                    "SELECT COUNT(*) FROM release_items WHERE blog_url = ?",
                    (old_url,),
                )
                repointed = cursor.fetchone()[0]
            else:
                cursor.execute(
                    "UPDATE release_items SET blog_url = ? WHERE blog_url = ?",
                    (final_url, old_url),
                )
                repointed = cursor.rowcount

            if survivor is not None:
                survivor_id, survivor_vector_null = survivor[0], bool(survivor[1])
                if not self.dry_run:
                    # The legacy row is usually richer than a survivor created
                    # from the new RSS feed (which carries no categories and
                    # no view counts), so fill the survivor's gaps before the
                    # legacy row is deleted. COALESCE only fills NULLs, so
                    # fresher feed-sourced values always win.
                    cursor.execute(
                        "UPDATE target SET "
                        "categories = COALESCE(target.categories, src.categories), "
                        "author = COALESCE(target.author, src.author), "
                        "views = COALESCE(target.views, src.views), "
                        "summary = COALESCE(target.summary, src.summary), "
                        "post_date = COALESCE(target.post_date, src.post_date), "
                        "updated_at = GETUTCDATE() "
                        "FROM fabric_blog_posts target "
                        "INNER JOIN fabric_blog_posts src ON src.id = ? "
                        "WHERE target.id = ?",
                        (old_id, survivor_id),
                    )
                if has_vector and survivor_vector_null and not self.dry_run:
                    # Carry the vector over so the survivor doesn't have to
                    # be re-embedded. Kept as a plain column assignment (no
                    # functions applied) because VECTOR has limited
                    # expression support.
                    cursor.execute(
                        "UPDATE fabric_blog_posts SET blog_vector = "
                        "(SELECT blog_vector FROM fabric_blog_posts WHERE id = ?) "
                        "WHERE id = ? AND blog_vector IS NULL",
                        (old_id, survivor_id),
                    )
                if not self.dry_run:
                    cursor.execute("DELETE FROM fabric_blog_posts WHERE id = ?", (old_id,))
                conn.commit()
                logger.info(
                    f"[{'DRY-RUN ' if self.dry_run else ''}DELETE] row {old_id}: "
                    f"{old_url} -> duplicate of {survivor_id} ({final_url}), "
                    f"{repointed} release(s) re-pointed"
                )
                return 'deleted'

            # No survivor: rewrite the URL in place (preserves vector, title,
            # summary, dates).
            if not self.dry_run:
                cursor.execute(
                    "UPDATE fabric_blog_posts SET url = ?, updated_at = GETUTCDATE() "
                    "WHERE id = ?",
                    (final_url, old_id),
                )
            conn.commit()
            logger.info(
                f"[{'DRY-RUN ' if self.dry_run else ''}UPDATE] row {old_id}: "
                f"{old_url} -> {final_url}, {repointed} release(s) re-pointed"
            )
            return 'updated'
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

    def _repoint_release_url(self, old_url: str, final_url: str) -> int:
        """Re-point ``release_items.blog_url`` for an orphaned old URL.

        Returns the number of affected release rows (counted in dry-run mode).
        """
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            if self.dry_run:
                cursor.execute(
                    "SELECT COUNT(*) FROM release_items WHERE blog_url = ?",
                    (old_url,),
                )
                return cursor.fetchone()[0]
            cursor.execute(
                "UPDATE release_items SET blog_url = ? WHERE blog_url = ?",
                (final_url, old_url),
            )
            conn.commit()
            return cursor.rowcount
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

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, int]:
        """Run the migration; returns a summary dict of counts."""
        stats = {
            'pending_rows': 0,
            'updated': 0,
            'deleted': 0,
            'orphaned_releases': 0,
            'skipped': 0,
            'failed': 0,
        }

        logger.info("=" * 60)
        if self.dry_run:
            logger.info("Blog URL migration (DRY RUN — no database writes)")
        else:
            logger.info("Blog URL migration")

        pending = self._fetch_pending_rows()
        stats['pending_rows'] = len(pending)
        logger.info(f"Found {len(pending)} blog rows with old-domain URLs")

        for old_id, old_url, has_vector in pending:
            try:
                final_url = fetch_final_url(
                    self.session,
                    old_url,
                    limiter=self.limiter,
                )
            except Exception as e:
                logger.error(f"Failed to resolve {old_url}: {e}")
                stats['failed'] += 1
                continue

            if final_url is None:
                stats['skipped'] += 1
                continue
            if not final_url.startswith(COMMUNITY_URL_PREFIX):
                logger.warning(
                    f"Redirect for {old_url} landed outside the community site "
                    f"({final_url}) — skipping"
                )
                stats['skipped'] += 1
                continue
            if final_url == old_url:
                continue

            try:
                result = self._migrate_row(old_id, old_url, final_url, has_vector)
            except Exception as e:
                logger.error(f"Failed to migrate row {old_id} ({old_url}): {e}")
                stats['failed'] += 1
                continue

            stats[result] += 1

        # Second pass: releases whose blog_url references an old URL that has
        # no (or no longer has a) matching fabric_blog_posts row.
        orphaned = self._fetch_orphaned_release_urls()
        for old_url in orphaned:
            try:
                final_url = fetch_final_url(self.session, old_url, limiter=self.limiter)
            except Exception as e:
                logger.error(f"Failed to resolve orphaned release URL {old_url}: {e}")
                continue
            if final_url is None or not final_url.startswith(COMMUNITY_URL_PREFIX):
                logger.warning(
                    f"Skipping orphaned release URL with no community redirect: {old_url}"
                )
                continue
            try:
                repointed = self._repoint_release_url(old_url, final_url)
            except Exception as e:
                logger.error(f"Failed to re-point releases for {old_url}: {e}")
                continue
            stats['orphaned_releases'] += repointed
            logger.info(
                f"[{'DRY-RUN' if self.dry_run else 'REPOINT'}] "
                f"{repointed} release(s): {old_url} -> {final_url}"
            )

        logger.info("=" * 60)
        logger.info("Migration complete!")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        return stats


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Migrate legacy blog.fabric.microsoft.com URLs to their '
                    'canonical community.fabric.microsoft.com equivalents',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_blog_urls.py --dry-run
  python migrate_blog_urls.py
  python migrate_blog_urls.py --limit 10

One-shot migration: safe to re-run (already-migrated rows are skipped).
        """
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Resolve redirects and log what would change without writing to the database'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Process at most N blog rows (default: all pending rows)'
    )

    args = parser.parse_args()

    connection_string = os.getenv('SQLSERVER_CONN')
    if not connection_string:
        logger.error("SQLSERVER_CONN environment variable not set")
        sys.exit(1)

    try:
        migration = BlogUrlMigration(
            connection_string,
            dry_run=args.dry_run,
            limit=args.limit,
        )
        stats = migration.run()
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)

    sys.exit(1 if stats['failed'] else 0)


if __name__ == "__main__":
    main()
