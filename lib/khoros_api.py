"""Client for the Microsoft Fabric Community API (Khoros Community API v2).

The community platform is Khoros, which exposes a public read API alongside
the rendered site. That API is the reliable way to read article content:

* Article **pages** sit behind bot protection. Identical requests return 403
  or 200 depending on the mood of the edge, so an HTML scrape is a coin flip
  (measured: 2 successes in 6 attempts) and costs ~235 KB per article.
* The **API** answers every time (6/6 in the same test), costs ~16 KB, and
  returns structured fields instead of markup we would have to guess selectors
  for.

It also carries content the RSS feed does not. The feed's ``<description>`` is
only the teaser — a few hundred characters — while the API returns the whole
post body, which is what makes a blog post worth embedding.

Two calls are needed per article because Khoros returns ``labels`` (the blog's
categories) as a lazy sub-query rather than inline, and the documented
``/messages/<id>/labels`` sub-resource is permission-gated for anonymous
callers. A LiQL query for the same data is not.
"""

import logging
import os
import re
from typing import Dict, List, Optional
from urllib.parse import quote

import requests

from lib.rate_limit import SlidingWindowLimiter

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = 'https://community.fabric.microsoft.com/api/2.0'

# Khoros message ids are numeric. Anything else is rejected rather than
# escaped: these values are interpolated into LiQL, so a strict allow-list is
# the safe way to keep a crafted id from altering the query.
_NUMERIC_ID_RE = re.compile(r'^\d+$')

# Fields worth pulling for a blog article. `body` is the full post, `teaser`
# is the same text the RSS feed publishes as its description.
_ARTICLE_FIELDS = (
    'id, subject, body, teaser, post_time, view_href, '
    'author.login, metrics.views'
)


class KhorosClient:
    """Minimal read-only client for the community's public API.

    Every method degrades to ``None`` rather than raising, because enrichment
    must never block ingestion: a post with a teaser and no categories is far
    better than no post at all.
    """

    def __init__(
        self,
        api_base: Optional[str] = None,
        session: Optional[requests.Session] = None,
        rate_limiter: Optional[SlidingWindowLimiter] = None,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        self.api_base = (
            api_base
            or os.getenv('FABRIC_COMMUNITY_API_BASE', DEFAULT_API_BASE)
        ).rstrip('/')
        self.session = session or requests.Session()
        self.rate_limiter = rate_limiter
        self.timeout = timeout
        self.max_retries = max_retries

    @staticmethod
    def _require_numeric_id(message_id) -> str:
        """Return ``message_id`` as a string, rejecting non-numeric values."""
        text = str(message_id).strip()
        if not _NUMERIC_ID_RE.match(text):
            raise ValueError(f"Invalid community message id: {message_id!r}")
        return text

    def _liql(self, query: str) -> Optional[List[Dict]]:
        """Run a LiQL query and return its result items.

        Returns ``None`` when the query could not be run (transport error, or
        an API-level error response). An empty list means the query ran and
        matched nothing, which is a meaningful and different answer.
        """
        url = f"{self.api_base}/search?q={quote(query)}"

        for attempt in range(self.max_retries):
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.wait_for_capacity()
                    self.rate_limiter.record()

                response = self.session.get(url, timeout=self.timeout)
                if response.status_code != 200:
                    logger.warning(
                        f"Community API HTTP {response.status_code} for query: {query}"
                    )
                    continue

                payload = response.json()
                if payload.get('status') != 'success':
                    # Khoros reports application errors in a 200 body.
                    logger.warning(
                        f"Community API error for query {query}: "
                        f"{payload.get('message') or payload}"
                    )
                    return None

                return payload.get('data', {}).get('items', [])
            except (requests.RequestException, ValueError) as exc:
                logger.warning(
                    f"Community API request failed "
                    f"(attempt {attempt + 1}/{self.max_retries}): {exc}"
                )

        return None

    def fetch_article(self, message_id) -> Optional[Dict]:
        """Fetch one article's content and metadata.

        Returns a dict with ``body`` (full post HTML), ``teaser``, ``subject``,
        ``url`` (the site's canonical form), ``author`` and ``views``, or
        ``None`` if the article could not be read.
        """
        mid = self._require_numeric_id(message_id)
        items = self._liql(
            f"SELECT {_ARTICLE_FIELDS} FROM messages WHERE id = '{mid}'"
        )
        if not items:
            if items is not None:
                logger.warning(f"Community API returned no article for id {mid}")
            return None

        item = items[0]
        return {
            'id': item.get('id'),
            'subject': item.get('subject'),
            'body': item.get('body'),
            'teaser': item.get('teaser'),
            'url': item.get('view_href'),
            'post_time': item.get('post_time'),
            'author': (item.get('author') or {}).get('login'),
            'views': (item.get('metrics') or {}).get('views'),
        }

    def fetch_labels(self, message_id) -> Optional[List[str]]:
        """Fetch an article's labels — the blog's categories.

        Returns ``None`` when the lookup failed, and ``[]`` when the article
        genuinely carries no labels, so callers can tell "unknown" apart from
        "none" and avoid overwriting good data with a blank.
        """
        mid = self._require_numeric_id(message_id)
        items = self._liql(f"SELECT * FROM labels WHERE messages.id = '{mid}'")
        if items is None:
            return None

        names = []
        for item in items:
            name = item.get('text') or item.get('id')
            if name and name not in names:
                names.append(name)
        return names
