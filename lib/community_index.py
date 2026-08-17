"""Resolve legacy blog URLs to their community equivalents by title.

The Fabric Updates Blog moved to the community site, and the old domain is
supposed to redirect. In practice it doesn't: every legacy URL tried from a
normal client returns HTTP 403, the same bot protection that makes the
community's own article pages unscrapable. A migration that depends on
following those redirects resolves nothing at all.

The community's public API has no such problem. Both URL forms are derived
from the article's title, so enumerating the board once and matching on a
normalized title slug recovers the mapping without touching the dead domain.
Measured against the production table: 1031 of 1035 legacy rows resolved.

Matching deliberately refuses to guess. Two articles whose titles normalize to
the same slug are recorded as ambiguous and resolve to ``None``, because
pointing a release at the wrong post is worse than leaving it on a URL that at
least still identifies the right article.
"""

import logging
import re
import unicodedata
from typing import Dict, List, Optional, Set
from urllib.parse import unquote, urlsplit

logger = logging.getLogger(__name__)

# Path segments that carry no article identity; the slug is the last segment
# that isn't one of these.
_NON_SLUG_SEGMENTS = {'blog', 'ba-p', 'td-p', 'm-p'}

_LOCALE_RE = re.compile(r'^[a-z]{2}(-[a-z]{2})?$')


def normalize_slug(text: Optional[str]) -> str:
    """Reduce a title or URL segment to a comparable slug.

    Percent-escapes are decoded first so a URL that spells a non-breaking
    hyphen as ``%E2%80%91`` collapses to the same separator the title uses,
    then anything that isn't ASCII alphanumeric becomes a single hyphen.
    """
    if not text:
        return ''
    decoded = unquote(text)
    folded = unicodedata.normalize('NFKD', decoded).lower()
    return re.sub(r'-+', '-', re.sub(r'[^a-z0-9]+', '-', folded)).strip('-')


def slug_from_url(url: Optional[str]) -> str:
    """Extract the article slug from a legacy or community blog URL."""
    if not url:
        return ''
    path = urlsplit(url).path
    segments = [seg for seg in path.split('/') if seg]
    while segments:
        candidate = segments[-1]
        lowered = candidate.lower()
        # Community permalinks end in `/ba-p/<id>`; step back past the id and
        # the marker to reach the title-derived segment.
        if (
            lowered in _NON_SLUG_SEGMENTS
            or candidate.isdigit()
            or _LOCALE_RE.match(lowered)
        ):
            segments.pop()
            continue
        return normalize_slug(candidate)
    return ''


class CommunityBlogIndex:
    """Lazily-built title index over the community blog board."""

    def __init__(self, client, board_id: Optional[str] = None):
        self.client = client
        self.board_id = board_id
        self._by_slug: Optional[Dict[str, Dict]] = None
        self._ambiguous: Set[str] = set()

    @property
    def available(self) -> bool:
        """True when the board index was built and holds at least one post."""
        return bool(self._ensure_index())

    def _ensure_index(self) -> Dict[str, Dict]:
        if self._by_slug is not None:
            return self._by_slug

        messages = self.client.fetch_board_messages(self.board_id)
        if messages is None:
            logger.warning(
                "Could not enumerate the community board; falling back to "
                "redirect resolution for every row"
            )
            self._by_slug = {}
            return self._by_slug

        index: Dict[str, Dict] = {}
        for message in messages:
            slug = normalize_slug(message.get('subject'))
            if not slug or not message.get('url'):
                continue
            existing = index.get(slug)
            if existing is not None:
                if existing.get('id') != message.get('id'):
                    self._ambiguous.add(slug)
                continue
            index[slug] = message

        for slug in self._ambiguous:
            index.pop(slug, None)

        logger.info(
            f"Community board index: {len(index)} unique title(s) from "
            f"{len(messages)} post(s)"
            + (f", {len(self._ambiguous)} ambiguous" if self._ambiguous else "")
        )
        self._by_slug = index
        return self._by_slug

    def resolve(
        self,
        url: Optional[str] = None,
        title: Optional[str] = None,
    ) -> Optional[str]:
        """Return the community URL for a legacy ``url`` or stored ``title``.

        The URL slug is tried first because it comes from the article's
        original title; the stored title is a fallback for rows whose URL slug
        was truncated or rewritten.
        """
        index = self._ensure_index()
        if not index:
            return None

        for candidate in (slug_from_url(url), normalize_slug(title)):
            if not candidate:
                continue
            if candidate in self._ambiguous:
                logger.warning(
                    f"Refusing to resolve ambiguous title slug {candidate!r} — "
                    f"more than one community post shares it"
                )
                continue
            match = index.get(candidate)
            if match:
                return match['url']
        return None

    def candidate_slugs(self, url: Optional[str], title: Optional[str]) -> List[str]:
        """Slugs that would be tried for this row (used for diagnostics)."""
        return [s for s in (slug_from_url(url), normalize_slug(title)) if s]
