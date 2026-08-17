"""Helpers for Microsoft Fabric Community blog URLs.

The community site serves the same post under two URL forms:

    RSS permalink : https://community.fabric.microsoft.com/t5/Fabric-Updates-Blog/<slug>/ba-p/5359124
    canonical page: https://community.fabric.microsoft.com/blog/fbc_fabricupdatesblogs/<slug>/5359124

Both end with the numeric message id, which is the stable identity of the
post. The scraper stores the permalink it gets from the feed, while rows
rewritten by the URL migration hold whichever form the legacy redirect landed
on, so anything that needs to decide "do we already have this post?" must
compare ids rather than URL strings.
"""

import re
from typing import Optional

# Matches the trailing numeric id of either community URL form.
_MESSAGE_ID_RE = re.compile(r'/(\d+)/?$')


def extract_message_id(url: Optional[str]) -> Optional[str]:
    """Return the community message id embedded in ``url``, if any.

    Legacy ``blog.fabric.microsoft.com`` URLs end with a slug rather than an
    id, so they yield ``None`` and callers fall back to exact-URL matching.
    """
    if not url:
        return None
    match = _MESSAGE_ID_RE.search(url)
    return match.group(1) if match else None


def message_id_like_pattern(message_id: str) -> str:
    """SQL ``LIKE`` pattern matching any community URL with this message id."""
    return f"%/{message_id}"
