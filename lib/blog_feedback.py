"""Bounded, canonical identities for anonymous release/blog feedback."""

import re
from urllib.parse import urlsplit, urlunsplit

from lib.community_urls import extract_message_id

VOTES = frozenset({"helpful", "not_related"})
VOTER_KEY_RE = re.compile(r"[0-9a-f]{64}")
FEEDBACK_RETENTION_DAYS = 180


def blog_identity(url):
    """Use community message IDs, or the canonical displayed URL elsewhere."""
    if not isinstance(url, str) or not url or len(url) > 1000:
        return None
    if "\\" in url or any(char.isspace() or ord(char) < 32 for char in url):
        return None
    try:
        url.encode("utf-8")
        parts = urlsplit(url)
        if parts.scheme not in {"http", "https"} or not parts.hostname:
            return None
        if parts.username is not None or parts.password is not None:
            return None
        port = parts.port
    except (ValueError, UnicodeError):
        return None
    host = parts.hostname.lower()
    if host == "community.fabric.microsoft.com" and port in (None, 443):
        message_id = extract_message_id(parts.path)
        if message_id and parts.path.startswith(("/t5/", "/blog/")):
            return f"community:{message_id}"
    # Fragments do not change the article; query strings can.
    authority = f"[{host}]" if ":" in host else host
    if port is not None and (parts.scheme, port) not in {("https", 443), ("http", 80)}:
        authority += f":{port}"
    return urlunsplit((parts.scheme, authority, parts.path or "/", parts.query, ""))


def valid_submission(release_id, blog_url, vote, voter_key):
    return (
        isinstance(release_id, str)
        and re.fullmatch(r"[A-Za-z0-9-]{1,36}", release_id) is not None
        and isinstance(vote, str)
        and vote in VOTES
        and isinstance(voter_key, str)
        and VOTER_KEY_RE.fullmatch(voter_key) is not None
        and blog_identity(blog_url) is not None
    )
