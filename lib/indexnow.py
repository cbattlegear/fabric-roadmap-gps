"""IndexNow protocol integration for notifying search engines of URL changes."""
import logging
import os
from typing import List
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

INDEXNOW_ENDPOINT = "https://api.indexnow.org/indexnow"
MAX_URLS_PER_REQUEST = 10_000


def submit_urls(changed_release_ids: List[str]) -> bool:
    """Submit changed release URLs to IndexNow.

    Reads INDEXNOW_API_KEY and BASE_URL / CANONICAL_HOST from env vars.
    Returns True if submission succeeded (or was skipped), False on error.
    """
    api_key = os.getenv("INDEXNOW_API_KEY", "")
    if not api_key:
        logger.info("INDEXNOW_API_KEY not set — skipping IndexNow submission")
        return True

    canonical_host = os.getenv("CANONICAL_HOST", "")
    base_url = f"https://{canonical_host}" if canonical_host else os.getenv("BASE_URL", "")
    if not base_url:
        logger.warning("Neither CANONICAL_HOST nor BASE_URL is set — skipping IndexNow")
        return True

    host = urlparse(base_url).hostname
    key_location = f"{base_url}/{api_key}.txt"

    # Always include the homepage and changelog since they reflect aggregate changes
    url_list = [base_url + "/", base_url + "/changelog"]
    for rid in changed_release_ids[:MAX_URLS_PER_REQUEST - 2]:
        url_list.append(f"{base_url}/release/{rid}")

    payload = {
        "host": host,
        "key": api_key,
        "keyLocation": key_location,
        "urlList": url_list,
    }

    try:
        resp = requests.post(INDEXNOW_ENDPOINT, json=payload, timeout=30)
        if resp.status_code in (200, 202):
            logger.info("IndexNow accepted %d URLs (status %d)", len(url_list), resp.status_code)
            return True
        else:
            logger.warning("IndexNow returned status %d: %s", resp.status_code, resp.text[:500])
            return False
    except requests.RequestException as exc:
        logger.error("IndexNow request failed: %s", exc)
        return False
