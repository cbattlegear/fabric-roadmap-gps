"""Digest content building: filtering, cache keys, and per-subscriber content.

Extracted from ``weekly_email_job.py``. The pure helpers
(:func:`build_cache_key`, :func:`filter_by_cadence`,
:func:`apply_subscription_filters`, :func:`select_top_changes`) are
trivially unit-testable. The :class:`DigestContentBuilder` class wires
those helpers to the network (``/api/releases``), Azure OpenAI, and the
DB-backed content cache, but takes its dependencies via constructor
arguments so tests can substitute mocks.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import requests
import sqlalchemy.exc

logger = logging.getLogger(__name__)

try:
    from openai import AzureOpenAI
    _OPENAI_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only when SDK absent
    _OPENAI_AVAILABLE = False


MAX_DIGEST_ITEMS = 50
PAGINATION_PAGE_LIMIT = 20


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------
def build_cache_key(subscription: Any) -> str:
    """SHA-256 cache key from cadence + filter combo.

    Using the full hash (not a truncation) avoids collisions when
    subscribers have long filter lists.
    """
    parts = [
        subscription.email_cadence or 'weekly',
        (subscription.product_filter or '').strip().lower(),
        (subscription.release_type_filter or '').strip().lower(),
        (subscription.release_status_filter or '').strip().lower(),
    ]
    raw = '|'.join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def filter_by_cadence(
    items: List[Dict[str, Any]],
    cadence: Optional[str],
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Filter raw changes to those within the cadence window.

    - ``cadence == 'daily'``: keep items whose ``last_modified`` is on or
      after ``(now - 1 day).date()``. ``/api/releases`` exposes
      ``last_modified`` as a date-only string (``YYYY-MM-DD``), so the
      comparison is a date-based one rather than a full timestamp.
    - Anything else (notably ``'weekly'`` or ``None``): pass through
      unchanged. The 7-day window is already applied upstream by the
      ``/api/releases`` query in ``DigestContentBuilder.fetch_raw_changes``.

    Items with missing or malformed ``last_modified`` are dropped from
    the daily path (and a warning is logged for the malformed case) so
    a single bad row can't crash the whole digest run. Both ``ValueError``
    (bad date format) and ``TypeError`` (non-string value, e.g. an int
    slipping through a future schema change) are handled.

    ``now`` is injectable for deterministic tests; defaults to
    ``datetime.utcnow()``.
    """
    if cadence != 'daily':
        return list(items)

    reference = now if now is not None else datetime.utcnow()
    cutoff_date = (reference - timedelta(days=1)).date()

    filtered: List[Dict[str, Any]] = []
    for c in items:
        last_modified = c.get('last_modified')
        if not last_modified:
            continue
        try:
            last_modified_date = datetime.strptime(last_modified, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            logger.warning("Skipping item with invalid last_modified value: %s", last_modified)
            continue
        if last_modified_date >= cutoff_date:
            filtered.append(c)
    return filtered


def _split_csv_lower(value: Optional[str]) -> set:
    if not value:
        return set()
    return {p.strip().lower() for p in value.split(',') if p.strip()}


def _split_csv(value: Optional[str]) -> set:
    if not value:
        return set()
    return {p.strip() for p in value.split(',') if p.strip()}


def apply_subscription_filters(
    items: List[Dict[str, Any]],
    subscription: Any,
) -> List[Dict[str, Any]]:
    """Apply product / release-type / release-status filters in order."""
    products = _split_csv_lower(subscription.product_filter)
    if products:
        items = [c for c in items if (c.get('product_name') or '').lower() in products]

    types = _split_csv(subscription.release_type_filter)
    if types:
        items = [c for c in items if c.get('release_type') in types]

    statuses = _split_csv(subscription.release_status_filter)
    if statuses:
        items = [c for c in items if c.get('release_status') in statuses]

    return items


def select_top_changes(
    items: List[Dict[str, Any]],
    max_count: int = MAX_DIGEST_ITEMS,
) -> List[Dict[str, Any]]:
    """Sort by ``last_modified`` desc and truncate to ``max_count``."""
    ordered = sorted(items, key=lambda x: x.get('last_modified') or '', reverse=True)
    return ordered[:max_count]


# ---------------------------------------------------------------------------
# AI summary
# ---------------------------------------------------------------------------
def generate_ai_summary(
    changes: List[Dict[str, Any]],
    *,
    client_factory: Optional[Callable[[], Any]] = None,
) -> Optional[str]:
    """Generate a single AI summary of the changes via Azure OpenAI.

    Returns the summary text or ``None`` if unavailable (no SDK, no
    config, empty input, or any error during the call). ``client_factory``
    is injectable for tests.
    """
    if not changes:
        return None
    if not _OPENAI_AVAILABLE:
        logger.info("OpenAI SDK not installed, skipping AI summary")
        return None
    endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
    api_key = os.getenv('AZURE_OPENAI_API_KEY')
    deployment = os.getenv('AZURE_OPENAI_CHAT_DEPLOYMENT', 'gpt-4o-mini')
    if not endpoint or not api_key:
        logger.info("Azure OpenAI not configured, skipping AI summary")
        return None
    try:
        if client_factory is not None:
            client = client_factory()
        else:
            client = AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version="2024-02-01",
            )

        change_lines = []
        for c in changes[:MAX_DIGEST_ITEMS]:
            removed_tag = " [REMOVED FROM ROADMAP]" if c.get('active') is False else ""
            line = (
                f"- {c.get('feature_name', 'Unknown')} "
                f"[{c.get('product_name', '')}] "
                f"({c.get('release_type', '')}, {c.get('release_status', '')})"
                f"{removed_tag}"
            )
            change_lines.append(line)
        changes_text = "\n".join(change_lines)

        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You summarize Microsoft Fabric roadmap changes for a weekly email newsletter. "
                        "Write a concise 2-4 sentence executive summary highlighting the most important "
                        "themes and notable changes. Items tagged [REMOVED FROM ROADMAP] have been taken "
                        "off the public roadmap - mention significant removals if any. "
                        "Be specific about product areas and feature names. "
                        "Do not use markdown formatting. Write in a professional but approachable tone."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Summarize these {len(changes)} Microsoft Fabric roadmap changes from the past week:\n\n{changes_text}",
                },
            ],
            max_completion_tokens=300,
        )
        summary = response.choices[0].message.content.strip()
        logger.info("AI summary generated (%d chars)", len(summary))
        return summary
    except Exception as e:  # pragma: no cover - defensive
        logger.error("AI summary generation failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Content builder (network + cache)
# ---------------------------------------------------------------------------
class DigestContentBuilder:
    """Per-run helper that fetches raw changes, filters per subscriber, and caches the payload.

    The builder is constructed once per job run and reused across all
    subscribers: the raw 7-day fetch happens once and is cached
    in-memory; per-subscriber filtering + AI summary results are cached
    in the DB by ``(date, cache_key)`` so every subscriber with
    identical settings gets the exact same email.

    Constructor dependencies are injectable to keep tests free of live
    network / SQL.
    """

    def __init__(
        self,
        engine: Any,
        base_url: str,
        *,
        session_scope: Callable[[Any], Any],
        cache_model: Any,
        ai_summary_fn: Callable[[List[Dict[str, Any]]], Optional[str]] = generate_ai_summary,
        http_get: Callable[..., Any] = requests.get,
        clock: Callable[[], datetime] = datetime.utcnow,
    ) -> None:
        self._engine = engine
        self._base_url = base_url
        self._session_scope = session_scope
        self._cache_model = cache_model
        self._ai_summary_fn = ai_summary_fn
        self._http_get = http_get
        self._clock = clock
        self._raw_changes: Optional[List[Dict[str, Any]]] = None

    # ---- network ---------------------------------------------------------
    @staticmethod
    def _extract_items(payload: Any) -> List[Dict[str, Any]]:
        """Support both new spec (envelope with 'data') and legacy raw list."""
        if isinstance(payload, dict) and 'data' in payload and isinstance(payload['data'], list):
            return payload['data']
        if isinstance(payload, list):
            return payload
        return []

    def _fetch_all_pages(self, base_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        page = 1
        all_items: List[Dict[str, Any]] = []
        while True:
            params = dict(base_params)
            params['page'] = page
            resp = self._http_get(f"{self._base_url}/api/releases", params=params, timeout=30)
            if resp.status_code != 200:
                logger.error("API page request failed (status %s) params=%s", resp.status_code, params)
                break
            payload = resp.json()
            items = self._extract_items(payload)
            if not items:
                break
            all_items.extend(items)
            pagination = payload.get('pagination') if isinstance(payload, dict) else None
            if not pagination or not pagination.get('has_next'):
                break
            page += 1
            if page > PAGINATION_PAGE_LIMIT:
                logger.warning("Pagination exceeded %d pages; stopping early.", PAGINATION_PAGE_LIMIT)
                break
        return all_items

    def fetch_raw_changes(self) -> List[Dict[str, Any]]:
        """Return all 7-day changes, cached in-memory for this run."""
        if self._raw_changes is None:
            base_params = {'modified_within_days': 7, 'page_size': 200, 'include_inactive': 'true'}
            items = self._fetch_all_pages(base_params)
            seen: Dict[str, Dict[str, Any]] = {}
            for item in items:
                rid = item.get('release_item_id')
                if rid and rid not in seen:
                    seen[rid] = item
            self._raw_changes = list(seen.values())
            logger.info("Fetched %d raw changes from API", len(self._raw_changes))
        return self._raw_changes

    # ---- per-subscriber content -----------------------------------------
    def get_for(self, subscription: Any) -> tuple:
        """Return ``(changes, ai_summary)`` for a subscriber, using DB cache."""
        from sqlalchemy import select as sa_select

        cache_key = build_cache_key(subscription)
        cache_date = self._clock().strftime('%Y-%m-%d')

        try:
            with self._session_scope(self._engine) as session:
                row = session.scalar(
                    sa_select(self._cache_model)
                    .where(self._cache_model.cache_date == cache_date)
                    .where(self._cache_model.cache_key == cache_key)
                )
                if row:
                    cached = json.loads(row.content_json)
                    logger.info("Cache hit for key=%s date=%s", cache_key[:40], cache_date)
                    return cached['changes'], cached.get('ai_summary')
        except Exception as e:
            logger.warning("Cache read failed for key=%s: %s", cache_key[:40], e)

        items = list(self.fetch_raw_changes())
        items = filter_by_cadence(items, subscription.email_cadence, now=self._clock())
        items = apply_subscription_filters(items, subscription)
        items = select_top_changes(items)

        ai_summary = self._ai_summary_fn(items) if items else None

        try:
            content = json.dumps(
                {'changes': items, 'ai_summary': ai_summary},
                separators=(',', ':'),
            )
            with self._session_scope(self._engine) as session:
                with session.begin():
                    session.add(self._cache_model(
                        cache_date=cache_date,
                        cache_key=cache_key,
                        generated_at=self._clock(),
                        content_json=content,
                    ))
            logger.info("Cached content for key=%s (%d items)", cache_key[:40], len(items))
        except sqlalchemy.exc.IntegrityError:
            logger.info(
                "Cache key=%s already exists (concurrent write), using generated content",
                cache_key[:40],
            )
        except Exception as e:
            logger.error("Cache write failed for key=%s: %s", cache_key[:40], e)

        return items, ai_summary
