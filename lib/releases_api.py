"""Pure helpers backing the ``/api/releases`` route.

These functions intentionally avoid Flask globals and database access so
they can be unit-tested in isolation. The route handler in ``server.py``
is responsible for orchestrating: parse → query DB → format → assemble.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib.parse import urlencode

from db.db_sqlserver import VALID_SORT_OPTIONS
from lib.quarter_date import format_as_quarter

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
MAX_MODIFIED_WITHIN_DAYS = 30
MIN_MODIFIED_WITHIN_DAYS = 1
DEFAULT_SORT = "last_modified"
RELEASES_PATH = "/api/releases"


@dataclass(frozen=True)
class ReleasesQuery:
    """Parsed and validated query parameters for ``/api/releases``."""

    page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    q: Optional[str] = None
    product_name: Optional[str] = None
    release_type: Optional[str] = None
    release_status: Optional[str] = None
    modified_within_days: Optional[int] = None
    include_inactive: bool = False
    sort: str = DEFAULT_SORT
    release_item_id: Optional[str] = None

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> Optional[str]:
    """Preserve the original ``request.args.get`` semantics: ``None`` if
    absent, otherwise the raw string (possibly empty)."""
    if value is None:
        return None
    return str(value)


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    return str(value).lower() in ("1", "true", "yes")


def _parse_releases_query(args: Mapping[str, Any]) -> ReleasesQuery:
    """Parse a Flask ``request.args``-like mapping into a ``ReleasesQuery``.

    Performs the same clamping/defaulting the inline route used to do:
      * ``page`` defaults to 1, clamped to ``>= 1``
      * ``page_size`` defaults to 50, clamped to ``[1, 200]``
      * ``sort`` falls back to ``"last_modified"`` if not in
        ``VALID_SORT_OPTIONS``
      * ``modified_within_days`` clamped to ``[1, 30]`` when present
      * ``include_inactive`` accepts ``1``/``true``/``yes``
    """
    page = _coerce_int(args.get("page")) or 1
    if page < 1:
        page = 1

    page_size = _coerce_int(args.get("page_size")) or DEFAULT_PAGE_SIZE
    if page_size < 1:
        page_size = 1
    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    sort = args.get("sort")
    if sort not in VALID_SORT_OPTIONS:
        sort = DEFAULT_SORT

    modified_within_days = _coerce_int(args.get("modified_within_days"))
    if modified_within_days is not None:
        modified_within_days = max(
            MIN_MODIFIED_WITHIN_DAYS,
            min(modified_within_days, MAX_MODIFIED_WITHIN_DAYS),
        )

    return ReleasesQuery(
        page=page,
        page_size=page_size,
        q=_coerce_str(args.get("q")),
        product_name=_coerce_str(args.get("product_name")),
        release_type=_coerce_str(args.get("release_type")),
        release_status=_coerce_str(args.get("release_status")),
        modified_within_days=modified_within_days,
        include_inactive=_truthy(args.get("include_inactive")),
        sort=sort,
        release_item_id=_coerce_str(args.get("release_item_id")),
    )


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if isoformat is None:
        return None
    return isoformat()


def _get(row: Any, key: str) -> Any:
    """Read ``key`` from either a mapping-like row or an ORM model."""
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _format_release_row(row: Any, *, distance: Optional[float] = None) -> dict:
    """Serialize one release row into the public JSON shape.

    Accepts either a ``ReleaseItemModel`` instance (regular search) or a
    mapping/dict-like row (vector search). When ``distance`` is provided
    (or present on the row), it is rounded to 4 decimal places to match
    the prior ``_format_vector_row`` behavior.
    """
    if distance is None:
        distance = _get(row, "distance")

    formatted = {
        "release_item_id": _get(row, "release_item_id"),
        "feature_name": _get(row, "feature_name"),
        "release_date": format_as_quarter(_get(row, "release_date")),
        "release_type": _get(row, "release_type"),
        "release_status": _get(row, "release_status"),
        "product_id": _get(row, "product_id"),
        "product_name": _get(row, "product_name"),
        "feature_description": _get(row, "feature_description"),
        "blog_title": _get(row, "blog_title"),
        "blog_url": _get(row, "blog_url"),
        "last_modified": _iso_or_none(_get(row, "last_modified")),
        "active": _get(row, "active"),
    }
    if distance is not None:
        formatted["distance"] = round(distance, 4)
    return formatted


def _build_pagination_links(query: ReleasesQuery, total: int) -> dict:
    """Build HATEOAS-style ``self/first/last/next/prev`` links.

    Mirrors the original inline logic: omits filter params that are
    ``None``/empty, suppresses ``sort`` when it equals the default, and
    always includes ``page_size``.
    """
    page_size = query.page_size
    total_pages = (total + page_size - 1) // page_size if total else 1

    base_params = {
        k: v
        for k, v in {
            "product_name": query.product_name,
            "release_type": query.release_type,
            "release_status": query.release_status,
            "modified_within_days": query.modified_within_days,
            "q": query.q,
            "page_size": page_size,
            "sort": query.sort if query.sort != DEFAULT_SORT else None,
        }.items()
        if v not in (None, "")
    }

    def _link(p: int) -> str:
        bp = dict(base_params)
        bp["page"] = p
        return f"{RELEASES_PATH}?{urlencode(bp)}"

    page = query.page
    return {
        "self": _link(page),
        "first": _link(1),
        "last": _link(total_pages),
        "next": _link(page + 1) if page < total_pages else None,
        "prev": _link(page - 1) if page > 1 else None,
    }


def _build_pagination_meta(query: ReleasesQuery, total: int) -> dict:
    """Build the ``pagination`` block of the response envelope."""
    page = query.page
    page_size = query.page_size
    total_pages = (total + page_size - 1) // page_size if total else 1
    return {
        "page": page,
        "page_size": page_size,
        "total_items": total,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
        "next_page": page + 1 if page < total_pages else None,
        "prev_page": page - 1 if page > 1 else None,
    }


__all__ = [
    "ReleasesQuery",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "DEFAULT_SORT",
    "_parse_releases_query",
    "_format_release_row",
    "_build_pagination_links",
    "_build_pagination_meta",
]
