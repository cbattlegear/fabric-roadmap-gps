"""Helpers for the Microsoft Fabric roadmap ``ReleaseDate`` field.

The upstream Fabric roadmap JSON used to publish ``ReleaseDate`` as a
calendar date (e.g. ``"10/01/2024"`` or ``"2024-10-01"``).  As of
mid-2026 it now publishes a quarter token instead, e.g. ``"Q4 2024"``.

This module centralizes the two operations that need to understand the
new format:

* :func:`parse_release_date_value` - convert any supported source value
  (legacy date string, ``date``/``datetime``, or ``"Q# YYYY"``) into a
  ``date`` so the existing ``release_date Date`` column, sort order, and
  history-diff stored procedures continue to work. Quarter tokens map to
  the *last day* of the quarter so "shipped by end of quarter" semantics
  are preserved.

* :func:`quarter_bounds` - if the supplied value is a quarter token,
  return its inclusive ``(start_date, end_date)`` so callers can decide
  whether an existing row's date already falls inside the quarter and
  should be kept as-is (avoids churning ``row_hash`` /
  ``last_modified``).

* :func:`format_as_quarter` - render a stored ``date``/``datetime``,
  ISO/legacy date string, or ``"Q# YYYY"`` token as ``"Q# YYYY"`` for
  display in the UI, RSS feed, API JSON, and email templates.
"""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime
from typing import Optional, Tuple, Union

# Accepts "Q1 2024", "q4 2025", "Q2-2026" (any whitespace/dash separator).
_QUARTER_RE = re.compile(r"^\s*[Qq]\s*([1-4])\s*[-/ ]?\s*(\d{4})\s*$")

# Legacy date formats we used to see from the Fabric roadmap source.
_LEGACY_DATE_FORMATS = ("%m/%d/%Y", "%Y-%m-%d")

DateLike = Union[date, datetime, str, None]


def _parse_quarter_token(value: object) -> Optional[Tuple[int, int]]:
    """Return ``(quarter, year)`` if ``value`` looks like ``"Q# YYYY"``."""
    if not isinstance(value, str):
        return None
    m = _QUARTER_RE.match(value)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def quarter_bounds(value: object) -> Optional[Tuple[date, date]]:
    """Return the inclusive ``(start, end)`` dates for a quarter token.

    Returns ``None`` for any value that is not a quarter token.
    """
    parsed = _parse_quarter_token(value)
    if not parsed:
        return None
    quarter, year = parsed
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    last_day = calendar.monthrange(year, end_month)[1]
    return date(year, start_month, 1), date(year, end_month, last_day)


def quarter_end(value: object) -> Optional[date]:
    """Return the last day of the quarter token, or ``None`` if not a token."""
    bounds = quarter_bounds(value)
    return bounds[1] if bounds else None


def parse_release_date_value(value: DateLike) -> Optional[date]:
    """Parse any supported ``ReleaseDate`` representation into a ``date``.

    Order of attempts:
      1. Already a ``date``/``datetime`` -> use directly.
      2. ``"Q# YYYY"`` quarter token -> last day of the quarter.
      3. Legacy date formats (``%m/%d/%Y``, ``%Y-%m-%d``).
      4. ``datetime.fromisoformat`` (handles full ISO timestamps).

    Returns ``None`` for empty / unparseable input.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    s = str(value).strip()
    if not s:
        return None

    end = quarter_end(s)
    if end is not None:
        return end

    for fmt in _LEGACY_DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        return None


def date_to_quarter_string(d: date) -> str:
    """Format a ``date`` as ``"Q# YYYY"``."""
    quarter = (d.month - 1) // 3 + 1
    return f"Q{quarter} {d.year}"


def format_as_quarter(value: DateLike) -> Optional[str]:
    """Render any supported ``ReleaseDate`` value as ``"Q# YYYY"``.

    - Already a quarter token -> returned normalized (``"Q4 2024"``).
    - ``date``/``datetime`` -> derived from month/year.
    - Legacy/ISO date string -> parsed then derived.
    - Empty/unparseable -> ``None`` so callers can substitute "TBD".
    """
    if value is None or value == "":
        return None

    parsed_token = _parse_quarter_token(value)
    if parsed_token:
        quarter, year = parsed_token
        return f"Q{quarter} {year}"

    parsed = parse_release_date_value(value)
    if parsed is None:
        return None
    return date_to_quarter_string(parsed)
