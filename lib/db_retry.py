"""Retry helpers for transient Azure SQL / network errors.

Both the SQLAlchemy paths in ``db.db_sqlserver`` and the raw-pyodbc pipeline
scripts (vectorize, match, scrape) need the same backoff behavior when Azure
SQL throttles, fails over, or drops a connection. This module centralizes that
logic so a single tweak in detection/timing applies everywhere.

Usage::

    from lib.db_retry import retry_on_transient_errors

    @retry_on_transient_errors(max_attempts=3, initial_delay=0.5)
    def update_blog_vector(post_id, embedding):
        ...
"""

import logging
import random
import time

# SQLAlchemy is optional for this module — pipeline scripts that use only
# raw pyodbc don't need it. Guard the import so importing ``lib.db_retry``
# in a SQLAlchemy-less context doesn't fail.
try:
    from sqlalchemy.exc import DBAPIError as _DBAPIError
except Exception:  # pragma: no cover — sqlalchemy missing
    _DBAPIError = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


# Known SQL Azure transient error codes & phrases. Conservative on purpose —
# anything in this set will trigger a retry, so non-transient phrases must
# stay out (e.g. plain "syntax error", "permission denied", etc.).
_TRANSIENT_CODES = frozenset({
    "40197", "40501", "40613", "10928", "10929", "49918", "49919", "49920", "4060",
    "10053", "10054", "10060",
    "1205",  # deadlock — frequently transient
})

_TRANSIENT_PHRASES = (
    "transport-level error",
    "a transport-level error has occurred",
    "the connection is broken",
    "connection reset by peer",
    "connection was closed by the server",
    "the server was not found or was not accessible",
    "cannot open database requested by the login",
    "login failed",
    "connection timeout",
    "timed out",
    "timeout expired",
    "operation timed out",
    "could not open connection",
    "server is busy",
    "service is currently paused",
    "service is busy",
    "deadlocked",
)


def is_transient_sql_azure_error(exc: Exception) -> bool:
    """Best-effort detection of common transient Azure SQL / network errors.

    Inspects the exception message and any DBAPI ``orig`` attribute for known
    transient SQL codes and phrases. Conservative — when in doubt, returns
    False so non-transient bugs aren't silently retried.
    """
    if exc is None:
        return False

    msg = str(exc).lower()

    for code in _TRANSIENT_CODES:
        if code in msg:
            return True

    for phrase in _TRANSIENT_PHRASES:
        if phrase in msg:
            return True

    # If SQLAlchemy is available, peek at DBAPIError.orig for the underlying
    # pyodbc / pymssql exception text, and honor connection_invalidated.
    if _DBAPIError is not None:
        try:
            if isinstance(exc, _DBAPIError):
                orig = getattr(exc, "orig", None)
                if orig is not None and any(code in str(orig) for code in _TRANSIENT_CODES):
                    return True
                if getattr(exc, "connection_invalidated", False):
                    return True
        except Exception:  # noqa: BLE001 — best-effort detection
            pass

    return False


# Backwards-compatible alias used by db.db_sqlserver before the extraction.
_is_transient_sql_azure_error = is_transient_sql_azure_error


def retry_on_transient_errors(
    max_attempts: int = 5,
    initial_delay: float = 1.0,
    backoff: float = 2.0,
    max_delay: float = 30.0,
):
    """Decorator that retries a callable on transient Azure SQL errors.

    Uses exponential backoff with up-to-0.5s jitter. Non-transient exceptions
    re-raise immediately (no silent swallowing).

    Args:
        max_attempts: Total attempts including the first call.
        initial_delay: Seconds to wait before the second attempt.
        backoff: Multiplier applied to the delay after each retry.
        max_delay: Upper bound on the per-retry delay.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 1
            delay = float(initial_delay)
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 — classification handled below
                    if not is_transient_sql_azure_error(exc):
                        logger.exception("Non-transient DB error encountered, not retrying.")
                        raise
                    if attempt >= max_attempts:
                        logger.exception("Transient DB error and max attempts reached (%d).", max_attempts)
                        raise
                    logger.warning(
                        "Transient DB error detected; attempt %d/%d will retry after %.2fs: %s",
                        attempt, max_attempts, delay, exc,
                    )
                    sleep_time = delay + (random.random() * 0.5)
                    time.sleep(sleep_time)
                    delay = min(delay * backoff, max_delay)
                    attempt += 1

        wrapper.__name__ = getattr(func, "__name__", "wrapped")
        wrapper.__doc__ = getattr(func, "__doc__", "")
        wrapper.__wrapped__ = func  # type: ignore[attr-defined]
        return wrapper

    return decorator
