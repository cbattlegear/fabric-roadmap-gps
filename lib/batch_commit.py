"""Batch-commit helper for the raw-pyodbc pipeline scripts.

The vectorize and match pipeline scripts used to open a fresh DB connection
and ``COMMIT`` per item, paying a TLS + auth round-trip on every row. With
100+ items per run that adds significant Azure SQL latency and pool churn.

This helper centralizes the "hold one connection, commit every N successful
items" pattern (issue #76 / M14) so both scripts share the same logic and
the logic itself is easy to unit-test without a live SQL Server.

Trade-off (documented for callers):
    A larger batch reduces round-trips but means a single transient connection
    failure mid-batch loses the *uncommitted* items in that window — they
    revert to ``WHERE x_vector IS NULL`` and get re-processed on the next
    pipeline run. The pipeline is idempotent, so this only delays work, it
    doesn't lose it. ``BATCH_COMMIT_SIZE`` defaults to 25 to keep that window
    small while still cutting commits ~25x.

Per-item retry (PR #67) is preserved: callers wrap each statement in a
``try/except`` that drops + reconnects on transient errors, then re-runs
the statement against the fresh connection. The ``BatchCommitter`` exposes
``replace_connection()`` for that path so the counter logic stays consistent
across reconnects.
"""

from __future__ import annotations

import logging
import os
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_BATCH_COMMIT_SIZE = 25
BATCH_COMMIT_SIZE_ENV = "BATCH_COMMIT_SIZE"


def get_batch_commit_size(
    default: int = DEFAULT_BATCH_COMMIT_SIZE,
    env_var: str = BATCH_COMMIT_SIZE_ENV,
) -> int:
    """Resolve the configured batch commit size.

    Reads ``BATCH_COMMIT_SIZE`` from the environment. Falls back to *default*
    when the env var is unset, empty, non-numeric, or non-positive.
    """
    raw = os.getenv(env_var)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        logger.warning(
            "Invalid %s=%r; using default %d", env_var, raw, default
        )
        return default
    if value <= 0:
        logger.warning(
            "Non-positive %s=%d; using default %d", env_var, value, default
        )
        return default
    return value


class BatchCommitter:
    """Commit a pyodbc connection every ``batch_size`` successful items.

    Usage::

        with BatchCommitter(conn, batch_size=25) as bc:
            for item in items:
                try:
                    cursor.execute("UPDATE ...", params)
                except Exception:
                    log_and_continue(item)  # failed item is NOT marked done
                    continue
                bc.mark_success()
            # __exit__ flushes any remaining pending items on success,
            # or rolls back the in-flight batch on exception.

    Notes:
        * ``mark_success()`` is called *after* the per-item statement returns
          without error. A raised exception means the item never landed in
          the transaction, so the counter is not advanced.
        * ``flush()`` is idempotent and a no-op when nothing is pending.
        * If a caller reconnects mid-batch (transient-error recovery), it
          should call ``replace_connection(new_conn)`` so subsequent commits
          target the live connection. The pending counter is reset because
          the in-flight transaction died with the old connection.
    """

    def __init__(
        self,
        conn,
        batch_size: int = DEFAULT_BATCH_COMMIT_SIZE,
        *,
        on_commit: Optional[Callable[[int], None]] = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        self._conn = conn
        self._batch_size = int(batch_size)
        self._on_commit = on_commit
        self.pending = 0
        self.committed = 0

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def connection(self):
        return self._conn

    def mark_success(self) -> None:
        """Record one successful item; commit when the batch fills up."""
        self.pending += 1
        if self.pending >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        """Commit any pending items. No-op when ``pending == 0``."""
        if self.pending == 0:
            return
        if self._conn is None:
            logger.warning(
                "BatchCommitter.flush() called with no connection; "
                "dropping %d pending item(s).",
                self.pending,
            )
            self.pending = 0
            return
        n = self.pending
        self._conn.commit()
        self.committed += n
        self.pending = 0
        if self._on_commit is not None:
            try:
                self._on_commit(n)
            except Exception:  # noqa: BLE001 — callback errors must not poison the batch
                logger.exception("BatchCommitter on_commit callback raised; ignoring.")

    def rollback_pending(self) -> int:
        """Roll back uncommitted work. Returns the number of items dropped."""
        dropped = self.pending
        if dropped == 0:
            return 0
        if self._conn is not None:
            try:
                self._conn.rollback()
            except Exception:  # noqa: BLE001 — best-effort, connection may already be dead
                logger.exception("BatchCommitter rollback raised; ignoring.")
        self.pending = 0
        return dropped

    def replace_connection(self, new_conn) -> int:
        """Swap to a fresh connection (e.g. after a transient-error reconnect).

        The pending counter is reset to 0 because the prior in-flight
        transaction died with the old connection. Returns the number of
        items that were lost from the in-flight batch (0 when the swap
        happened on a clean batch boundary).
        """
        lost = self.pending
        self._conn = new_conn
        self.pending = 0
        if lost:
            logger.warning(
                "BatchCommitter: connection replaced with %d uncommitted "
                "item(s) in flight; those rows will be re-processed on the "
                "next pipeline run.",
                lost,
            )
        return lost

    def __enter__(self) -> "BatchCommitter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self.flush()
        else:
            self.rollback_pending()
        return False
