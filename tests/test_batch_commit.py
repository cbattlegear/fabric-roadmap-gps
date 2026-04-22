"""Tests for the batch-commit helper used by the vectorize/match pipelines.

Covers:
    * Happy path: ``mark_success`` commits exactly every N items.
    * Final partial batch flushed on context-manager exit.
    * Empty input does not commit.
    * A failed item (exception path, no ``mark_success``) does not advance
      the counter and does not get committed.
    * Mid-batch reconnect (transient retry) drops the in-flight pending
      counter and points subsequent commits at the new connection.
    * Exception in the ``with`` block triggers rollback, not flush.
    * ``get_batch_commit_size`` env-var resolution: default, override,
      empty, non-numeric, non-positive.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest

from lib.batch_commit import (
    BATCH_COMMIT_SIZE_ENV,
    DEFAULT_BATCH_COMMIT_SIZE,
    BatchCommitter,
    get_batch_commit_size,
)


class FakeConn:
    """Minimal pyodbc.Connection stand-in that records commit/rollback calls."""

    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.commit_should_raise = False

    def commit(self):
        if self.commit_should_raise:
            raise RuntimeError("boom")
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


# --- BatchCommitter ----------------------------------------------------------


def test_commits_every_n_items():
    conn = FakeConn()
    with BatchCommitter(conn, batch_size=3) as bc:
        for _ in range(7):
            bc.mark_success()
        assert conn.commits == 2
        assert bc.pending == 1
    assert conn.commits == 3
    assert bc.pending == 0
    assert bc.committed == 7


def test_final_partial_batch_flushed_on_exit():
    conn = FakeConn()
    with BatchCommitter(conn, batch_size=25) as bc:
        for _ in range(4):
            bc.mark_success()
        assert conn.commits == 0
    assert conn.commits == 1
    assert bc.committed == 4


def test_empty_batch_does_not_commit():
    conn = FakeConn()
    with BatchCommitter(conn, batch_size=25):
        pass
    assert conn.commits == 0
    assert conn.rollbacks == 0


def test_failed_item_is_skipped_and_not_committed():
    """Per-item failures don't call mark_success; counter must not advance."""
    conn = FakeConn()
    items = ["good", "bad", "good", "good"]
    with BatchCommitter(conn, batch_size=2) as bc:
        for item in items:
            try:
                if item == "bad":
                    raise ValueError("simulated per-item failure")
                bc.mark_success()
            except ValueError:
                continue
        assert conn.commits == 1
        assert bc.pending == 1
    assert conn.commits == 2
    assert bc.committed == 3


def test_exception_in_with_block_rolls_back_pending():
    conn = FakeConn()
    with pytest.raises(RuntimeError):
        with BatchCommitter(conn, batch_size=10) as bc:
            bc.mark_success()
            bc.mark_success()
            raise RuntimeError("kaboom")
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_exception_with_no_pending_still_safe():
    conn = FakeConn()
    with pytest.raises(ValueError):
        with BatchCommitter(conn, batch_size=10):
            raise ValueError("nope")
    assert conn.commits == 0
    assert conn.rollbacks == 0


def test_replace_connection_drops_in_flight_pending():
    """Mid-batch reconnect (transient retry) loses pending in-flight items."""
    old_conn = FakeConn()
    new_conn = FakeConn()
    with BatchCommitter(old_conn, batch_size=10) as bc:
        bc.mark_success()
        bc.mark_success()
        lost = bc.replace_connection(new_conn)
        assert lost == 2
        assert bc.pending == 0
        assert bc.connection is new_conn
        bc.mark_success()
    assert old_conn.commits == 0
    assert new_conn.commits == 1
    assert new_conn.rollbacks == 0


def test_replace_connection_on_clean_boundary_returns_zero():
    old_conn = FakeConn()
    new_conn = FakeConn()
    bc = BatchCommitter(old_conn, batch_size=5)
    assert bc.replace_connection(new_conn) == 0


def test_flush_is_idempotent_when_nothing_pending():
    conn = FakeConn()
    bc = BatchCommitter(conn, batch_size=5)
    bc.flush()
    bc.flush()
    assert conn.commits == 0


def test_rollback_pending_returns_count_and_clears_state():
    conn = FakeConn()
    bc = BatchCommitter(conn, batch_size=10)
    bc.mark_success()
    bc.mark_success()
    bc.mark_success()
    dropped = bc.rollback_pending()
    assert dropped == 3
    assert bc.pending == 0
    assert conn.rollbacks == 1


def test_rollback_pending_swallows_connection_errors():
    """A dead connection's rollback() must not propagate out of the helper."""
    conn = FakeConn()

    def boom():
        raise RuntimeError("connection already dead")

    conn.rollback = boom  # type: ignore[assignment]
    bc = BatchCommitter(conn, batch_size=10)
    bc.mark_success()
    bc.rollback_pending()
    assert bc.pending == 0


def test_invalid_batch_size_rejected():
    conn = FakeConn()
    with pytest.raises(ValueError):
        BatchCommitter(conn, batch_size=0)
    with pytest.raises(ValueError):
        BatchCommitter(conn, batch_size=-3)


def test_on_commit_callback_invoked_with_batch_size():
    conn = FakeConn()
    seen = []
    with BatchCommitter(conn, batch_size=2, on_commit=seen.append) as bc:
        bc.mark_success()
        bc.mark_success()  # commits 2
        bc.mark_success()  # 1 pending
    assert seen == [2, 1]


def test_on_commit_callback_failure_does_not_break_batch():
    conn = FakeConn()

    def bad_cb(_n):
        raise RuntimeError("callback exploded")

    with BatchCommitter(conn, batch_size=2, on_commit=bad_cb) as bc:
        bc.mark_success()
        bc.mark_success()  # triggers commit + callback
        bc.mark_success()
    assert conn.commits == 2
    assert bc.committed == 3


# --- get_batch_commit_size ---------------------------------------------------


def test_get_batch_commit_size_default_when_unset():
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop(BATCH_COMMIT_SIZE_ENV, None)
        assert get_batch_commit_size() == DEFAULT_BATCH_COMMIT_SIZE


def test_get_batch_commit_size_respects_env_override():
    with mock.patch.dict(os.environ, {BATCH_COMMIT_SIZE_ENV: "50"}):
        assert get_batch_commit_size() == 50


def test_get_batch_commit_size_strips_whitespace():
    with mock.patch.dict(os.environ, {BATCH_COMMIT_SIZE_ENV: "  10  "}):
        assert get_batch_commit_size() == 10


def test_get_batch_commit_size_falls_back_on_empty():
    with mock.patch.dict(os.environ, {BATCH_COMMIT_SIZE_ENV: ""}):
        assert get_batch_commit_size(default=7) == 7


def test_get_batch_commit_size_falls_back_on_non_numeric():
    with mock.patch.dict(os.environ, {BATCH_COMMIT_SIZE_ENV: "lots"}):
        assert get_batch_commit_size(default=12) == 12


def test_get_batch_commit_size_falls_back_on_non_positive():
    with mock.patch.dict(os.environ, {BATCH_COMMIT_SIZE_ENV: "0"}):
        assert get_batch_commit_size(default=9) == 9
    with mock.patch.dict(os.environ, {BATCH_COMMIT_SIZE_ENV: "-5"}):
        assert get_batch_commit_size(default=9) == 9


def test_get_batch_commit_size_custom_env_var_name():
    with mock.patch.dict(os.environ, {"MY_COMMIT_N": "11"}):
        assert get_batch_commit_size(env_var="MY_COMMIT_N") == 11
