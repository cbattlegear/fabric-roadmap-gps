"""Pipeline-level regression tests for issue #76 (batch commits).

These assertions would have failed against ``main`` before the refactor:
    * ``BlogVectorizer.update_post_vector`` used to ``conn.commit()`` itself
      and use a brand-new connection per call. After the fix it must reuse
      ``self.conn`` and leave commit timing to the BatchCommitter.
    * ``vectorize_all_posts`` over N items must produce exactly ⌈N/B⌉
      commits, not N commits.
    * A single per-item failure must not abort the batch.
"""

from __future__ import annotations

import sys
import types
from unittest import mock

import pytest

# Stub out optional Azure SDKs the module imports at module load time so
# the test environment doesn't need them. Only inject if missing.
for missing in ("openai",):
    if missing not in sys.modules:
        stub = types.ModuleType(missing)
        stub.AzureOpenAI = lambda **kwargs: None  # type: ignore[attr-defined]
        sys.modules[missing] = stub


@pytest.fixture
def env_setup(monkeypatch):
    monkeypatch.setenv("SQLSERVER_CONN", "DRIVER=stub")
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://stub")
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "stub")
    monkeypatch.delenv("BATCH_COMMIT_SIZE", raising=False)
    yield


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.executed = []
        self.fetch_rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self.conn.executions += 1
        return self

    def fetchall(self):
        return self.fetch_rows

    def fetchone(self):
        return self.fetch_rows[0] if self.fetch_rows else None

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.executions = 0
        self._cursor = None

    def cursor(self):
        if self._cursor is None:
            self._cursor = FakeCursor(self)
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakeEmbeddingResponse:
    def model_dump_json(self):
        return '{"data":[{"embedding":[0.1,0.2]}]}'


def _load_vectorizer_module():
    import importlib

    if "vectorize_blog_posts" in sys.modules:
        del sys.modules["vectorize_blog_posts"]
    return importlib.import_module("vectorize_blog_posts")


def test_update_post_vector_does_not_commit_per_call(env_setup):
    mod = _load_vectorizer_module()
    fake_conn = FakeConn()

    with mock.patch.object(mod.pyodbc, "connect", return_value=fake_conn):
        v = mod.BlogVectorizer()
        v.update_post_vector(post_id=42, embedding=FakeEmbeddingResponse())

    # Regression: pre-#76 this was 1 (per-item commit). Now must be 0 —
    # commits are deferred to BatchCommitter.
    assert fake_conn.commits == 0
    assert fake_conn.executions == 1


def test_vectorize_all_posts_commits_once_per_batch(env_setup, monkeypatch):
    monkeypatch.setenv("BATCH_COMMIT_SIZE", "3")
    mod = _load_vectorizer_module()

    fake_conn = FakeConn()
    cur = fake_conn.cursor()
    cur.fetch_rows = [
        (i, f"title-{i}", "cat", "summary", "https://example.com")
        for i in range(1, 8)
    ]

    with mock.patch.object(mod.pyodbc, "connect", return_value=fake_conn):
        v = mod.BlogVectorizer()
        v.generate_embedding = lambda text: FakeEmbeddingResponse()  # type: ignore[assignment]
        v.vectorize_all_posts()

    # 7 items, batch_size=3 → commits at 3, 6, and final flush of 1 == 3 commits
    assert fake_conn.commits == 3


def test_vectorize_per_item_failure_does_not_abort_batch(env_setup, monkeypatch):
    monkeypatch.setenv("BATCH_COMMIT_SIZE", "10")
    mod = _load_vectorizer_module()

    fake_conn = FakeConn()
    cur = fake_conn.cursor()
    cur.fetch_rows = [
        (i, f"title-{i}", "cat", "summary", "https://example.com")
        for i in range(1, 5)
    ]

    with mock.patch.object(mod.pyodbc, "connect", return_value=fake_conn):
        v = mod.BlogVectorizer()

        def gen(text):
            if "title-2" in text:
                return None
            return FakeEmbeddingResponse()

        v.generate_embedding = gen  # type: ignore[assignment]
        v.vectorize_all_posts()

    # 4 posts, 1 failed embedding, so 3 UPDATEs succeed.
    # batch_size=10 means a single final flush == 1 commit.
    assert fake_conn.commits == 1


def test_vectorize_no_posts_does_not_commit(env_setup):
    mod = _load_vectorizer_module()

    fake_conn = FakeConn()
    cur = fake_conn.cursor()
    cur.fetch_rows = []

    with mock.patch.object(mod.pyodbc, "connect", return_value=fake_conn):
        v = mod.BlogVectorizer()
        v.vectorize_all_posts()

    assert fake_conn.commits == 0
