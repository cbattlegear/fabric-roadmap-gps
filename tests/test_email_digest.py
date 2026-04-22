"""Tests for ``lib.email_digest`` (issue #74 / M5).

Pure helpers (cache key, cadence filter, subscription filters, top-N
selection) are tested with simple inputs. The :class:`DigestContentBuilder`
is tested with all dependencies (HTTP, session_scope, cache_model,
ai_summary_fn, clock) mocked so no live network or DB is involved.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import sqlalchemy.exc

from lib import email_digest


def _sub(**overrides):
    base = dict(
        email_cadence="weekly",
        product_filter=None,
        release_type_filter=None,
        release_status_filter=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _item(**overrides):
    base = dict(
        release_item_id="00000000-0000-0000-0000-000000000000",
        feature_name="Test feature",
        product_name="Power BI",
        release_type="Public preview",
        release_status="Planned",
        last_modified="2026-04-22",
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# build_cache_key
# ---------------------------------------------------------------------------
class TestBuildCacheKey:
    def test_same_inputs_yield_same_key(self):
        a = email_digest.build_cache_key(_sub(product_filter="Power BI"))
        b = email_digest.build_cache_key(_sub(product_filter="Power BI"))
        assert a == b

    def test_different_filters_yield_different_keys(self):
        a = email_digest.build_cache_key(_sub(product_filter="Power BI"))
        b = email_digest.build_cache_key(_sub(product_filter="Fabric"))
        assert a != b

    def test_filter_is_normalized_to_lower_and_trimmed(self):
        a = email_digest.build_cache_key(_sub(product_filter="Power BI"))
        b = email_digest.build_cache_key(_sub(product_filter="  power bi  "))
        assert a == b

    def test_none_cadence_treated_as_weekly(self):
        a = email_digest.build_cache_key(_sub(email_cadence=None))
        b = email_digest.build_cache_key(_sub(email_cadence="weekly"))
        assert a == b

    def test_returns_64_hex_chars(self):
        key = email_digest.build_cache_key(_sub())
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)


# ---------------------------------------------------------------------------
# filter_by_cadence
# ---------------------------------------------------------------------------
class TestFilterByCadence:
    NOW = datetime(2026, 4, 22, 12, 0, 0)

    def test_weekly_passes_through(self):
        items = [_item(last_modified="2026-04-15"), _item(last_modified=None)]
        assert email_digest.filter_by_cadence(items, "weekly") == items

    def test_returns_a_new_list(self):
        items = [_item()]
        assert email_digest.filter_by_cadence(items, "weekly") is not items

    def test_daily_keeps_items_within_window(self):
        items = [_item(last_modified="2026-04-22"), _item(last_modified="2026-04-21")]
        result = email_digest.filter_by_cadence(items, "daily", now=self.NOW)
        assert result == items

    def test_daily_drops_older_items(self):
        items = [_item(last_modified="2026-04-19"), _item(last_modified="2026-04-22")]
        result = email_digest.filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]

    def test_daily_drops_missing_last_modified(self):
        items = [_item(last_modified=None), _item(last_modified="2026-04-22")]
        result = email_digest.filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]

    @pytest.mark.parametrize("bad", ["xx", "2026/04/22", 20260422])
    def test_daily_drops_malformed_without_crashing(self, bad):
        items = [_item(last_modified=bad), _item(last_modified="2026-04-22")]
        result = email_digest.filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]


# ---------------------------------------------------------------------------
# apply_subscription_filters
# ---------------------------------------------------------------------------
class TestApplySubscriptionFilters:
    def test_no_filters_passes_through(self):
        items = [_item(product_name="Power BI"), _item(product_name="Fabric")]
        result = email_digest.apply_subscription_filters(items, _sub())
        assert result == items

    def test_product_filter_is_case_insensitive(self):
        items = [_item(product_name="Power BI"), _item(product_name="Fabric")]
        result = email_digest.apply_subscription_filters(items, _sub(product_filter="POWER BI"))
        assert [c["product_name"] for c in result] == ["Power BI"]

    def test_csv_product_filter(self):
        items = [
            _item(product_name="Power BI"),
            _item(product_name="Fabric"),
            _item(product_name="Synapse"),
        ]
        result = email_digest.apply_subscription_filters(items, _sub(product_filter="Power BI, Fabric"))
        assert [c["product_name"] for c in result] == ["Power BI", "Fabric"]

    def test_release_type_filter_is_exact_match(self):
        items = [_item(release_type="GA"), _item(release_type="Preview")]
        result = email_digest.apply_subscription_filters(items, _sub(release_type_filter="GA"))
        assert [c["release_type"] for c in result] == ["GA"]

    def test_release_status_filter_is_exact_match(self):
        items = [_item(release_status="Shipped"), _item(release_status="Planned")]
        result = email_digest.apply_subscription_filters(items, _sub(release_status_filter="Planned"))
        assert [c["release_status"] for c in result] == ["Planned"]

    def test_filters_combine_intersection(self):
        items = [
            _item(product_name="Power BI", release_type="GA"),
            _item(product_name="Power BI", release_type="Preview"),
            _item(product_name="Fabric", release_type="GA"),
        ]
        result = email_digest.apply_subscription_filters(
            items, _sub(product_filter="Power BI", release_type_filter="GA")
        )
        assert len(result) == 1
        assert result[0]["product_name"] == "Power BI"
        assert result[0]["release_type"] == "GA"


# ---------------------------------------------------------------------------
# select_top_changes
# ---------------------------------------------------------------------------
class TestSelectTopChanges:
    def test_sorts_by_last_modified_desc(self):
        items = [
            _item(last_modified="2026-01-01", feature_name="old"),
            _item(last_modified="2026-04-01", feature_name="new"),
        ]
        result = email_digest.select_top_changes(items)
        assert [c["feature_name"] for c in result] == ["new", "old"]

    def test_truncates_to_max_count(self):
        items = [_item(last_modified=f"2026-04-{i:02d}") for i in range(1, 11)]
        result = email_digest.select_top_changes(items, max_count=3)
        assert len(result) == 3

    def test_handles_missing_last_modified(self):
        items = [_item(last_modified=None), _item(last_modified="2026-04-22")]
        result = email_digest.select_top_changes(items)
        assert result[0]["last_modified"] == "2026-04-22"


# ---------------------------------------------------------------------------
# generate_ai_summary
# ---------------------------------------------------------------------------
class TestGenerateAiSummary:
    def test_empty_changes_returns_none(self):
        assert email_digest.generate_ai_summary([]) is None

    def test_no_config_returns_none(self, monkeypatch):
        monkeypatch.delenv("AZURE_OPENAI_ENDPOINT", raising=False)
        monkeypatch.delenv("AZURE_OPENAI_API_KEY", raising=False)
        # Even with SDK present, missing config short-circuits
        monkeypatch.setattr(email_digest, "_OPENAI_AVAILABLE", True)
        assert email_digest.generate_ai_summary([_item()]) is None

    def test_sdk_unavailable_returns_none(self, monkeypatch):
        monkeypatch.setattr(email_digest, "_OPENAI_AVAILABLE", False)
        assert email_digest.generate_ai_summary([_item()]) is None

    def test_calls_injected_factory_and_returns_summary(self, monkeypatch):
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://x.openai.azure.com")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "key")
        monkeypatch.setattr(email_digest, "_OPENAI_AVAILABLE", True)

        fake_choice = SimpleNamespace(message=SimpleNamespace(content="  Big news.  "))
        fake_response = SimpleNamespace(choices=[fake_choice])
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = fake_response

        summary = email_digest.generate_ai_summary(
            [_item()], client_factory=lambda: fake_client
        )
        assert summary == "Big news."
        fake_client.chat.completions.create.assert_called_once()


# ---------------------------------------------------------------------------
# DigestContentBuilder
# ---------------------------------------------------------------------------
class FakeCacheRow:
    def __init__(self, content_json: str) -> None:
        self.content_json = content_json


class FakeSession:
    """Minimal mock session that records adds and returns scripted scalars."""

    def __init__(self, scalar_result=None, integrity_error=False):
        self._scalar_result = scalar_result
        self._integrity_error = integrity_error
        self.added = []
        self.committed = False

    # context-manager-friendly transaction
    @contextmanager
    def begin(self):
        if self._integrity_error:
            raise sqlalchemy.exc.IntegrityError("stmt", "params", Exception("dup"))
        yield
        self.committed = True

    def scalar(self, _stmt):
        return self._scalar_result

    def add(self, row):
        self.added.append(row)


def _make_session_scope(session):
    @contextmanager
    def _scope(_engine):
        yield session
    return _scope


from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base

_FakeBase = declarative_base()


class FakeCacheModel(_FakeBase):
    """Stand-in for ``EmailContentCacheModel``.

    Real SQLAlchemy mapped class so the builder's
    ``select(FakeCacheModel).where(FakeCacheModel.cache_date == ...)``
    builds a valid statement. The builder also instantiates the model
    with kwargs to ``session.add()``, which declarative classes
    support natively.
    """

    __tablename__ = "fake_cache"
    cache_date = Column(String, primary_key=True)
    cache_key = Column(String, primary_key=True)
    generated_at = Column(String)
    content_json = Column(String)


def _ok_response(payload):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = payload
    return resp


class TestDigestContentBuilderFetch:
    def test_fetch_dedupes_by_release_item_id(self):
        calls = []

        def http_get(url, params=None, timeout=None):
            calls.append(params)
            if params["page"] == 1:
                return _ok_response({
                    "data": [_item(release_item_id="a"), _item(release_item_id="b")],
                    "pagination": {"has_next": True},
                })
            return _ok_response({
                "data": [_item(release_item_id="b"), _item(release_item_id="c")],
                "pagination": {"has_next": False},
            })

        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(FakeSession()),
            cache_model=FakeCacheModel,
            http_get=http_get,
        )
        items = builder.fetch_raw_changes()
        assert sorted(c["release_item_id"] for c in items) == ["a", "b", "c"]

    def test_fetch_supports_legacy_list_payload(self):
        def http_get(url, params=None, timeout=None):
            if params["page"] == 1:
                return _ok_response([_item(release_item_id="a")])
            return _ok_response([])  # empty -> stop

        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(FakeSession()),
            cache_model=FakeCacheModel, http_get=http_get,
        )
        items = builder.fetch_raw_changes()
        assert [c["release_item_id"] for c in items] == ["a"]

    def test_fetch_caches_in_memory(self):
        call_count = {"n": 0}

        def http_get(url, params=None, timeout=None):
            call_count["n"] += 1
            return _ok_response({"data": [], "pagination": {"has_next": False}})

        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(FakeSession()),
            cache_model=FakeCacheModel, http_get=http_get,
        )
        builder.fetch_raw_changes()
        builder.fetch_raw_changes()
        assert call_count["n"] == 1

    def test_fetch_stops_on_non_200(self):
        def http_get(url, params=None, timeout=None):
            resp = MagicMock()
            resp.status_code = 500
            return resp

        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(FakeSession()),
            cache_model=FakeCacheModel, http_get=http_get,
        )
        assert builder.fetch_raw_changes() == []


class TestDigestContentBuilderGetFor:
    def test_cache_hit_returns_cached_changes_and_summary(self):
        cached_payload = {"changes": [_item(release_item_id="x")], "ai_summary": "S"}
        row = FakeCacheRow(json.dumps(cached_payload))
        session = FakeSession(scalar_result=row)

        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(session),
            cache_model=FakeCacheModel,
            http_get=lambda *a, **kw: pytest.fail("HTTP should not be called on cache hit"),
        )
        changes, summary = builder.get_for(_sub())
        assert summary == "S"
        assert changes[0]["release_item_id"] == "x"
        assert session.added == []  # no write on cache hit

    def test_cache_miss_filters_and_writes(self):
        session = FakeSession(scalar_result=None)
        items_returned = [
            _item(release_item_id="1", product_name="Power BI", last_modified="2026-04-22"),
            _item(release_item_id="2", product_name="Fabric", last_modified="2026-04-21"),
        ]

        def http_get(url, params=None, timeout=None):
            return _ok_response({"data": items_returned, "pagination": {"has_next": False}})

        ai_calls = []

        def fake_ai(items):
            ai_calls.append(list(items))
            return "summary"

        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(session),
            cache_model=FakeCacheModel,
            ai_summary_fn=fake_ai,
            http_get=http_get,
            clock=lambda: datetime(2026, 4, 22, 12, 0, 0),
        )
        changes, summary = builder.get_for(_sub(product_filter="Power BI"))
        assert [c["release_item_id"] for c in changes] == ["1"]
        assert summary == "summary"
        assert len(ai_calls) == 1
        # Cache row was written
        assert len(session.added) == 1
        written = session.added[0]
        payload = json.loads(written.content_json)
        assert payload["changes"][0]["release_item_id"] == "1"
        assert payload["ai_summary"] == "summary"

    def test_no_items_skips_ai_summary(self):
        session = FakeSession(scalar_result=None)

        def http_get(url, params=None, timeout=None):
            return _ok_response({"data": [], "pagination": {"has_next": False}})

        ai_calls = []
        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(session),
            cache_model=FakeCacheModel,
            ai_summary_fn=lambda items: ai_calls.append(items) or "x",
            http_get=http_get,
        )
        changes, summary = builder.get_for(_sub())
        assert changes == []
        assert summary is None
        assert ai_calls == []

    def test_integrity_error_on_cache_write_does_not_propagate(self):
        session = FakeSession(scalar_result=None, integrity_error=True)

        def http_get(url, params=None, timeout=None):
            return _ok_response({"data": [_item()], "pagination": {"has_next": False}})

        builder = email_digest.DigestContentBuilder(
            engine=None, base_url="http://x",
            session_scope=_make_session_scope(session),
            cache_model=FakeCacheModel,
            ai_summary_fn=lambda items: None,
            http_get=http_get,
        )
        changes, _ = builder.get_for(_sub())
        # Even though cache write raised IntegrityError, builder returns content
        assert len(changes) == 1
