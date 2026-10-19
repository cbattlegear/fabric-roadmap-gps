"""Tests for ``WeeklyEmailSender._filter_by_cadence`` (H6 fix).

The daily-cadence path used to live inline inside ``_get_subscriber_content``
with no test coverage — a date-format change there would silently send
every daily subscriber an empty digest. The logic now lives in a pure
static helper that takes injectable ``now`` so we can deterministically
test the cutoff boundary without freezing the clock for the whole module.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# Importing weekly_email_job pulls in heavy DB / opentelemetry side effects
# only at construction time, not at import. The class itself is safe to
# reach for the static helper.
from weekly_email_job import WeeklyEmailSender


@pytest.mark.parametrize("cadence", ["daily", "weekly"])
def test_digest_subject_uses_full_count(cadence):
    sender = WeeklyEmailSender.__new__(WeeklyEmailSender)
    sender.base_url = "https://example.com"
    changes = [_item("2026-04-22") for _ in range(75)]
    builder = MagicMock()
    builder.get_for.return_value = (changes, None)
    sender.send_azure_email = MagicMock(return_value=True)
    subscription = SimpleNamespace(
        email="test@example.com", email_cadence=cadence,
        unsubscribe_token="token", product_filter=None,
        release_type_filter=None, release_status_filter=None,
    )
    with patch.object(sender, "_builder", return_value=builder):
        assert sender.send_digest_email(subscription)
    message = sender.send_azure_email.call_args.kwargs
    assert message["subject"] == f"Fabric GPS {cadence.title()} Update - 75 Changes"
    assert "75 Fabric roadmap item change(s)" in message["html_content"]
    assert "(75 items)" in message["text_content"]


def _item(last_modified, **extra):
    base = {
        "release_item_id": "00000000-0000-0000-0000-000000000000",
        "feature_name": "Test feature",
        "product_name": "Power BI",
        "last_modified": last_modified,
    }
    base.update(extra)
    return base


class TestFilterByCadenceWeekly:
    def test_weekly_passes_through_unchanged(self):
        items = [_item("2026-04-01"), _item("2026-04-15"), _item(None)]
        result = WeeklyEmailSender._filter_by_cadence(items, "weekly")
        assert result == items

    def test_none_cadence_passes_through_unchanged(self):
        items = [_item("2026-04-01"), _item(None)]
        result = WeeklyEmailSender._filter_by_cadence(items, None)
        assert result == items

    def test_unknown_cadence_passes_through_unchanged(self):
        items = [_item("2026-04-01")]
        result = WeeklyEmailSender._filter_by_cadence(items, "monthly")
        assert result == items

    def test_returns_a_new_list_not_the_input(self):
        items = [_item("2026-04-01")]
        result = WeeklyEmailSender._filter_by_cadence(items, "weekly")
        assert result == items
        assert result is not items


class TestFilterByCadenceDaily:
    NOW = datetime(2026, 4, 22, 12, 0, 0)
    # cutoff_date = (now - 1 day).date() = 2026-04-21

    def test_item_within_window_is_kept(self):
        items = [_item("2026-04-22"), _item("2026-04-21")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert result == items

    def test_item_at_exact_cutoff_is_included(self):
        # last_modified == cutoff_date passes the >= check
        items = [_item("2026-04-21")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert result == items

    def test_older_item_is_dropped(self):
        items = [_item("2026-04-19"), _item("2026-04-22")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]

    def test_future_dated_item_is_kept(self):
        # Roadmap items often have future last_modified dates; never drop them.
        items = [_item("2026-12-31")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert result == items

    def test_missing_last_modified_is_dropped(self):
        items = [_item(None), _item(""), _item("2026-04-22")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]

    def test_item_without_last_modified_key_is_dropped(self):
        items = [{"feature_name": "no key"}, _item("2026-04-22")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]

    @pytest.mark.parametrize("bad_value", ["not-a-date", "2026/04/22", "20260422", "April 22"])
    def test_malformed_string_is_dropped_not_crashed(self, bad_value):
        items = [_item(bad_value), _item("2026-04-22")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]

    def test_non_string_last_modified_is_dropped_not_crashed(self):
        # Defensive: the API contract is YYYY-MM-DD strings, but a future
        # change shouldn't crash daily filtering — log + skip.
        items = [_item(20260422), _item({"bad": "type"}), _item("2026-04-22")]
        result = WeeklyEmailSender._filter_by_cadence(items, "daily", now=self.NOW)
        assert [c["last_modified"] for c in result] == ["2026-04-22"]

    def test_empty_input_returns_empty(self):
        assert WeeklyEmailSender._filter_by_cadence([], "daily", now=self.NOW) == []

    def test_default_now_is_used_when_not_provided(self):
        # Verify the default ``now=None`` path uses the real wall clock.
        # freezegun intercepts datetime.utcnow() regardless of import style,
        # which is more robust than ad-hoc datetime patching.
        from freezegun import freeze_time

        items = [_item("2026-04-22"), _item("2026-04-19")]
        with freeze_time("2026-04-22 12:00:00"):
            result = WeeklyEmailSender._filter_by_cadence(items, "daily")

        assert [c["last_modified"] for c in result] == ["2026-04-22"]
