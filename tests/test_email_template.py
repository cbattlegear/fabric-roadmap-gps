"""Tests for ``lib.email_template`` (issue #74 / M5).

Pure rendering — no DB, no ACS, no network. Subscriptions are
``SimpleNamespace`` stand-ins for the real ``EmailSubscriptionModel``
since the renderers only need attribute access.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from lib import email_template as et


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sub(**overrides):
    base = dict(
        email="alice@example.com",
        unsubscribe_token="tok-123",
        email_cadence="weekly",
        product_filter=None,
        release_type_filter=None,
        release_status_filter=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _change(**overrides):
    base = dict(
        release_item_id="11111111-1111-1111-1111-111111111111",
        feature_name="Direct Lake mode",
        product_name="Power BI",
        release_type="General availability",
        release_status="Shipped",
        feature_description="A new query mode.",
        release_date="2026-05-01",
        last_modified="2026-04-20",
        active=True,
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# escape_html
# ---------------------------------------------------------------------------
class TestEscapeHtml:
    def test_handles_none_and_empty(self):
        assert et.escape_html(None) == ""
        assert et.escape_html("") == ""

    def test_escapes_all_specials(self):
        assert et.escape_html("<a href=\"x\">'&</a>") == (
            "&lt;a href=&quot;x&quot;&gt;&#x27;&amp;&lt;/a&gt;"
        )

    def test_coerces_non_string(self):
        assert et.escape_html(42) == "42"


# ---------------------------------------------------------------------------
# add_utm
# ---------------------------------------------------------------------------
class TestAddUtm:
    def test_appends_to_url_without_query(self):
        assert et.add_utm("https://x.com/y") == (
            "https://x.com/y?utm_source=email&utm_medium=email&utm_campaign=weekly-digest"
        )

    def test_appends_to_url_with_query(self):
        result = et.add_utm("https://x.com/y?foo=1")
        assert result.startswith("https://x.com/y?foo=1&utm_source=email")

    def test_preserves_fragment(self):
        result = et.add_utm("https://x.com/y#bottom")
        assert result.endswith("#bottom")
        assert "utm_source=email" in result

    def test_preserves_fragment_with_query(self):
        result = et.add_utm("https://x.com/y?a=1#bottom")
        assert result == "https://x.com/y?a=1&utm_source=email&utm_medium=email&utm_campaign=weekly-digest#bottom"

    def test_custom_campaign_used(self):
        result = et.add_utm("https://x.com", campaign="watch-alert")
        assert "utm_campaign=watch-alert" in result


# ---------------------------------------------------------------------------
# Badge / button
# ---------------------------------------------------------------------------
class TestBadge:
    def test_known_variant_uses_known_color(self):
        assert "#107c10" in et.build_badge("Shipped", "success")

    def test_unknown_variant_falls_back_to_neutral(self):
        assert "#605e5c" in et.build_badge("Whatever", "bogus")

    def test_empty_text_renders_unknown(self):
        assert ">Unknown<" in et.build_badge("", "neutral")

    def test_text_is_escaped(self):
        assert "&lt;b&gt;" in et.build_badge("<b>", "neutral")


class TestButton:
    def test_renders_anchor_with_label_and_href(self):
        html = et.build_button("https://x.com/y?z=1", "Click")
        assert 'href="https://x.com/y?z=1"' in html
        assert ">Click</a>" in html


# ---------------------------------------------------------------------------
# Digest HTML
# ---------------------------------------------------------------------------
class TestRenderDigestHtml:
    def test_empty_changes_renders_empty_state(self):
        html = et.render_digest_html([], _sub(), "https://example.com")
        assert "No roadmap item changes in the past week" in html

    def test_includes_unsubscribe_and_preferences_links(self):
        html = et.render_digest_html([_change()], _sub(), "https://example.com")
        assert "https://example.com/unsubscribe?token=tok-123" in html
        assert "https://example.com/preferences?token=tok-123" in html

    def test_uses_daily_campaign_when_cadence_label_daily(self):
        html = et.render_digest_html([_change()], _sub(), "https://example.com", cadence_label="Daily")
        assert "utm_campaign=daily-digest" in html
        assert "Fabric GPS — Daily Update" in html

    def test_uses_weekly_campaign_by_default(self):
        html = et.render_digest_html([_change()], _sub(), "https://example.com")
        assert "utm_campaign=weekly-digest" in html
        assert "Fabric GPS — Weekly Update" in html

    def test_truncates_to_max_cards_and_shows_browse_more(self):
        many = [_change(release_item_id=str(i), last_modified=f"2026-04-{i:02d}") for i in range(1, 25)]
        html = et.render_digest_html(many, _sub(), "https://example.com")
        # 4 over the cap of 20
        assert "... and 4 more change(s)" in html
        assert "View All on Fabric GPS" in html

    def test_no_browse_more_when_at_or_below_cap(self):
        items = [_change() for _ in range(20)]
        html = et.render_digest_html(items, _sub(), "https://example.com")
        assert "more change(s)" not in html

    def test_renders_ai_summary_block_when_present(self):
        html = et.render_digest_html(
            [_change()], _sub(), "https://example.com", ai_summary="Big news this week."
        )
        assert "AI Summary" in html
        assert "Big news this week." in html

    def test_no_summary_block_when_summary_none(self):
        html = et.render_digest_html([_change()], _sub(), "https://example.com")
        assert "AI Summary" not in html

    def test_removed_item_gets_removed_badge(self):
        html = et.render_digest_html([_change(active=False)], _sub(), "https://example.com")
        assert ">Removed<" in html

    def test_browse_more_passes_single_filter_to_url(self):
        many = [_change(release_item_id=str(i)) for i in range(25)]
        html = et.render_digest_html(many, _sub(product_filter="Power BI"), "https://example.com")
        assert "product_name=Power+BI" in html

    def test_browse_more_drops_csv_filters(self):
        many = [_change(release_item_id=str(i)) for i in range(25)]
        html = et.render_digest_html(many, _sub(product_filter="Power BI,Fabric"), "https://example.com")
        assert "product_name=" not in html

    def test_subscriber_email_is_escaped(self):
        html = et.render_digest_html([_change()], _sub(email="<x>@y"), "https://example.com")
        assert "&lt;x&gt;@y" in html

    def test_html_escapes_feature_with_special_chars(self):
        html = et.render_digest_html(
            [_change(feature_name="A & B <test>")], _sub(), "https://example.com"
        )
        assert "A &amp; B &lt;test&gt;" in html


# ---------------------------------------------------------------------------
# Digest text
# ---------------------------------------------------------------------------
class TestRenderDigestText:
    def test_includes_header_and_links(self):
        text = et.render_digest_text([_change()], _sub(), "https://example.com")
        assert "FABRIC GPS - WEEKLY UPDATE" in text
        assert "Unsubscribe: https://example.com/unsubscribe?token=tok-123" in text

    def test_daily_subscription_uses_daily_header(self):
        text = et.render_digest_text([_change()], _sub(email_cadence="daily"), "https://example.com")
        assert "FABRIC GPS - DAILY UPDATE" in text
        assert "today's update" in text

    def test_includes_summary_section_when_provided(self):
        text = et.render_digest_text(
            [_change()], _sub(), "https://example.com", ai_summary="Summary line"
        )
        assert "AI SUMMARY" in text
        assert "Summary line" in text

    def test_no_summary_section_when_summary_absent(self):
        text = et.render_digest_text([_change()], _sub(), "https://example.com")
        assert "AI SUMMARY" not in text

    def test_marks_removed_items(self):
        text = et.render_digest_text([_change(active=False)], _sub(), "https://example.com")
        assert "[REMOVED]" in text

    def test_handles_empty_changes_list(self):
        text = et.render_digest_text([], _sub(), "https://example.com")
        assert "(0 items)" in text

    def test_falls_back_to_raw_dates_when_unparseable(self):
        text = et.render_digest_text(
            [_change(release_date="not-a-date", last_modified="also-bad")],
            _sub(), "https://example.com",
        )
        assert "Release Date: not-a-date" in text
        assert "Last Modified: also-bad" in text


# ---------------------------------------------------------------------------
# Watch alerts
# ---------------------------------------------------------------------------
@pytest.fixture
def watch_one():
    return [{
        "watch_id": "w1",
        "release_item_id": "rid-1",
        "feature_name": "Watched feature",
        "product_name": "Power BI",
        "release_type": "Public preview",
        "release_status": "Planned",
        "last_modified": "2026-04-22",
        "current_hash": "h1",
    }]


@pytest.fixture
def watch_three(watch_one):
    return watch_one + [
        {**watch_one[0], "watch_id": "w2", "release_item_id": "rid-2", "feature_name": "Two"},
        {**watch_one[0], "watch_id": "w3", "release_item_id": "rid-3", "feature_name": "Three"},
    ]


class TestWatchAlertSubject:
    def test_single_watch_uses_feature_name(self, watch_one):
        assert et.render_watch_alert_subject(watch_one) == "Fabric GPS Alert: Watched feature Updated"

    def test_multiple_watches_uses_count(self, watch_three):
        assert et.render_watch_alert_subject(watch_three) == "Fabric GPS Alert: 3 Watched Features Updated"


class TestRenderWatchAlertHtml:
    def test_includes_each_feature(self, watch_three):
        html = et.render_watch_alert_html(_sub(), watch_three, "https://example.com")
        assert "Watched feature" in html
        assert "Two" in html
        assert "Three" in html

    def test_release_link_uses_watch_alert_campaign(self, watch_one):
        html = et.render_watch_alert_html(_sub(), watch_one, "https://example.com")
        # URL ampersands are HTML-escaped inside href attribute.
        assert "https://example.com/release/rid-1?utm_source=email&amp;utm_medium=email&amp;utm_campaign=watch-alert" in html

    def test_removed_watch_shows_removed_badge(self, watch_one):
        watch_one[0]["active"] = False
        html = et.render_watch_alert_html(_sub(), watch_one, "https://example.com")
        assert "REMOVED" in html

    def test_html_escapes_feature_names(self, watch_one):
        watch_one[0]["feature_name"] = "<script>"
        html = et.render_watch_alert_html(_sub(), watch_one, "https://example.com")
        assert "&lt;script&gt;" in html
        assert "<script>" not in html


class TestRenderWatchAlertText:
    def test_includes_feature_lines(self, watch_three):
        text = et.render_watch_alert_text(_sub(), watch_three, "https://example.com")
        assert "• Watched feature" in text
        assert "• Two" in text
        assert "• Three" in text

    def test_includes_unsubscribe_and_manage_links(self, watch_one):
        text = et.render_watch_alert_text(_sub(), watch_one, "https://example.com")
        assert "Manage Watches: https://example.com/preferences?token=tok-123" in text
        assert "Unsubscribe: https://example.com/unsubscribe?token=tok-123" in text

    def test_includes_release_link(self, watch_one):
        text = et.render_watch_alert_text(_sub(), watch_one, "https://example.com")
        assert "Link: https://example.com/release/rid-1" in text
