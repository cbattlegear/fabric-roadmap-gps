"""Pure HTML/text rendering helpers for outbound emails.

Extracted from ``weekly_email_job.py`` so the rendering surface area can
be exercised without standing up ACS, Azure OpenAI, or SQL Server.

Every function in this module is pure: it takes a dict / duck-typed
subscription object and returns a string. There are no I/O side
effects, no logging, and no module-level state besides the shared style
tokens. This makes the renderers safe to unit-test with
``SimpleNamespace`` stand-ins and to call from any orchestrator.

The "subscription" parameters are duck-typed — they only need to expose
the same attributes the production ``EmailSubscriptionModel`` does
(``email``, ``unsubscribe_token``, ``email_cadence``, ``product_filter``,
``release_type_filter``, ``release_status_filter``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from lib.quarter_date import format_as_quarter


# ---------------------------------------------------------------------------
# Style tokens shared across all email templates
# ---------------------------------------------------------------------------
BODY_BG = "#f3f2f1"
CARD_BG = "#ffffff"
CARD_BORDER = "#e1e5e9"
CARD_SHADOW = "0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06)"
TEXT_PRIMARY = "#323130"
TEXT_SECONDARY = "#605e5c"
HERO_GRADIENT = "linear-gradient(135deg,#19433c 0%,#286c61 100%)"

MAX_EMAIL_CARDS = 20

_BADGE_COLORS = {
    "product": ("#004578", "#ffffff"),
    "success": ("#107c10", "#ffffff"),
    "warning": ("#ca5010", "#ffffff"),
    "neutral": ("#605e5c", "#ffffff"),
    "removed": ("#d13438", "#ffffff"),
}


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------
def escape_html(text: Optional[str]) -> str:
    """Escape HTML special characters; ``None`` / empty -> empty string."""
    if not text:
        return ""
    return (
        str(text)
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#x27;')
    )


def add_utm(
    url: str,
    source: str = 'email',
    medium: str = 'email',
    campaign: str = 'weekly-digest',
) -> str:
    """Append UTM tracking parameters, preserving any ``#fragment``."""
    params = f"utm_source={source}&utm_medium={medium}&utm_campaign={campaign}"
    if '#' in url:
        base, fragment = url.split('#', 1)
        sep = '&' if '?' in base else '?'
        return f"{base}{sep}{params}#{fragment}"
    sep = '&' if '?' in url else '?'
    return f"{url}{sep}{params}"


def build_badge(text: str, variant: str) -> str:
    """Return a styled inline ``<span>`` badge."""
    if not text:
        text = "Unknown"
    bg, fg = _BADGE_COLORS.get(variant, _BADGE_COLORS["neutral"])
    return (
        f'<span style="display:inline-block;margin:0 6px 6px 0;'
        f'padding:4px 10px;font-size:12px;font-weight:600;letter-spacing:.25px;'
        f'border-radius:999px;background:{bg};color:{fg};white-space:nowrap;">'
        f'{escape_html(text)}</span>'
    )


def build_button(href: str, label: str) -> str:
    """Return a styled call-to-action ``<a>`` button."""
    return (
        f'<a href="{escape_html(href)}" '
        f'style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);'
        f'color:#ffffff;text-decoration:none;padding:10px 18px;font-size:14px;font-weight:600;'
        f'border-radius:6px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.15);">'
        f'{escape_html(label)}</a>'
    )


def _fmt_date(dt_str: Optional[str], fallback: str = "TBD", out_fmt: str = "%b %d, %Y") -> str:
    if not dt_str:
        return fallback
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00')).strftime(out_fmt)
    except (ValueError, TypeError, AttributeError):
        return dt_str


def _fmt_release_date(value: Any, fallback: str = "TBD") -> str:
    """Render a release date value as ``"Q# YYYY"`` for emails."""
    if value is None or value == "":
        return fallback
    quarter = format_as_quarter(value)
    return quarter if quarter else str(value)


# ---------------------------------------------------------------------------
# Digest email rendering
# ---------------------------------------------------------------------------
def _render_change_card(change: Dict[str, Any], base_url: str, utm_campaign: str) -> str:
    feature_name = change.get('feature_name') or 'Unnamed Feature'
    product_name = change.get('product_name') or 'Unknown'
    release_type = change.get('release_type') or 'Unknown'
    release_status = change.get('release_status') or 'Unknown'
    description = change.get('feature_description') or 'No description available.'
    rel_id = change.get('release_item_id')
    release_date = _fmt_release_date(change.get('release_date'))
    modified_date = _fmt_date(change.get('last_modified'), fallback="Unknown")
    release_type_variant = "success" if release_type == "General availability" else "warning"
    release_status_variant = "success" if release_status == "Shipped" else "warning"
    is_removed = change.get('active') is False
    detail_url = (
        add_utm(f"{base_url}/release/{rel_id}", campaign=utm_campaign)
        if rel_id else add_utm(base_url, campaign=utm_campaign)
    )
    removed_badge = build_badge("Removed", "removed") if is_removed else ""
    badges_html = (
        removed_badge
        + build_badge(product_name, "product")
        + build_badge(release_type, release_type_variant)
        + build_badge(release_status, release_status_variant)
    )
    return f"""
                <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;
                            padding:18px;margin:0 0 18px 0;box-shadow:{CARD_SHADOW};\">
                    <h3 style=\"margin:0 0 10px 0;font-size:18px;line-height:1.3;color:{TEXT_PRIMARY};font-weight:600;\">
                        <a href=\"{escape_html(detail_url)}\" style=\"color:{TEXT_PRIMARY};text-decoration:none;\">{escape_html(feature_name)}
                        </a>
                    </h3>
                    <div style=\"margin:0 0 10px 0;\">{badges_html}</div>
                    <div style=\"font-size:12px;color:{TEXT_SECONDARY};margin:0 0 12px 0;\">
                        <strong>Last Modified:</strong> {escape_html(modified_date)} &nbsp;|&nbsp;
                        <strong>Release Date:</strong> {escape_html(release_date)}
                    </div>
                    <p style=\"margin:0 0 14px 0;font-size:14px;line-height:1.5;color:{TEXT_PRIMARY};\">{escape_html(description)}
                    </p>
                    {build_button(detail_url, "View in Fabric GPS")}
                </div>
                """


def _render_browse_more_card(
    remaining: int,
    subscription: Any,
    base_url: str,
    utm_campaign: str,
    cadence_label: str,
) -> str:
    browse_params: Dict[str, Any] = {
        'modified_within_days': 1 if cadence_label == 'Daily' else 7,
        'include_inactive': 'true',
    }
    if subscription.product_filter and ',' not in subscription.product_filter:
        browse_params['product_name'] = subscription.product_filter.strip()
    if subscription.release_type_filter and ',' not in subscription.release_type_filter:
        browse_params['release_type'] = subscription.release_type_filter.strip()
    if subscription.release_status_filter and ',' not in subscription.release_status_filter:
        browse_params['release_status'] = subscription.release_status_filter.strip()
    browse_url = add_utm(f"{base_url}/?{urlencode(browse_params)}", campaign=utm_campaign)
    return (
        f'<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;'
        f'padding:22px;margin:0 0 18px 0;box-shadow:{CARD_SHADOW};text-align:center;">'
        f'<p style="margin:0 0 14px 0;font-size:15px;color:{TEXT_SECONDARY};">'
        f'... and {remaining} more change(s)</p>'
        f'{build_button(browse_url, "View All on Fabric GPS")}'
        f'</div>'
    )


def _render_empty_state_card() -> str:
    return f"""
                <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;
                            padding:24px;margin:0 0 18px 0;box-shadow:{CARD_SHADOW};text-align:center;\">
                    <p style=\"margin:0;font-size:15px;color:{TEXT_SECONDARY};\">No roadmap item changes in the past week for your filters.</p>
                </div>
                """


def render_digest_html(
    changes: List[Dict[str, Any]],
    subscription: Any,
    base_url: str,
    *,
    ai_summary: Optional[str] = None,
    cadence_label: str = "Weekly",
) -> str:
    """Render the cadence digest email as HTML."""
    utm_campaign = "daily-digest" if cadence_label == "Daily" else "weekly-digest"
    unsubscribe_url = add_utm(
        f"{base_url}/unsubscribe?token={subscription.unsubscribe_token}",
        campaign=utm_campaign,
    )
    preferences_url = add_utm(
        f"{base_url}/preferences?token={subscription.unsubscribe_token}",
        campaign=utm_campaign,
    )

    preheader = (
        f"{len(changes)} Fabric roadmap item change(s) in this {cadence_label.lower()} update."
        if changes else f"Your {cadence_label.lower()} Fabric GPS update."
    )

    total_changes = len(changes)
    displayed_changes = changes[:MAX_EMAIL_CARDS]

    card_blocks: List[str] = [
        _render_change_card(c, base_url, utm_campaign) for c in displayed_changes
    ]

    if total_changes > MAX_EMAIL_CARDS:
        remaining = total_changes - MAX_EMAIL_CARDS
        card_blocks.append(
            _render_browse_more_card(remaining, subscription, base_url, utm_campaign, cadence_label)
        )

    if not card_blocks:
        card_blocks.append(_render_empty_state_card())

    changes_section = "\n".join(card_blocks)

    summary_html = ""
    if ai_summary:
        summary_html = (
            f'<tr><td style="padding:0 0 18px 0;">'
            f'<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;'
            f'padding:20px 22px;box-shadow:{CARD_SHADOW};">'
            f'<h2 style="margin:0 0 10px 0;font-size:16px;color:{TEXT_PRIMARY};font-weight:600;">'
            f'\U0001f4a1 AI Summary</h2>'
            f'<p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};">'
            f'{escape_html(ai_summary)}</p>'
            f'</div></td></tr>'
        )

    footer_links = (
        f'<a href="{escape_html(add_utm(base_url, campaign=utm_campaign))}" '
        f'style="color:#19433c;text-decoration:none;font-weight:500;">Fabric GPS</a>'
    )

    cadence_period = "the past day" if cadence_label == "Daily" else "the past 7 days"
    cadence_sub_text = f"{cadence_label.lower()} updates"

    return f"""\
<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"UTF-8\">
<title>Fabric GPS {cadence_label} Update</title>
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
<style>
body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}
</style>
</head>
<body style=\"margin:0;padding:0;background:{BODY_BG};\">
<span style=\"display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;\">{escape_html(preheader)}</span>
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"background:{BODY_BG};padding:24px 0;\">
  <tr>
    <td align=\"center\" style=\"padding:0 12px;\">
      <table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"max-width:640px;\">
        <tr>
          <td style=\"background:{HERO_GRADIENT};color:#ffffff;border-radius:14px; padding:34px 34px 38px 34px; text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);\">
            <h1 style=\"margin:0 0 10px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;\">Fabric GPS — {cadence_label} Update</h1>
            <p style=\"margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);\">Microsoft Fabric roadmap items modified during {cadence_period}.</p>
          </td>
        </tr>
        <tr><td style=\"height:28px;\"></td></tr>
        {summary_html}
        <tr><td style=\"padding:0;\">{changes_section}</td></tr>
        <tr>
          <td>
            <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px; padding:22px;margin:6px 0 26px 0;box-shadow:{CARD_SHADOW};text-align:center;\">
              <p style=\"margin:0 0 14px 0;font-size:14px;color:{TEXT_SECONDARY};\">Tune your filters or explore more history on the site.</p>
              {build_button(add_utm(base_url, campaign=utm_campaign), "Open Fabric GPS")}
            </div>
          </td>
        </tr>
        <tr>
          <td style=\"background:#f8f9fa;border:1px solid {CARD_BORDER};border-radius:10px; padding:18px 20px;text-align:center;font-size:12px;color:{TEXT_SECONDARY}; line-height:1.5;\">
            <p style=\"margin:0 0 6px 0;\">Sent to {escape_html(subscription.email)} — you’re subscribed to {cadence_sub_text}.</p>
            <p style=\"margin:0 0 6px 0;\">
              <a href=\"{escape_html(unsubscribe_url)}\" style=\"color:#19433c;text-decoration:none;font-weight:500;\">Unsubscribe</a>&nbsp;|&nbsp;<a href=\"{escape_html(preferences_url)}\" style=\"color:#19433c;text-decoration:none;font-weight:500;\">Manage Preferences</a>&nbsp;|&nbsp; Data sourced from Microsoft Fabric Roadmap
            </p>
            <p style=\"margin:8px 0 0 0;color:#8a8886;\">{footer_links}</p>
          </td>
        </tr>
        <tr><td style=\"height:30px;\"></td></tr>
      </table>
    </td>
  </tr>
</table>
</body>
</html>
"""


def render_digest_text(
    changes: List[Dict[str, Any]],
    subscription: Any,
    base_url: str,
    *,
    ai_summary: Optional[str] = None,
) -> str:
    """Render the cadence digest email as plain text."""
    utm_campaign = "daily-digest" if subscription.email_cadence == "daily" else "weekly-digest"
    unsubscribe_url = add_utm(
        f"{base_url}/unsubscribe?token={subscription.unsubscribe_token}",
        campaign=utm_campaign,
    )
    preferences_url = add_utm(
        f"{base_url}/preferences?token={subscription.unsubscribe_token}",
        campaign=utm_campaign,
    )
    cadence_label = "DAILY" if subscription.email_cadence == "daily" else "WEEKLY"

    text_parts: List[str] = [
        f"FABRIC GPS - {cadence_label} UPDATE",
        "=" * 50,
        "",
    ]

    if ai_summary:
        text_parts.extend([
            "AI SUMMARY",
            "-" * 50,
            ai_summary,
            "",
        ])

    cadence_period_text = "today's" if subscription.email_cadence == "daily" else "this week's"

    text_parts.extend([
        f"Microsoft Fabric roadmap changes — {cadence_period_text} update ({len(changes)} items):",
        "",
    ])

    for i, change in enumerate(changes, 1):
        release_date = _fmt_release_date(change.get('release_date'))

        modified_date = 'Unknown'
        if change.get('last_modified'):
            try:
                modified_date = datetime.strptime(change['last_modified'], '%Y-%m-%d').strftime('%B %d, %Y')
            except Exception:
                modified_date = change['last_modified']

        text_parts.extend([
            f"{i}. {change.get('feature_name', 'Unnamed Feature')}{' [REMOVED]' if change.get('active') is False else ''}",
            f"   Product: {change.get('product_name', 'Unknown')}",
            f"   Type: {change.get('release_type', 'Unknown')}",
            f"   Status: {change.get('release_status', 'Unknown')}",
            f"   Last Modified: {modified_date}",
            f"   Release Date: {release_date}",
            f"   Description: {change.get('feature_description', 'No description available.')}",
            "",
        ])

    text_parts.extend([
        "-" * 50,
        f"Visit {add_utm(base_url, campaign=utm_campaign)} to explore the full roadmap.",
        "",
        f"Unsubscribe: {unsubscribe_url}",
        f"Manage Preferences: {preferences_url}",
        "Data sourced from Microsoft Fabric Roadmap",
    ])

    return "\n".join(text_parts)


# ---------------------------------------------------------------------------
# Watch alert rendering
# ---------------------------------------------------------------------------
def render_watch_alert_subject(changed_watches: List[Dict[str, Any]]) -> str:
    """Return the subject line for a watch-alert email."""
    if len(changed_watches) == 1:
        return f"Fabric GPS Alert: {changed_watches[0]['feature_name']} Updated"
    return f"Fabric GPS Alert: {len(changed_watches)} Watched Features Updated"


def render_watch_alert_html(
    subscription: Any,
    changed_watches: List[Dict[str, Any]],
    base_url: str,
) -> str:
    """Render the watch-alert email as HTML."""
    unsubscribe_url = add_utm(f"{base_url}/unsubscribe?token={subscription.unsubscribe_token}")
    preferences_url = add_utm(f"{base_url}/preferences?token={subscription.unsubscribe_token}")

    items_html = ""
    for w in changed_watches:
        release_url = add_utm(
            f"{base_url}/release/{w['release_item_id']}", campaign='watch-alert'
        )
        removed_badge = (
            ' <span style="background:#d13438;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;">REMOVED</span>'
            if w.get('active') is False else ''
        )
        items_html += f"""<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;padding:18px 20px;margin-bottom:12px;box-shadow:{CARD_SHADOW};">
  <div style="font-weight:600;font-size:15px;color:#323130;margin-bottom:6px;">
    <a href="{escape_html(release_url)}" style="color:#19433c;text-decoration:none;">{escape_html(w.get('feature_name', 'Unknown'))}</a>{removed_badge}
  </div>
  <div style="font-size:13px;color:{TEXT_SECONDARY};">{escape_html(w.get('product_name', ''))} · {escape_html(w.get('release_type', ''))} · {escape_html(w.get('release_status', ''))}</div>
  <div style="font-size:12px;color:{TEXT_SECONDARY};margin-top:4px;">Last modified: {escape_html(w.get('last_modified', 'Unknown'))}</div>
</div>"""

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fabric GPS Watch Alert</title>
<style>body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}</style>
</head><body style="margin:0;padding:0;background:{BODY_BG};">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{BODY_BG};padding:24px 0;">
  <tr><td align="center" style="padding:0 12px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;">
      <tr><td style="background:{HERO_GRADIENT};color:#ffffff;border-radius:14px;padding:30px 34px;text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);">
        <h1 style="margin:0 0 8px 0;font-size:24px;font-weight:600;">Fabric GPS</h1>
        <p style="margin:0 0 4px 0;font-size:16px;font-weight:600;">Feature Watch Alert</p>
        <p style="margin:0;font-size:14px;color:rgba(255,255,255,0.9);">Features you're watching have been updated.</p>
      </td></tr>
      <tr><td style="height:24px;"></td></tr>
      <tr><td style="padding:0;">{items_html}</td></tr>
      <tr><td style="height:12px;"></td></tr>
      <tr><td style="background:#f8f9fa;border:1px solid {CARD_BORDER};border-radius:10px;padding:18px 20px;text-align:center;font-size:12px;color:{TEXT_SECONDARY};line-height:1.5;">
        <p style="margin:0 0 6px 0;">Sent to {escape_html(subscription.email)}</p>
        <p style="margin:0 0 6px 0;">
          <a href="{escape_html(preferences_url)}" style="color:#19433c;text-decoration:none;font-weight:500;">Manage Watches</a>&nbsp;|&nbsp;
          <a href="{escape_html(unsubscribe_url)}" style="color:#19433c;text-decoration:none;font-weight:500;">Unsubscribe</a>
        </p>
      </td></tr>
      <tr><td style="height:30px;"></td></tr>
    </table>
  </td></tr>
</table></body></html>"""


def render_watch_alert_text(
    subscription: Any,
    changed_watches: List[Dict[str, Any]],
    base_url: str,
) -> str:
    """Render the watch-alert email as plain text."""
    unsubscribe_url = add_utm(f"{base_url}/unsubscribe?token={subscription.unsubscribe_token}")
    preferences_url = add_utm(f"{base_url}/preferences?token={subscription.unsubscribe_token}")

    text_parts: List[str] = ["FABRIC GPS - FEATURE WATCH ALERT", "=" * 50, ""]
    for w in changed_watches:
        removed = " [REMOVED]" if w.get('active') is False else ""
        text_parts.extend([
            f"• {w.get('feature_name', 'Unknown')}{removed}",
            f"  Product: {w.get('product_name', '')}",
            f"  Status: {w.get('release_status', '')}",
            f"  Link: {base_url}/release/{w['release_item_id']}",
            "",
        ])
    text_parts.extend([
        "-" * 50,
        f"Manage Watches: {preferences_url}",
        f"Unsubscribe: {unsubscribe_url}",
    ])
    return "\n".join(text_parts)
