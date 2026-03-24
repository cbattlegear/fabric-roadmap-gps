#!/usr/bin/env python3
"""
Weekly Email Job for Fabric GPS
Sends weekly summary emails to all active subscribers using JSON API and Azure Communication Services

Run this script weekly via cron job:
0 9 * * 1 /path/to/python /path/to/weekly_email_job.py
"""

import os
import sys
import json
import requests
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from azure.communication.email import EmailClient

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.db_sqlserver import (
    make_engine, get_unsent_active_subscriptions, EmailSubscriptionModel, EmailVerificationModel
)

try:
    from openai import AzureOpenAI
    _OPENAI_AVAILABLE = True
except ImportError:
    _OPENAI_AVAILABLE = False

os.environ['OTEL_SERVICE_NAME'] = 'fabric-gps-email-job'

# Configure logging
logger_name = 'fabric-gps-email'
opentelemetery_logger_name = f'{logger_name}.opentelemetry'

if os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING") and os.getenv("CURRENT_ENVIRONMENT") != "development":
    configure_azure_monitor(
        logger_name=opentelemetery_logger_name,
        enable_live_metrics=True 
    )

logger = logging.getLogger(opentelemetery_logger_name)
stream = logging.StreamHandler()
logger.addHandler(stream)
logger.setLevel(logging.INFO)
logger.info('Fabric-GPS Email Batch Job started')


class WeeklyEmailSender:
    def __init__(self):
        # Azure Communication Services configuration
        self.connection_string = os.getenv('AZURE_COMMUNICATION_CONNECTION_STRING')
        self.from_email = os.getenv('FROM_EMAIL', 'noreply@yourdomain.com')
        self.from_name = os.getenv('FROM_NAME', 'Fabric GPS')
        self.base_url = os.getenv('BASE_URL', 'http://localhost:8000')
        
        if not self.connection_string:
            raise ValueError("AZURE_COMMUNICATION_CONNECTION_STRING environment variable is required")
        
        # Initialize Azure Email Client
        self.email_client = EmailClient.from_connection_string(self.connection_string)

    @staticmethod
    def _add_utm(url: str, source: str = 'email', medium: str = 'email',
                 campaign: str = 'weekly-digest') -> str:
        """Append UTM tracking parameters to a URL, respecting hash fragments."""
        params = f"utm_source={source}&utm_medium={medium}&utm_campaign={campaign}"
        if '#' in url:
            base, fragment = url.split('#', 1)
            sep = '&' if '?' in base else '?'
            return f"{base}{sep}{params}#{fragment}"
        sep = '&' if '?' in url else '?'
        return f"{url}{sep}{params}"

    def send_weekly_emails(self):
        """Send weekly emails to all active subscribers"""
        try:
            engine = make_engine()
            SQLAlchemyInstrumentor().instrument(engine=engine)
            subscriptions = get_unsent_active_subscriptions(engine, 7)
            
            logger.info(f"Found {len(subscriptions)} active subscriptions")

            if not subscriptions:
                logger.info("No subscriptions to process, exiting")
                return

            # Fetch all unfiltered changes once and generate AI summary once
            all_changes = self._fetch_all_changes_unfiltered()
            ai_summary = self.generate_ai_summary(all_changes)
            if ai_summary:
                logger.info("AI summary generated successfully")
            else:
                logger.info("AI summary not available, emails will be sent without it")
            
            sent_count = 0
            error_count = 0
            
            for subscription in subscriptions:
                try:
                    if self.send_weekly_email(subscription, ai_summary=ai_summary):
                        sent_count += 1
                        self.update_last_email_sent(subscription.id)
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error sending email to {subscription.email}: {e}")
                    error_count += 1
            
            logger.info(f"Weekly email job completed. Sent: {sent_count}, Errors: {error_count}")
            # Run cleanup after sending
            try:
                cleanup_counts = self.cleanup_expired(engine)
                logger.info(
                    "Cleanup complete: expired_or_used_verifications=%d, stale_unverified_subscriptions=%d",
                    cleanup_counts.get('expired_or_used_verifications', 0),
                    cleanup_counts.get('stale_unverified', 0)
                )
            except Exception as cleanup_exc:
                logger.error(f"Cleanup step failed: {cleanup_exc}")
            
        except Exception as e:
            logger.error(f"Fatal error in weekly email job: {e}")
            raise

    def send_weekly_email(self, subscription: EmailSubscriptionModel, ai_summary: Optional[str] = None) -> bool:
        """Send weekly email to a single subscriber using JSON API"""
        try:
            # Get changes using JSON API with subscriber's filters
            changes = self.get_changes_from_api(subscription)
            
            if not changes:
                logger.info(f"No changes for {subscription.email}, skipping")
                return True
            
            # Generate email content
            subject = f"Fabric GPS Weekly Update - {len(changes)} Changes"
            html_content = self.generate_email_html(changes, subscription, ai_summary=ai_summary)
            text_content = self.generate_email_text(changes, subscription, ai_summary=ai_summary)
            
            # Send email using Azure Communication Services
            success = self.send_azure_email(
                to_email=subscription.email,
                subject=subject,
                html_content=html_content,
                text_content=text_content,
                unsubscribe_token=subscription.unsubscribe_token
            )
            
            if success:
                logger.info(f"Successfully sent weekly email to {subscription.email}")
                return True
            else:
                logger.error(f"Failed to send email to {subscription.email}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing subscription {subscription.email}: {e}")
            return False

    def _extract_items(self, payload: Any) -> List[Dict[str, Any]]:
        """
        Support both new spec (envelope with 'data') and legacy raw list.
        """
        if isinstance(payload, dict) and 'data' in payload and isinstance(payload['data'], list):
            return payload['data']
        if isinstance(payload, list):
            return payload
        return []

    def _fetch_all_pages(self, base_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetch all pages for given param set using new paginated API.
        Stops if API returns an empty page or pagination.has_next is False.
        """
        page = 1
        all_items: List[Dict[str, Any]] = []
        while True:
            params = dict(base_params)
            params['page'] = page
            resp = requests.get(f"{self.base_url}/api/releases", params=params, timeout=30)
            if resp.status_code != 200:
                logger.error(f"API page request failed (status {resp.status_code}) params={params}")
                break
            payload = resp.json()
            items = self._extract_items(payload)
            if not items:
                break
            all_items.extend(items)
            # Determine if more pages
            pagination = payload.get('pagination') if isinstance(payload, dict) else None
            if not pagination or not pagination.get('has_next'):
                break
            page += 1
            # Safety cap to avoid runaway loops (unlikely)
            if page > 20:
                logger.warning("Pagination exceeded 20 pages; stopping early.")
                break
        return all_items

    def _fetch_all_changes_unfiltered(self) -> List[Dict[str, Any]]:
        """Fetch all changes from the past week without subscriber-specific filters."""
        base_params = {'modified_within_days': 7, 'page_size': 200}
        items = self._fetch_all_pages(base_params)
        # Deduplicate
        seen: Dict[str, Dict[str, Any]] = {}
        for item in items:
            rid = item.get('release_item_id')
            if rid and rid not in seen:
                seen[rid] = item
        return list(seen.values())

    def generate_ai_summary(self, changes: List[Dict[str, Any]]) -> Optional[str]:
        """Generate a single AI summary of all weekly changes using Azure OpenAI.

        Returns the summary text or None if unavailable.
        """
        if not changes:
            return None
        if not _OPENAI_AVAILABLE:
            logger.info("OpenAI SDK not installed, skipping AI summary")
            return None
        endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        deployment = os.getenv('AZURE_OPENAI_CHAT_DEPLOYMENT', 'gpt-4o-mini')
        if not endpoint or not api_key:
            logger.info("Azure OpenAI not configured, skipping AI summary")
            return None
        try:
            client = AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version="2024-02-01"
            )
            # Build a compact representation of changes for the prompt
            change_lines = []
            for c in changes[:50]:
                line = (
                    f"- {c.get('feature_name', 'Unknown')} "
                    f"[{c.get('product_name', '')}] "
                    f"({c.get('release_type', '')}, {c.get('release_status', '')})"
                )
                change_lines.append(line)
            changes_text = "\n".join(change_lines)

            response = client.chat.completions.create(
                model=deployment,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You summarize Microsoft Fabric roadmap changes for a weekly email newsletter. "
                            "Write a concise 2-4 sentence executive summary highlighting the most important "
                            "themes and notable changes. Be specific about product areas and feature names. "
                            "Do not use markdown formatting. Write in a professional but approachable tone."
                        )
                    },
                    {
                        "role": "user",
                        "content": f"Summarize these {len(changes)} Microsoft Fabric roadmap changes from the past week:\n\n{changes_text}"
                    }
                ],
                max_completion_tokens=300
            )
            summary = response.choices[0].message.content.strip()
            logger.info(f"AI summary generated ({len(summary)} chars)")
            return summary
        except Exception as e:
            logger.error(f"AI summary generation failed: {e}")
            return None

# Replace the existing get_changes_from_api method with:

    def get_changes_from_api(self, subscription: EmailSubscriptionModel) -> List[Dict[str, Any]]:
        """
        Get last week's changes via paginated API, honoring subscriber filters.
        De-duplicates across multiple product queries, applies additional filters,
        sorts desc by last_modified, and caps to 50 items.
        """
        try:
            BASE_PARAMS = {
                'modified_within_days': 7,
                'page_size': 200  # request larger page size to reduce pagination loops
            }

            aggregated: List[Dict[str, Any]] = []

            if subscription.product_filter:
                products = [p.strip() for p in subscription.product_filter.split(',') if p.strip()]
                if products:
                    for product in products:
                        per_product_params = dict(BASE_PARAMS)
                        per_product_params['product_name'] = product
                        aggregated.extend(self._fetch_all_pages(per_product_params))
                else:
                    aggregated.extend(self._fetch_all_pages(BASE_PARAMS))
            else:
                aggregated.extend(self._fetch_all_pages(BASE_PARAMS))

            # Deduplicate by release_item_id
            deduped: Dict[str, Dict[str, Any]] = {}
            for item in aggregated:
                rid = item.get('release_item_id')
                if not rid:
                    continue
                # Keep first occurrence (they should be identical across pages/products)
                if rid not in deduped:
                    deduped[rid] = item
            items = list(deduped.values())

            # Apply optional release type/status filters
            if subscription.release_type_filter:
                types = {t.strip() for t in subscription.release_type_filter.split(',') if t.strip()}
                if types:
                    items = [c for c in items if c.get('release_type') in types]

            if subscription.release_status_filter:
                statuses = {s.strip() for s in subscription.release_status_filter.split(',') if s.strip()}
                if statuses:
                    items = [c for c in items if c.get('release_status') in statuses]

            # Robust sort by last_modified (ISO date or fallback)
            def _lm_key(x):
                lm = x.get('last_modified')
                return lm or ""
            items.sort(key=_lm_key, reverse=True)

            return items[:50]

        except requests.RequestException as e:
            logger.error(f"Network/API error retrieving changes for {subscription.email}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error processing changes for {subscription.email}: {e}")
            return []

    def update_last_email_sent(self, subscription_id: str):
        """Update the last_email_sent date for a subscription"""
        try:
            engine = make_engine()
            from sqlalchemy.orm import sessionmaker
            SessionLocal = sessionmaker(bind=engine, future=True)
            with SessionLocal() as session:
                subscription = session.get(EmailSubscriptionModel, subscription_id)
                if subscription:
                    subscription.last_email_sent = date.today()
                    session.commit()
        except Exception as e:
            logger.error(f"Error updating last_email_sent for {subscription_id}: {e}")

    def _build_badge(self, text: str, variant: str) -> str:
        """Return a styled badge span consistent with site palette."""
        if not text:
            text = "Unknown"
        colors = {
            "product": ("#004578", "#ffffff"),
            "success": ("#107c10", "#ffffff"),
            "warning": ("#ca5010", "#ffffff"),
            "neutral": ("#605e5c", "#ffffff"),
        }
        bg, fg = colors.get(variant, colors["neutral"])
        return (
            f'<span style="display:inline-block;margin:0 6px 6px 0;'
            f'padding:4px 10px;font-size:12px;font-weight:600;letter-spacing:.25px;'
            f'border-radius:999px;background:{bg};color:{fg};white-space:nowrap;">'
            f'{self.escape_html(text)}</span>'
        )

    def _build_button(self, href: str, label: str) -> str:
        return (
            f'<a href="{self.escape_html(href)}" '
            f'style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);'
            f'color:#ffffff;text-decoration:none;padding:10px 18px;font-size:14px;font-weight:600;'
            f'border-radius:6px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.15);">'
            f'{self.escape_html(label)}</a>'
        )

    def generate_email_html(self, changes: List[Dict[str, Any]], subscription: EmailSubscriptionModel, ai_summary: Optional[str] = None) -> str:
        """Generate HTML email content styled to match index page design."""
        unsubscribe_url = self._add_utm(f"{self.base_url}/unsubscribe?token={subscription.unsubscribe_token}")

        # Style tokens (aligned with site)
        BODY_BG = "#f3f2f1"
        CARD_BG = "#ffffff"
        CARD_BORDER = "#e1e5e9"
        CARD_SHADOW = "0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06)"
        TEXT_PRIMARY = "#323130"
        TEXT_SECONDARY = "#605e5c"
        HERO_GRADIENT = "linear-gradient(135deg,#19433c 0%,#286c61 100%)"

        preheader = f"{len(changes)} Fabric roadmap item change(s) this week." if changes else "Your weekly Fabric GPS update." 

        def fmt_date(dt_str: str, fallback: str = "TBD", out_fmt: str = "%b %d, %Y") -> str:
            if not dt_str:
                return fallback
            for pattern in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
                try:
                    return datetime.strptime(dt_str[:len(pattern)], pattern).strftime(out_fmt)
                except Exception:
                    continue
            return dt_str

        card_blocks: List[str] = []
        for change in changes:
            feature_name = change.get('feature_name') or 'Unnamed Feature'
            product_name = change.get('product_name') or 'Unknown'
            release_type = change.get('release_type') or 'Unknown'
            release_status = change.get('release_status') or 'Unknown'
            description = change.get('feature_description') or 'No description available.'
            rel_id = change.get('release_item_id')
            release_date = fmt_date(change.get('release_date'))
            modified_date = fmt_date(change.get('last_modified'), fallback="Unknown")
            release_type_variant = "success" if release_type == "General availability" else "warning"
            release_status_variant = "success" if release_status == "Shipped" else "warning"
            detail_url = self._add_utm(f"{self.base_url}/#release/{rel_id}") if rel_id else self._add_utm(self.base_url)
            badges_html = (
                self._build_badge(product_name, "product") +
                self._build_badge(release_type, release_type_variant) +
                self._build_badge(release_status, release_status_variant)
            )
            card_blocks.append(
                f"""
                <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;\n                            padding:18px;margin:0 0 18px 0;box-shadow:{CARD_SHADOW};\">\n                    <h3 style=\"margin:0 0 10px 0;font-size:18px;line-height:1.3;color:{TEXT_PRIMARY};font-weight:600;\">\n                        <a href=\"{self.escape_html(detail_url)}\" style=\"color:{TEXT_PRIMARY};text-decoration:none;\">{self.escape_html(feature_name)}\n                        </a>\n                    </h3>\n                    <div style=\"margin:0 0 10px 0;\">{badges_html}</div>\n                    <div style=\"font-size:12px;color:{TEXT_SECONDARY};margin:0 0 12px 0;\">\n                        <strong>Last Modified:</strong> {self.escape_html(modified_date)} &nbsp;|&nbsp;\n                        <strong>Release Date:</strong> {self.escape_html(release_date)}\n                    </div>\n                    <p style=\"margin:0 0 14px 0;font-size:14px;line-height:1.5;color:{TEXT_PRIMARY};\">{self.escape_html(description)}\n                    </p>\n                    {self._build_button(detail_url, "View in Fabric GPS")}\n                </div>\n                """
            )

        if not card_blocks:
            card_blocks.append(
                f"""
                <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;\n                            padding:24px;margin:0 0 18px 0;box-shadow:{CARD_SHADOW};text-align:center;\">\n                    <p style=\"margin:0;font-size:15px;color:{TEXT_SECONDARY};\">No roadmap item changes in the past week for your filters.</p>\n                </div>\n                """
            )

        changes_section = "\n".join(card_blocks)

        # AI summary block (inserted between hero and change cards)
        summary_html = ""
        if ai_summary:
            summary_html = (
                f'<tr><td style="padding:0 0 18px 0;">'
                f'<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;'
                f'padding:20px 22px;box-shadow:{CARD_SHADOW};">'
                f'<h2 style="margin:0 0 10px 0;font-size:16px;color:{TEXT_PRIMARY};font-weight:600;">'
                f'\U0001f4a1 AI Weekly Summary</h2>'
                f'<p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};">'
                f'{self.escape_html(ai_summary)}</p>'
                f'</div></td></tr>'
            )

        footer_links = (
            f'<a href="{self.escape_html(self._add_utm(self.base_url))}" style="color:#19433c;text-decoration:none;font-weight:500;">Fabric GPS</a>'
        )

        return f"""\
<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"UTF-8\">
<title>Fabric GPS Weekly Update</title>
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
<style>
body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}
</style>
</head>
<body style=\"margin:0;padding:0;background:{BODY_BG};\">
<span style=\"display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;\">{self.escape_html(preheader)}</span>
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"background:{BODY_BG};padding:24px 0;\">\n  <tr>\n    <td align=\"center\" style=\"padding:0 12px;\">\n      <table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"max-width:640px;\">\n        <tr>\n          <td style=\"background:{HERO_GRADIENT};color:#ffffff;border-radius:14px; padding:34px 34px 38px 34px; text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);\">\n            <h1 style=\"margin:0 0 10px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;\">🗺️ Fabric GPS Weekly Update</h1>\n            <p style=\"margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);\">Microsoft Fabric roadmap items modified during the past 7 days.</p>\n          </td>\n        </tr>\n        <tr><td style=\"height:28px;\"></td></tr>\n        {summary_html}\n        <tr><td style=\"padding:0;\">{changes_section}</td></tr>\n        <tr>\n          <td>\n            <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px; padding:22px;margin:6px 0 26px 0;box-shadow:{CARD_SHADOW};text-align:center;\">\n              <p style=\"margin:0 0 14px 0;font-size:14px;color:{TEXT_SECONDARY};\">Tune your filters or explore more history on the site.</p>\n              {self._build_button(self._add_utm(self.base_url), "Open Fabric GPS")}\n            </div>\n          </td>\n        </tr>\n        <tr>\n          <td style=\"background:#f8f9fa;border:1px solid {CARD_BORDER};border-radius:10px; padding:18px 20px;text-align:center;font-size:12px;color:{TEXT_SECONDARY}; line-height:1.5;\">\n            <p style=\"margin:0 0 6px 0;\">Sent to {self.escape_html(subscription.email)} — you’re subscribed to weekly updates.</p>\n            <p style=\"margin:0 0 6px 0;\">\n              <a href=\"{self.escape_html(unsubscribe_url)}\" style=\"color:#19433c;text-decoration:none;font-weight:500;\">Unsubscribe</a>&nbsp;|&nbsp; Data sourced from Microsoft Fabric Roadmap\n            </p>\n            <p style=\"margin:8px 0 0 0;color:#8a8886;\">{footer_links}</p>\n          </td>\n        </tr>\n        <tr><td style=\"height:30px;\"></td></tr>\n      </table>\n    </td>\n  </tr>\n</table>\n</body>\n</html>\n"""

    def generate_email_text(self, changes: List[Dict[str, Any]], subscription: EmailSubscriptionModel, ai_summary: Optional[str] = None) -> str:
        """Generate plain text email content from JSON API data"""
        unsubscribe_url = self._add_utm(f"{self.base_url}/unsubscribe?token={subscription.unsubscribe_token}")

        text_parts = [
            "FABRIC GPS - WEEKLY UPDATE",
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

        text_parts.extend([
            f"This week's Microsoft Fabric roadmap changes ({len(changes)} items):",
            ""
        ])
        
        for i, change in enumerate(changes, 1):
            release_date = 'TBD'
            if change.get('release_date'):
                try:
                    release_date = datetime.strptime(change['release_date'], '%Y-%m-%d').strftime('%B %d, %Y')
                except:
                    release_date = change['release_date']
            
            modified_date = 'Unknown'
            if change.get('last_modified'):
                try:
                    modified_date = datetime.strptime(change['last_modified'], '%Y-%m-%d').strftime('%B %d, %Y')
                except:
                    modified_date = change['last_modified']
            
            text_parts.extend([
                f"{i}. {change.get('feature_name', 'Unnamed Feature')}",
                f"   Product: {change.get('product_name', 'Unknown')}",
                f"   Type: {change.get('release_type', 'Unknown')}",
                f"   Status: {change.get('release_status', 'Unknown')}",
                f"   Last Modified: {modified_date}",
                f"   Release Date: {release_date}",
                f"   Description: {change.get('feature_description', 'No description available.')}",
                ""
            ])
        
        text_parts.extend([
            "-" * 50,
            f"Visit {self._add_utm(self.base_url)} to explore the full roadmap.",
            "",
            f"Unsubscribe: {unsubscribe_url}",
            "Data sourced from Microsoft Fabric Roadmap"
        ])
        
        return "\n".join(text_parts)

    def send_azure_email(self, to_email: str, subject: str, html_content: str, text_content: str, unsubscribe_token: str) -> bool:
        """Send an email using Azure Communication Services"""
        try:
            unsubscribe_url = self._add_utm(f"{self.base_url}/unsubscribe?token={unsubscribe_token}")
            
            message = {
                "senderAddress": self.from_email,
                "recipients": {
                    "to": [{"address": to_email}]
                },
                "content": {
                    "subject": subject,
                    "plainText": text_content,
                    "html": html_content
                },
                "headers": {
                    "List-Unsubscribe": f"<{unsubscribe_url}>",
                    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"
                }
            }
            
            POLLER_WAIT_TIME = 10

            poller = self.email_client.begin_send(message)
            time_elapsed = 0
            while not poller.done():
                print("Email send poller status: " + poller.status())

                poller.wait(POLLER_WAIT_TIME)
                time_elapsed += POLLER_WAIT_TIME

                if time_elapsed > 18 * POLLER_WAIT_TIME:
                    raise RuntimeError("Polling timed out.")

            if poller.result()["status"] == "Succeeded":
                print(f"Successfully sent the email (operation id: {poller.result()['id']})")
                return True
            else:
                raise RuntimeError(str(poller.result()["error"]))
            
        except Exception as e:
            logger.error(f"Azure Communication Services error sending to {to_email}: {e}")
            return False

    def escape_html(self, text: str) -> str:
        """Escape HTML special characters"""
        if not text:
            return ""
        return (text.replace('&', '&amp;')
                   .replace('<', '&lt;')
                   .replace('>', '&gt;')
                   .replace('"', '&quot;')
                   .replace("'", '&#x27;'))

    def cleanup_expired(self, engine):
        """Remove:
        - Expired verification records (expires_at < now or is_used True)
        - Used verification records (is_used = True)
        - Stale unverified subscriptions (verification_token not null, is_verified False, created_at older than 24h)
        Returns dict of counts removed.
        """
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import delete, or_, and_
        SessionLocal = sessionmaker(bind=engine, future=True)
        now = datetime.utcnow()
        threshold = now - timedelta(hours=24)
        counts = {"expired_or_used_verifications": 0, "stale_unverified": 0}
        with SessionLocal() as session:
            # Expired or used verifications
            expired_stmt = delete(EmailVerificationModel).where(
                or_(EmailVerificationModel.expires_at < now, EmailVerificationModel.is_used == True)
            )
            result_expired = session.execute(expired_stmt)
            counts["expired_or_used_verifications"] = result_expired.rowcount or 0

            # Stale unverified subscriptions
            stale_stmt = delete(EmailSubscriptionModel).where(
                and_(
                    EmailSubscriptionModel.is_verified == False,
                    EmailSubscriptionModel.verification_token.isnot(None),
                    EmailSubscriptionModel.created_at < threshold,
                )
            )
            result_stale = session.execute(stale_stmt)
            counts["stale_unverified"] = result_stale.rowcount or 0

            session.commit()
        return counts


def main():
    """Main function to run the weekly email job"""
    try:
        logger.info("Starting weekly email job")
        sender = WeeklyEmailSender()
        sender.send_weekly_emails()
        logger.info("Weekly email job completed successfully")
    except Exception as e:
        logger.error(f"Weekly email job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
