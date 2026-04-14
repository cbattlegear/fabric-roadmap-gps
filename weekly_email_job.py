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
import time
import hashlib
import requests
import logging
import sqlalchemy.exc
from datetime import datetime, timedelta
from urllib.parse import urlencode
from typing import List, Dict, Any, Optional
from azure.communication.email import EmailClient

from azure.monitor.opentelemetry import configure_azure_monitor

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.db_sqlserver import (
    make_engine, get_unsent_active_subscriptions, EmailSubscriptionModel,
    EmailVerificationModel, EmailContentCacheModel,
    get_digest_eligible_subscriptions, get_subscriptions_with_changed_watches,
    update_watch_hashes,
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
        canonical = os.getenv('CANONICAL_HOST', '')
        self.base_url = f"https://{canonical}" if canonical else os.getenv('BASE_URL', 'http://localhost:8000')
        
        if not self.connection_string:
            raise ValueError("AZURE_COMMUNICATION_CONNECTION_STRING environment variable is required")
        
        # Initialize Azure Email Client
        self.email_client = EmailClient.from_connection_string(self.connection_string)

        # In-memory cache for raw 7-day data (populated once per run)
        self._raw_changes = None
        # Engine shared across the current job run (set by send_emails)
        self._engine = None

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

    def _fetch_raw_changes(self) -> List[Dict[str, Any]]:
        """Fetch all 7-day changes, cached in-memory for this run."""
        if self._raw_changes is None:
            self._raw_changes = self._fetch_all_changes_unfiltered()
            logger.info("Fetched %d raw changes from API", len(self._raw_changes))
        return self._raw_changes

    @staticmethod
    def _build_cache_key(subscription: EmailSubscriptionModel) -> str:
        """Build a deterministic, collision-free cache key from subscriber cadence + filters.

        Uses SHA-256 of the full key string to avoid truncation collisions when
        subscribers have long filter lists.
        """
        parts = [
            subscription.email_cadence or 'weekly',
            (subscription.product_filter or '').strip().lower(),
            (subscription.release_type_filter or '').strip().lower(),
            (subscription.release_status_filter or '').strip().lower(),
        ]
        raw = '|'.join(parts)
        return hashlib.sha256(raw.encode()).hexdigest()

    def _get_subscriber_content(self, subscription: EmailSubscriptionModel):
        """Return (changes, ai_summary) for a subscriber, using per-date per-filter DB cache.

        On the first call for a given (date, filter-combo) the content is
        generated from the raw 7-day data, an AI summary is produced, and
        both are stored in the cache. Subsequent calls with the same key
        on the same date reuse the cached content so every subscriber
        with identical settings gets the exact same email.
        """
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import select as sa_select

        cache_key = self._build_cache_key(subscription)
        cache_date = datetime.utcnow().strftime('%Y-%m-%d')

        engine = self._engine or make_engine()
        SessionLocal = sessionmaker(bind=engine, future=True)

        # Try DB cache
        try:
            with SessionLocal() as session:
                row = session.scalar(
                    sa_select(EmailContentCacheModel)
                    .where(EmailContentCacheModel.cache_date == cache_date)
                    .where(EmailContentCacheModel.cache_key == cache_key)
                )
                if row:
                    cached = json.loads(row.content_json)
                    logger.info("Cache hit for key=%s date=%s", cache_key[:40], cache_date)
                    return cached['changes'], cached.get('ai_summary')
        except Exception as e:
            logger.warning(f"Cache read failed for key={cache_key[:40]}: {e}")

        # Cache miss: filter from raw data
        items = list(self._fetch_raw_changes())

        if subscription.email_cadence == 'daily':
            # `/api/releases` exposes `last_modified` as a date-only string (`YYYY-MM-DD`),
            # so daily filtering must use a date-based cutoff rather than a timestamp.
            cutoff_date = (datetime.utcnow() - timedelta(days=1)).date()
            filtered_items = []
            for c in items:
                last_modified = c.get('last_modified')
                if not last_modified:
                    continue
                try:
                    last_modified_date = datetime.strptime(last_modified, '%Y-%m-%d').date()
                except ValueError:
                    logger.warning("Skipping item with invalid last_modified value: %s", last_modified)
                    continue
                if last_modified_date >= cutoff_date:
                    filtered_items.append(c)
            items = filtered_items

        if subscription.product_filter:
            products = {p.strip().lower() for p in subscription.product_filter.split(',') if p.strip()}
            if products:
                items = [c for c in items if (c.get('product_name') or '').lower() in products]

        if subscription.release_type_filter:
            types = {t.strip() for t in subscription.release_type_filter.split(',') if t.strip()}
            if types:
                items = [c for c in items if c.get('release_type') in types]

        if subscription.release_status_filter:
            statuses = {s.strip() for s in subscription.release_status_filter.split(',') if s.strip()}
            if statuses:
                items = [c for c in items if c.get('release_status') in statuses]

        items.sort(key=lambda x: x.get('last_modified') or '', reverse=True)
        items = items[:50]

        # Generate AI summary specific to this filtered set
        ai_summary = self.generate_ai_summary(items) if items else None

        # Write to cache
        try:
            content = json.dumps({'changes': items, 'ai_summary': ai_summary},
                                 separators=(',', ':'))
            with SessionLocal() as session:
                with session.begin():
                    session.add(EmailContentCacheModel(
                        cache_date=cache_date,
                        cache_key=cache_key,
                        generated_at=datetime.utcnow(),
                        content_json=content,
                    ))
            logger.info("Cached content for key=%s (%d items)", cache_key[:40], len(items))
        except sqlalchemy.exc.IntegrityError:
            # Another process already cached this key; safe to ignore
            logger.info("Cache key=%s already exists (concurrent write), using generated content", cache_key[:40])
        except Exception as e:
            logger.error(f"Cache write failed for key={cache_key[:40]}: {e}")

        return items, ai_summary

    def send_emails(self):
        """Send digest and watch alert emails to eligible subscribers."""
        try:
            engine = make_engine()
            self._engine = engine  # share engine across _get_subscriber_content calls

            # --- Digest queue: daily/weekly subscribers whose interval elapsed ---
            subscriptions = get_digest_eligible_subscriptions(engine)
            logger.info(f"Digest queue: {len(subscriptions)} eligible subscriptions")

            sent_count = 0
            error_count = 0
            # Azure Communication Services quota: 100 emails/min, 1000 emails/hr.
            # Reserve ~20% headroom for transactional emails (verification, preferences links)
            # sent by the web server from the same quota.
            MIN_SEND_INTERVAL = 0.75  # seconds between sends (~80/min)
            MAX_HOURLY_SENDS = 800    # leave 200/hr for transactional emails
            last_send_time = 0.0
            hourly_sent = 0
            hourly_window_start = time.monotonic()

            for subscription in subscriptions:
                try:
                    # Enforce hourly quota
                    elapsed_in_window = time.monotonic() - hourly_window_start
                    if elapsed_in_window >= 3600:
                        hourly_sent = 0
                        hourly_window_start = time.monotonic()
                    elif hourly_sent >= MAX_HOURLY_SENDS:
                        wait_time = 3600 - elapsed_in_window
                        logger.info(f"Hourly quota reached ({MAX_HOURLY_SENDS}), pausing {wait_time:.0f}s")
                        time.sleep(wait_time)
                        hourly_sent = 0
                        hourly_window_start = time.monotonic()

                    # Enforce per-minute rate
                    elapsed = time.monotonic() - last_send_time
                    if elapsed < MIN_SEND_INTERVAL:
                        time.sleep(MIN_SEND_INTERVAL - elapsed)

                    if self.send_digest_email(subscription):
                        sent_count += 1
                        hourly_sent += 1
                        last_send_time = time.monotonic()
                        self.update_last_email_sent(subscription.id)
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error sending digest to {subscription.email}: {e}")
                    error_count += 1

            logger.info(f"Digest queue completed. Sent: {sent_count}, Errors: {error_count}")

            # --- Watch alert queue: all subscribers with changed watches ---
            watch_sent = 0
            watch_errors = 0
            try:
                watch_results = get_subscriptions_with_changed_watches(engine)
                logger.info(f"Watch alert queue: {len(watch_results)} subscribers with changed watches")

                for sub, changed_watches in watch_results:
                    try:
                        # Enforce hourly quota (shared with digest sends)
                        elapsed_in_window = time.monotonic() - hourly_window_start
                        if elapsed_in_window >= 3600:
                            hourly_sent = 0
                            hourly_window_start = time.monotonic()
                        elif hourly_sent >= MAX_HOURLY_SENDS:
                            wait_time = 3600 - elapsed_in_window
                            logger.info(f"Hourly quota reached ({MAX_HOURLY_SENDS}), pausing {wait_time:.0f}s")
                            time.sleep(wait_time)
                            hourly_sent = 0
                            hourly_window_start = time.monotonic()

                        elapsed = time.monotonic() - last_send_time
                        if elapsed < MIN_SEND_INTERVAL:
                            time.sleep(MIN_SEND_INTERVAL - elapsed)

                        if self.send_watch_alert_email(sub, changed_watches):
                            watch_sent += 1
                            hourly_sent += 1
                            last_send_time = time.monotonic()
                            hash_updates = [(w['watch_id'], w['current_hash']) for w in changed_watches]
                            update_watch_hashes(engine, hash_updates)
                        else:
                            watch_errors += 1
                    except Exception as e:
                        logger.error(f"Error sending watch alert to {sub.email}: {e}")
                        watch_errors += 1

                logger.info(f"Watch alert queue completed. Sent: {watch_sent}, Errors: {watch_errors}")
            except Exception as e:
                logger.error(f"Error processing watch alerts: {e}")

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
            logger.error(f"Fatal error in email job: {e}")
            raise

    # Keep old name as alias for backward compatibility
    send_weekly_emails = send_emails

    def send_digest_email(self, subscription: EmailSubscriptionModel) -> bool:
        """Send a digest email to a single subscriber."""
        try:
            # Get filtered changes + AI summary from per-key cache
            changes, ai_summary = self._get_subscriber_content(subscription)
            
            if not changes:
                logger.info(f"No changes for {subscription.email}, skipping")
                return True
            
            # Generate email content
            cadence_label = "Daily" if subscription.email_cadence == "daily" else "Weekly"
            subject = f"Fabric GPS {cadence_label} Update - {len(changes)} Changes"

            html_content = self.generate_email_html(changes, subscription, ai_summary=ai_summary, cadence_label=cadence_label)
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
        base_params = {'modified_within_days': 7, 'page_size': 200, 'include_inactive': 'true'}
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
                removed_tag = " [REMOVED FROM ROADMAP]" if c.get('active') is False else ""
                line = (
                    f"- {c.get('feature_name', 'Unknown')} "
                    f"[{c.get('product_name', '')}] "
                    f"({c.get('release_type', '')}, {c.get('release_status', '')})"
                    f"{removed_tag}"
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
                            "themes and notable changes. Items tagged [REMOVED FROM ROADMAP] have been taken "
                            "off the public roadmap - mention significant removals if any. "
                            "Be specific about product areas and feature names. "
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

    def update_last_email_sent(self, subscription_id: str):
        """Update the last_email_sent timestamp for a subscription"""
        try:
            engine = self._engine or make_engine()
            from sqlalchemy.orm import sessionmaker
            SessionLocal = sessionmaker(bind=engine, future=True)
            with SessionLocal() as session:
                subscription = session.get(EmailSubscriptionModel, subscription_id)
                if subscription:
                    subscription.last_email_sent = datetime.utcnow()
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
            "removed": ("#d13438", "#ffffff"),
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

    def generate_email_html(self, changes: List[Dict[str, Any]], subscription: EmailSubscriptionModel, ai_summary: Optional[str] = None, cadence_label: str = "Weekly") -> str:
        """Generate HTML email content styled to match index page design."""
        utm_campaign = "daily-digest" if cadence_label == "Daily" else "weekly-digest"
        unsubscribe_url = self._add_utm(f"{self.base_url}/unsubscribe?token={subscription.unsubscribe_token}", campaign=utm_campaign)
        preferences_url = self._add_utm(f"{self.base_url}/preferences?token={subscription.unsubscribe_token}", campaign=utm_campaign)

        # Style tokens (aligned with site)
        BODY_BG = "#f3f2f1"
        CARD_BG = "#ffffff"
        CARD_BORDER = "#e1e5e9"
        CARD_SHADOW = "0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06)"
        TEXT_PRIMARY = "#323130"
        TEXT_SECONDARY = "#605e5c"
        HERO_GRADIENT = "linear-gradient(135deg,#19433c 0%,#286c61 100%)"

        preheader = f"{len(changes)} Fabric roadmap item change(s) in this {cadence_label.lower()} update." if changes else f"Your {cadence_label.lower()} Fabric GPS update."

        def fmt_date(dt_str: str, fallback: str = "TBD", out_fmt: str = "%b %d, %Y") -> str:
            if not dt_str:
                return fallback
            try:
                return datetime.fromisoformat(dt_str.replace('Z', '+00:00')).strftime(out_fmt)
            except (ValueError, TypeError):
                return dt_str

        MAX_EMAIL_CARDS = 20
        total_changes = len(changes)
        displayed_changes = changes[:MAX_EMAIL_CARDS]

        card_blocks: List[str] = []
        for change in displayed_changes:
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
            is_removed = change.get('active') is False
            detail_url = self._add_utm(f"{self.base_url}/release/{rel_id}", campaign=utm_campaign) if rel_id else self._add_utm(self.base_url, campaign=utm_campaign)
            removed_badge = self._build_badge("Removed", "removed") if is_removed else ""
            badges_html = (
                removed_badge +
                self._build_badge(product_name, "product") +
                self._build_badge(release_type, release_type_variant) +
                self._build_badge(release_status, release_status_variant)
            )
            card_blocks.append(
                f"""
                <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;\n                            padding:18px;margin:0 0 18px 0;box-shadow:{CARD_SHADOW};\">\n                    <h3 style=\"margin:0 0 10px 0;font-size:18px;line-height:1.3;color:{TEXT_PRIMARY};font-weight:600;\">\n                        <a href=\"{self.escape_html(detail_url)}\" style=\"color:{TEXT_PRIMARY};text-decoration:none;\">{self.escape_html(feature_name)}\n                        </a>\n                    </h3>\n                    <div style=\"margin:0 0 10px 0;\">{badges_html}</div>\n                    <div style=\"font-size:12px;color:{TEXT_SECONDARY};margin:0 0 12px 0;\">\n                        <strong>Last Modified:</strong> {self.escape_html(modified_date)} &nbsp;|&nbsp;\n                        <strong>Release Date:</strong> {self.escape_html(release_date)}\n                    </div>\n                    <p style=\"margin:0 0 14px 0;font-size:14px;line-height:1.5;color:{TEXT_PRIMARY};\">{self.escape_html(description)}\n                    </p>\n                    {self._build_button(detail_url, "View in Fabric GPS")}\n                </div>\n                """
            )

        if total_changes > MAX_EMAIL_CARDS:
            remaining = total_changes - MAX_EMAIL_CARDS
            browse_params = {
                'modified_within_days': 1 if cadence_label == 'Daily' else 7,
                'include_inactive': 'true',
            }
            if subscription.product_filter and ',' not in subscription.product_filter:
                browse_params['product_name'] = subscription.product_filter.strip()
            if subscription.release_type_filter and ',' not in subscription.release_type_filter:
                browse_params['release_type'] = subscription.release_type_filter.strip()
            if subscription.release_status_filter and ',' not in subscription.release_status_filter:
                browse_params['release_status'] = subscription.release_status_filter.strip()
            browse_url = self._add_utm(f"{self.base_url}/?{urlencode(browse_params)}", campaign=utm_campaign)
            card_blocks.append(
                f'<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;'
                f'padding:22px;margin:0 0 18px 0;box-shadow:{CARD_SHADOW};text-align:center;">'
                f'<p style="margin:0 0 14px 0;font-size:15px;color:{TEXT_SECONDARY};">'
                f'... and {remaining} more change(s)</p>'
                f'{self._build_button(browse_url, "View All on Fabric GPS")}'
                f'</div>'
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
                f'\U0001f4a1 AI Summary</h2>'
                f'<p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};">'
                f'{self.escape_html(ai_summary)}</p>'
                f'</div></td></tr>'
            )

        footer_links = (
            f'<a href="{self.escape_html(self._add_utm(self.base_url, campaign=utm_campaign))}" style="color:#19433c;text-decoration:none;font-weight:500;">Fabric GPS</a>'
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
<span style=\"display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;\">{self.escape_html(preheader)}</span>
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"background:{BODY_BG};padding:24px 0;\">\n  <tr>\n    <td align=\"center\" style=\"padding:0 12px;\">\n      <table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"max-width:640px;\">\n        <tr>\n          <td style=\"background:{HERO_GRADIENT};color:#ffffff;border-radius:14px; padding:34px 34px 38px 34px; text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);\">\n            <h1 style=\"margin:0 0 10px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;\">Fabric GPS — {cadence_label} Update</h1>\n            <p style=\"margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);\">Microsoft Fabric roadmap items modified during {cadence_period}.</p>\n          </td>\n        </tr>\n        <tr><td style=\"height:28px;\"></td></tr>\n        {summary_html}\n        <tr><td style=\"padding:0;\">{changes_section}</td></tr>\n        <tr>\n          <td>\n            <div style=\"background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px; padding:22px;margin:6px 0 26px 0;box-shadow:{CARD_SHADOW};text-align:center;\">\n              <p style=\"margin:0 0 14px 0;font-size:14px;color:{TEXT_SECONDARY};\">Tune your filters or explore more history on the site.</p>\n              {self._build_button(self._add_utm(self.base_url, campaign=utm_campaign), "Open Fabric GPS")}\n            </div>\n          </td>\n        </tr>\n        <tr>\n          <td style=\"background:#f8f9fa;border:1px solid {CARD_BORDER};border-radius:10px; padding:18px 20px;text-align:center;font-size:12px;color:{TEXT_SECONDARY}; line-height:1.5;\">\n            <p style=\"margin:0 0 6px 0;\">Sent to {self.escape_html(subscription.email)} — you’re subscribed to {cadence_sub_text}.</p>\n            <p style=\"margin:0 0 6px 0;\">\n              <a href=\"{self.escape_html(unsubscribe_url)}\" style=\"color:#19433c;text-decoration:none;font-weight:500;\">Unsubscribe</a>&nbsp;|&nbsp;<a href=\"{self.escape_html(preferences_url)}\" style=\"color:#19433c;text-decoration:none;font-weight:500;\">Manage Preferences</a>&nbsp;|&nbsp; Data sourced from Microsoft Fabric Roadmap\n            </p>\n            <p style=\"margin:8px 0 0 0;color:#8a8886;\">{footer_links}</p>\n          </td>\n        </tr>\n        <tr><td style=\"height:30px;\"></td></tr>\n      </table>\n    </td>\n  </tr>\n</table>\n</body>\n</html>\n"""

    def generate_email_text(self, changes: List[Dict[str, Any]], subscription: EmailSubscriptionModel, ai_summary: Optional[str] = None) -> str:
        """Generate plain text email content from JSON API data"""
        utm_campaign = "daily-digest" if subscription.email_cadence == "daily" else "weekly-digest"
        unsubscribe_url = self._add_utm(f"{self.base_url}/unsubscribe?token={subscription.unsubscribe_token}", campaign=utm_campaign)
        preferences_url = self._add_utm(f"{self.base_url}/preferences?token={subscription.unsubscribe_token}", campaign=utm_campaign)
        cadence_label = "DAILY" if subscription.email_cadence == "daily" else "WEEKLY"

        text_parts = [
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
                f"{i}. {change.get('feature_name', 'Unnamed Feature')}{' [REMOVED]' if change.get('active') is False else ''}",
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
            f"Visit {self._add_utm(self.base_url, campaign=utm_campaign)} to explore the full roadmap.",
            "",
            f"Unsubscribe: {unsubscribe_url}",
            f"Manage Preferences: {preferences_url}",
            "Data sourced from Microsoft Fabric Roadmap"
        ])
        
        return "\n".join(text_parts)

    def send_watch_alert_email(self, subscription: EmailSubscriptionModel, changed_watches: List[Dict[str, Any]]) -> bool:
        """Send a watch alert email for changed watched features."""
        if not changed_watches:
            return True

        unsubscribe_url = self._add_utm(f"{self.base_url}/unsubscribe?token={subscription.unsubscribe_token}")
        preferences_url = self._add_utm(f"{self.base_url}/preferences?token={subscription.unsubscribe_token}")

        if len(changed_watches) == 1:
            subject = f"Fabric GPS Alert: {changed_watches[0]['feature_name']} Updated"
        else:
            subject = f"Fabric GPS Alert: {len(changed_watches)} Watched Features Updated"

        # Build HTML
        BODY_BG = "#f3f2f1"
        CARD_BG = "#ffffff"
        CARD_BORDER = "#e1e5e9"
        CARD_SHADOW = "0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06)"
        TEXT_SECONDARY = "#605e5c"
        HERO_GRADIENT = "linear-gradient(135deg,#19433c 0%,#286c61 100%)"
        ALERT_BG = "#fff4ce"
        ALERT_BORDER = "#f0c800"

        items_html = ""
        for w in changed_watches:
            release_url = self._add_utm(f"{self.base_url}/release/{w['release_item_id']}", campaign='watch-alert')
            removed_badge = ' <span style="background:#d13438;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;">REMOVED</span>' if w.get('active') is False else ''
            items_html += f"""<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;padding:18px 20px;margin-bottom:12px;box-shadow:{CARD_SHADOW};">
  <div style="font-weight:600;font-size:15px;color:#323130;margin-bottom:6px;">
    <a href="{self.escape_html(release_url)}" style="color:#19433c;text-decoration:none;">{self.escape_html(w.get('feature_name', 'Unknown'))}</a>{removed_badge}
  </div>
  <div style="font-size:13px;color:{TEXT_SECONDARY};">{self.escape_html(w.get('product_name', ''))} · {self.escape_html(w.get('release_type', ''))} · {self.escape_html(w.get('release_status', ''))}</div>
  <div style="font-size:12px;color:{TEXT_SECONDARY};margin-top:4px;">Last modified: {self.escape_html(w.get('last_modified', 'Unknown'))}</div>
</div>"""

        html_content = f"""<!DOCTYPE html>
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
        <p style="margin:0 0 6px 0;">Sent to {self.escape_html(subscription.email)}</p>
        <p style="margin:0 0 6px 0;">
          <a href="{self.escape_html(preferences_url)}" style="color:#19433c;text-decoration:none;font-weight:500;">Manage Watches</a>&nbsp;|&nbsp;
          <a href="{self.escape_html(unsubscribe_url)}" style="color:#19433c;text-decoration:none;font-weight:500;">Unsubscribe</a>
        </p>
      </td></tr>
      <tr><td style="height:30px;"></td></tr>
    </table>
  </td></tr>
</table></body></html>"""

        # Plain text
        text_parts = ["FABRIC GPS - FEATURE WATCH ALERT", "=" * 50, ""]
        for w in changed_watches:
            removed = " [REMOVED]" if w.get('active') is False else ""
            text_parts.extend([
                f"• {w.get('feature_name', 'Unknown')}{removed}",
                f"  Product: {w.get('product_name', '')}",
                f"  Status: {w.get('release_status', '')}",
                f"  Link: {self.base_url}/release/{w['release_item_id']}",
                "",
            ])
        text_parts.extend([
            "-" * 50,
            f"Manage Watches: {preferences_url}",
            f"Unsubscribe: {unsubscribe_url}",
        ])
        text_content = "\n".join(text_parts)

        return self.send_azure_email(
            to_email=subscription.email,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
            unsubscribe_token=subscription.unsubscribe_token
        )

    def send_azure_email(self, to_email: str, subject: str, html_content: str, text_content: str, unsubscribe_token: str) -> bool:
        """Send an email using Azure Communication Services"""
        try:
            unsubscribe_url = f"{self.base_url}/unsubscribe?token={unsubscribe_token}"
            
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
            # begin_send() queues the email with Azure — no need to poll
            # for delivery status. Log the operation for traceability.
            logger.info(f"Email queued for {to_email} (operation id: {poller.result()['id'] if poller.done() else 'pending'})")
            return True
            
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
        - Old email content cache entries (older than 2 days)
        Returns dict of counts removed.
        """
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import delete, or_, and_
        SessionLocal = sessionmaker(bind=engine, future=True)
        now = datetime.utcnow()
        threshold = now - timedelta(hours=24)
        counts = {"expired_or_used_verifications": 0, "stale_unverified": 0, "old_cache_entries": 0}
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

            # Old cache entries (keep last 2 days)
            old_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
            cache_stmt = delete(EmailContentCacheModel).where(
                EmailContentCacheModel.cache_date < old_date
            )
            result_cache = session.execute(cache_stmt)
            counts["old_cache_entries"] = result_cache.rowcount or 0

            session.commit()
        return counts


def main():
    """Main function to run the email job (digest + watch alerts)."""
    try:
        logger.info("Starting email job")
        sender = WeeklyEmailSender()
        sender.send_emails()
        logger.info("Email job completed successfully")
    except Exception as e:
        logger.error(f"Email job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
