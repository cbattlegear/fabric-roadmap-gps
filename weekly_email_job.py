#!/usr/bin/env python3
"""Email orchestrator for Fabric GPS.

Thin wrapper that:

1. Pulls eligible digest subscribers + subscribers with changed watches
   from the DB.
2. Builds per-subscriber content via :class:`lib.email_digest.DigestContentBuilder`.
3. Renders HTML/text bodies via ``lib.email_template``.
4. Sends through Azure Communication Services, throttled by
   :class:`lib.acs_rate_limit.SlidingWindowRateLimiter`.
5. Cleans up expired verifications, stale unverified subscriptions, and
   old cache rows.

All template rendering, content filtering, and rate-limit logic now
live in ``lib/`` modules with their own focused tests. This file
should stay small enough to read top-to-bottom in a single pass.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from azure.communication.email import EmailClient

# Add the project root to Python path so this script runs both as a
# module and as ``python weekly_email_job.py``.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.db_sqlserver import (
    EmailContentCacheModel,
    EmailSubscriptionModel,
    EmailVerificationModel,
    get_digest_eligible_subscriptions,
    get_subscriptions_with_changed_watches,
    make_engine,
    session_scope,
    update_watch_hashes,
)
from lib.acs_rate_limit import SlidingWindowRateLimiter, acs_default_config
from lib import email_template
from lib.email_digest import DigestContentBuilder, filter_by_cadence
from lib.telemetry import init_telemetry

logger = init_telemetry(
    "fabric-gps-email-job",
    logger_name="fabric-gps-email.opentelemetry",
)
logger.info('Fabric-GPS Email Batch Job started')


class WeeklyEmailSender:
    """Orchestrator: wires DB, content builder, templates, ACS sender."""

    # Re-exported for backwards compatibility with existing tests that
    # call ``WeeklyEmailSender._filter_by_cadence(...)`` directly.
    _filter_by_cadence = staticmethod(filter_by_cadence)

    def __init__(self) -> None:
        self.connection_string = os.getenv('AZURE_COMMUNICATION_CONNECTION_STRING')
        self.from_email = os.getenv('FROM_EMAIL', 'noreply@yourdomain.com')
        self.from_name = os.getenv('FROM_NAME', 'Fabric GPS')
        canonical = os.getenv('CANONICAL_HOST', '')
        self.base_url = (
            f"https://{canonical}" if canonical
            else os.getenv('BASE_URL', 'http://localhost:8000')
        )

        if not self.connection_string:
            raise ValueError("AZURE_COMMUNICATION_CONNECTION_STRING environment variable is required")

        self.email_client = EmailClient.from_connection_string(self.connection_string)
        self._engine: Any = None
        self._content_builder: Optional[DigestContentBuilder] = None

    def _builder(self) -> DigestContentBuilder:
        if self._content_builder is None:
            self._content_builder = DigestContentBuilder(
                engine=self._engine,
                base_url=self.base_url,
                session_scope=session_scope,
                cache_model=EmailContentCacheModel,
            )
        return self._content_builder

    # ---- top-level loop ------------------------------------------------
    def send_emails(self) -> None:
        """Send digest and watch-alert emails to eligible subscribers."""
        try:
            self._engine = make_engine()
            limiter = SlidingWindowRateLimiter(acs_default_config(), logger=logger)

            sent_count, error_count = self._send_digest_queue(limiter)
            logger.info(f"Digest queue completed. Sent: {sent_count}, Errors: {error_count}")

            watch_sent, watch_errors = self._send_watch_alert_queue(limiter)
            logger.info(f"Watch alert queue completed. Sent: {watch_sent}, Errors: {watch_errors}")

            try:
                cleanup_counts = self.cleanup_expired(self._engine)
                logger.info(
                    "Cleanup complete: expired_or_used_verifications=%d, stale_unverified_subscriptions=%d",
                    cleanup_counts.get('expired_or_used_verifications', 0),
                    cleanup_counts.get('stale_unverified', 0),
                )
            except Exception as cleanup_exc:
                logger.error(f"Cleanup step failed: {cleanup_exc}")
        except Exception as e:
            logger.error(f"Fatal error in email job: {e}")
            raise

    # Backwards-compatible alias
    send_weekly_emails = send_emails

    def _send_digest_queue(self, limiter: SlidingWindowRateLimiter) -> tuple:
        subscriptions = get_digest_eligible_subscriptions(self._engine)
        logger.info(f"Digest queue: {len(subscriptions)} eligible subscriptions")

        sent = 0
        errors = 0
        for subscription in subscriptions:
            try:
                limiter.wait_for_capacity()
                if self.send_digest_email(subscription):
                    limiter.record_send()
                    sent += 1
                    self.update_last_email_sent(subscription.id)
                else:
                    errors += 1
            except Exception as e:
                logger.error(f"Error sending digest to {subscription.email}: {e}")
                errors += 1
        return sent, errors

    def _send_watch_alert_queue(self, limiter: SlidingWindowRateLimiter) -> tuple:
        sent = 0
        errors = 0
        try:
            watch_results = get_subscriptions_with_changed_watches(self._engine)
            logger.info(f"Watch alert queue: {len(watch_results)} subscribers with changed watches")

            for sub, changed_watches in watch_results:
                try:
                    limiter.wait_for_capacity()
                    if self.send_watch_alert_email(sub, changed_watches):
                        limiter.record_send()
                        sent += 1
                        hash_updates = [(w['watch_id'], w['current_hash']) for w in changed_watches]
                        update_watch_hashes(self._engine, hash_updates)
                    else:
                        errors += 1
                except Exception as e:
                    logger.error(f"Error sending watch alert to {sub.email}: {e}")
                    errors += 1
        except Exception as e:
            logger.error(f"Error processing watch alerts: {e}")
        return sent, errors

    # ---- per-message senders ------------------------------------------
    def send_digest_email(self, subscription: EmailSubscriptionModel) -> bool:
        try:
            changes, ai_summary = self._builder().get_for(subscription)
            if not changes:
                logger.info(f"No changes for {subscription.email}, skipping")
                return True

            cadence_label = "Daily" if subscription.email_cadence == "daily" else "Weekly"
            subject = f"Fabric GPS {cadence_label} Update - {len(changes)} Changes"

            html_content = email_template.render_digest_html(
                changes, subscription, self.base_url,
                ai_summary=ai_summary, cadence_label=cadence_label,
            )
            text_content = email_template.render_digest_text(
                changes, subscription, self.base_url, ai_summary=ai_summary,
            )

            success = self.send_azure_email(
                to_email=subscription.email,
                subject=subject,
                html_content=html_content,
                text_content=text_content,
                unsubscribe_token=subscription.unsubscribe_token,
            )
            if success:
                logger.info(f"Successfully sent weekly email to {subscription.email}")
                return True
            logger.error(f"Failed to send email to {subscription.email}")
            return False
        except Exception as e:
            logger.error(f"Error processing subscription {subscription.email}: {e}")
            return False

    def send_watch_alert_email(
        self,
        subscription: EmailSubscriptionModel,
        changed_watches: List[Dict[str, Any]],
    ) -> bool:
        if not changed_watches:
            return True

        subject = email_template.render_watch_alert_subject(changed_watches)
        html_content = email_template.render_watch_alert_html(subscription, changed_watches, self.base_url)
        text_content = email_template.render_watch_alert_text(subscription, changed_watches, self.base_url)

        return self.send_azure_email(
            to_email=subscription.email,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
            unsubscribe_token=subscription.unsubscribe_token,
        )

    def send_azure_email(
        self,
        to_email: str,
        subject: str,
        html_content: str,
        text_content: str,
        unsubscribe_token: str,
    ) -> bool:
        """Hand the message to ACS. Returns True on enqueue success."""
        try:
            unsubscribe_url = f"{self.base_url}/unsubscribe?token={unsubscribe_token}"
            message = {
                "senderAddress": self.from_email,
                "recipients": {"to": [{"address": to_email}]},
                "content": {
                    "subject": subject,
                    "plainText": text_content,
                    "html": html_content,
                },
                "headers": {
                    "List-Unsubscribe": f"<{unsubscribe_url}>",
                    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
                },
            }
            poller = self.email_client.begin_send(message)
            logger.info(
                f"Email queued for {to_email} (operation id: "
                f"{poller.result()['id'] if poller.done() else 'pending'})"
            )
            return True
        except Exception as e:
            logger.error(f"Azure Communication Services error sending to {to_email}: {e}")
            return False

    # ---- bookkeeping ---------------------------------------------------
    def update_last_email_sent(self, subscription_id: str) -> None:
        try:
            engine = self._engine or make_engine()
            with session_scope(engine) as session:
                subscription = session.get(EmailSubscriptionModel, subscription_id)
                if subscription:
                    subscription.last_email_sent = datetime.utcnow()
                    session.commit()
        except Exception as e:
            logger.error(f"Error updating last_email_sent for {subscription_id}: {e}")

    def cleanup_expired(self, engine: Any) -> Dict[str, int]:
        """Remove expired verifications, stale unverified subs, old cache rows."""
        from sqlalchemy import and_, delete, or_

        now = datetime.utcnow()
        threshold = now - timedelta(hours=24)
        counts = {
            "expired_or_used_verifications": 0,
            "stale_unverified": 0,
            "old_cache_entries": 0,
        }
        with session_scope(engine) as session:
            expired_stmt = delete(EmailVerificationModel).where(
                or_(EmailVerificationModel.expires_at < now, EmailVerificationModel.is_used == True)  # noqa: E712
            )
            counts["expired_or_used_verifications"] = session.execute(expired_stmt).rowcount or 0

            stale_stmt = delete(EmailSubscriptionModel).where(
                and_(
                    EmailSubscriptionModel.is_verified == False,  # noqa: E712
                    EmailSubscriptionModel.verification_token.isnot(None),
                    EmailSubscriptionModel.created_at < threshold,
                )
            )
            counts["stale_unverified"] = session.execute(stale_stmt).rowcount or 0

            old_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
            cache_stmt = delete(EmailContentCacheModel).where(
                EmailContentCacheModel.cache_date < old_date
            )
            counts["old_cache_entries"] = session.execute(cache_stmt).rowcount or 0

            session.commit()
        return counts


def main() -> None:
    """Entry point for ``python weekly_email_job.py``."""
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
