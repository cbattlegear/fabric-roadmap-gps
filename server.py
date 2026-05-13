import json
import os
import urllib.parse
import threading
import logging
from datetime import datetime, date, time, timezone
from typing import Optional, List

from flask import Flask, request, Response, jsonify, render_template, redirect, send_from_directory, url_for
from flask_limiter import Limiter
from html import escape
from email.utils import format_datetime
import hashlib

# Import the `configure_azure_monitor()` function from the
# `azure.monitor.opentelemetry` package.
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.instrumentation.flask import FlaskInstrumentor


# Azure Communication Services for email
try:
    from azure.communication.email import EmailClient
    AZURE_EMAIL_AVAILABLE = True
except ImportError:
    AZURE_EMAIL_AVAILABLE = False
    logging.warning("Azure Communication Services not available. Email sending will be disabled.")

from db.db_sqlserver import (
    make_engine, get_recently_modified_releases, init_db, ReleaseItemModel, get_distinct_values, fetch_history_rows,
    create_email_subscription,
    verify_email_subscription, unsubscribe_email, fetch_history_rows, count_recently_modified_releases,
    vector_search_releases, count_vector_search_releases,
    record_bounce,
    healthcheck as db_healthcheck,
    get_changelog_with_changes,
    get_subscription_by_unsubscribe_token, update_subscription_preferences,
    get_verified_subscription_by_email,
    add_feature_watch, remove_feature_watch, get_feature_watches_for_subscription,
    create_watch_verification, get_release_item_by_id,
    get_latest_release_version, get_active_releases_for_sitemap,
    get_verify_email_context,
    EmailRateLimitExceeded,
    rotate_unsubscribe_token,
)
from lib.embeddings import get_embedding, is_available as embeddings_available
from lib.quarter_date import format_as_quarter
from lib.releases_api import (
    ReleasesQuery,
    _build_pagination_links,
    _build_pagination_meta,
    _format_release_row,
    _parse_releases_query,
)

os.environ['OTEL_SERVICE_NAME'] = 'fabric-gps-web-frontend'

app = Flask(__name__)


@app.template_filter('release_date_quarter')
def _release_date_quarter_filter(value):
    """Render a release_date date/string as ``"Q# YYYY"`` for templates."""
    return format_as_quarter(value)


FlaskInstrumentor().instrument_app(app, excluded_urls="healthcheck")

logger_name = __name__
opentelemetery_logger_name = f'{logger_name}.opentelemetry'

if os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING") and os.getenv("CURRENT_ENVIRONMENT") != "development":
    configure_azure_monitor(
        logger_name=opentelemetery_logger_name,
        enable_live_metrics=True 
    )

otelLogger= logging.getLogger(opentelemetery_logger_name)
stream = logging.StreamHandler()
otelLogger.addHandler(stream)
otelLogger.setLevel(logging.INFO)
otelLogger.info('Fabric-GPS Website started')

if os.getenv("CURRENT_ENVIRONMENT") == "development":
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    # Set up the tracer provider
    tracer_provider = TracerProvider()
    trace.set_tracer_provider(tracer_provider)

    # Configure the ConsoleSpanExporter
    span_processor = BatchSpanProcessor(ConsoleSpanExporter())
    tracer_provider.add_span_processor(span_processor)

ENGINE = None

# Cache-Control max-age for HTTP responses (Azure Front Door caching)
# With a 1-hour TTL the cache stays reasonably fresh without needing active purges.
_FRONT_END_TTL = 1 * 60 * 60  # 1 hour fresh
_STALE_TTL = 10 * 60          # 10 minute stale-while-revalidate

# Email configuration
EMAIL_CLIENT = None
FROM_EMAIL = os.getenv('FROM_EMAIL', 'noreply@yourdomain.com')
FROM_NAME = os.getenv('FROM_NAME', 'Fabric GPS')
BASE_URL = os.getenv('BASE_URL', 'http://localhost:8000')
ASYNC_EMAIL_VERIFICATION = os.getenv('ASYNC_EMAIL_VERIFICATION', '1') != '0'
# ACS resource ID for validating Event Grid webhook events. Required in non-dev
# so the /webhooks/email-events endpoint can reject events from unknown topics
# instead of silently accepting any caller's bounce data.
ACS_RESOURCE_ID = os.getenv('ACS_RESOURCE_ID', '')
if os.getenv("CURRENT_ENVIRONMENT") != "development" and not ACS_RESOURCE_ID:
    raise RuntimeError(
        "ACS_RESOURCE_ID must be set in production so the email events webhook "
        "can validate the Event Grid topic."
    )


CANONICAL_HOST = os.getenv('CANONICAL_HOST', '')
REDIRECT_HOSTS = {h.strip() for h in os.getenv('REDIRECT_HOSTS', '').split(',') if h.strip()}

# Use canonical host for email URLs if configured, otherwise fall back to BASE_URL
EMAIL_BASE_URL = f"https://{CANONICAL_HOST}" if CANONICAL_HOST else BASE_URL

# Enforce HTTPS for outbound email URLs in non-development environments. Tokens
# embedded in these links must not be transmitted over cleartext HTTP.
if os.getenv("CURRENT_ENVIRONMENT") != "development" and not EMAIL_BASE_URL.startswith("https://"):
    raise RuntimeError(
        f"EMAIL_BASE_URL must use HTTPS in production (got {EMAIL_BASE_URL!r}). "
        "Set CANONICAL_HOST or BASE_URL to an https:// URL."
    )

_NO_CACHE_PATHS = ("/subscribe", "/verify-email", "/unsubscribe", "/preferences", "/watch/", "/api/subscribe", "/api/verify-email", "/api/preferences", "/api/watch", "/api/send-preferences-link", "/dev/")


# ---------------------------------------------------------------------------
# Rate limiting (per-worker in-memory; effective behind Azure Front Door)
# ---------------------------------------------------------------------------
def _get_real_ip():
    """Get real client IP from X-Forwarded-For header (set by Azure Front Door)."""
    forwarded = request.headers.get('X-Forwarded-For', '')
    if forwarded:
        return forwarded.split(',')[0].strip()
    return request.remote_addr or '127.0.0.1'


limiter = Limiter(
    key_func=_get_real_ip,
    app=app,
    default_limits=[],
    storage_uri="memory://",
)


# ---------------------------------------------------------------------------
# CSRF protection for form-based POST endpoints
# ---------------------------------------------------------------------------
_CSRF_FORM_PATHS = ('/verify-email', '/unsubscribe', '/watch/')


@app.before_request
def validate_form_origin():
    """Reject cross-origin form POST submissions.

    JSON API endpoints are inherently CSRF-safe because browsers cannot
    send Content-Type: application/json cross-origin without a CORS
    preflight. This check only targets HTML form POSTs.
    """
    if request.method != 'POST':
        return
    if not request.path.startswith(_CSRF_FORM_PATHS):
        return
    if request.content_type and 'application/json' in request.content_type:
        return
    origin = request.headers.get('Origin', '')
    if origin and not origin.startswith(EMAIL_BASE_URL) and not origin.startswith(BASE_URL):
        otelLogger.warning(f"Blocked cross-origin form POST from {origin} to {request.path}")
        return Response("Forbidden: cross-origin form submission", status=403)


@app.after_request
def set_security_headers(response):
    """Apply baseline security headers to every response.

    CSP is deliberately omitted — see issue tracking the in-depth CSP
    refactor (inline scripts, onclick handlers, and external trackers
    need to be reworked before a strict policy can be enforced).
    """
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'DENY')
    response.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    return response


@app.after_request
def prevent_caching_sensitive_pages(response):
    """Prevent Front Door / browsers from caching pages with user tokens."""
    if request.path.startswith(_NO_CACHE_PATHS):
        response.headers['Cache-Control'] = 'no-store'
    return response


@app.after_request
def set_static_cache_headers(response):
    """Ensure static files have proper cache headers."""
    if request.path.startswith('/static/'):
        response.headers['Cache-Control'] = 'public, max-age=86400, immutable'
    return response


@app.before_request
def redirect_to_canonical_host():
    """301 redirect specific hosts (e.g. apex domain) to the canonical host."""
    if CANONICAL_HOST and request.host in REDIRECT_HOSTS:
        return redirect(
            f"https://{CANONICAL_HOST}{request.full_path}".rstrip("?"),
            code=301,
        )


@app.before_request
def track_traffic_source():
    """Classify each request into a traffic source for App Insights."""
    span = trace.get_current_span()

    utm_source = request.args.get('utm_source', '')
    if utm_source:
        traffic_source = utm_source
    elif request.path.startswith('/rss'):
        traffic_source = 'rss'
    elif request.path.startswith('/api/'):
        traffic_source = 'api'
    else:
        traffic_source = 'web'

    span.set_attribute('traffic_source', traffic_source)

    utm_medium = request.args.get('utm_medium', '')
    utm_campaign = request.args.get('utm_campaign', '')
    if utm_medium:
        span.set_attribute('utm_medium', utm_medium)
    if utm_campaign:
        span.set_attribute('utm_campaign', utm_campaign)


def get_email_client():
    """Get Azure Communication Services email client"""
    global EMAIL_CLIENT
    if EMAIL_CLIENT is None and AZURE_EMAIL_AVAILABLE:
        connection_string = os.getenv('AZURE_COMMUNICATION_CONNECTION_STRING')
        if connection_string:
            try:
                EMAIL_CLIENT = EmailClient.from_connection_string(connection_string)
                otelLogger.info("Azure Communication Services email client initialized")
            except Exception as e:
                otelLogger.error(f"Failed to initialize Azure email client: {e}")
        else:
            otelLogger.warning("AZURE_COMMUNICATION_CONNECTION_STRING not set. Email sending disabled.")
    return EMAIL_CLIENT


def _monitor_acs_send(poller, *, recipient: str, kind: str) -> None:
    """Run on a background thread; logs the outcome of an async ACS send.

    *kind* is the human-readable email type (``'verification'``,
    ``'preferences link'``, ``'goodbye'``) used in the log messages. Status
    extraction tries the most common ACS shapes in order:

    1. ``result.status`` — ``SendStatusResult``-style attribute (most common
       in current SDK versions). Checked first so successful sends aren't
       logged as ``status=None``.
    2. ``result['status']`` — dict-shaped result.
    3. ``result.get('status')`` — mapping-like with a ``get`` method.

    All three branches converge on the same success/warning/error logging
    so the three call sites can no longer drift.
    """
    try:
        result = poller.result()
        status = getattr(result, 'status', None)
        if status is None and isinstance(result, dict):
            status = result.get('status')
        if status is None:
            getter = getattr(result, 'get', None)
            if callable(getter):
                status = getter('status')
        kind_label = kind.capitalize()
        if status == 'Succeeded':
            otelLogger.info(f"{kind_label} email sent to {recipient}")
        else:
            otelLogger.warning(f"{kind_label} email status for {recipient}: {status}")
    except Exception as monitor_exc:
        otelLogger.error(f"Error monitoring {kind} email send to {recipient}: {monitor_exc}")


def send_verification_email(email: str, verification_token: str,
                            cadence: str = 'weekly', watch_feature_name: Optional[str] = None) -> bool:
    """Send verification email using Azure Communication Services"""
    email_client = get_email_client()
    if not email_client:
        otelLogger.error("Email client not available for verification email")
        return False
    
    try:
        verification_url = f"{EMAIL_BASE_URL}/verify-email?token={verification_token}"

        # Build context-aware copy
        is_watch = bool(watch_feature_name)
        if is_watch:
            escaped_watch_feature_name = escape(watch_feature_name)
            preheader = f"Verify your email to start watching {escaped_watch_feature_name}."
            hero_text = "Verify your email to start watching a feature on the Microsoft Fabric roadmap."
            subject = f"Verify your email to watch {watch_feature_name} — Fabric GPS"
            heading = "Start Watching a Feature"
            body_text = (
                f"You requested to watch <strong>{escaped_watch_feature_name}</strong> on the Fabric roadmap. "
                f"Click the button below to confirm your email and start receiving alerts when this feature changes."
            )
            after_text = "After verifying, you&rsquo;ll be notified whenever this feature is updated on the roadmap."
        else:
            cadence_desc = "daily updates" if cadence == "daily" else "weekly updates every Monday"
            preheader = f"Confirm your email to start {cadence_desc}."
            hero_text = f"Verify your email to begin receiving {cadence_desc} about Microsoft Fabric roadmap changes."
            subject = f"Verify your email for Fabric GPS {cadence_desc}"
            heading = "Confirm Your Subscription"
            body_text = (
                f"Thanks for signing up! One quick step: click the button below to confirm this address "
                f"and activate {cadence_desc}."
            )
            after_text = f"After verifying, you&rsquo;ll receive {cadence_desc} with the latest Microsoft Fabric roadmap changes. No spam &mdash; unsubscribe anytime."

        # HTML email content
        html_content = f"""<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <meta name='viewport' content='width=device-width,initial-scale=1'>
    <title>Verify Your Email - Fabric GPS</title>
    <style>
        body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}
    </style>
</head>
<body style='margin:0;padding:0;background:#f3f2f1;'>
    <span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;">{preheader}</span>
    <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='background:#f3f2f1;padding:28px 0;'>
        <tr>
            <td align='center' style='padding:0 14px;'>
                <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='max-width:620px;'>
                    <tr>
                        <td style="background:linear-gradient(135deg,#19433c 0%,#286c61 100%);color:#ffffff;border-radius:14px;padding:40px 36px 42px 36px;text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);">
                            <h1 style='margin:0 0 12px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;'>Fabric GPS</h1>
                            <p style='margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);'>{hero_text}</p>
                        </td>
                    </tr>
                    <tr><td style='height:30px;'></td></tr>
                    <tr>
                        <td style='background:#ffffff;border:1px solid #e1e5e9;border-radius:12px;padding:34px 32px;box-shadow:0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06);'>
                            <h2 style='margin:0 0 18px 0;font-size:22px;line-height:1.25;color:#323130;font-weight:600;text-align:center;'>{heading}</h2>
                            <p style='margin:0 0 24px 0;font-size:15px;line-height:1.55;color:#605e5c;text-align:center;'>{body_text}</p>
                            <div style='text-align:center;margin:10px 0 34px 0;'>
                                <a href='{verification_url}' style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);color:#ffffff;text-decoration:none;padding:14px 26px;font-size:15px;font-weight:600;border-radius:8px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.18);">Verify Email</a>
                            </div>
                            <p style='margin:0 0 18px 0;font-size:13px;line-height:1.5;color:#605e5c;text-align:center;'>If the button doesn't work, copy &amp; paste this link:</p>
                            <p style='word-break:break-all;margin:0 0 26px 0;font-size:12px;line-height:1.4;text-align:center;'>
                                <a href='{verification_url}' style='color:#19433c;text-decoration:none;'>{verification_url}</a>
                            </p>
                            <div style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:16px 18px;'>
                                <p style='margin:0;font-size:13px;line-height:1.5;color:#605e5c;'>{after_text}</p>
                            </div>
                        </td>
                    </tr>
                    <tr><td style='height:26px;'></td></tr>
                    <tr>
                        <td style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:20px 22px;text-align:center;font-size:12px;line-height:1.55;color:#605e5c;'>
                            <p style='margin:0 0 6px 0;'>This verification link expires in 24 hours.</p>
                            <p style='margin:0 0 6px 0;'>If you didn't request this, ignore the message and you won't be subscribed.</p>
                            <p style='margin:10px 0 0 0;color:#8a8886;'>&copy; Fabric GPS</p>
                        </td>
                    </tr>
                    <tr><td style='height:34px;'></td></tr>
                </table>
            </td>
        </tr>
    </table>
 </body>
</html>"""

        # Plain text content
        if is_watch:
            text_content = f"""FABRIC GPS - VERIFY YOUR EMAIL

You requested to watch {watch_feature_name} on the Fabric roadmap.

Please verify your email address by clicking the link below:
{verification_url}

Once verified, you'll be notified whenever this feature is updated.

This verification link will expire in 24 hours.
If you didn't request this, you can safely ignore this email.

--
Fabric GPS
"""
        else:
            cadence_text = "daily updates" if cadence == "daily" else "weekly updates"
            text_content = f"""FABRIC GPS - VERIFY YOUR EMAIL

Thank you for subscribing to Fabric GPS {cadence_text}!

Please verify your email address by clicking the link below:
{verification_url}

Once verified, you'll receive {cadence_text} with the latest Microsoft Fabric roadmap changes.

This verification link will expire in 24 hours.
If you didn't subscribe to Fabric GPS, you can safely ignore this email.

--
Fabric GPS
"""
        
        message = {
            "senderAddress": FROM_EMAIL,
            "recipients": {
                "to": [{"address": email}]
            },
            "content": {
                "subject": subject,
                "plainText": text_content,
                "html": html_content
            }
        }
        # Non-blocking strategy: start send, monitor in background (default). Set ASYNC_EMAIL_VERIFICATION=0 to restore blocking behavior.
        if ASYNC_EMAIL_VERIFICATION:
            try:
                poller = email_client.begin_send(message)
            except Exception as e:
                otelLogger.error(f"Failed to initiate async verification email send to {email}: {e}")
                return False

            def _monitor_send(poller_obj, target_email):
                _monitor_acs_send(poller_obj, recipient=target_email, kind='verification')

            threading.Thread(target=_monitor_send, args=(poller, email), daemon=True).start()
            return True
        else:
            # Legacy blocking path (if explicitly requested)
            POLLER_WAIT_TIME = 10
            poller = email_client.begin_send(message)
            time_elapsed = 0
            while not poller.done():
                otelLogger.info("Verification email poller status: " + poller.status())
                poller.wait(POLLER_WAIT_TIME)
                time_elapsed += POLLER_WAIT_TIME
                if time_elapsed > 18 * POLLER_WAIT_TIME:
                    raise RuntimeError("Polling timed out.")
            result = poller.result()
            if result["status"] == "Succeeded":
                otelLogger.info(f"Successfully sent verification email (operation id: {result['id']})")
                return True
            else:
                raise RuntimeError(str(result["error"]))
        
    except Exception as e:
        otelLogger.error(f"Error sending verification email to {email}: {e}")
        return False


def send_preferences_link_email(email: str, preferences_url: str) -> bool:
    """Send an email with a link to manage subscription preferences."""
    email_client = get_email_client()
    if not email_client:
        otelLogger.error("Email client not available for preferences link email")
        return False

    try:
        html_content = f"""<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <meta name='viewport' content='width=device-width,initial-scale=1'>
    <title>Manage Your Subscription - Fabric GPS</title>
    <style>
        body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}
    </style>
</head>
<body style='margin:0;padding:0;background:#f3f2f1;'>
    <span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;">Your Fabric GPS preferences link is inside.</span>
    <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='background:#f3f2f1;padding:28px 0;'>
        <tr>
            <td align='center' style='padding:0 14px;'>
                <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='max-width:620px;'>
                    <tr>
                        <td style="background:linear-gradient(135deg,#19433c 0%,#286c61 100%);color:#ffffff;border-radius:14px;padding:40px 36px 42px 36px;text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);">
                            <h1 style='margin:0 0 12px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;'>Fabric GPS</h1>
                            <p style='margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);'>Manage your subscription preferences</p>
                        </td>
                    </tr>
                    <tr><td style='height:30px;'></td></tr>
                    <tr>
                        <td style='background:#ffffff;border:1px solid #e1e5e9;border-radius:12px;padding:34px 32px;box-shadow:0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06);'>
                            <h2 style='margin:0 0 18px 0;font-size:22px;line-height:1.25;color:#323130;font-weight:600;text-align:center;'>Your Preferences Link</h2>
                            <p style='margin:0 0 24px 0;font-size:15px;line-height:1.55;color:#605e5c;text-align:center;'>Click the button below to manage your Fabric GPS subscription &mdash; update your email cadence, filters, and feature watches.</p>
                            <div style='text-align:center;margin:10px 0 34px 0;'>
                                <a href='{preferences_url}' style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);color:#ffffff;text-decoration:none;padding:14px 26px;font-size:15px;font-weight:600;border-radius:8px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.18);">Manage Preferences</a>
                            </div>
                            <p style='margin:0 0 18px 0;font-size:13px;line-height:1.5;color:#605e5c;text-align:center;'>If the button doesn't work, copy &amp; paste this link:</p>
                            <p style='word-break:break-all;margin:0 0 26px 0;font-size:12px;line-height:1.4;text-align:center;'>
                                <a href='{preferences_url}' style='color:#19433c;text-decoration:none;'>{preferences_url}</a>
                            </p>
                            <div style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:16px 18px;'>
                                <p style='margin:0;font-size:13px;line-height:1.5;color:#605e5c;'>Keep this link private &mdash; anyone with it can change your subscription settings.</p>
                            </div>
                        </td>
                    </tr>
                    <tr><td style='height:26px;'></td></tr>
                    <tr>
                        <td style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:20px 22px;text-align:center;font-size:12px;line-height:1.55;color:#605e5c;'>
                            <p style='margin:0 0 6px 0;'>If you didn't request this, you can safely ignore this email.</p>
                            <p style='margin:10px 0 0 0;color:#8a8886;'>&copy; Fabric GPS</p>
                        </td>
                    </tr>
                    <tr><td style='height:34px;'></td></tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""

        text_content = f"""FABRIC GPS - MANAGE YOUR SUBSCRIPTION

Click the link below to manage your Fabric GPS subscription preferences:
{preferences_url}

You can update your email cadence, product filters, and feature watches.

Keep this link private - anyone with it can change your subscription settings.
If you didn't request this, you can safely ignore this email.

--
Fabric GPS
"""

        message = {
            "senderAddress": FROM_EMAIL,
            "recipients": {
                "to": [{"address": email}]
            },
            "content": {
                "subject": "Manage your Fabric GPS subscription",
                "plainText": text_content,
                "html": html_content
            }
        }

        try:
            poller = email_client.begin_send(message)
        except Exception as e:
            otelLogger.error(f"Failed to send preferences link email to {email}: {e}")
            return False

        def _monitor_send(poller_obj, target_email):
            _monitor_acs_send(poller_obj, recipient=target_email, kind='preferences link')

        threading.Thread(target=_monitor_send, args=(poller, email), daemon=True).start()
        return True

    except Exception as e:
        otelLogger.error(f"Error sending preferences link email to {email}: {e}")
        return False


def send_goodbye_email(email: str) -> bool:
    """Send a goodbye/confirmation email after a user unsubscribes."""
    email_client = get_email_client()
    if not email_client:
        otelLogger.warning("Email client not available for goodbye email")
        return False

    subscribe_url = f"{EMAIL_BASE_URL}/subscribe"

    html_content = f"""<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <meta name='viewport' content='width=device-width,initial-scale=1'>
    <title>Unsubscribed - Fabric GPS</title>
    <style>
        body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}
    </style>
</head>
<body style='margin:0;padding:0;background:#f3f2f1;'>
    <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='background:#f3f2f1;padding:28px 0;'>
        <tr>
            <td align='center' style='padding:0 14px;'>
                <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='max-width:620px;'>
                    <tr>
                        <td style="background:linear-gradient(135deg,#19433c 0%,#286c61 100%);color:#ffffff;border-radius:14px;padding:40px 36px 42px 36px;text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);">
                            <h1 style='margin:0 0 12px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;'>Fabric GPS</h1>
                            <p style='margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);'>You have been unsubscribed from weekly updates.</p>
                        </td>
                    </tr>
                    <tr><td style='height:30px;'></td></tr>
                    <tr>
                        <td style='background:#ffffff;border:1px solid #e1e5e9;border-radius:12px;padding:34px 32px;box-shadow:0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06);'>
                            <h2 style='margin:0 0 18px 0;font-size:22px;line-height:1.25;color:#323130;font-weight:600;text-align:center;'>You're Unsubscribed</h2>
                            <p style='margin:0 0 24px 0;font-size:15px;line-height:1.55;color:#605e5c;text-align:center;'>This confirms that you have been fully removed from the Fabric GPS weekly email list. You will not receive any further emails from us.</p>
                            <p style='margin:0 0 24px 0;font-size:15px;line-height:1.55;color:#605e5c;text-align:center;'>If you change your mind, you can re-subscribe at any time:</p>
                            <div style='text-align:center;margin:10px 0 34px 0;'>
                                <a href='{subscribe_url}' style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);color:#ffffff;text-decoration:none;padding:14px 26px;font-size:15px;font-weight:600;border-radius:8px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.18);">Re-subscribe</a>
                            </div>
                        </td>
                    </tr>
                    <tr><td style='height:26px;'></td></tr>
                    <tr>
                        <td style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:20px 22px;text-align:center;font-size:12px;line-height:1.55;color:#605e5c;'>
                            <p style='margin:0 0 6px 0;'>This is a one-time confirmation. You will not receive further emails.</p>
                            <p style='margin:10px 0 0 0;color:#8a8886;'>&copy; Fabric GPS</p>
                        </td>
                    </tr>
                    <tr><td style='height:34px;'></td></tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""

    text_content = f"""FABRIC GPS - UNSUBSCRIBE CONFIRMATION

You have been fully unsubscribed from Fabric GPS weekly updates.
You will not receive any further emails from us.

If you change your mind, you can re-subscribe at any time:
{subscribe_url}

This is a one-time confirmation. You will not receive further emails.

--
Fabric GPS
"""

    message = {
        "senderAddress": FROM_EMAIL,
        "recipients": {
            "to": [{"address": email}]
        },
        "content": {
            "subject": "You've been unsubscribed from Fabric GPS",
            "plainText": text_content,
            "html": html_content
        }
    }

    try:
        poller = email_client.begin_send(message)

        def _monitor_send(poller_obj, target_email):
            _monitor_acs_send(poller_obj, recipient=target_email, kind='goodbye')

        threading.Thread(target=_monitor_send, args=(poller, email), daemon=True).start()
        return True
    except Exception as e:
        otelLogger.error(f"Failed to send goodbye email to {email}: {e}")
        return False


def get_engine():
    global ENGINE
    if ENGINE is None:
        ENGINE = make_engine()
        init_db(ENGINE)
    return ENGINE


def _to_rfc2822(dt_or_date: Optional[date]) -> Optional[str]:
    if not dt_or_date:
        return None
    if isinstance(dt_or_date, datetime):
        dt = dt_or_date
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        # last_modified is a Date in our schema; set midnight UTC
        dt = datetime.combine(dt_or_date, time.min, tzinfo=timezone.utc)
    return format_datetime(dt)


def _item_link(row: ReleaseItemModel) -> str:
    return f"{EMAIL_BASE_URL}/release/{row.release_item_id}"

def _description(row: ReleaseItemModel) -> str:
    desc = ""
    if row.release_date:
        quarter = format_as_quarter(row.release_date) or row.release_date.isoformat()
        desc += f"<p><strong>{row.release_status} {row.release_type} Date:</strong> {escape(quarter)}</p>"
    if row.feature_description:
        desc += f"<p>{escape(row.feature_description)}</p>"
    return desc or "(no description)"



def build_rss_xml(rows: List[ReleaseItemModel],
                  title: str = "Fabric Roadmap - Recently Modified",
                  link: str = "https://roadmap.fabric.microsoft.com",
                  description: str = "25 most recently modified releases",
                  language: str = "en-us") -> str:
    now = datetime.now(timezone.utc)
    last_build = format_datetime(now)

    # Channel header
    parts = [
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
        "<rss version=\"2.0\">",
        "  <channel>",
        f"    <title>{escape(title)}</title>",
        f"    <link>{escape(link)}</link>",
        f"    <description>{escape(description)}</description>",
        f"    <language>{escape(language)}</language>",
        f"    <lastBuildDate>{last_build}</lastBuildDate>",
    ]

    for r in rows:
        item_title = r.feature_name or "(no title)"
        item_link = _item_link(r)
        guid = r.release_item_id
        pub_date = _to_rfc2822(r.last_modified) or last_build
        desc = _description(r)

        parts.extend([
            "    <item>",
            f"      <title>{escape(item_title)}</title>",
            f"      <link>{escape(item_link)}</link>",
            f"      <guid isPermaLink=\"true\">{escape(item_link)}</guid>",
            f"      <pubDate>{pub_date}</pubDate>",
            f"      <category>{escape(r.release_type or '')}</category>",
            f"      <category>{escape(r.release_status or '')}</category>",
            f"      <description><![CDATA[{desc}]]></description>",
            "    </item>",
        ])

    parts.extend([
        "  </channel>",
        "</rss>",
    ])

    return "\n".join(parts)


def _make_cached_response(
    body: str,
    mimetype: str = "application/json; charset=utf-8",
    last_modified: "datetime | date | None" = None,
) -> Response:
    """Build a Response with ETag, Cache-Control, and Last-Modified headers for Front Door caching.

    ``last_modified`` should be the actual most-recent modification time of the
    underlying data (date or datetime). When omitted, falls back to "now",
    which prevents `If-Modified-Since` short-circuits — pass a data-derived
    value whenever available.
    """
    etag = 'W/"' + hashlib.sha256(body.encode('utf-8')).hexdigest() + '"'
    inm = request.headers.get("If-None-Match")
    if inm and inm == etag:
        return Response(status=304)
    resp = Response(body, mimetype=mimetype)
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f'public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_STALE_TTL}, must-revalidate'

    lm_dt: datetime
    if isinstance(last_modified, datetime):
        lm_dt = last_modified if last_modified.tzinfo else last_modified.replace(tzinfo=timezone.utc)
    elif isinstance(last_modified, date):
        lm_dt = datetime.combine(last_modified, time.min, tzinfo=timezone.utc)
    else:
        lm_dt = datetime.now(timezone.utc)
    resp.headers['Last-Modified'] = format_datetime(lm_dt)
    return resp


def _max_last_modified(items, attr: str = "last_modified"):
    """Pick the maximum non-null last_modified value from a list of dicts or ORM rows.

    Accepts ISO strings, date, or datetime values. Returns the most recent as a
    UTC datetime, or None when nothing is set.
    """
    best: "datetime | None" = None
    for it in items or ():
        val = it.get(attr) if isinstance(it, dict) else getattr(it, attr, None)
        if val is None:
            continue
        if isinstance(val, str):
            try:
                val = datetime.fromisoformat(val)
            except ValueError:
                continue
        if isinstance(val, datetime):
            dt_val = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        elif isinstance(val, date):
            dt_val = datetime.combine(val, time.min, tzinfo=timezone.utc)
        else:
            continue
        if best is None or dt_val > best:
            best = dt_val
    return best


def _clamp_int(value: int, lo: int, hi: int) -> int:
    """Clamp ``value`` into the inclusive range ``[lo, hi]``."""
    return max(lo, min(value, hi))


_TRUTHY_STRINGS = ("1", "true", "yes", "on")
_FALSY_STRINGS = ("0", "false", "no", "off")


def _parse_bool(value, default: bool = False) -> bool:
    """Parse a query-string / form value into a bool.

    Accepts the common truthy strings ("1", "true", "yes", "on") and falsy
    strings ("0", "false", "no", "off"), case-insensitive. Returns ``default``
    for ``None`` or anything unrecognized.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in _TRUTHY_STRINGS:
        return True
    if s in _FALSY_STRINGS:
        return False
    return default


@app.get("/rss")
@app.get("/rss.xml")
def rss_feed():
    product_name = request.args.get("product_name")
    release_type = request.args.get("release_type")
    release_status = request.args.get("release_status")
    try:
        limit = int(request.args.get("limit", "25"))
    except ValueError:
        limit = 25
    limit = _clamp_int(limit, 1, 25)

    rows = get_recently_modified_releases(
        get_engine(),
        limit=limit,
        product_name=product_name,
        release_type=release_type,
        release_status=release_status,
    )
    xml = build_rss_xml(rows, link=EMAIL_BASE_URL, description=f"Up to {limit} most recently modified releases")
    return _make_cached_response(
        xml,
        mimetype="application/rss+xml; charset=utf-8",
        last_modified=_max_last_modified(rows),
    )


@app.get("/")
def index():
    """Modern homepage with Microsoft Fabric design language"""
    return render_template('index.html')


@app.get("/endpoints")
def endpoints():
    """API documentation page (moved from old index)"""
    html = _build_endpoints_html()
    return _make_cached_response(html, mimetype="text/html; charset=utf-8")


def _build_endpoints_html() -> str:
    """Build the endpoints documentation page with examples"""
    # Fetch distinct options (limited for brevity)
    engine = get_engine()
    product_names = get_distinct_values(engine, 'product_name')
    release_types = get_distinct_values(engine, 'release_type')
    release_statuses = get_distinct_values(engine, 'release_status')

    def _mk_examples_data(base: str, values: list[str], param: str, is_rss: bool = True, label: str = "RSS"):
        """Generate data structure for examples instead of HTML"""
        examples = []
        for v in values[:5]:
            qv = urllib.parse.quote_plus(v)
            url = f"{base}?{param}={qv}"
            if is_rss:
                url += "&limit=10"
            examples.append({
                'label': label,
                'title': v,
                'url': url,
                'param': param
            })
        return examples

    def _mk_examples_days_data(base: str, days_values: list[int], is_rss: bool = True, label: str = "RSS"):
        """Generate data structure for days filter examples instead of HTML"""
        examples = []
        for d in days_values:
            url = f"{base}?modified_within_days={d}"
            if is_rss:
                url += "&limit=10"
            examples.append({
                'label': label,
                'title': f'modified_within_days={d}',
                'url': url,
                'param': 'modified_within_days',
                'value': d
            })
        return examples

    # Build RSS examples data
    rss_examples_data = {
        'product_name': _mk_examples_data("/rss", product_names, "product_name", is_rss=True, label="RSS"),
        'release_type': _mk_examples_data("/rss", release_types, "release_type", is_rss=True, label="RSS"),
        'release_status': _mk_examples_data("/rss", release_statuses, "release_status", is_rss=True, label="RSS")
    }

    # Build API examples data
    api_examples_data = {
        'product_name': _mk_examples_data("/api/releases", product_names, "product_name", is_rss=False, label="JSON"),
        'release_type': _mk_examples_data("/api/releases", release_types, "release_type", is_rss=False, label="JSON"),
        'release_status': _mk_examples_data("/api/releases", release_statuses, "release_status", is_rss=False, label="JSON"),
        'modified_within_days': _mk_examples_days_data("/api/releases", [7, 14, 30], is_rss=False, label="JSON")
    }

    # Render template with data structures
    with app.app_context():
        return render_template('endpoints.html', 
                             rss_examples=rss_examples_data,
                             api_examples=api_examples_data)


@app.get("/subscribe")
def subscribe():
    """Email subscription page"""
    return render_template("subscribe.html")


@app.get("/api/releases")
def api_releases():
    """Return paginated JSON array of releases (always wrapped with pagination),
    or a single item if release_item_id is provided.

    Query Params:
      release_item_id: if provided, returns single item (not paginated wrapper)
      product_name, release_type, release_status, modified_within_days, q (search)
      page (1-based, default 1)
      page_size (default 50, max 200)
    """
    query = _parse_releases_query(request.args)

    # Single item path
    if query.release_item_id:
        engine = get_engine()
        row = get_release_item_by_id(engine, query.release_item_id)
        if not row:
            return jsonify({"error": "release_item_id not found"}), 404
        data = _format_release_row(row)
        json_item = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return _make_cached_response(json_item, last_modified=row.last_modified)

    # Generate embedding for vector search when q is provided
    query_embedding = None
    if query.q and query.q.strip() and embeddings_available():
        query_embedding = get_embedding(query.q.strip())

    engine = get_engine()
    if query_embedding is not None:
        total = count_vector_search_releases(
            engine, product_name=query.product_name, release_type=query.release_type,
            release_status=query.release_status, modified_within_days=query.modified_within_days,
            include_inactive=query.include_inactive
        )
        vs_rows = vector_search_releases(
            engine, query_embedding, limit=query.page_size, offset=query.offset,
            product_name=query.product_name, release_type=query.release_type,
            release_status=query.release_status, modified_within_days=query.modified_within_days,
            include_inactive=query.include_inactive
        )
        data_rows = [_format_release_row(r) for r in vs_rows]
    else:
        total = count_recently_modified_releases(
            engine, product_name=query.product_name, release_type=query.release_type,
            release_status=query.release_status, modified_within_days=query.modified_within_days,
            q=query.q, include_inactive=query.include_inactive
        )
        rows = get_recently_modified_releases(
            engine, product_name=query.product_name, release_type=query.release_type,
            release_status=query.release_status, modified_within_days=query.modified_within_days,
            q=query.q, limit=query.page_size, offset=query.offset, sort=query.sort,
            include_inactive=query.include_inactive
        )
        data_rows = [_format_release_row(r) for r in rows]

    pagination = _build_pagination_meta(query, total)
    links = _build_pagination_links(query, total)

    envelope = {
        "data": data_rows,
        "pagination": pagination,
        "links": links,
    }
    json_str = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    resp = _make_cached_response(json_str, last_modified=_max_last_modified(data_rows))
    if links.get("next") or links.get("prev"):
        link_header_parts = []
        if links.get("next"): link_header_parts.append(f"<{links['next']}>; rel=\"next\"")
        if links.get("prev"): link_header_parts.append(f"<{links['prev']}>; rel=\"prev\"")
        if link_header_parts:
            resp.headers['Link'] = ", ".join(link_header_parts)
    return resp


@app.get("/api/filter-options")
def api_filter_options():
    """Return JSON object with all available filter options"""
    engine = get_engine()
    filter_data = {
        "product_names": get_distinct_values(engine, 'product_name'),
        "release_types": get_distinct_values(engine, 'release_type'),
        "release_statuses": get_distinct_values(engine, 'release_status')
    }
    json_str = json.dumps(filter_data, separators=(',', ':'))
    return _make_cached_response(json_str)


@app.get("/api/version")
def api_version():
    """Return the current data version (most recently modified release_item_id).

    Always served with no-cache headers so the client gets a fresh value
    that can be appended to other API calls as a cache-buster.
    """
    engine = get_engine()
    version = get_latest_release_version(engine)
    resp = jsonify({"version": version})
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@app.route("/api/subscribe", methods=["POST"])
@limiter.limit("5 per hour")
def api_subscribe():
    """Subscribe to weekly email updates"""
    data = request.get_json()
    if not data or 'email' not in data:
        return jsonify({"error": "Email is required"}), 400
    
    email = data['email'].strip().lower()
    if not email or '@' not in email:
        return jsonify({"error": "Valid email is required"}), 400
    
    # Optional filters
    filters = {}
    if 'products' in data and data['products']:
        filters['products'] = ','.join(data['products']) if isinstance(data['products'], list) else data['products']
    if 'types' in data and data['types']:
        filters['types'] = ','.join(data['types']) if isinstance(data['types'], list) else data['types']
    if 'statuses' in data and data['statuses']:
        filters['statuses'] = ','.join(data['statuses']) if isinstance(data['statuses'], list) else data['statuses']

    cadence = data.get('cadence', 'weekly')
    if cadence not in ('daily', 'weekly'):
        cadence = 'weekly'
    
    # Generic message used for every non-error outcome to avoid leaking whether
    # the email is already subscribed, newly created, or pending verification.
    generic_success_message = (
        "If this email isn't already subscribed, a verification email is on its way. "
        "Check your Spam or Junk folder if you don't see it soon."
    )

    try:
        engine = get_engine()
        subscription_id, verification_token = create_email_subscription(engine, email, filters, cadence=cadence)

        if not verification_token:
            # Already verified — silently accept without re-sending.
            return jsonify({"message": generic_success_message, "async": False}), 200

        email_queued = send_verification_email(email, verification_token, cadence=cadence)
        if not email_queued:
            otelLogger.warning(f"Subscription created for {email} but verification email failed to initiate")
        return jsonify({
            "message": generic_success_message,
            "async": ASYNC_EMAIL_VERIFICATION,
        }), 200

    except EmailRateLimitExceeded:
        otelLogger.warning(f"Subscribe rate limit hit for {email}")
        return jsonify({"error": "Too many requests. Please try again later."}), 429
    except Exception as e:
        otelLogger.error(f"Error creating subscription: {e}")
        return jsonify({"error": "Failed to create subscription"}), 500


@app.route("/api/send-preferences-link", methods=["POST"])
@limiter.limit("3 per hour")
def api_send_preferences_link():
    """Send an email with a link to manage subscription preferences.

    Always returns 200 to prevent email enumeration.
    """
    data = request.get_json()
    if not data or 'email' not in data:
        return jsonify({"error": "Email is required"}), 400

    email = data['email'].strip().lower()
    if not email or '@' not in email:
        return jsonify({"error": "Valid email is required"}), 400

    # Always return the same message regardless of whether the email exists
    generic_message = "If this email has an active subscription, a preferences link has been sent. Check your inbox (and spam folder)."

    try:
        engine = get_engine()
        sub = get_verified_subscription_by_email(engine, email)
        if sub:
            preferences_url = f"{EMAIL_BASE_URL}/preferences?token={sub.unsubscribe_token}"
            send_preferences_link_email(email, preferences_url)
    except Exception as e:
        otelLogger.error(f"Error processing preferences link request: {e}")

    return jsonify({"message": generic_message}), 200


@app.route("/api/verify-email", methods=["GET"])
@limiter.limit("10 per hour")
def api_verify_email():
    """Verify email subscription"""
    token = request.args.get('token')
    if not token:
        return jsonify({"error": "Verification token is required"}), 400
    
    try:
        engine = get_engine()
        success = verify_email_subscription(engine, token)
        
        if success:
            return jsonify({"message": "Email verified successfully! You will now receive weekly updates."}), 200
        else:
            return jsonify({"error": "Invalid or expired verification token"}), 400
            
    except Exception as e:
        otelLogger.error(f"Error verifying email: {e}")
        return jsonify({"error": "Failed to verify email"}), 500


@app.route("/verify-email", methods=["GET", "POST"])
@limiter.limit("10 per hour", methods=["POST"])
def verify_email_page():
    """HTML page for email verification with explicit confirm step.
    GET: display confirmation page (does NOT verify).
    POST: perform verification using token then show result.
    """
    token = request.values.get('token')
    if not token:
        return render_template('verify_email.html', error="Missing verification token", pending=False), 400

    if request.method == 'POST':
        try:
            engine = get_engine()
            success = verify_email_subscription(engine, token)
            if success:
                return render_template('verify_email.html', success=True, pending=False)
            else:
                return render_template('verify_email.html', error="Invalid or expired verification token", pending=False)
        except Exception as e:
            otelLogger.error(f"Error verifying email: {e}")
            return render_template('verify_email.html', error="Failed to verify email", pending=False), 500

    # GET -> look up cadence and watch context for display
    cadence = 'weekly'
    watch_feature_name = None
    try:
        engine = get_engine()
        ctx = get_verify_email_context(engine, token)
        if ctx is not None:
            cadence = ctx.cadence
            watch_feature_name = ctx.watch_feature_name
    except Exception as e:
        # Surface, but don't fail the page — the GET is purely informational
        # (cadence + watch-feature label). The user can still POST the form
        # to actually verify. Logging means a regression doesn't go silent.
        otelLogger.warning("verify-email context lookup failed: %s", e)
    return render_template('verify_email.html', pending=True, token=token,
                           cadence=cadence, watch_feature_name=watch_feature_name)


@app.route("/unsubscribe", methods=["GET", "POST"])
@limiter.limit("10 per hour", methods=["POST"])
def unsubscribe_page():
    """HTML page for unsubscribing with explicit confirm step.
    GET: display confirmation page (does NOT unsubscribe).
    POST: perform unsubscription using token, send goodbye email, then show result.
    """
    token = request.values.get('token')
    if not token:
        return render_template('unsubscribe.html',
                               error="Missing unsubscribe token",
                               show_request_link=True), 400

    if request.method == 'POST':
        try:
            engine = get_engine()
            email = unsubscribe_email(engine, token)

            if email:
                send_goodbye_email(email)
                return render_template('unsubscribe.html', success=True)
            else:
                return render_template('unsubscribe.html',
                                       error="Invalid unsubscribe token",
                                       show_request_link=True)

        except Exception as e:
            otelLogger.error(f"Error unsubscribing: {e}")
            return render_template('unsubscribe.html',
                                   error="Failed to unsubscribe",
                                   show_request_link=True), 500

    # GET -> verify token resolves before showing the confirm UI so users with
    # stale links see the recovery option instead of a button that will fail.
    engine = get_engine()
    sub = get_subscription_by_unsubscribe_token(engine, token)
    if not sub:
        return render_template('unsubscribe.html',
                               error="This unsubscribe link has expired or is invalid",
                               show_request_link=True), 404

    return render_template('unsubscribe.html', pending=True, token=token)


@app.route("/preferences", methods=["GET"])
def preferences_page():
    """HTML page for managing subscription preferences.

    Rotates the unsubscribe_token on first use so a leaked URL token doesn't
    grant indefinite access. Recently-rotated tokens remain valid via the
    history-table grace window so older email links keep working.
    """
    token = request.args.get('token')
    if not token:
        return render_template('preferences.html',
                               error="Missing preferences token",
                               show_request_link=True), 400

    engine = get_engine()
    sub = get_subscription_by_unsubscribe_token(engine, token)
    if not sub:
        return render_template('preferences.html',
                               error="Invalid or expired token",
                               show_request_link=True), 404

    # If the URL token is the current one, rotate and redirect with the new
    # token so the now-leaked old value (which may live in browser history,
    # access logs, or referrer headers) becomes a history-only token. If the
    # URL token resolved via the history table, don't rotate — just redirect
    # to the current token so the user lands on the canonical URL.
    if token == sub.unsubscribe_token:
        new_token = rotate_unsubscribe_token(engine, token)
        if new_token:
            return redirect(url_for('preferences_page', token=new_token), code=303)
    else:
        # Resolved via the history-table grace window — send the user to the
        # canonical URL so subsequent API calls use the current token.
        return redirect(url_for('preferences_page', token=sub.unsubscribe_token), code=303)

    watches = get_feature_watches_for_subscription(engine, sub.id)
    return render_template('preferences.html', subscription=sub, watches=watches, token=token)


@app.route("/api/preferences", methods=["GET", "POST"])
def api_preferences():
    """Get or update subscription preferences."""
    token = request.values.get('token')
    if not token and request.is_json:
        json_data = request.get_json(silent=True)
        if json_data:
            token = json_data.get('token')
    if not token:
        return jsonify({"error": "Token is required"}), 400

    engine = get_engine()

    if request.method == 'GET':
        sub = get_subscription_by_unsubscribe_token(engine, token)
        if not sub:
            return jsonify({"error": "Invalid token"}), 404
        watches = get_feature_watches_for_subscription(engine, sub.id)
        return jsonify({
            "email": sub.email,
            "cadence": sub.email_cadence,
            "product_filter": sub.product_filter or '',
            "release_type_filter": sub.release_type_filter or '',
            "release_status_filter": sub.release_status_filter or '',
            "watches": watches,
        })

    # POST — update preferences
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    preferences = {}
    if 'cadence' in data:
        cadence = data['cadence']
        if cadence not in ('daily', 'weekly'):
            return jsonify({"error": "cadence must be 'daily' or 'weekly'"}), 400
        preferences['email_cadence'] = cadence
    if 'products' in data:
        preferences['product_filter'] = ','.join(data['products']) if isinstance(data['products'], list) else data['products']
    if 'types' in data:
        preferences['release_type_filter'] = ','.join(data['types']) if isinstance(data['types'], list) else data['types']
    if 'statuses' in data:
        preferences['release_status_filter'] = ','.join(data['statuses']) if isinstance(data['statuses'], list) else data['statuses']

    if not preferences:
        return jsonify({"error": "No valid preferences provided"}), 400

    success = update_subscription_preferences(engine, token, preferences)
    if success:
        return jsonify({"message": "Preferences updated"})
    return jsonify({"error": "Invalid token"}), 404


@app.route("/api/watch", methods=["POST"])
def api_add_watch():
    """Add a feature watch for a subscriber."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    token = data.get('token')
    release_item_id = data.get('release_item_id')
    if not token or not release_item_id:
        return jsonify({"error": "token and release_item_id are required"}), 400

    engine = get_engine()
    sub = get_subscription_by_unsubscribe_token(engine, token)
    if not sub:
        return jsonify({"error": "Invalid token"}), 404

    if not get_release_item_by_id(engine, release_item_id):
        return jsonify({"error": "Release item not found"}), 404

    created = add_feature_watch(engine, sub.id, release_item_id)
    if created:
        return jsonify({"message": "Watch added"}), 201
    return jsonify({"message": "Already watching this feature"})


@app.route("/api/watch", methods=["DELETE"])
def api_remove_watch():
    """Remove a feature watch for a subscriber."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    token = data.get('token')
    release_item_id = data.get('release_item_id')
    if not token or not release_item_id:
        return jsonify({"error": "token and release_item_id are required"}), 400

    engine = get_engine()
    sub = get_subscription_by_unsubscribe_token(engine, token)
    if not sub:
        return jsonify({"error": "Invalid token"}), 404

    removed = remove_feature_watch(engine, sub.id, release_item_id)
    if removed:
        return jsonify({"message": "Watch removed"})
    return jsonify({"error": "Watch not found"}), 404


@app.get("/api/releases/history/<release_item_id>")
def api_release_history(release_item_id: str):
    """Return change history (ChangedColumns + last_modified) for a single release_item_id using stored procedure.

    Stored procedure expected: [dbo].[GetReleaseItemHistoryById]
    Returns columns: VersionNum, release_item_id, ChangedColumns, last_modified
    We only expose ChangedColumns and last_modified sorted desc by VersionNum.
    """
    rows = fetch_history_rows(get_engine(), release_item_id)
    json_str = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return _make_cached_response(json_str, last_modified=_max_last_modified(rows))


@app.get("/about")
def about_page():
    return render_template('about.html')


@app.get("/release/<release_item_id>")
def release_detail(release_item_id):
    """Server-rendered release detail page for SEO."""
    engine = get_engine()
    row = get_release_item_by_id(engine, release_item_id)
    if not row:
        return render_template('release.html', release=None, history=None), 404
    history = fetch_history_rows(engine, release_item_id)
    return render_template('release.html', release=row, history=history)


@app.route("/watch/<release_item_id>", methods=["GET", "POST"])
@limiter.limit("5 per hour", methods=["POST"])
def watch_feature_page(release_item_id):
    """Page to subscribe to watch alerts for a specific release item."""
    engine = get_engine()
    release = get_release_item_by_id(engine, release_item_id)
    if not release:
        return render_template('watch.html', release=None, error="Release not found"), 404
    feature_name = release.feature_name
    product_name = release.product_name

    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        if not email or '@' not in email:
            return render_template('watch.html', release_item_id=release_item_id,
                                   feature_name=feature_name, product_name=product_name,
                                   error="Please enter a valid email address.")

        try:
            _sub_id, verification_token = create_watch_verification(
                engine, email, release_item_id
            )

            send_verification_email(email, verification_token, watch_feature_name=feature_name)
            return render_template('watch.html', release_item_id=release_item_id,
                                   feature_name=feature_name, product_name=product_name,
                                   success=True)
        except EmailRateLimitExceeded:
            otelLogger.warning(f"Watch verification rate limit hit for {email}")
            return render_template('watch.html', release_item_id=release_item_id,
                                   feature_name=feature_name, product_name=product_name,
                                   error="Too many requests. Please try again later."), 429
        except Exception as e:
            otelLogger.error(f"Error creating watch subscription: {e}")
            return render_template('watch.html', release_item_id=release_item_id,
                                   feature_name=feature_name, product_name=product_name,
                                   error="Something went wrong. Please try again.")

    return render_template('watch.html', release_item_id=release_item_id,
                           feature_name=feature_name, product_name=product_name)


@app.get("/changelog")
def changelog_page():
    """Changelog page — data loaded client-side via /api/changelog."""
    return render_template('changelog.html')


@app.get("/api/changelog")
def api_changelog():
    """Return releases grouped by last_modified date for changelog display.

    Query Params:
      days: look-back window (default 30, max 90)
      include_inactive: include removed items (default true)
      product_name, release_type, release_status: filter by field
    """
    days = request.args.get("days", type=int) or 30
    days = _clamp_int(days, 1, 90)
    # Preserve historical behavior: missing param defaults to True, but any
    # unrecognized non-empty value (e.g. "maybe") falls through to False.
    _inc_raw = request.args.get("include_inactive")
    include_inactive = True if _inc_raw is None else _parse_bool(_inc_raw, default=False)
    product_name = request.args.get("product_name") or None
    release_type = request.args.get("release_type") or None
    release_status = request.args.get("release_status") or None

    items = get_changelog_with_changes(
        get_engine(), days=days, include_inactive=include_inactive,
        product_name=product_name, release_type=release_type,
        release_status=release_status,
    )

    # Serialize dates for JSON output. release_date is rendered as
    # "Q# YYYY" to match the post-2026 source format and the public API
    # representation; last_modified stays as an ISO date.
    for item in items:
        rd = item.get("release_date")
        lm = item.get("last_modified")
        item["release_date"] = format_as_quarter(rd) if rd is not None else None
        item["last_modified"] = lm.isoformat() if hasattr(lm, 'isoformat') else lm

    grouped: dict[str, list] = {}
    for item in items:
        grouped.setdefault(item["last_modified"] or "unknown", []).append(item)

    days_list = [{"date": d, "count": len(itms), "items": itms}
                 for d, itms in sorted(grouped.items(), reverse=True)]

    body = json.dumps({"days": days_list, "total_items": len(items)},
                       separators=(",", ":"), sort_keys=True)
    return _make_cached_response(body, last_modified=_max_last_modified(items))


@app.post("/webhooks/email-events")
def email_events_webhook():
    """Handle Azure Event Grid email delivery events (bounces).

    Security:
    - Validates the Event Grid subscription handshake
    - Rejects events not from the configured ACS resource ID
    - Always returns 200 to prevent information leakage
    - Rate-limits bounce counting to once per subscriber per day
    """
    try:
        events = request.get_json(force=True, silent=True)
        if not events or not isinstance(events, list):
            return jsonify({"status": "ok"}), 200

        for event in events:
            event_type = event.get("eventType", "")

            # Event Grid subscription validation handshake
            if event_type == "Microsoft.EventGrid.SubscriptionValidationEvent":
                validation_code = event.get("data", {}).get("validationCode", "")
                otelLogger.info("Event Grid validation handshake received")
                return jsonify({"validationResponse": validation_code}), 200

            # Validate the event comes from our ACS resource
            topic = event.get("topic", "")
            if ACS_RESOURCE_ID and topic != ACS_RESOURCE_ID:
                otelLogger.warning("Rejected email event from unknown resource: %s", topic)
                continue

            # Handle bounce/suppression events
            if event_type == "Microsoft.Communication.EmailDeliveryReportReceived":
                data = event.get("data", {})
                status = data.get("status", "")
                recipient = data.get("recipient", {}).get("address", "")

                if status in ("Bounced", "Suppressed") and recipient:
                    result = record_bounce(get_engine(), recipient)
                    if result:
                        otelLogger.info("Bounce recorded for %s: %s", recipient, result)

    except Exception as e:
        otelLogger.error("Error processing email webhook: %s", e)

    # Always return 200 to prevent email enumeration
    return jsonify({"status": "ok"}), 200

@app.get("/favicon.ico")
def favicon():
    return send_from_directory(app.static_folder, "favicon.ico", mimetype="image/x-icon")


@app.get("/robots.txt")
def robots_txt():
    """Serve robots.txt for search engine crawlers."""
    base = EMAIL_BASE_URL
    body = f"""User-agent: *
Allow: /
Disallow: /verify-email
Disallow: /unsubscribe
Disallow: /preferences
Disallow: /api/subscribe
Disallow: /api/verify-email
Disallow: /api/preferences
Disallow: /api/watch
Disallow: /healthcheck
Disallow: /webhooks/

Sitemap: {base}/sitemap.xml
"""
    return Response(body, mimetype="text/plain")


INDEXNOW_API_KEY = os.getenv("INDEXNOW_API_KEY", "")


@app.get("/<key>.txt")
def indexnow_key_file(key):
    """Serve the IndexNow API key verification file."""
    if not INDEXNOW_API_KEY or key != INDEXNOW_API_KEY:
        return Response("Not found", status=404)
    return Response(INDEXNOW_API_KEY, mimetype="text/plain")


_STATIC_SITEMAP_PAGES = (
    {"loc": "/",          "priority": "1.0", "changefreq": "hourly"},
    {"loc": "/changelog", "priority": "0.9", "changefreq": "hourly"},
    {"loc": "/about",     "priority": "0.7", "changefreq": "monthly"},
    {"loc": "/endpoints", "priority": "0.6", "changefreq": "monthly"},
    {"loc": "/subscribe", "priority": "0.6", "changefreq": "monthly"},
    {"loc": "/rss",       "priority": "0.5", "changefreq": "hourly"},
)


def _render_sitemap_xml(base_url: str, release_rows) -> str:
    """Render the sitemap XML body for the static pages plus per-release URLs.

    ``release_rows`` is any iterable of objects with ``release_item_id`` and
    ``last_modified`` attributes (e.g. SQLAlchemy rows or test stubs). Each
    interpolated component (``base_url``, ``release_item_id``, ``lastmod``)
    is XML-escaped individually — today ``release_item_id`` is a Fabric API
    GUID with no XML-special characters, so this is defense-in-depth, not a
    bug fix.
    """
    parts = []
    for p in _STATIC_SITEMAP_PAGES:
        parts.append(
            "  <url>\n"
            f"    <loc>{escape(base_url + p['loc'])}</loc>\n"
            f"    <changefreq>{escape(p['changefreq'])}</changefreq>\n"
            f"    <priority>{escape(p['priority'])}</priority>\n"
            "  </url>\n"
        )
    for r in release_rows:
        lastmod = ""
        if r.last_modified and hasattr(r.last_modified, 'strftime'):
            lastmod = f"\n    <lastmod>{escape(r.last_modified.strftime('%Y-%m-%d'))}</lastmod>"
        parts.append(
            "  <url>\n"
            f"    <loc>{escape(base_url)}/release/{escape(r.release_item_id)}</loc>{lastmod}\n"
            "    <changefreq>weekly</changefreq>\n"
            "    <priority>0.8</priority>\n"
            "  </url>\n"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + ''.join(parts)
        + '</urlset>\n'
    )


@app.get("/sitemap.xml")
def sitemap_xml():
    """Serve a dynamic XML sitemap for search engines.

    Uses ``_make_cached_response`` so Front Door (and well-behaved bots) can
    cache for an hour rather than re-rendering the full release list on
    every Bing/Google miss. The data only changes hourly via the refresh
    job, so a one-hour TTL aligns with the freshness window.
    """
    engine = get_engine()
    release_rows = get_active_releases_for_sitemap(engine)
    last_modified = _max_last_modified(release_rows)

    body = _render_sitemap_xml(EMAIL_BASE_URL, release_rows)
    return _make_cached_response(
        body,
        mimetype="application/xml; charset=utf-8",
        last_modified=last_modified,
    )


@app.get("/healthcheck")
def healthcheck():
    sql_health = db_healthcheck(get_engine())
    return jsonify(
        {
            "web_server_status": "healthy",
            "sql_status": "healthy" if sql_health else "unhealthy",
        }), 200


@app.context_processor
def inject_no_analytics():
    """Suppress third-party analytics scripts on pages whose URLs carry
    sensitive query tokens. Without this the Umami and App Insights snippets
    in base.html could read window.location.href and exfiltrate the token to
    third-party endpoints (referrer policy doesn't help for in-page JS).
    """
    try:
        path = request.path
    except RuntimeError:
        return {"no_analytics": False}
    no_analytics = path in ("/preferences", "/unsubscribe", "/verify-email")
    return {"no_analytics": no_analytics}


@app.context_processor
def inject_nav():
    nav_items = [
        {"label": "Home", "url": "/"},
        {"label": "Changelog", "url": "/changelog"},
        {"label": "API", "url": "/endpoints"},
        {"label": "Subscribe", "url": "/subscribe"},
        {"label": "About", "url": "/about"},
    ]
    return {"nav_items": nav_items, "current_year": datetime.utcnow().year, "canonical_base": EMAIL_BASE_URL}


# ---------------------------------------------------------------------------
# Email preview (development only)
# ---------------------------------------------------------------------------
if os.getenv("CURRENT_ENVIRONMENT") == "development":

    @app.get("/dev/email-preview")
    def email_preview_index():
        """Index page listing all email previews."""
        html = """<!DOCTYPE html><html><head><title>Email Previews</title>
        <style>body{font-family:Segoe UI,sans-serif;max-width:700px;margin:2rem auto;padding:0 1rem;}
        a{color:#19433c;display:block;margin:.5rem 0;font-size:1.1rem;}</style></head><body>
        <h1>📧 Email Previews</h1>
        <h3>Digest Emails</h3>
        <a href="/dev/email-preview/weekly-digest" target="_blank">Weekly Digest</a>
        <a href="/dev/email-preview/daily-digest" target="_blank">Daily Digest</a>
        <h3>Watch Alert</h3>
        <a href="/dev/email-preview/watch-alert" target="_blank">Feature Watch Alert</a>
        <h3>Verification Emails</h3>
        <a href="/dev/email-preview/verify-weekly" target="_blank">Verify — Weekly Subscription</a>
        <a href="/dev/email-preview/verify-daily" target="_blank">Verify — Daily Subscription</a>
        <a href="/dev/email-preview/verify-watch" target="_blank">Verify — Feature Watch</a>
        <h3>Account Emails</h3>
        <a href="/dev/email-preview/preferences-link" target="_blank">Preferences Link</a>
        </body></html>"""
        return html

    @app.get("/dev/email-preview/<email_type>")
    def email_preview(email_type):
        """Render a preview of an email type. Development only."""
        from datetime import datetime, timedelta
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from weekly_email_job import WeeklyEmailSender

        sample_changes = [
            {
                "release_item_id": "preview-001",
                "feature_name": "Real-Time Intelligence in Power BI",
                "product_name": "Power BI",
                "release_type": "General availability",
                "release_status": "Shipped",
                "feature_description": "Bring real-time data streams into Power BI dashboards with automatic refresh and live alerting capabilities.",
                "release_date": (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d"),
                "last_modified": datetime.utcnow().strftime("%Y-%m-%d"),
                "active": True,
            },
            {
                "release_item_id": "preview-002",
                "feature_name": "Dataflow Gen2 Incremental Refresh",
                "product_name": "Data Factory",
                "release_type": "Public preview",
                "release_status": "Coming soon",
                "feature_description": "Configure incremental refresh policies for Dataflow Gen2 to process only new and changed data.",
                "release_date": (datetime.utcnow() + timedelta(days=60)).strftime("%Y-%m-%d"),
                "last_modified": (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
                "active": True,
            },
            {
                "release_item_id": "preview-003",
                "feature_name": "Lakehouse Shortcut Enhancements",
                "product_name": "OneLake",
                "release_type": "General availability",
                "release_status": "Shipped",
                "feature_description": "New shortcut types and improved metadata sync for OneLake Lakehouse.",
                "release_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "last_modified": (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d"),
                "active": True,
            },
            {
                "release_item_id": "preview-004",
                "feature_name": "Legacy Workspace Migration Tool",
                "product_name": "Power BI",
                "release_type": "General availability",
                "release_status": "Shipped",
                "feature_description": "Automated migration tool for converting legacy workspaces to the new workspace experience.",
                "release_date": (datetime.utcnow() - timedelta(days=10)).strftime("%Y-%m-%d"),
                "last_modified": (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d"),
                "active": False,
            },
        ]

        # Build a mock subscription
        mock_sub = type("MockSub", (), {
            "email": "preview@example.com",
            "email_cadence": "weekly",
            "product_filter": None,
            "release_type_filter": None,
            "release_status_filter": None,
            "unsubscribe_token": "preview-unsub-token",
        })()

        # Create sender without Azure connection (we only need HTML generation)
        sender = object.__new__(WeeklyEmailSender)
        sender.base_url = EMAIL_BASE_URL or "http://localhost:8000"

        sample_ai_summary = (
            "This week saw significant movement across Power BI and OneLake. "
            "Real-Time Intelligence reached general availability in Power BI, "
            "while Data Factory introduced incremental refresh for Dataflow Gen2 in public preview. "
            "The Legacy Workspace Migration Tool was removed from the roadmap."
        )

        if email_type == "weekly-digest":
            mock_sub.email_cadence = "weekly"
            html = sender.generate_email_html(sample_changes, mock_sub, ai_summary=sample_ai_summary, cadence_label="Weekly")
        elif email_type == "daily-digest":
            mock_sub.email_cadence = "daily"
            html = sender.generate_email_html(sample_changes[:2], mock_sub, ai_summary=None, cadence_label="Daily")
        elif email_type == "watch-alert":
            watch_changes = [
                {
                    "release_item_id": "preview-001",
                    "feature_name": "Real-Time Intelligence in Power BI",
                    "product_name": "Power BI",
                    "release_type": "General availability",
                    "release_status": "Shipped",
                    "last_modified": datetime.utcnow().strftime("%Y-%m-%d"),
                    "active": True,
                },
                {
                    "release_item_id": "preview-004",
                    "feature_name": "Legacy Workspace Migration Tool",
                    "product_name": "Power BI",
                    "release_type": "General availability",
                    "release_status": "Shipped",
                    "last_modified": datetime.utcnow().strftime("%Y-%m-%d"),
                    "active": False,
                },
            ]
            # Generate watch HTML inline (reuse the method's HTML generation logic)
            unsubscribe_url = f"{sender.base_url}/unsubscribe?token=preview-unsub-token"
            preferences_url = f"{sender.base_url}/preferences?token=preview-unsub-token"
            BODY_BG = "#f3f2f1"
            CARD_BG = "#ffffff"
            CARD_BORDER = "#e1e5e9"
            CARD_SHADOW = "0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06)"
            TEXT_SECONDARY = "#605e5c"
            HERO_GRADIENT = "linear-gradient(135deg,#19433c 0%,#286c61 100%)"
            items_html = ""
            for w in watch_changes:
                release_url = f"{sender.base_url}/release/{w['release_item_id']}"
                removed_badge = ' <span style="background:#d13438;color:#fff;padding:2px 8px;border-radius:4px;font-size:11px;">REMOVED</span>' if w.get('active') is False else ''
                items_html += f"""<div style="background:{CARD_BG};border:1px solid {CARD_BORDER};border-radius:10px;padding:18px 20px;margin-bottom:12px;box-shadow:{CARD_SHADOW};">
  <div style="font-weight:600;font-size:15px;color:#323130;margin-bottom:6px;">
    <a href="{release_url}" style="color:#19433c;text-decoration:none;">{w.get('feature_name', 'Unknown')}</a>{removed_badge}
  </div>
  <div style="font-size:13px;color:{TEXT_SECONDARY};">{w.get('product_name', '')} · {w.get('release_type', '')} · {w.get('release_status', '')}</div>
  <div style="font-size:12px;color:{TEXT_SECONDARY};margin-top:4px;">Last modified: {w.get('last_modified', 'Unknown')}</div>
</div>"""
            html = f"""<!DOCTYPE html>
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
        <p style="margin:0 0 6px 0;">Sent to preview@example.com</p>
        <p style="margin:0 0 6px 0;">
          <a href="{preferences_url}" style="color:#19433c;text-decoration:none;font-weight:500;">Manage Watches</a>&nbsp;|&nbsp;
          <a href="{unsubscribe_url}" style="color:#19433c;text-decoration:none;font-weight:500;">Unsubscribe</a>
        </p>
      </td></tr>
      <tr><td style="height:30px;"></td></tr>
    </table>
  </td></tr>
</table></body></html>"""
        elif email_type in ("verify-weekly", "verify-daily", "verify-watch"):
            # Build verification email HTML using the same function logic
            base = EMAIL_BASE_URL or "http://localhost:8000"
            verification_url = f"{base}/verify-email?token=preview-token"
            cadence = "daily" if email_type == "verify-daily" else "weekly"
            watch_name = "Real-Time Intelligence in Power BI" if email_type == "verify-watch" else None
            is_watch = bool(watch_name)
            if is_watch:
                preheader = f"Verify your email to start watching {watch_name}."
                hero_text = "Verify your email to start watching a feature on the Microsoft Fabric roadmap."
                heading = "Start Watching a Feature"
                body_text = (f"You requested to watch <strong>{watch_name}</strong> on the Fabric roadmap. "
                             f"Click the button below to confirm your email and start receiving alerts when this feature changes.")
                after_text = "After verifying, you&rsquo;ll be notified whenever this feature is updated on the roadmap."
            else:
                cadence_desc = "daily updates" if cadence == "daily" else "weekly updates every Monday"
                preheader = f"Confirm your email to start {cadence_desc}."
                hero_text = f"Verify your email to begin receiving {cadence_desc} about Microsoft Fabric roadmap changes."
                heading = "Confirm Your Subscription"
                body_text = f"Thanks for signing up! One quick step: click the button below to confirm this address and activate {cadence_desc}."
                after_text = f"After verifying, you&rsquo;ll receive {cadence_desc} with the latest Microsoft Fabric roadmap changes. No spam &mdash; unsubscribe anytime."
            html = f"""<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Verify Your Email - Fabric GPS</title>
<style>body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}</style>
</head><body style='margin:0;padding:0;background:#f3f2f1;'>
<span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;">{preheader}</span>
<table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='background:#f3f2f1;padding:28px 0;'>
<tr><td align='center' style='padding:0 14px;'>
<table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='max-width:620px;'>
<tr><td style="background:linear-gradient(135deg,#19433c 0%,#286c61 100%);color:#ffffff;border-radius:14px;padding:40px 36px 42px 36px;text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);">
<h1 style='margin:0 0 12px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;'>Fabric GPS</h1>
<p style='margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);'>{hero_text}</p>
</td></tr><tr><td style='height:30px;'></td></tr><tr>
<td style='background:#ffffff;border:1px solid #e1e5e9;border-radius:12px;padding:34px 32px;box-shadow:0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06);'>
<h2 style='margin:0 0 18px 0;font-size:22px;line-height:1.25;color:#323130;font-weight:600;text-align:center;'>{heading}</h2>
<p style='margin:0 0 24px 0;font-size:15px;line-height:1.55;color:#605e5c;text-align:center;'>{body_text}</p>
<div style='text-align:center;margin:10px 0 34px 0;'>
<a href='{verification_url}' style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);color:#ffffff;text-decoration:none;padding:14px 26px;font-size:15px;font-weight:600;border-radius:8px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.18);">Verify Email</a>
</div>
<div style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:16px 18px;'>
<p style='margin:0;font-size:13px;line-height:1.5;color:#605e5c;'>{after_text}</p>
</div></td></tr><tr><td style='height:26px;'></td></tr><tr>
<td style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:20px 22px;text-align:center;font-size:12px;line-height:1.55;color:#605e5c;'>
<p style='margin:0 0 6px 0;'>This verification link expires in 24 hours.</p>
<p style='margin:0 0 6px 0;'>If you didn't request this, ignore the message and you won't be subscribed.</p>
<p style='margin:10px 0 0 0;color:#8a8886;'>&copy; Fabric GPS</p>
</td></tr><tr><td style='height:34px;'></td></tr></table></td></tr></table></body></html>"""
        elif email_type == "preferences-link":
            base = EMAIL_BASE_URL or "http://localhost:8000"
            preferences_url = f"{base}/preferences?token=preview-token"
            html = f"""<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Manage Your Subscription - Fabric GPS</title>
<style>body,table,td,p,a {{ font-family:'Segoe UI',system-ui,-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif; }}</style>
</head><body style='margin:0;padding:0;background:#f3f2f1;'>
<span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;">Your Fabric GPS preferences link is inside.</span>
<table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='background:#f3f2f1;padding:28px 0;'>
<tr><td align='center' style='padding:0 14px;'>
<table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='max-width:620px;'>
<tr><td style="background:linear-gradient(135deg,#19433c 0%,#286c61 100%);color:#ffffff;border-radius:14px;padding:40px 36px 42px 36px;text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);">
<h1 style='margin:0 0 12px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;'>Fabric GPS</h1>
<p style='margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);'>Manage your subscription preferences</p>
</td></tr><tr><td style='height:30px;'></td></tr><tr>
<td style='background:#ffffff;border:1px solid #e1e5e9;border-radius:12px;padding:34px 32px;box-shadow:0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06);'>
<h2 style='margin:0 0 18px 0;font-size:22px;line-height:1.25;color:#323130;font-weight:600;text-align:center;'>Your Preferences Link</h2>
<p style='margin:0 0 24px 0;font-size:15px;line-height:1.55;color:#605e5c;text-align:center;'>Click the button below to manage your Fabric GPS subscription &mdash; update your email cadence, filters, and feature watches.</p>
<div style='text-align:center;margin:10px 0 34px 0;'>
<a href='{preferences_url}' style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);color:#ffffff;text-decoration:none;padding:14px 26px;font-size:15px;font-weight:600;border-radius:8px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.18);">Manage Preferences</a>
</div>
<div style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:16px 18px;'>
<p style='margin:0;font-size:13px;line-height:1.5;color:#605e5c;'>Keep this link private &mdash; anyone with it can change your subscription settings.</p>
</div></td></tr><tr><td style='height:26px;'></td></tr><tr>
<td style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:20px 22px;text-align:center;font-size:12px;line-height:1.55;color:#605e5c;'>
<p style='margin:0 0 6px 0;'>If you didn't request this, you can safely ignore this email.</p>
<p style='margin:10px 0 0 0;color:#8a8886;'>&copy; Fabric GPS</p>
</td></tr><tr><td style='height:34px;'></td></tr></table></td></tr></table></body></html>"""
        else:
            return "Unknown email type", 404

        return html
