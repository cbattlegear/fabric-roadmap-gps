import json
import os
import urllib.parse
import threading
import logging
from datetime import datetime, date, time, timezone, timedelta
from typing import Optional, List

from flask import Flask, request, Response, jsonify, render_template, redirect
from html import escape
from email.utils import format_datetime
import hashlib
from sqlalchemy.orm import sessionmaker

import logging
# Import the `configure_azure_monitor()` function from the
# `azure.monitor.opentelemetry` package.
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
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
    EmailSubscriptionModel, EmailVerificationModel, create_email_subscription, 
    verify_email_subscription, unsubscribe_email, fetch_history_rows, count_recently_modified_releases,
    vector_search_releases, count_vector_search_releases,
    record_bounce,
    healthcheck as db_healthcheck,
    VALID_SORT_OPTIONS,
)
from lib.embeddings import get_embedding, is_available as embeddings_available

os.environ['OTEL_SERVICE_NAME'] = 'fabric-gps-web-frontend'

app = Flask(__name__)

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
# ACS resource ID for validating Event Grid webhook events
ACS_RESOURCE_ID = os.getenv('ACS_RESOURCE_ID', '')


CANONICAL_HOST = os.getenv('CANONICAL_HOST', '')
REDIRECT_HOSTS = {h.strip() for h in os.getenv('REDIRECT_HOSTS', '').split(',') if h.strip()}

# Use canonical host for email URLs if configured, otherwise fall back to BASE_URL
EMAIL_BASE_URL = f"https://{CANONICAL_HOST}" if CANONICAL_HOST else BASE_URL

_NO_CACHE_PATHS = ("/subscribe", "/verify-email", "/unsubscribe", "/api/subscribe", "/api/verify-email")


@app.after_request
def prevent_caching_sensitive_pages(response):
    """Prevent Front Door / browsers from caching pages with user tokens."""
    if request.path.startswith(_NO_CACHE_PATHS):
        response.headers['Cache-Control'] = 'no-store'
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


def send_verification_email(email: str, verification_token: str) -> bool:
    """Send verification email using Azure Communication Services"""
    email_client = get_email_client()
    if not email_client:
        otelLogger.error("Email client not available for verification email")
        return False
    
    try:
        verification_url = f"{EMAIL_BASE_URL}/verify-email?token={verification_token}"

        # HTML email content (restyled to match weekly email design language)
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
    <span style="display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden;mso-hide:all;color:transparent;">Confirm your email to start weekly Fabric roadmap updates.</span>
    <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='background:#f3f2f1;padding:28px 0;'>
        <tr>
            <td align='center' style='padding:0 14px;'>
                <table role='presentation' width='100%' cellpadding='0' cellspacing='0' border='0' style='max-width:620px;'>
                    <tr>
                        <td style="background:linear-gradient(135deg,#19433c 0%,#286c61 100%);color:#ffffff;border-radius:14px;padding:40px 36px 42px 36px;text-align:center;box-shadow:0 4px 14px rgba(0,0,0,0.12);">
                            <h1 style='margin:0 0 12px 0;font-size:26px;line-height:1.2;font-weight:600;letter-spacing:.5px;'>🗺️ Fabric GPS</h1>
                            <p style='margin:0;font-size:15px;line-height:1.5;max-width:520px;display:inline-block;color:rgba(255,255,255,0.95);'>Verify your email to begin receiving weekly Microsoft Fabric roadmap change summaries.</p>
                        </td>
                    </tr>
                    <tr><td style='height:30px;'></td></tr>
                    <tr>
                        <td style='background:#ffffff;border:1px solid #e1e5e9;border-radius:12px;padding:34px 32px;box-shadow:0 1px 2px rgba(0,0,0,0.04),0 4px 10px rgba(0,0,0,0.06);'>
                            <h2 style='margin:0 0 18px 0;font-size:22px;line-height:1.25;color:#323130;font-weight:600;text-align:center;'>Confirm Your Email Address</h2>
                            <p style='margin:0 0 24px 0;font-size:15px;line-height:1.55;color:#605e5c;text-align:center;'>Thanks for signing up! One quick step: click the button below to confirm this address. Then every Monday you will get a concise, styled summary of roadmap item changes from the past week.</p>
                            <div style='text-align:center;margin:10px 0 34px 0;'>
                                <a href='{verification_url}' style="background:#19433c;background-image:linear-gradient(90deg,#19433c,#286c61);color:#ffffff;text-decoration:none;padding:14px 26px;font-size:15px;font-weight:600;border-radius:8px;display:inline-block;box-shadow:0 2px 4px rgba(0,0,0,0.18);">Verify Email</a>
                            </div>
                            <p style='margin:0 0 18px 0;font-size:13px;line-height:1.5;color:#605e5c;text-align:center;'>If the button doesn't work, copy & paste this link:</p>
                            <p style='word-break:break-all;margin:0 0 26px 0;font-size:12px;line-height:1.4;text-align:center;'>
                                <a href='{verification_url}' style='color:#19433c;text-decoration:none;'>{verification_url}</a>
                            </p>
                            <div style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:16px 18px;'>
                                <p style='margin:0;font-size:13px;line-height:1.5;color:#605e5c;'>After verifying, you'll get one email per week (Mondays). No spam &mdash; unsubscribe anytime from those emails.</p>
                            </div>
                        </td>
                    </tr>
                    <tr><td style='height:26px;'></td></tr>
                    <tr>
                        <td style='background:#f8f9fa;border:1px solid #e1e5e9;border-radius:10px;padding:20px 22px;text-align:center;font-size:12px;line-height:1.55;color:#605e5c;'>
                            <p style='margin:0 0 6px 0;'>This verification link expires in 24 hours.</p>
                            <p style='margin:0 0 6px 0;'>If you didn't request this, ignore the message and you won't be subscribed.</p>
                            <p style='margin:10px 0 0 0;color:#8a8886;'>© Fabric GPS</p>
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
        text_content = f"""
            FABRIC GPS - EMAIL VERIFICATION
            
            Thank you for subscribing to Fabric GPS weekly updates!
            
            Please verify your email address by clicking the link below:
            {verification_url}
            
            Once verified, you'll receive weekly emails with the latest Microsoft Fabric roadmap changes.
            
            This verification link will expire in 24 hours.
            If you didn't subscribe to Fabric GPS, you can safely ignore this email.
            
            --
            Fabric GPS Team
        """
        
        message = {
            "senderAddress": FROM_EMAIL,
            "recipients": {
                "to": [{"address": email}]
            },
            "content": {
                "subject": "Verify your email for Fabric GPS weekly updates",
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
                try:
                    # Wait up to 3 minutes; adjust if needed
                    result = poller_obj.result()
                    status = result.get('status') if isinstance(result, dict) else getattr(result, 'get', lambda *_: None)('status')
                    if status == 'Succeeded':
                        otelLogger.info(f"Verification email sent to {target_email}")
                    else:
                        otelLogger.warning(f"Verification email status for {target_email}: {status}")
                except Exception as monitor_exc:
                    otelLogger.error(f"Error monitoring verification email send to {target_email}: {monitor_exc}")

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
        desc += f"<p><strong>{row.release_status} {row.release_type} Date:</strong> {escape(row.release_date.isoformat())}</p>"
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


def _make_cached_response(body: str, mimetype: str = "application/json; charset=utf-8") -> Response:
    """Build a Response with ETag, Cache-Control, and Last-Modified headers for Front Door caching."""
    etag = 'W/"' + hashlib.sha256(body.encode('utf-8')).hexdigest() + '"'
    inm = request.headers.get("If-None-Match")
    if inm and inm == etag:
        return Response(status=304)
    resp = Response(body, mimetype=mimetype)
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f'public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_STALE_TTL}'
    resp.headers['Last-Modified'] = format_datetime(datetime.now(timezone.utc))
    return resp


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
    limit = max(1, min(limit, 25))

    rows = get_recently_modified_releases(
        get_engine(),
        limit=limit,
        product_name=product_name,
        release_type=release_type,
        release_status=release_status,
    )
    xml = build_rss_xml(rows, link=EMAIL_BASE_URL, description=f"Up to {limit} most recently modified releases")
    return _make_cached_response(xml, mimetype="application/rss+xml; charset=utf-8")


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
    release_item_id = request.args.get("release_item_id")
    product_name = request.args.get("product_name")
    release_type = request.args.get("release_type")
    release_status = request.args.get("release_status")
    modified_within_days = request.args.get("modified_within_days", type=int)
    q = request.args.get("q")  # partial search
    page = request.args.get("page", type=int) or 1
    page_size = request.args.get("page_size", type=int) or 50
    sort = request.args.get("sort")
    include_inactive = request.args.get("include_inactive", "").lower() in ("1", "true", "yes")

    if sort not in VALID_SORT_OPTIONS:
        sort = "last_modified"

    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1
    if page_size > 200:
        page_size = 200

    if modified_within_days is not None:
        modified_within_days = max(1, min(modified_within_days, 30))

    def _row_to_dict(r: ReleaseItemModel):
        return {
            "release_item_id": r.release_item_id,
            "feature_name": r.feature_name,
            "release_date": r.release_date.isoformat() if r.release_date else None,
            "release_type": r.release_type,
            "release_status": r.release_status,
            "product_id": r.product_id,
            "product_name": r.product_name,
            "feature_description": r.feature_description,
            "blog_title": r.blog_title,
            "blog_url": r.blog_url,
            "last_modified": r.last_modified.isoformat() if hasattr(r.last_modified, 'isoformat') and r.last_modified else None,
            "active": r.active,
        }

    def _format_vector_row(row):
        rd = row.get("release_date")
        lm = row.get("last_modified")
        return {
            "release_item_id": row.get("release_item_id"),
            "feature_name": row.get("feature_name"),
            "release_date": rd.isoformat() if rd and hasattr(rd, 'isoformat') else None,
            "release_type": row.get("release_type"),
            "release_status": row.get("release_status"),
            "product_id": row.get("product_id"),
            "product_name": row.get("product_name"),
            "feature_description": row.get("feature_description"),
            "blog_title": row.get("blog_title"),
            "blog_url": row.get("blog_url"),
            "last_modified": lm.isoformat() if lm and hasattr(lm, 'isoformat') else None,
            "active": row.get("active"),
            "distance": round(row.get("distance"), 4) if row.get("distance") is not None else None,
        }

    # Single item path
    if release_item_id:
        engine = get_engine()
        SessionLocal = sessionmaker(bind=engine, future=True)
        with SessionLocal() as session:
            row = session.get(ReleaseItemModel, release_item_id)
            if not row:
                return jsonify({"error": "release_item_id not found"}), 404
            data = _row_to_dict(row)
            json_item = json.dumps(data, sort_keys=True, separators=(",", ":"))
            return _make_cached_response(json_item)

    # Generate embedding for vector search when q is provided
    query_embedding = None
    if q and str(q).strip() and embeddings_available():
        query_embedding = get_embedding(str(q).strip())

    offset = (page - 1) * page_size

    # Build response data
    if query_embedding is not None:
        total = count_vector_search_releases(
            get_engine(), product_name=product_name, release_type=release_type,
            release_status=release_status, modified_within_days=modified_within_days,
            include_inactive=include_inactive
        )
        vs_rows = vector_search_releases(
            get_engine(), query_embedding, limit=page_size, offset=offset,
            product_name=product_name, release_type=release_type,
            release_status=release_status, modified_within_days=modified_within_days,
            include_inactive=include_inactive
        )
        data_rows = [_format_vector_row(r) for r in vs_rows]
    else:
        total = count_recently_modified_releases(
            get_engine(), product_name=product_name, release_type=release_type, release_status=release_status,
            modified_within_days=modified_within_days, q=q, include_inactive=include_inactive
        )
        rows = get_recently_modified_releases(
            get_engine(), product_name=product_name, release_type=release_type, release_status=release_status,
            modified_within_days=modified_within_days, q=q, limit=page_size, offset=offset, sort=sort,
            include_inactive=include_inactive
        )
        data_rows = [_row_to_dict(r) for r in rows]
    total_pages = (total + page_size - 1) // page_size if total else 1
    pagination = {
        "page": page,
        "page_size": page_size,
        "total_items": total,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
        "next_page": page + 1 if page < total_pages else None,
        "prev_page": page - 1 if page > 1 else None,
    }

    # Basic HATEOAS-style links (omit base filters if None)
    from urllib.parse import urlencode
    base_params = {
        k: v for k, v in {
            "product_name": product_name,
            "release_type": release_type,
            "release_status": release_status,
            "modified_within_days": modified_within_days,
            "q": q,
            "page_size": page_size,
            "sort": sort if sort != "last_modified" else None,
        }.items() if v not in (None, "")
    }
    def _link(p):
        bp = base_params.copy()
        bp["page"] = p
        return f"/api/releases?{urlencode(bp)}"
    links = {
        "self": _link(page),
        "first": _link(1),
        "last": _link(total_pages),
        "next": _link(page + 1) if page < total_pages else None,
        "prev": _link(page - 1) if page > 1 else None,
    }

    envelope = {
        "data": data_rows,
        "pagination": pagination,
        "links": links,
    }
    json_str = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
    resp = _make_cached_response(json_str)
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


@app.route("/api/subscribe", methods=["POST"])
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
    
    try:
        engine = get_engine()
        subscription_id, verification_token = create_email_subscription(engine, email, filters)
        
        if not verification_token:
            return jsonify({"message": "Email is already subscribed and verified"}), 200
        
        # Send verification email (async by default)
        email_queued = send_verification_email(email, verification_token)
        if email_queued:
            if ASYNC_EMAIL_VERIFICATION:
                return jsonify({
                    "message": "Subscription created! Verification email is being sent. Make sure to check your Spam or Junk folder if you don't see it soon.",
                    "async": True
                }), 201
            else:
                return jsonify({
                    "message": "Subscription created! Verification email sent.",
                    "async": False
                }), 201
        else:
            otelLogger.warning(f"Subscription created for {email} but verification email failed to initiate")
            return jsonify({
                "message": "Subscription created, but the verification email couldn't be sent. Try again later.",
                "async": ASYNC_EMAIL_VERIFICATION
            }), 201
        
    except Exception as e:
        otelLogger.error(f"Error creating subscription: {e}")
        return jsonify({"error": "Failed to create subscription"}), 500

@app.route("/api/verify-email", methods=["GET"])
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

    # GET -> show pending confirmation UI
    return render_template('verify_email.html', pending=True, token=token)


@app.route("/unsubscribe", methods=["GET"])
def unsubscribe_page():
    """HTML page for unsubscribing"""
    token = request.args.get('token')
    if not token:
        return render_template('unsubscribe.html', error="Missing unsubscribe token"), 400
    
    try:
        engine = get_engine()
        success = unsubscribe_email(engine, token)
        
        if success:
            return render_template('unsubscribe.html', success=True)
        else:
            return render_template('unsubscribe.html', error="Invalid unsubscribe token")
            
    except Exception as e:
        otelLogger.error(f"Error unsubscribing: {e}")
        return render_template('unsubscribe.html', error="Failed to unsubscribe"), 500


@app.get("/api/releases/history/<release_item_id>")
def api_release_history(release_item_id: str):
    """Return change history (ChangedColumns + last_modified) for a single release_item_id using stored procedure.

    Stored procedure expected: [dbo].[GetReleaseItemHistoryById]
    Returns columns: VersionNum, release_item_id, ChangedColumns, last_modified
    We only expose ChangedColumns and last_modified sorted desc by VersionNum.
    """
    rows = fetch_history_rows(get_engine(), release_item_id)
    json_str = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return _make_cached_response(json_str)


@app.get("/about")
def about_page():
    return render_template('about.html')


@app.get("/release/<release_item_id>")
def release_detail(release_item_id):
    """Server-rendered release detail page for SEO."""
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        row = session.get(ReleaseItemModel, release_item_id)
        if not row:
            return render_template('release.html', release=None, history=None), 404
        history = fetch_history_rows(engine, release_item_id)
        return render_template('release.html', release=row, history=history)


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

@app.get("/robots.txt")
def robots_txt():
    """Serve robots.txt for search engine crawlers."""
    base = EMAIL_BASE_URL
    body = f"""User-agent: *
Allow: /
Disallow: /verify-email
Disallow: /unsubscribe
Disallow: /api/subscribe
Disallow: /api/verify-email
Disallow: /healthcheck
Disallow: /webhooks/

Sitemap: {base}/sitemap.xml
"""
    return Response(body, mimetype="text/plain")


@app.get("/sitemap.xml")
def sitemap_xml():
    """Serve a dynamic XML sitemap for search engines."""
    base = EMAIL_BASE_URL
    pages = [
        {"loc": "/",          "priority": "1.0", "changefreq": "hourly"},
        {"loc": "/about",     "priority": "0.7", "changefreq": "monthly"},
        {"loc": "/endpoints", "priority": "0.6", "changefreq": "monthly"},
        {"loc": "/subscribe", "priority": "0.6", "changefreq": "monthly"},
        {"loc": "/rss",       "priority": "0.5", "changefreq": "hourly"},
    ]
    urls = ""
    for p in pages:
        urls += f"""  <url>
    <loc>{base}{p["loc"]}</loc>
    <changefreq>{p["changefreq"]}</changefreq>
    <priority>{p["priority"]}</priority>
  </url>
"""
    # Add individual release pages
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        releases = session.query(
            ReleaseItemModel.release_item_id,
            ReleaseItemModel.last_modified
        ).filter(ReleaseItemModel.active == True).all()
        for r in releases:
            lastmod = ""
            if r.last_modified and hasattr(r.last_modified, 'strftime'):
                lastmod = f"\n    <lastmod>{r.last_modified.strftime('%Y-%m-%d')}</lastmod>"
            urls += f"""  <url>
    <loc>{base}/release/{r.release_item_id}</loc>{lastmod}
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>
"""
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{urls}</urlset>
"""
    return Response(body, mimetype="application/xml")


@app.get("/healthcheck")
def healthcheck():
    sql_health = db_healthcheck(get_engine())
    return jsonify(
        {
            "web_server_status": "healthy",
            "sql_status": "healthy" if sql_health else "unhealthy",
        }), 200


@app.context_processor
def inject_nav():
    from datetime import datetime
    nav_items = [
        {"label": "Home", "url": "/"},
        {"label": "API", "url": "/endpoints"},
        {"label": "Subscribe", "url": "/subscribe"},
        {"label": "About", "url": "/about"},
    ]
    return {"nav_items": nav_items, "current_year": datetime.utcnow().year, "canonical_base": EMAIL_BASE_URL}
