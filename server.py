import json
import os
import urllib.parse
import threading
import logging
from datetime import datetime, date, time, timezone, timedelta
from typing import Optional, List

from flask import Flask, request, Response, jsonify, render_template
from html import escape
from email.utils import format_datetime
import time as _time
import hashlib
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from db.redis_cache import RedisCache

import logging
# Import the `configure_azure_monitor()` function from the
# `azure.monitor.opentelemetry` package.
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

# Azure Communication Services for email
try:
    from azure.communication.email import EmailClient
    AZURE_EMAIL_AVAILABLE = True
except ImportError:
    AZURE_EMAIL_AVAILABLE = False
    logging.warning("Azure Communication Services not available. Email sending will be disabled.")

from db.db_sqlserver import (
    make_engine, get_recently_modified_releases, init_db, ReleaseItemModel, get_distinct_values,
    EmailSubscriptionModel, EmailVerificationModel, create_email_subscription, 
    verify_email_subscription, unsubscribe_email
)


app = Flask(__name__)

FlaskInstrumentor().instrument_app(app)

logger_name = __name__
opentelemetery_logger_name = f'{logger_name}.opentelemetry'
configure_azure_monitor(
    logger_name=opentelemetery_logger_name,
    enable_live_metrics=True 
)
otelLogger= logging.getLogger(opentelemetery_logger_name)
stream = logging.StreamHandler()
otelLogger.addHandler(stream)
otelLogger.setLevel(logging.INFO)
otelLogger.info('Fabric-GPS Website started')

ENGINE = None
REDIS = None

# Cache settings: 24h fresh + 24h stale-while-revalidate window
_TTL_SECONDS = 24 * 60 * 60  # 24 hours fresh
_STALE_TTL_SECONDS = _TTL_SECONDS  # additional stale window
_LOCK_TTL_SECONDS = 60  # lock TTL to avoid stampede during refresh

# Redis cache instance (fresh = 24h, stale = 24h)
CACHE = RedisCache(ttl_seconds=_TTL_SECONDS, lock_ttl_seconds=_LOCK_TTL_SECONDS, stale_ttl_seconds=_STALE_TTL_SECONDS)

# Email configuration
EMAIL_CLIENT = None
FROM_EMAIL = os.getenv('FROM_EMAIL', 'noreply@yourdomain.com')
FROM_NAME = os.getenv('FROM_NAME', 'Fabric GPS')
BASE_URL = os.getenv('BASE_URL', 'http://localhost:8000')


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
        verification_url = f"{BASE_URL}/verify-email?token={verification_token}"
        
        # HTML email content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Verify Your Email - Fabric GPS</title>
        </head>
        <body style="font-family: 'Segoe UI', system-ui, sans-serif; line-height: 1.6; color: #323130; background-color: #f3f2f1; margin: 0; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                <!-- Header -->
                <div style="background: linear-gradient(135deg, #0078d4 0%, #106ebe 100%); color: white; padding: 24px; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px;">🗺️ Fabric GPS</h1>
                    <p style="margin: 8px 0 0 0; opacity: 0.9;">Microsoft Fabric Roadmap Tracker</p>
                </div>
                
                <!-- Content -->
                <div style="padding: 32px 24px;">
                    <h2 style="color: #323130; margin-top: 0; text-align: center;">Verify Your Email Address</h2>
                    <p style="color: #605e5c; margin-bottom: 24px; text-align: center;">
                        Thank you for subscribing to Fabric GPS weekly updates! Please verify your email address to start receiving weekly summaries of Microsoft Fabric roadmap changes.
                    </p>
                    
                    <div style="text-align: center; margin: 32px 0;">
                        <a href="{verification_url}" style="display: inline-block; background: #0078d4; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; font-weight: 600;">
                            Verify Email Address
                        </a>
                    </div>
                    
                    <p style="color: #605e5c; font-size: 14px; text-align: center;">
                        If the button above doesn't work, copy and paste this link into your browser:
                        <br>
                        <a href="{verification_url}" style="color: #0078d4; word-break: break-all;">{verification_url}</a>
                    </p>
                    
                    <div style="margin-top: 32px; padding: 16px; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #0078d4;">
                        <p style="margin: 0; color: #605e5c; font-size: 14px;">
                            <strong>What to expect:</strong> Once verified, you'll receive a weekly email every Monday with the latest Microsoft Fabric roadmap changes that happened in the past week.
                        </p>
                    </div>
                </div>
                
                <!-- Footer -->
                <div style="background: #f8f9fa; padding: 16px; border-top: 1px solid #e1e5e9; text-align: center; font-size: 12px; color: #605e5c;">
                    <p style="margin: 0;">
                        This verification link will expire in 24 hours. If you didn't subscribe to Fabric GPS, you can safely ignore this email.
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
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
        
        poller = email_client.begin_send(message)
        result = poller.result()
        
        if result and hasattr(result, 'status'):
            otelLogger.info(f"Verification email sent to {email} with message ID: {result.id}")
            return True
        else:
            otelLogger.error(f"Unexpected response when sending verification email to {email}")
            return False
            
    except Exception as e:
        otelLogger.error(f"Error sending verification email to {email}: {e}")
        return False


def get_engine():
    global ENGINE
    if ENGINE is None:
        ENGINE = make_engine()
        init_db(ENGINE)
        SQLAlchemyInstrumentor().instrument(engine=ENGINE)
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
    return f"https://roadmap.fabric.microsoft.com/?product={escape(urllib.parse.quote_plus(row.product_name.lower().replace(' ', '')))}"

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
            f"      <guid isPermaLink=\"false\">{escape(guid)}</guid>",
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


@app.get("/rss")
@app.get("/rss.xml")
def rss_feed():
    # Optional filters via query string; exact matches per current helper
    product_name = request.args.get("product_name")
    release_type = request.args.get("release_type")
    release_status = request.args.get("release_status")
    try:
        limit = int(request.args.get("limit", "25"))
    except ValueError:
        limit = 25
    limit = max(1, min(limit, 25))

    # Redis-backed cache key from parameters
    parts = (product_name or "", release_type or "", release_status or "", str(limit))
    cached = CACHE.get("rss", parts)
    now_ts = _time.time()
    if cached:
        fresh_until = cached.get("fresh_until_ts", 0)
        stale_until = cached.get("stale_until_ts", 0)
        built_iso = cached.get("built_iso")
        built_dt = datetime.fromisoformat(built_iso) if built_iso else datetime.now(timezone.utc)
        # fresh
        if now_ts <= fresh_until:
            inm = request.headers.get("If-None-Match")
            if inm and inm == cached.get("etag"):
                return Response(status=304)
            resp = Response(cached.get("body", ""), mimetype="application/rss+xml; charset=utf-8")
            resp.headers['ETag'] = cached.get("etag", "")
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            return resp
        # stale but within serveable window
        if now_ts <= stale_until:
            # Attempt to acquire refresh lock and refresh in background
            if CACHE.try_acquire_lock("rss", parts):
                def _refresh_rss_bg():
                    try:
                        rows_bg = get_recently_modified_releases(
                            get_engine(),
                            limit=int(parts[3]),
                            product_name=parts[0] or None,
                            release_type=parts[1] or None,
                            release_status=parts[2] or None,
                        )
                        xml_bg = build_rss_xml(rows_bg, description=f"Up to {parts[3]} most recently modified releases")
                        built_bg = datetime.now(timezone.utc)
                        etag_bg = 'W/"' + hashlib.sha256(xml_bg.encode('utf-8')).hexdigest() + '"'
                        CACHE.set("rss", parts, xml_bg, etag_bg, built_bg)
                    finally:
                        CACHE.release_lock("rss", parts)

                threading.Thread(target=_refresh_rss_bg, daemon=True).start()
            resp = Response(cached.get("body", ""), mimetype="application/rss+xml; charset=utf-8")
            resp.headers['ETag'] = cached.get("etag", "")
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            resp.headers['X-Cache'] = 'STALE'
            return resp

    # No cache: build synchronously
    rows = get_recently_modified_releases(
        get_engine(),
        limit=limit,
        product_name=product_name,
        release_type=release_type,
        release_status=release_status,
    )
    xml = build_rss_xml(rows, description=f"Up to {limit} most recently modified releases")
    built_dt = datetime.now(timezone.utc)
    etag = 'W/"' + hashlib.sha256(xml.encode('utf-8')).hexdigest() + '"'
    CACHE.set("rss", parts, xml, etag, built_dt)
    resp = Response(xml, mimetype="application/rss+xml; charset=utf-8")
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
    resp.headers['Last-Modified'] = format_datetime(built_dt)
    return resp


@app.get("/")
def index():
    """Modern homepage with Microsoft Fabric design language"""
    return render_template('index.html')


@app.get("/endpoints")
def endpoints():
    """API documentation page (moved from old index)"""
    parts = ("endpoints",)
    cached = CACHE.get("endpoints", parts)
    now_ts = _time.time()
    if cached:
        fresh_until = cached.get("fresh_until_ts", 0)
        stale_until = cached.get("stale_until_ts", 0)
        built_iso = cached.get("built_iso")
        built_dt = datetime.fromisoformat(built_iso) if built_iso else datetime.now(timezone.utc)
        if now_ts <= fresh_until:
            inm = request.headers.get("If-None-Match")
            if inm and inm == cached.get("etag"):
                return Response(status=304)
            return Response(cached.get("body", ""), mimetype="text/html; charset=utf-8", headers={
                'ETag': cached.get("etag", ""),
                'Cache-Control': f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}",
                'Last-Modified': format_datetime(built_dt)
            })
        if now_ts <= stale_until:
            if CACHE.try_acquire_lock("endpoints", parts):
                def _refresh_endpoints_bg():
                    try:
                        html_bg = _build_endpoints_html()
                        etag_bg = 'W/"' + hashlib.sha256(html_bg.encode('utf-8')).hexdigest() + '"'
                        built_bg = datetime.now(timezone.utc)
                        CACHE.set("endpoints", parts, html_bg, etag_bg, built_bg)
                    finally:
                        CACHE.release_lock("endpoints", parts)

                threading.Thread(target=_refresh_endpoints_bg, daemon=True).start()
            return Response(cached.get("body", ""), mimetype="text/html; charset=utf-8", headers={
                'ETag': cached.get("etag", ""),
                'Cache-Control': f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}",
                'Last-Modified': format_datetime(built_dt),
                'X-Cache': 'STALE'
            })

    # No cache or expired beyond stale window: build synchronously
    html = _build_endpoints_html()
    etag = 'W/"' + hashlib.sha256(html.encode('utf-8')).hexdigest() + '"'
    built_dt = datetime.now(timezone.utc)
    CACHE.set("endpoints", parts, html, etag, built_dt)
    return Response(html, mimetype="text/html; charset=utf-8", headers={
        'ETag': etag,
        'Cache-Control': f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}",
        'Last-Modified': format_datetime(built_dt)
    })


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
    """Return JSON array of releases with optional exact-match filters, no limit."""
    product_name = request.args.get("product_name")
    release_type = request.args.get("release_type")
    release_status = request.args.get("release_status")
    modified_within_days = request.args.get("modified_within_days", type=int)
    # Search query (partial/case-insensitive)
    q = request.args.get("q")

    if modified_within_days is not None:
        modified_within_days = max(1, min(modified_within_days, 30))

    # If a search query is provided, bypass caching to avoid filling up cache with one-off queries
    use_cache = not (q and str(q).strip())

    # Redis-backed cache key and lookup
    parts = (product_name or "", release_type or "", release_status or "", modified_within_days, q or "")
    cached = CACHE.get("api", parts) if use_cache else None
    now_ts = _time.time()


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
            "last_modified": r.last_modified.isoformat() if hasattr(r.last_modified, 'isoformat') and r.last_modified else None,
        }

    if cached:
        fresh_until = cached.get("fresh_until_ts", 0)
        stale_until = cached.get("stale_until_ts", 0)
        built_iso = cached.get("built_iso")
        built_dt = datetime.fromisoformat(built_iso) if built_iso else datetime.now(timezone.utc)
        if now_ts <= fresh_until:
            inm = request.headers.get("If-None-Match")
            if inm and inm == cached.get("etag"):
                return Response(status=304)
            resp = Response(cached.get("body", ""), mimetype="application/json; charset=utf-8")
            resp.headers['ETag'] = cached.get("etag", "")
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            return resp
        if now_ts <= stale_until:
            if CACHE.try_acquire_lock("api", parts):
                def _refresh_api_bg():
                    try:
                        rows = get_recently_modified_releases(
                            get_engine(),
                            product_name=product_name,
                            release_type=release_type,
                            release_status=release_status,
                            modified_within_days=modified_within_days,
                            q=q,
                        )
                        data = [_row_to_dict(r) for r in rows]

                        json_bg = json.dumps(data, sort_keys=True, separators=(",", ":"))
                        etag_bg = 'W/"' + hashlib.sha256(json_bg.encode('utf-8')).hexdigest() + '"'
                        built_bg = datetime.now(timezone.utc)
                        CACHE.set("api", parts, json_bg, etag_bg, built_bg)
                    finally:
                        CACHE.release_lock("api", parts)

                threading.Thread(target=_refresh_api_bg, daemon=True).start()
            resp = Response(cached.get("body", ""), mimetype="application/json; charset=utf-8")
            resp.headers['ETag'] = cached.get("etag", "")
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            resp.headers['X-Cache'] = 'STALE'
            return resp

    rows = get_recently_modified_releases(
        get_engine(),
        product_name=product_name,
        release_type=release_type,
        release_status=release_status,
        modified_within_days=modified_within_days,
        q=q,
    )
    data = [_row_to_dict(r) for r in rows]

    # Serialize to stable JSON for ETag
    json_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    etag = 'W/"' + hashlib.sha256(json_str.encode('utf-8')).hexdigest() + '"'
    built_dt = datetime.now(timezone.utc)
    if use_cache:
        CACHE.set("api", parts, json_str, etag, built_dt)
    resp = Response(json_str, mimetype="application/json; charset=utf-8")
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
    resp.headers['Last-Modified'] = format_datetime(built_dt)
    return resp


@app.get("/api/filter-options")
def api_filter_options():
    """Return JSON object with all available filter options"""
    # Redis-backed cache key for filter options
    parts = ("filter-options",)
    cached = CACHE.get("filter-options", parts)
    now_ts = _time.time()
    
    if cached:
        fresh_until = cached.get("fresh_until_ts", 0)
        stale_until = cached.get("stale_until_ts", 0)
        built_iso = cached.get("built_iso")
        built_dt = datetime.fromisoformat(built_iso) if built_iso else datetime.now(timezone.utc)
        
        # Fresh cache
        if now_ts <= fresh_until:
            inm = request.headers.get("If-None-Match")
            if inm and inm == cached.get("etag"):
                return Response(status=304)
            resp = Response(cached.get("body", ""), mimetype="application/json; charset=utf-8")
            resp.headers['ETag'] = cached.get("etag", "")
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            return resp
        
        # Stale but serveable
        if now_ts <= stale_until:
            if CACHE.try_acquire_lock("filter-options", parts):
                def _refresh_filter_options_bg():
                    try:
                        engine_bg = get_engine()
                        filter_data_bg = {
                            "product_names": get_distinct_values(engine_bg, 'product_name'),
                            "release_types": get_distinct_values(engine_bg, 'release_type'),
                            "release_statuses": get_distinct_values(engine_bg, 'release_status')
                        }
                        json_bg = json.dumps(filter_data_bg, separators=(',', ':'))
                        etag_bg = 'W/"' + hashlib.sha256(json_bg.encode('utf-8')).hexdigest() + '"'
                        built_bg = datetime.now(timezone.utc)
                        CACHE.set("filter-options", parts, json_bg, etag_bg, built_bg)
                    finally:
                        CACHE.release_lock("filter-options", parts)

                threading.Thread(target=_refresh_filter_options_bg, daemon=True).start()
            
            resp = Response(cached.get("body", ""), mimetype="application/json; charset=utf-8")
            resp.headers['ETag'] = cached.get("etag", "")
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            resp.headers['X-Cache'] = 'STALE'
            return resp

    # No cache or expired: build synchronously
    engine = get_engine()
    filter_data = {
        "product_names": get_distinct_values(engine, 'product_name'),
        "release_types": get_distinct_values(engine, 'release_type'),
        "release_statuses": get_distinct_values(engine, 'release_status')
    }
    
    json_str = json.dumps(filter_data, separators=(',', ':'))
    etag = 'W/"' + hashlib.sha256(json_str.encode('utf-8')).hexdigest() + '"'
    built_dt = datetime.now(timezone.utc)
    CACHE.set("filter-options", parts, json_str, etag, built_dt)
    
    resp = Response(json_str, mimetype="application/json; charset=utf-8")
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_STALE_TTL_SECONDS}"
    resp.headers['Last-Modified'] = format_datetime(built_dt)
    return resp


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
        
        # Send verification email
        email_sent = send_verification_email(email, verification_token)
        
        if email_sent:
            return jsonify({
                "message": "Subscription created successfully! Please check your email for a verification link."
            }), 201
        else:
            # Even if email fails, subscription was created - user can try again
            otelLogger.warning(f"Subscription created for {email} but verification email failed to send")
            return jsonify({
                "message": "Subscription created, but there was an issue sending the verification email. Please try again or contact support.",
                "verification_url": f"/verify-email?token={verification_token}"  # Fallback for testing
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


@app.route("/verify-email", methods=["GET"])
def verify_email_page():
    """HTML page for email verification"""
    token = request.args.get('token')
    if not token:
        return render_template('verify_email.html', error="Missing verification token"), 400
    
    try:
        engine = get_engine()
        success = verify_email_subscription(engine, token)
        
        if success:
            return render_template('verify_email.html', success=True)
        else:
            return render_template('verify_email.html', error="Invalid or expired verification token")
            
    except Exception as e:
        otelLogger.error(f"Error verifying email: {e}")
        return render_template('verify_email.html', error="Failed to verify email"), 500


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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    # For local dev; in production, run with gunicorn/uvicorn, etc.
    app.run(host="0.0.0.0", port=port)
