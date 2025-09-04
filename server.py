import json
import os
import urllib.parse
import threading
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
from opentelemetry.instrumentation.redis import RedisInstrumentor

#FlaskInstrumentor().instrument(enable_commenter=True, commenter_options={})
from db.db_sqlserver import make_engine, get_recently_modified_releases, init_db, ReleaseItemModel, get_distinct_values


app = Flask(__name__)

RedisInstrumentor().instrument()
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

_FRONT_END_TTL = 30 * 60  # 30 minutes

# Redis cache instance (fresh = 24h, stale = 24h)
CACHE = RedisCache(ttl_seconds=_TTL_SECONDS, lock_ttl_seconds=_LOCK_TTL_SECONDS, stale_ttl_seconds=_STALE_TTL_SECONDS)


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
        otelLogger.info(f"Cache hit for RSS feed {'/'.join(parts)}")
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
            resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
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
            resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            resp.headers['X-Cache'] = 'STALE'
            return resp

    otelLogger.info(f"Cache miss for RSS feed {'/'.join(parts)}")
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
    resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
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
        otelLogger.info(f"Cache hit for /endpoints")
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
    otelLogger.info(f"Cache miss for /endpoints")
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
    parts = (product_name or "", release_type or "", release_status or "", str(modified_within_days), q or "")
    print(parts)
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
        otelLogger.info(f"Cache hit for API {'/'.join(parts)}")
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
            resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
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
            resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
            resp.headers['Last-Modified'] = format_datetime(built_dt)
            resp.headers['X-Cache'] = 'STALE'
            return resp
    otelLogger.info(f"Cache miss for API {'/'.join(parts)}")
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
    resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
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
            resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
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
            resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
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
    resp.headers['Cache-Control'] = f"public, max-age={_FRONT_END_TTL}, stale-while-revalidate={_FRONT_END_TTL/2}"
    resp.headers['Last-Modified'] = format_datetime(built_dt)
    return resp


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    # For local dev; in production, run with gunicorn/uvicorn, etc.
    app.run(host="0.0.0.0", port=port)
