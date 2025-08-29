import os
import json
import threading
from datetime import datetime, date, time, timezone, timedelta
from typing import Optional, List

from flask import Flask, request, Response, jsonify
from html import escape
import urllib.parse
from email.utils import format_datetime
import time as _time
import hashlib
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from db_sqlserver import make_engine, get_recently_modified_releases, init_db, ReleaseItemModel, get_distinct_values


app = Flask(__name__)

# Lazily create engine on first use to avoid connecting at import time
ENGINE = None

# Simple in-memory cache for RSS XML: key -> { 'xml': str, 'expires': float, 'etag': str, 'built': datetime }
_RSS_CACHE = {}
_API_CACHE = {}
_INDEX_CACHE = {}
_TTL_SECONDS = 24 * 60 * 60  # 24 hours
_CACHE_LOCK = threading.Lock()
_RSS_REFRESHING = set()
_API_REFRESHING = set()
_INDEX_REFRESHING = set()


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
    # Prefer VSO link if present; otherwise fall back to roadmap site with GUID
    return f"https://roadmap.fabric.microsoft.com/?product={escape(urllib.parse.quote_plus(row.product_name.lower().replace(' ', '')))}"

def _description(row: ReleaseItemModel) -> str:
    desc = ""
    if row.release_date:
        desc += f"<p><strong>{row.release_status} {row.release_type} Date:</strong> {escape(row.release_date.isoformat())}</p>"
    if row.feature_description:
        desc += f"<p>{escape(row.feature_description)}</p>"
    return desc or "(no description)"
def _build_index_html() -> str:
    # Fetch distinct options (limited for brevity)
    engine = get_engine()
    product_names = get_distinct_values(engine, 'product_name')
    release_types = get_distinct_values(engine, 'release_type')
    release_statuses = get_distinct_values(engine, 'release_status')

    def _mk_examples(base: str, values: list[str], param: str, is_rss: bool = True):
        examples = []
        for v in values[:5]:
            qv = urllib.parse.quote_plus(v)
            url = f"{base}?{param}={qv}"
            if is_rss:
                url += "&limit=10"
            examples.append(f"<li><a href=\"{url}\">{escape(v)}</a></li>")
        return "\n".join(examples) or "<li>(none)</li>"

    html = [
        "<html><body>",
        "<h1>Fabric GPS Feeds</h1>",
        "<h2>Endpoints</h2>",
        "<ul>",
        "  <li>RSS: <a href=\"/rss\">/rss</a> (<a href=\"/rss.xml\">/rss.xml</a>)</li>",
        "  <li>API: <a href=\"/api/releases\">/api/releases</a></li>",
        "</ul>",
        "<p>Filters (exact match): product_name, release_type, release_status</p>",
        "<p>Responses are cached for 24 hours with background refresh on expiry.</p>",
        "<h2>Filter options (sample)</h2>",
        "<h3>product_name</h3>",
        "<ul>", _mk_examples("/rss", product_names, "product_name"), "</ul>",
        "<ul>", _mk_examples("/api/releases", product_names, "product_name", is_rss=False), "</ul>",
        "<h3>release_type</h3>",
        "<ul>", _mk_examples("/rss", release_types, "release_type"), "</ul>",
        "<ul>", _mk_examples("/api/releases", release_types, "release_type", is_rss=False), "</ul>",
        "<h3>release_status</h3>",
        "<ul>", _mk_examples("/rss", release_statuses, "release_status"), "</ul>",
        "<ul>", _mk_examples("/api/releases", release_statuses, "release_status", is_rss=False), "</ul>",
        "</body></html>",
    ]
    return "".join(html)



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

    # cache key from parameters
    cache_key = (product_name or "", release_type or "", release_status or "", str(limit))
    now_ts = _time.time()
    # Read cache under lock
    with _CACHE_LOCK:
        cached = _RSS_CACHE.get(cache_key)
    if cached:
        # Valid cache: serve fresh
        if cached.get('expires', 0) > now_ts:
            inm = request.headers.get("If-None-Match")
            if inm and inm == cached['etag']:
                return Response(status=304)
            resp = Response(cached['xml'], mimetype="application/rss+xml; charset=utf-8")
            resp.headers['ETag'] = cached['etag']
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(cached['built'].astimezone(timezone.utc))
            return resp
        # Expired: trigger background refresh and serve stale
        def _refresh_rss():
            try:
                rows_bg = get_recently_modified_releases(
                    get_engine(),
                    limit=int(cache_key[3]),
                    product_name=cache_key[0] or None,
                    release_type=cache_key[1] or None,
                    release_status=cache_key[2] or None,
                )
                xml_bg = build_rss_xml(rows_bg, description=f"Up to {cache_key[3]} most recently modified releases")
                built_bg = datetime.now(timezone.utc)
                etag_bg = 'W/"' + hashlib.sha256(xml_bg.encode('utf-8')).hexdigest() + '"'
                with _CACHE_LOCK:
                    _RSS_CACHE[cache_key] = {
                        'xml': xml_bg,
                        'expires': _time.time() + _TTL_SECONDS,
                        'etag': etag_bg,
                        'built': built_bg,
                    }
            finally:
                with _CACHE_LOCK:
                    _RSS_REFRESHING.discard(cache_key)

        with _CACHE_LOCK:
            if cache_key not in _RSS_REFRESHING:
                _RSS_REFRESHING.add(cache_key)
                threading.Thread(target=_refresh_rss, daemon=True).start()
        resp = Response(cached['xml'], mimetype="application/rss+xml; charset=utf-8")
        resp.headers['ETag'] = cached['etag']
        resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_TTL_SECONDS}"
        resp.headers['Last-Modified'] = format_datetime(cached['built'].astimezone(timezone.utc))
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
    with _CACHE_LOCK:
        _RSS_CACHE[cache_key] = {
            'xml': xml,
            'expires': now_ts + _TTL_SECONDS,
            'etag': etag,
            'built': built_dt,
        }
    resp = Response(xml, mimetype="application/rss+xml; charset=utf-8")
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}"
    resp.headers['Last-Modified'] = format_datetime(built_dt)
    return resp


@app.get("/")
def index():
    cache_key = ("index",)
    now_ts = _time.time()
    with _CACHE_LOCK:
        cached = _INDEX_CACHE.get(cache_key)
    if cached:
        if cached.get('expires', 0) > now_ts:
            inm = request.headers.get("If-None-Match")
            if inm and inm == cached['etag']:
                return Response(status=304)
            resp = Response(cached['html'], mimetype="text/html; charset=utf-8")
            resp.headers['ETag'] = cached['etag']
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(cached['built'].astimezone(timezone.utc))
            return resp
        # Expired: refresh in background and serve stale
        def _refresh_index():
            try:
                html_bg = _build_index_html()
                etag_bg = 'W/"' + hashlib.sha256(html_bg.encode('utf-8')).hexdigest() + '"'
                built_bg = datetime.now(timezone.utc)
                with _CACHE_LOCK:
                    _INDEX_CACHE[cache_key] = {
                        'html': html_bg,
                        'expires': _time.time() + _TTL_SECONDS,
                        'etag': etag_bg,
                        'built': built_bg,
                    }
            finally:
                with _CACHE_LOCK:
                    _INDEX_REFRESHING.discard(cache_key)

        with _CACHE_LOCK:
            if cache_key not in _INDEX_REFRESHING:
                _INDEX_REFRESHING.add(cache_key)
                threading.Thread(target=_refresh_index, daemon=True).start()
        resp = Response(cached['html'], mimetype="text/html; charset=utf-8")
        resp.headers['ETag'] = cached['etag']
        resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_TTL_SECONDS}"
        resp.headers['Last-Modified'] = format_datetime(cached['built'].astimezone(timezone.utc))
        resp.headers['X-Cache'] = 'STALE'
        return resp

    # No cache: build synchronously
    html = _build_index_html()
    etag = 'W/"' + hashlib.sha256(html.encode('utf-8')).hexdigest() + '"'
    built_dt = datetime.now(timezone.utc)
    with _CACHE_LOCK:
        _INDEX_CACHE[cache_key] = {
            'html': html,
            'expires': now_ts + _TTL_SECONDS,
            'etag': etag,
            'built': built_dt,
        }
    resp = Response(html, mimetype="text/html; charset=utf-8")
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}"
    resp.headers['Last-Modified'] = format_datetime(built_dt)
    return resp


@app.get("/api/releases")
def api_releases():
    """Return JSON array of releases with optional exact-match filters, no limit."""
    product_name = request.args.get("product_name")
    release_type = request.args.get("release_type")
    release_status = request.args.get("release_status")
    # Cache key and lookup
    cache_key = (product_name or "", release_type or "", release_status or "")
    now_ts = _time.time()
    with _CACHE_LOCK:
        cached = _API_CACHE.get(cache_key)
    if cached:
        if cached.get('expires', 0) > now_ts:
            inm = request.headers.get("If-None-Match")
            if inm and inm == cached['etag']:
                return Response(status=304)
            resp = Response(cached['json'], mimetype="application/json; charset=utf-8")
            resp.headers['ETag'] = cached['etag']
            resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}"
            resp.headers['Last-Modified'] = format_datetime(cached['built'].astimezone(timezone.utc))
            return resp
        # Expired: trigger background refresh and serve stale
        def _refresh_api():
            try:
                SessionLocal = sessionmaker(bind=get_engine(), future=True)
                stmt = select(ReleaseItemModel)
                if cache_key[0]:
                    stmt = stmt.where(ReleaseItemModel.feature_name == cache_key[0])
                if cache_key[1]:
                    stmt = stmt.where(ReleaseItemModel.release_type == cache_key[1])
                if cache_key[2]:
                    stmt = stmt.where(ReleaseItemModel.release_status == cache_key[2])
                stmt = stmt.order_by(ReleaseItemModel.last_modified.desc(), ReleaseItemModel.release_date.desc())
                with SessionLocal() as session:
                    rows = session.scalars(stmt).all()
                    data = [{
                        "release_item_id": r.release_item_id,
                        "feature_name": r.feature_name,
                        "release_date": r.release_date.isoformat() if r.release_date else None,
                        "release_type": r.release_type,
                        "release_status": r.release_status,
                        "product_id": r.product_id,
                        "product_name": r.product_name,
                        "feature_description": r.feature_description,
                        "last_modified": r.last_modified.isoformat() if hasattr(r.last_modified, 'isoformat') and r.last_modified else None,
                    } for r in rows]
                json_bg = json.dumps(data, sort_keys=True, separators=(",", ":"))
                etag_bg = 'W/"' + hashlib.sha256(json_bg.encode('utf-8')).hexdigest() + '"'
                built_bg = datetime.now(timezone.utc)
                with _CACHE_LOCK:
                    _API_CACHE[cache_key] = {
                        'json': json_bg,
                        'expires': _time.time() + _TTL_SECONDS,
                        'etag': etag_bg,
                        'built': built_bg,
                    }
            finally:
                with _CACHE_LOCK:
                    _API_REFRESHING.discard(cache_key)

        with _CACHE_LOCK:
            if cache_key not in _API_REFRESHING:
                _API_REFRESHING.add(cache_key)
                threading.Thread(target=_refresh_api, daemon=True).start()
        resp = Response(cached['json'], mimetype="application/json; charset=utf-8")
        resp.headers['ETag'] = cached['etag']
        resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}, stale-while-revalidate={_TTL_SECONDS}"
        resp.headers['Last-Modified'] = format_datetime(cached['built'].astimezone(timezone.utc))
        resp.headers['X-Cache'] = 'STALE'
        return resp

    SessionLocal = sessionmaker(bind=get_engine(), future=True)
    stmt = select(ReleaseItemModel)
    if product_name is not None:
        stmt = stmt.where(ReleaseItemModel.product_name == product_name)
    if release_type is not None:
        stmt = stmt.where(ReleaseItemModel.release_type == release_type)
    if release_status is not None:
        stmt = stmt.where(ReleaseItemModel.release_status == release_status)

    stmt = stmt.order_by(ReleaseItemModel.last_modified.desc(), ReleaseItemModel.release_date.desc())

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

    with SessionLocal() as session:
        rows = session.scalars(stmt).all()
        data = [_row_to_dict(r) for r in rows]

    # Serialize to stable JSON for ETag
    json_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    etag = 'W/"' + hashlib.sha256(json_str.encode('utf-8')).hexdigest() + '"'
    built_dt = datetime.now(timezone.utc)

    with _CACHE_LOCK:
        _API_CACHE[cache_key] = {
            'json': json_str,
            'expires': now_ts + _TTL_SECONDS,
            'etag': etag,
            'built': built_dt,
        }

    resp = Response(json_str, mimetype="application/json; charset=utf-8")
    resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = f"public, max-age={_TTL_SECONDS}"
    resp.headers['Last-Modified'] = format_datetime(built_dt)
    return resp


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    # For local dev; in production, run with gunicorn/uvicorn, etc.
    app.run(host="0.0.0.0", port=port)
