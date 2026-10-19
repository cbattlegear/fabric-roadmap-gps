"""Optional ASGI host: one managed listener for MCP and the existing Flask app."""

import asyncio
import logging
import os
import time
from urllib.parse import urlsplit

from a2wsgi import WSGIMiddleware
from mcp.server.transport_security import TransportSecuritySettings
from sqlalchemy import event
from starlette.responses import JSONResponse

from db.db_sqlserver import make_engine
from lib.roadmap_mcp import create_roadmap_server

logger = logging.getLogger(__name__)
MAX_BODY = 16 * 1024
BODY_TIMEOUT_SECONDS = 5


class RoadmapApplication:
    def __init__(self, mcp_app, flask_app, *, rate_limit=60):
        self.mcp_app = mcp_app
        self.flask_app = WSGIMiddleware(flask_app, workers=4)
        self.rate_limit = rate_limit
        self.window_start = time.monotonic()
        self.requests = 0
        self.inflight = 0

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            # The SDK app owns its session manager lifecycle, once per worker.
            return await self.mcp_app(scope, receive, send)
        if scope["type"] != "http" or not (
            scope["path"] == "/mcp" or scope["path"].startswith("/mcp/")
        ):
            return await self.flask_app(scope, receive, send)

        async def no_cache(message):
            if message["type"] == "http.response.start":
                message["headers"] = [
                    (k, v) for k, v in message.get("headers", [])
                    if k.lower() not in (b"cache-control", b"x-content-type-options")
                ] + [(b"cache-control", b"no-store"), (b"x-content-type-options", b"nosniff")]
            await send(message)

        async def reject(status, code):
            logger.info("mcp_transport status=%d code=%s", status, code)
            response = JSONResponse(
                {"error": code}, status_code=status,
                headers=(
                    {"Retry-After": "60"} if status in (429, 503)
                    else {"Allow": "POST"} if status == 405 else None
                ),
            )
            await response(scope, receive, no_cache)

        if scope["path"] != "/mcp":
            return await reject(404, "use_exact_path_/mcp")
        now = time.monotonic()
        if now - self.window_start >= 60:
            self.window_start, self.requests = now, 0
        if self.requests >= self.rate_limit:
            return await reject(429, "rate_limit")
        self.requests += 1
        if scope["method"] != "POST":
            return await reject(405, "stateless_post_only")
        if self.inflight >= 4:
            return await reject(503, "concurrency_limit")
        self.inflight += 1
        try:
            body = bytearray()
            try:
                async with asyncio.timeout(BODY_TIMEOUT_SECONDS):
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            return
                        body.extend(message.get("body", b""))
                        if len(body) > MAX_BODY:
                            return await reject(413, "request_too_large")
                        if not message.get("more_body", False):
                            break
            except TimeoutError:
                return await reject(408, "body_timeout")

            delivered = False

            async def buffered_receive():
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await receive()

            await self.mcp_app(scope, buffered_receive, no_cache)
        finally:
            self.inflight -= 1


def create_app(flask_app, engine_provider, *, base_url, allowed_hosts, allowed_origins):
    if not allowed_hosts or any("*" in host for host in allowed_hosts):
        raise ValueError("MCP_ALLOWED_HOSTS requires exact host[:port] entries, no wildcards")
    if any("*" in origin for origin in allowed_origins):
        raise ValueError("MCP_ALLOWED_ORIGINS must contain exact origins, no wildcards")
    parsed = urlsplit(base_url)
    if parsed.scheme not in ("https", "http") or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("MCP base URL must be an absolute HTTP(S) URL without query/fragment")
    mcp = create_roadmap_server(engine_provider, base_url.rstrip("/"))
    transport = mcp.streamable_http_app(
        json_response=True, stateless_http=True, max_request_body_size=MAX_BODY,
        transport_security=TransportSecuritySettings(
            allowed_hosts=allowed_hosts, allowed_origins=allowed_origins,
        ),
    )
    return RoadmapApplication(transport, flask_app)


def production_app():
    from server import app as flask_app

    base_url = os.environ.get("BASE_URL", "")
    if os.environ.get("CURRENT_ENVIRONMENT") != "development" and not base_url.startswith("https://"):
        raise ValueError("MCP requires an HTTPS BASE_URL in production")
    engine = make_engine(
        connect_args={"timeout": 5}, pool_size=4, max_overflow=0,
        pool_timeout=2, pool_pre_ping=False,
    )

    @event.listens_for(engine, "connect")
    def set_query_timeout(connection, _record):
        connection.timeout = 5

    return create_app(
        flask_app, lambda: engine, base_url=base_url,
        allowed_hosts=[v.strip() for v in os.environ.get("MCP_ALLOWED_HOSTS", "").split(",") if v.strip()],
        allowed_origins=[v.strip() for v in os.environ.get("MCP_ALLOWED_ORIGINS", "").split(",") if v.strip()],
    )
