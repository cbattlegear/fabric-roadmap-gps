# Public roadmap MCP

## Status and compatibility

Implemented, **disabled by default** (`MCP_ENABLED=0`). This is not a claim that
the production Front Door endpoint is live or verified. Keep the About MCP
status Planned until the deployment smoke test below succeeds.

The official [`mcp==2.3.0` Python SDK](https://py.sdk.modelcontextprotocol.io/)
owns JSON-RPC parsing, initialization, capability/tool discovery, validation,
tool calls, errors and transport lifecycle. The endpoint uses **Streamable HTTP**
at exactly `/mcp`, JSON responses, and stateless legacy sessions. No SSE endpoint,
push notifications, resumability, sampling, or sticky sessions are needed.
Pytest exercises the real SDK `ClientSession` + `streamable_http_client`
initialize/discover/call sequence on negotiated **2025-11-25**, and the SDK's
**2026-07-28** sessionless request path. Other versions remain SDK-negotiated;
they are not separately certified here. Never replace this with REST-shaped JSON
or a homemade token/authorization protocol.

Official integration references:
[ASGI lifespan](https://py.sdk.modelcontextprotocol.io/run/asgi/),
[deployment/hosts](https://py.sdk.modelcontextprotocol.io/run/deploy/),
[protocol versions](https://py.sdk.modelcontextprotocol.io/protocol-versions/).

## Tool contracts

All four tools are read-only, idempotent, and return structured content plus
SDK-generated text content for older clients. Release, blog and history text
is **untrusted data**, not assistant instructions. No tools manage subscriptions,
watches, email, feedback, credentials or arbitrary SQL/URLs. There are no
embedding, HTTP fetch, or write calls.

| Tool | Arguments | Result |
| --- | --- | --- |
| `search_releases` | `query` object (required, may be `{}`) | `data`, `pagination`, `search_mode: "substring"`, `page_limit` |
| `get_release` | `release_item_id` | Public release with canonical `url`, including inactive records |
| `get_release_history` | `release_item_id`, `limit` (default 10, max 50) | Newest changes in `data`, `has_more`, canonical `url` |
| `get_filter_options` | none | `filters`, `truncated_filters`, canonical home `url` |

`query` rejects unknown fields and coerced types. It accepts `q` (up to 200
characters), exact `product_name`, `release_type`, `release_status` (each up to
200 characters), `include_inactive` (boolean, default false),
`modified_within_days` (1-30), `sort` (`last_modified` or `release_date`, default
`last_modified`), `page` (1-100) and `page_size` (1-25, default 10).
Use filter discovery for available values. IDs accept 1-36 alphanumeric/hyphen
characters, matching current roadmap IDs.

Search uses the **same retry-decorated SQL readers, filters and public formatter
as REST's non-semantic path**. It performs case-insensitive SQL `LIKE` matching
on feature name/description/product; `%` and `_` retain SQL wildcard semantics.
It is not relevance filtering. REST continues to use embeddings when configured;
MCP never does, even if OpenAI credentials exist. Both sorts are descending with
stable release-ID tie breaking. Pagination returns REST-style counts and next
page numbers, but is not a snapshot: a refresh may shift rows. At page 100,
`has_next` is false even when `total_pages` exceeds 100; narrow filters instead.
Dates/windows use UTC date semantics (inclusive lower bound); release dates are
quarters such as `Q2 2026`, not exact promised launch dates.

Public output is allowlisted: ID, name, quarter, type, status, product, description,
blog title/URL, last-modified date, active flag and canonical GPS link. It excludes
vectors, hashes, internal VSO links, ORM internals, email and subscription data.
HTML duplicates are omitted. Descriptions are capped at 4,000 characters; other
release strings at 1,000, with `truncated_fields` indicating shortened values.
Non-HTTP(S) blog links are removed. History fetches at most `limit+1` rows from
the existing ordered history procedure, emits at most 12 changes per row,
1,000 characters each, and flags truncated text. Filters return at most 100
values per list, 200 characters per value, with explicit truncation metadata.
Use canonical links for full content.

Tool errors use SDK `isError=true` (NOT_FOUND, BUSY, TIMEOUT, UNAVAILABLE,
RESULT_TOO_LARGE); never a success-shaped fallback. Invalid JSON-RPC uses SDK
protocol errors. Transport rejections use HTTP 400/403/404/405/408/413/421/429/503,
not fake JSON-RPC success. On 429/503 respect `Retry-After: 60`.

## Hosting and launch policy

The image and refresh pipeline are unchanged. In **web** mode only,
`MCP_ENABLED=1` selects `mcp_gunicorn.conf.py`: Gunicorn supervises two
`uvicorn_worker.UvicornWorker` ASGI workers on the existing port. The top-level
ASGI dispatcher sends `/mcp` to the SDK Starlette app and every existing route
to Flask through `a2wsgi` (four WSGI threads per worker). It forwards lifespan
directly to the SDK app so each worker starts/stops its session manager exactly
once. No extra listener/server is started inside a worker. Normal Flask-only
hosting remains `gunicorn.conf.py`; `APP_MODE=refresh` never imports the MCP host.
Do not use preload or override the MCP worker class with `gthread`.

Example production environment (use the **actual** public/forwarded host):

```text
APP_MODE=web
MCP_ENABLED=1
BASE_URL=https://www.fabric-gps.com
MCP_ALLOWED_HOSTS=www.fabric-gps.com
MCP_ALLOWED_ORIGINS=https://www.fabric-gps.com
GUNICORN_WORKERS=2
FORWARDED_ALLOW_IPS=<trusted proxy IPs>
```

Existing SQL/email/telemetry environment requirements still apply. Hosts are exact
`host[:port]` entries; no wildcards, including wildcard ports. Non-browser clients
may omit Origin; when sent it must match the exact allowlist. CORS is intentionally
not enabled, so this MVP supports native/CLI clients, not cross-origin browsers.
Never disable DNS-rebinding protection to fix a misconfigured proxy Host.
For local development use `CURRENT_ENVIRONMENT=development`, an explicit
`BASE_URL=http://127.0.0.1:8000` and `MCP_ALLOWED_HOSTS=127.0.0.1:8000`,
then `python -m uvicorn mcp_app:production_app --factory --port 8000`.

Anonymous opt-in access is affordable only under these fixed per-worker budgets:
60 HTTP requests/minute globally (includes initialize/discovery and invalid
requests), four inflight MCP HTTP requests, four live DB tool jobs, 16 KiB bodies,
five seconds to receive a body, 15-second tool response deadline, 8-second retry
budget, five-second ODBC login/statement timeouts, pool size four/no overflow,
two-second pool wait. Global worker limits do not trust spoofable forwarded IPs
and use constant memory. Two workers allow up to 120 requests/minute, eight
DB jobs; more replicas multiply budgets. Use Front Door WAF global/per-IP
rate limiting and an origin access restriction as the cross-instance control.
Short-lived SQL operations can finish after a caller's deadline/disconnect;
their slot is held until they actually finish, so cancellation cannot create
unbounded background SQL work. The retry budget prevents new retries/queries
after expiry; an already-running statement can take another five seconds.
ODBC driver/OS failures are still subject to Gunicorn's worker-health watchdog.
The application caps each structured payload at 256 KiB; legacy-compatible
text duplication can approximately double the wire payload.

Launch is a deliberate operator decision: **do not enable** anonymous public
access until origin restrictions, WAF budgets and the smoke test are in place.
If these budgets prove unaffordable, disable MCP and adopt the SDK's supported
MCP OAuth authorization flow; do not add a bespoke API-key scheme.

Telemetry logs only tool operation/outcome/duration and transport rejection code.
Bounded DB retry logs omit SQL, parameter values, credentials and traceback text.
Do not enable SDK HTTP/body debug logging or proxy POST-body capture. Existing
Flask telemetry continues for Flask routes. Collect aggregate 429/503, latency,
timeouts and DB pool pressure; do not record user query bodies.

## Azure Front Door / reverse proxy

Create a dedicated **uncached** route/rule matching `/mcp` and `/mcp/*`, ahead of
generic REST caching and canonical redirects. Disable caching regardless of
method/status; all application MCP responses also send `Cache-Control: no-store`.
Do not rewrite `/mcp` to `/mcp/`, and do not redirect protocol POSTs.

Preserve `Host` matching `MCP_ALLOWED_HOSTS`, `Origin`, `Content-Type`,
`Accept`, `MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`, `Mcp-Session-Id`,
`Last-Event-ID`, JSON response content types, and any SDK protocol/session
response headers. Permit POST; GET/DELETE return 405 (`Allow: POST`) because
streaming/session termination are unsupported in this stateless deployment.
Preserve 202 notification acknowledgments,
JSON-RPC error bodies/statuses, `Retry-After` and no-store. Set the origin
response timeout above 20 seconds (30 recommended), with request-body limits
at or below 16 KiB and a slow-client timeout. JSON-only responses do not require
SSE buffering exceptions or long-lived streams; if enabling SSE later, revisit
buffering, idle timeouts and concurrency before deployment.

Restrict direct origin ingress to Front Door (including the specific Front Door
instance identity); do not trust arbitrary `X-Forwarded-For` or forwarded scheme.
Configure `FORWARDED_ALLOW_IPS` only for trusted hops. A host-allowlist 421 is a
configuration error, not justification to accept every host.

## Clients and smoke test

Compatible native clients must support remote Streamable HTTP (not stdio-only).
For example, Claude Code:

```text
claude mcp add --transport http fabric-gps https://www.fabric-gps.com/mcp
```

Official pinned Python client (`pip install mcp==2.3.0`):

```python
import asyncio
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client

async def smoke():
    async with streamable_http_client("https://www.fabric-gps.com/mcp") as streams:
        async with ClientSession(*streams) as client:
            print(await client.initialize())
            print(await client.list_tools())
            result = await client.call_tool("search_releases", {"query": {"page_size": 1}})
            assert not result.is_error
            print(result.structured_content)

asyncio.run(smoke())
```

**Required deployment gate (not possible in offline pytest):** on staging, then
through the public Front Door URL, run this client to initialize, list all four
tools and call search, filters, detail and history for a returned ID. Compare
records against `/api/releases?release_item_id=...` and its history endpoint.
Check no `Age`/cache-hit response on POST, no redirected POST, JSON content
types and no-store on successful/error requests. Test disallowed Host/Origin
(421/403), 16 KiB overflow (413), invalid input, empty results and rate limiting
(429); use staging for load tests. Confirm `/`, RSS/API, healthcheck and refresh
mode still work; test multiple workers/restarts without session affinity. Record
deployment version, client/negotiated protocol, Front Door rule and results in
the release/PR before enabling generally or updating About to delivered.
