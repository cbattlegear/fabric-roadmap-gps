# Public roadmap MCP

## Status and compatibility

MCP is **included automatically in normal web startup** when this version is
deployed. It has no opt-in feature flag, separate listener, or new required
environment setting. This is not a claim that the production Front Door
endpoint has been deployed or verified: Linux Gunicorn and public Front Door
smoke checks remain validation gaps until run in that environment.

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

## Standard web hosting

The image and refresh pipeline are unchanged. In **web** mode,
`start.sh` runs the normal `gunicorn.conf.py`: Gunicorn supervises two
`uvicorn_worker.UvicornWorker` ASGI workers on the existing port. The top-level
ASGI dispatcher sends `/mcp` to the SDK Starlette app and every existing route
to Flask through `a2wsgi` (four WSGI threads per worker). It forwards lifespan
directly to the SDK app so each worker starts/stops its session manager exactly
once. No extra listener/server is started inside a worker.
`APP_MODE=refresh` never imports the MCP host and retains the sequential data
and email pipeline. There is no Flask-only alternative selected by a flag.
The required ASGI worker replaces `gthread`; the old `GUNICORN_WORKER_CLASS`
and `GUNICORN_THREADS` settings no longer select a WSGI-only host. Flask has
four adapter threads per worker. Do not use preload or replace the ASGI worker.

Example production environment (use the **actual** public/forwarded host):

```text
APP_MODE=web
BASE_URL=https://www.fabric-gps.com
GUNICORN_WORKERS=2
FORWARDED_ALLOW_IPS=<trusted proxy IPs>
```

Existing SQL/email/telemetry environment requirements still apply. Canonical
release/client links and Host/Origin allowlists derive from the existing
`CANONICAL_HOST` as HTTPS when set, otherwise `BASE_URL` (development default:
`http://localhost:8000`). Production already requires an HTTPS public URL.
No new setting is needed for the normal public hostname. If a proxy forwards a
different Host, optional `MCP_ALLOWED_HOSTS` and `MCP_ALLOWED_ORIGINS` overrides
accept comma-separated exact values. They are deployment-specific overrides,
not enablement requirements. Hosts are exact `host[:port]` entries; no wildcards,
including wildcard ports. Non-browser clients
may omit Origin; when sent it must match the exact allowlist. CORS is intentionally
not enabled, so this MVP supports native/CLI clients, not cross-origin browsers.
Never disable DNS-rebinding protection to fix a misconfigured proxy Host.
For local development use `CURRENT_ENVIRONMENT=development`, an explicit
`BASE_URL=http://127.0.0.1:8000` (without a production `CANONICAL_HOST`),
then `python -m uvicorn mcp_app:production_app --factory --port 8000`.

Automatic anonymous access is constrained by these fixed per-worker budgets:
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

Deploy this version with origin restrictions and WAF budgets in place, and
perform the smoke checks below as part of deployment validation. They are not
an application feature gate: web startup always serves MCP. If anonymous usage
proves unaffordable, apply an explicit edge policy and plan the SDK's supported
MCP OAuth authorization flow; do not add a bespoke API-key scheme.

## Usage and diagnostic telemetry

MCP uses the existing web Azure Monitor setup, service name
`fabric-gps-web-frontend`, and logger `server.opentelemetry.mcp` (a child of the
configured `server.opentelemetry` logger). Configuration runs once at web startup,
not per request. Azure export remains disabled in development or without
`APPLICATIONINSIGHTS_CONNECTION_STRING`. Refresh service names/setup, Flask
instrumentation and its healthcheck exclusion are unchanged. No analytics
database, browser tracking, user identity measurement or new dependency is added.

Each handled MCP HTTP request emits one `mcp_request` log and `mcp.request`
server span, including initialization, discovery, notification acknowledgments,
invalid requests, Host/Origin rejections, throttling, disconnects and cancellations.
Each SDK-dispatched `tools/call` request additionally emits one `mcp_tool` log
and child `mcp.tool` span, including rejected arguments, unknown tools, BUSY,
timeouts and errors. Polls, discovery and notifications are not executed tools.
The pinned SDK's default inbound telemetry middleware is replaced, not stacked:
its raw IDs/names/exception details are unsuitable for anonymous public traffic.
SDK protocol parsing, validation and security middleware remain in place.

Logs and spans carry these fixed fields:

| Field | Meaning |
| --- | --- |
| `mcp.kind` | `request` or `tool` |
| `mcp.method` | Allowlisted MCP operation, otherwise `unknown` |
| `mcp.tool` | One of the four public tools, `unknown`, or `none` |
| `mcp.outcome` | `ok`, `validation_error`, `protocol_error`, `tool_error`, `unavailable`, `busy`, `timeout`, `cancelled`, `error`, or a fixed transport rejection outcome |
| `mcp.executed` | Whether the call entered bounded tool execution; false for validation failures/BUSY and non-tool requests |
| `mcp.duration_ms` | Elapsed milliseconds through completion, rejection or cancellation, not background SQL lifetime |
| `http.response.status_code` | Request logs/spans only; zero if no response was started |

HTTP 200 with SDK `isError=true` has a failure outcome and error span status,
not `ok`. Notification success means acknowledgment, not tool execution or a
promise that background notification handling completed. Body identification
only labels the already-bounded body with allowlisted names; it does not validate
or replace SDK parsing. Rejections before body reading have `unknown` method.
Ambient OTel context parents request spans, tool spans and logs; no request IDs,
headers or client metadata are copied to telemetry. Deadline/cancellation
observations do not release a still-running SQL job's slot.

No bodies, arguments, filters, results, IDs, query URLs, Origin/Host values,
clientInfo, IPs, auth/cookies/headers, exception messages or tracebacks are added
to these records. Bounded DB retry logs also omit SQL, parameter values,
credentials and traceback text. Do not enable root/SDK HTTP/body debug export,
access logging or proxy POST-body capture.

After deployment, use **Application Insights > Logs** (Application Insights
table names below; workspace equivalents are `AppTraces`, `Message`,
`Properties`, `TimeGenerated`, `AppRoleName` and `ItemCount`):

```kusto
traces
| where timestamp > ago(24h) and cloud_RoleName == "fabric-gps-web-frontend"
| where message == "mcp_request"
| summarize requests = sum(itemCount),
    p95_ms = percentile(todouble(customDimensions["mcp.duration_ms"]), 95)
    by method = tostring(customDimensions["mcp.method"]),
       outcome = tostring(customDimensions["mcp.outcome"]),
       status = toint(customDimensions["http.response.status_code"])
```

```kusto
traces
| where timestamp > ago(24h) and cloud_RoleName == "fabric-gps-web-frontend"
| where message == "mcp_tool"
| summarize attempts = sum(itemCount),
    p95_ms = percentile(todouble(customDimensions["mcp.duration_ms"]), 95)
    by tool = tostring(customDimensions["mcp.tool"]),
       outcome = tostring(customDimensions["mcp.outcome"]),
       executed = tobool(customDimensions["mcp.executed"])
```

Use `operation_Id` to correlate logs with the `mcp.request` span in `requests`
and `mcp.tool` spans in `dependencies`. Do not sum request and tool observations
as a single usage count. These are operational observations subject to existing
export/sampling/retention policy, not billing-grade or unique-user analytics.
Offline tests use in-memory log/span exporters with no Azure credentials or
network export; actual deployed ingestion/dashboard validation remains unverified
until deployment.

## Azure Front Door / reverse proxy

Create a dedicated **uncached** route/rule matching `/mcp` and `/mcp/*`, ahead of
generic REST caching and canonical redirects. Disable caching regardless of
method/status; all application MCP responses also send `Cache-Control: no-store`.
Do not rewrite `/mcp` to `/mcp/`, and do not redirect protocol POSTs.

Preserve `Host` matching the derived public hostname (or the exact
`MCP_ALLOWED_HOSTS` override), `Origin`, `Content-Type`,
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

**Deployment validation (not possible in offline pytest):** on staging, then
through the public Front Door URL, run this client to initialize, list all four
tools and call search, filters, detail and history for a returned ID. Compare
records against `/api/releases?release_item_id=...` and its history endpoint.
Check no `Age`/cache-hit response on POST, no redirected POST, JSON content
types and no-store on successful/error requests. Test disallowed Host/Origin
(421/403), 16 KiB overflow (413), invalid input, empty results and rate limiting
(429); use staging for load tests. Confirm `/`, RSS/API, healthcheck and refresh
mode still work; test multiple workers/restarts without session affinity. Record
deployment version, client/negotiated protocol, Front Door rule and results in
the release/PR. Until then report the deployment checks as unverified, without
claiming the endpoint is disabled or requiring a manual feature enablement step.
