# Fabric GPS server

Small Flask server that exposes an RSS 2.0 feed and basic REST API for the Fabric Roadmap

The homepage puts roadmap filters, saved Views, and results directly below a
compact header. **Get email updates** opens the existing subscription page for
frequency and preference choices. RSS and API documentation remain in the footer.
On mobile, Product and Search stay visible; **More filters** reveals the existing
type/status/window/sort/removed controls and names the active secondary criteria
alongside their count. Saved views, URL/history restoration, and Clear update
both. Mobile homepage Views keep viewport gutters.
Desktop retains the original expanded fields and ordering. Without JavaScript,
all fields remain visible.

Failed homepage loads offer **Retry** for the same view and failed page.
Pagination errors retain existing cards and pause automatic loading until Retry
or a new filter request; retrying does not update changelog visit checkpoints.

The API page (`/endpoints`) starts with copyable RSS/REST/MCP connections,
followed by endpoint-specific parameters and response examples. It does not
query the database to generate documentation. Current filter values remain
available from `/api/filter-options`; detailed MCP clients, protocols, limits,
and hosting requirements live in [MCP.md](MCP.md).

## Tests

Unit tests live in `tests/` and run with pytest. They cover pure-Python helpers (no live SQL Server or network needed) — currently the Azure SQL retry decorator, the daily/weekly email-cadence filter, and the `ReleaseItem` parsing helpers.

CI runs `pytest` on every push to `main` and every PR (see `.github/workflows/tests.yml`).

### Run locally — virtualenv

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt -r requirements-dev.txt
pytest
```

### Run locally — Docker

If you do your manual testing by spinning up the Docker image, you can run the test suite inside the same container without rebuilding. Mount the working tree, install the dev deps on top of the image's existing site-packages, and invoke pytest:

```bash
docker build -t fabric-gps .
docker run --rm -v "${PWD}:/app" fabric-gps \
  bash -c "pip install -r requirements-dev.txt && pytest"
```

(On PowerShell, use `-v "${PWD}:/app"` as written; on `cmd.exe`, use `-v "%cd%:/app"`.)

The tests don't need `SQLSERVER_CONN` or any other env var — they run entirely in-memory.
Node.js 22+ is also required: pytest invokes the dependency-free browser-logic
checks with `node --test tests/checkpoint.test.cjs`.

## Automatic browser-local changelog visits

`/changelog` retrieves one complete unfiltered `GET /api/changelog?visit=1`
snapshot per document, then highlights events absent from the previous
successful visit. The first successful visit establishes history without
labeling existing events new. After successful rendering, the complete
snapshot is saved automatically in `localStorage`; opening a cached HTML
shell, failed/incomplete loads, or filter-option failures never acknowledge it.
Filters, saved views, and browser back/forward reuse the document's snapshot
and prior event set, so highlights remain stable until the next page visit.
Historical `/changelog/YYYY-MM-DD` pages do not read or advance visit history.

The snapshot contains the last **90 days** of tracked-field temporal events,
an exclusive SQL Server UTC cutoff, and opaque stable event IDs derived from
release identity and the full seven-digit `datetime2` event time. It excludes
pipeline-only embedding/blog writes and preserves same-day edits, removals,
and restorations. A timestamp alone is insufficient: SQL Server `ValidFrom`
is transaction **begin** time. Comparing complete event sets detects a
transaction committed after the previous snapshot even if its temporal start
precedes that snapshot's cutoff. Events outside retained coverage cannot be
highlighted; first recorded appearance is not proof of an original announcement.

The query returns at most 10,001 events to detect overflow; only snapshots
with at most **10,000** events are complete. Larger histories return 503 and
fall back to the ordinary changelog **without saving any checkpoint**. Nothing
is silently capped and marked seen. Temporal diff processing keeps only versions
overlapping the retained window and its boundary predecessor, not every older
version of each selected feature. Browser storage contains only version,
cutoff and at most 10,000 SHA-256 event IDs with their precise public event times
(roughly 980 KB maximum JSON), never
titles, descriptions, filters, or article-click records. Old day-only v1 data
is ignored as inexact and removed after a successful v2 save; damaged history
restarts with a conditional notice. Browser Web Locks serialize writes across
tabs; older tabs cannot replace a newer saved cutoff. Completed snapshots are
unioned so slower requests cannot lose late commits observed by another tab;
individual event times expire IDs outside the rolling 90-day retained window.
A merged manifest over the browser limit fails explicitly without replacing
saved history. Existing documents keep
their original highlights. If locks/storage are unavailable or full, a short
conditional error is shown and ordinary browsing remains usable.

Snapshot responses are `private, no-store`, have no validators, use the
server's clock rather than the browser clock, and never embed personal state
in cached HTML, shared/saved URLs, cookies, or server-side visitor records.
Normal date/product/type/status/removed filters are display-only for the full
manifest acknowledgement.

### Legacy inclusive-day API compatibility

`GET /api/changelog?since=YYYY-MM-DD` accepts a canonical date from today
through 90 days ago, inclusive, and cannot be combined with `date`.
The existing product/type/status and `include_inactive` filters apply to event
values. `days` is ignored in this mode. Temporal tracked-field transitions
include additions, edits, removals and restorations, even when later edits
occur, and exclude embedding/blog-only writes. Catch-up dates each transition
by the UTC day of SQL Server's temporal `ValidFrom`, not the legacy feed's
`last_modified` date, which can intentionally remain unchanged for removals.
In this mode the returned `last_modified` is that observed event day.
Each result item is one change
event (possibly with several changed fields), not a unique feature.
`total_events`, `total_features`, `since`, `through` and `complete` accompany
the existing daily groups. Historical coverage begins August 27, 2025;
first recorded versions are not proof of an earlier original announcement.

Legacy `since` responses remain `private, no-store` with no date-only
conditional validator. They are no longer used for browser visit tracking.
The ordinary API and permanent daily links retain their existing caching/read paths.

## Saved and shared views

On the roadmap (`/`) or full changelog (`/changelog`), choose filters, enter a
view name, and select **Save view**. The saved-view chooser reopens a view;
edit the name and select **Rename**, or select **Delete** to remove it.
Saving always creates a new view, never overwrites an existing one.
Up to 20 views (across both pages), with names of 1-80 characters, are stored
in this browser's localStorage in a validated version-1 record. Collections
do not sync between browsers or accounts. Each page lists its own saved views.
Storage failures are visible; **Reset saved views** clears only this feature's
storage, on both pages, after confirmation.

**Copy view link** shares only public filter state, without a login, saved
names, collection identifiers, subscription tokens, page position, fragments,
or cache-buster parameters. If clipboard access fails, a selected, read-only
link appears for manual copying. A link opens a **live filtered view, not a
historical snapshot**: "Last N days" is relative to when the link is opened.
Roadmap search requires at least two non-whitespace characters to query the
API; shorter text is preserved in a view but does not filter results.

The existing query format remains compatible (no URL version parameter is
required). Supported fields are:

| Page | Query fields |
| --- | --- |
| Both | `product_name`, `release_type`, `release_status`, `include_inactive` |
| Roadmap | `modified_within_days` (`7`, `14`, `30`, or omitted for all time), `sort` (`last_modified` or `release_date`), `q` (search text) |
| Full changelog | `days` (`7`, `14`, `30`, `60`, `90`; default `30`) |

Default values are omitted from links. Removed items remain included by default
as in existing bookmarks; `include_inactive=false` excludes them. Unsupported
fields are ignored and stripped when sharing. Invalid date/sort/boolean values
and products/types/statuses no longer offered by the filter API reset to their
defaults with a visible notice. Filter restoration waits for options, resets
missing values to defaults, and handles browser back/forward. Opening a different
saved view adds a history entry; ordinary filter edits replace the current entry
as before. Editing filters clears the saved-view selection so the same view can
be reopened directly. Reopening a saved roadmap view also resets pagination.

Saved views are managed from the compact **Views** disclosure beside Clear on
the roadmap and full changelog. Its native popover keeps save/open/rename/delete
and link-copy controls out of the closed filter layout, with Escape and outside
click dismissal. Storage errors reveal recovery controls when needed.

Frontend behavior checks use Node.js 22's built-in test runner, launched by
pytest (`tests/test_saved_views.py`). No npm dependencies, network, browser, or
credentials are required. Install Node.js 22+ alongside Python to run them;
missing Node fails with an explicit setup message.

## Roadmap Insights

`/insights` is a compact, chart-first Analytics Dashboard: stacked product
activity, a daily status trend, target-change share, and upcoming-quarter bars.
On desktop the product activity panel sits beside an independent stack of the
three shorter panels; cards stay content-sized instead of stretching to a
shared row height. Mobile preserves the same activity/status/target/upcoming
reading order in one column.
Charts are server-rendered; accessible data tables and event evidence live in
keyboard-operable disclosures rather than filling the overview. Filter by `product_name` and
`days=30` or `days=90`. Each product or quarter row is one full-width native link
to exact, paginated evidence; change-type and daily drilldowns remain in the data
disclosures, with one comfortably sized link per row and counts kept visible.
No login or JavaScript is required.

The same bounded data is available at `/api/insights`, for example:

```text
/api/insights?days=90&product_name=Power%20BI
/api/insights?days=30&metric=target
/api/insights?days=30&metric=status&bucket=2026-10-19
/api/insights?metric=upcoming&bucket=2027-Q1&page=2
```

**Metric definitions**

- History windows are UTC calendar days: `start` inclusive, `end` exclusive.
  The default end is tomorrow's UTC midnight, including the current partial day.
  An optional canonical `end=YYYY-MM-DD` fixes the window; it must be within the
  last 366 days or tomorrow. Repeated retained content transitions for the same
  feature count as separate **events**; distinct-feature counts are also shown.
- Activity categories are mutually exclusive: first retained observation
  (addition), removal, restoration, otherwise substantive edit. The tracked
  fields match the release-ingestion schema. Vectors, blog references,
  `row_hash` and `last_modified` alone never constitute roadmap activity.
  Product attribution uses the version **after** each event, including removals
  and product moves. Missing/empty products are grouped as `Unknown product`.
- Status transitions are events with a known predecessor and a change to
  status/type or their numeric values. Description-only changes and first
  observations are excluded. Target movements compare the previous and revised
  stored dates, including transitions to/from null. These two metrics can overlap
  each other and activity categories; they must not be added together.
- Upcoming targets are a **current snapshot**, independent of historical filters:
  active features not marked `Shipped`, grouped by current/future published
  quarter plus an `unknown` bucket. Unknown statuses are included, never assumed
  delivered; older targets are excluded. Quarter precision follows the current
  source and existing date normalization. Exact stored dates in evidence are not
  promised shipping days, actual delays, or delivery guarantees.
- Coverage is deliberately `complete: false`: retention and the initial import
  are not verified. An addition is only the first **retained observation**.
  A baseline version overlapping the window start supplies the predecessor and
  is not itself counted. Missing predecessors cannot prove transitions.
  SQL Server temporal queries omit zero-duration versions from updates in a
  single transaction; these are not separately observable events. The last
  stored write includes pipeline work and is not a source-refresh heartbeat.

**API and operating bounds**

Technical coverage limitations remain in the API and these metric definitions,
not a prominent dashboard warning. No complete historical coverage is implied.

The JSON response contains `filters`, `coverage`, `activity`, `transitions`, `visuals`,
`target_changes`, `upcoming`, `products`, and `details` (total, items and page
links). `metric` accepts `all`, `addition`, `edit`, `removal`, `restoration`,
`status`, `target`, or `upcoming`. `bucket` accepts only a UTC day for `status`
or `YYYY-QN` / `unknown` for `upcoming`. `product_name` is an exact match, limited
to 200 characters. Detail pages contain at most 50 rows; pages are limited to
1-200 (10,000 browsable observations; totals remain exact). Follow the returned
links to preserve filters. Each endpoint is rate-limited to 30 requests/minute
per worker/IP. Invalid filters return 400; database failures are logged and return
a visible 503 with `no-store`, never zero-filled metrics.

The set-based SQL uses a maximum 90-day temporal range plus one overlapping
baseline, filters products after computing predecessors, returns aggregates and
one evidence page, limits parallelism to two, and sets a 15-second query timeout
(at most two attempts for transient errors). Successful HTML/JSON responses use
one-hour public CDN caching and content-hash ETags. Validators vary with data,
filters, date-window rollover, and pagination.

**Rollout gate:** before enabling this page in production, run the opt-in
`tests/test_insights_sqlserver.py` fixture against a dedicated non-production SQL
Server/Azure SQL database and capture representative actual execution plans and
`STATISTICS IO/TIME` for 30/90 days, all products and a selective product.
Check repeated same-day edits, removals/restorations, pipeline-only writes,
boundary versions, missing retained history, and aggregate/evidence agreement.
Set `INSIGHTS_TEST_SQLSERVER_CONN` only for that disposable test database.
The fixture creates and drops its own uniquely named temporal table; it never
modifies `dbo.release_items`. CI requires no SQL Server, network, or secrets.
No index or persisted aggregate is proposed without those measurements.
This rollout gate still applies before deployment; About no longer publishes a
separate planned-feature list.

`tests/test_native_roadmap_sqlserver.py` additionally requires
`NATIVE_TEST_DISPOSABLE=1`. It exercises native feedback constraints and concurrent
votes, UTC changelog boundaries, and real database-backed MCP protocol calls.
Its feedback/changelog fixtures create uniquely named `gps_validation_*` tables
and drop their current and temporal history tables on completion; MCP reads the
copy's existing roadmap without modifying it. Verify the disposable database
identity and certificate-validated TLS connection before opting in. Keep driver
exceptions and captured test output private: SQL errors can contain connection
details. Never point these tests at production or enable refresh/email jobs.

Empty Insights datasets and evidence pages return JSON arrays, including
`details.items: []`. Origin requests, including conditional requests that return
304, still execute the aggregation; CDN hits are not reproduced by local origin
tests. Product filters and evidence pagination do not eliminate the underlying
aggregate work. Measure sequential and conservatively bounded concurrent cache
misses on deployment-equivalent hardware before treating CDN caching as a
capacity mitigation.

## Email Subscriptions

Subscribers can receive digest emails with customizable options:

- **Cadence**: Daily or Weekly (Mondays)
- **Content filters**: Filter by product, release type, and/or release status
- **Feature watches**: Watch individual release items for instant alerts (checked hourly regardless of digest cadence)

Digest subjects, titles, and counts reflect all changes matching the subscriber's
cadence and filters. HTML emails show up to 20 cards, with the remaining count and
a link to browse the roadmap; plain-text emails show up to 50 items. AI summaries
use at most the 50 most recently modified items.

Roadmap cards, individual release pages, and digest emails share a sanitized
description formatter (`lib/description_format.py`). It repairs explicit heading,
list, and encoded line-break markers in flattened Markdown, without guessing
ambiguous missing word boundaries. Code and link destinations are preserved.
Adjacent bold headings and their explicit Overview/Key Capabilities boundaries
are supported too. A leading heading is omitted only when it exactly matches
the separately displayed feature title and other description content remains.
Email HTML uses inline formatting; plain-text email keeps readable lists and
link destinations.

Formatting happens only when presenting data: no descriptions, row hashes,
timestamps, vectors, or release history are updated. The API's
`feature_description` remains the original source; the additive
`feature_description_html` field contains the display-ready, sanitized HTML.

### Preferences

Existing subscribers can manage their preferences at `/preferences?token=<unsubscribe_token>`. This link is included in the footer of every email.

### Scheduling

The refresh job (`APP_MODE=refresh`) runs the data fetch pipeline (roadmap, blog scrape, vectorization, matching) followed by the email send job. It should be scheduled to run **hourly**. The email step handles both cadences internally:
- Digest emails are sent when each subscriber's cadence interval has elapsed
- Feature watch alerts are checked every run

```bash
# Example cron (every hour at minute 0)
0 * * * * docker run --rm -e APP_MODE=refresh -e SQLSERVER_CONN="..." fabric-gps
```

## Related-article feedback

Roadmap cards and release details offer **Helpful / Not related** for a valid
related article after its link is activated (roadmap cards label it **Related blog**
and expose the article title through its accessible name and hover title). The article opens normally
in a new tab; a compact prompt appears in the original card without taking
focus or claiming that the article was read. Reveal state stays in page memory
for that release/article pair across rerenders, not in persistent click tracking.
`POST /api/blog-feedback` accepts only
`{"release_item_id":"...","blog_url":"...","vote":"helpful"}` (or `not_related`).
It requires an exact configured `BASE_URL` / canonical origin, JSON content type,
and an `X-Feedback-Voter` header containing a browser-generated 64-character
lowercase hex random identifier. Requests are bounded to 4 KiB. Missing releases
return 404, changed/removed matches 409, invalid input 400/415, oversized requests
413, rate limits 429, and unavailable database operations 503. All responses,
including errors, are `no-store`; never cache this endpoint at Front Door.

Feedback is keyed by release ID, canonical article identity, and a SHA-256 hash
of the random browser identifier. Community permalink/canonical URL variants
share the same message ID **only on the community host**; other articles use the
displayed URL with normalized origin and no fragment (query strings are retained).
The original displayed URL is retained for review. Pair verification and upsert
share a transaction with a SQL Server update/serializable lock on the release,
plus a unique pair/voter constraint. Retries cannot inflate totals. A changed
choice updates the existing row, while a rematch gets a separate history.
There is no cascade to roadmap or blog ingestion records.
Pending requests and confirmed choices are kept in page-local state keyed by
release/article identity, so pagination and filter rerenders cannot enable
conflicting votes or lose the displayed result of an in-flight submission.

The identifier is created in local storage only on the first explicit vote and
expires after 180 days. It is sent only on feedback POSTs, never inserted in HTML,
API reads, URLs, or cookies, so cached pages remain shared and anonymous. Storage
must be enabled to vote. Clearing storage, expiry, other browsers, and deliberate
identifier rotation defeat deduplication; counts are **not unique people**.
No IP, email, subscription token, or user-agent is stored as feedback identity.
The visitor-facing article feedback and retention notice lives at `/privacy`,
linked only in the global footer immediately after Email Notifications.
Existing operational telemetry/logs and IP-based Flask-Limiter controls still
apply. Limits are 20/minute and 100/hour **per worker**, in memory, reset on worker
restart, using the existing trusted-proxy IP extraction. Deployments must prevent
direct origin access and sanitize forwarded client IP headers at the trusted
edge; use edge/shared limits for stronger abuse protection.

`get_current_releases.py` deletes rows whose last changed vote is older than
180 days before each hourly refresh. Duplicate unchanged submissions do not
extend retention. Keep that job running and monitor failures; during outages
cleanup is delayed. Database backups follow the configured Azure SQL backup
retention policy. Deleting browser storage does not delete existing server rows.

### Maintainer review (database access only)

Use a read-only database account and bind `:since` to a UTC datetime through your
SQL client (do not interpolate input). This report groups exact pairs and keeps
historical/disappeared matches visible. URLs represent the first submission by
each voter, so `MIN` selects a representative spelling of the canonical article.

```sql
SELECT release_item_id, blog_key, MIN(blog_url) AS example_blog_url,
       SUM(CASE WHEN vote = 'helpful' THEN 1 ELSE 0 END) AS helpful,
       SUM(CASE WHEN vote = 'not_related' THEN 1 ELSE 0 END) AS not_related,
       COUNT(*) AS browser_votes, MAX(updated_at) AS last_vote_utc
FROM blog_match_feedback
WHERE updated_at >= :since
GROUP BY release_item_id, blog_key
ORDER BY not_related DESC, browser_votes DESC;
```

Sample both positive and negative pairs across workloads and vote volumes;
manually label relevance and compare with matching distance/thresholds in a
separate evaluation. Anonymous votes are review evidence only: no suppression,
threshold changes, reranking, retraining, public leaderboard, or new admin UI.

Frontend state/navigation tests are invoked by pytest using Node.js (22 in CI),
with no npm packages, live database, network, or secrets.

## Application release notes

`/about` gives a short product overview, with source/matching details and
community contact behind text links rather than nested promotional cards.

The bottom of `/about` shows, in a white bordered panel matching its neighboring sections, the three most recently published stable
[GitHub Releases](https://github.com/cbattlegear/fabric-roadmap-gps/releases),
ordered by publication time (not tag spelling). These are application updates;
`/changelog` continues to track Microsoft's Fabric roadmap only.

The Docker workflow generates `release_notes.json` before building the image,
using a read-only contents token supplied only to that step. The token is never
written into the snapshot or passed to Docker. Each successful daily
default-branch build reconciles publication, edits, and removals in the
published image; a manual run on the default branch can rebuild sooner. Release events also
rebuild the default branch's `latest` image when the release's tagged commit
contains this workflow trigger. Historical tags without the trigger cannot
start that event-driven build and use the daily/manual path instead.
The repository workflow only builds and publishes image tags: it does **not**
deploy the image, pull it into running containers, or restart the app. Publishing
`latest` does not update an already running container. Notes on the live site
refresh on the next deployment of the updated image and restart of web workers,
not automatically every day. Any external deployment automation must be verified
separately; none is configured in this repository. This does not rely on files
shared with the refresh-job container. The About response uses
`Cache-Control: no-cache` so edge caches revalidate after deployment.

The snapshot records the last successful synchronization in UTC. Updates fail
the workflow explicitly on timeouts, rate limits, invalid data, or other GitHub
errors; no new image is published on failure. An already deployed image retains
its last-known-good snapshot. Snapshots older than 48 hours are labeled as
possibly out of date; missing/invalid snapshots show an unavailable message and
log an error. A successful empty snapshot shows an explicit no-stable-releases
message. No About-page request calls GitHub, and release Markdown has raw HTML
and remote images disabled and links restricted to HTTP(S).

For a local image, run `python -m lib.github_releases` before `docker build`
(optionally provide `GITHUB_TOKEN` in the shell to avoid anonymous rate limits).
Without that step, local About pages show the unavailable state. The generated
snapshot is intentionally ignored by Git.

Maintainers should publish user-facing notes using **Added**, **Improved**, and
**Fixed** sections where useful; historical notes need no reformatting. Drafts
and prereleases are never displayed. Edit the original GitHub release to correct
notes, and use the Docker workflow's manual run to retry a failed synchronization.

## Caching

Caching is handled by Azure Front Door at the CDN edge. The server sets HTTP cache headers on all API and RSS responses:

- `Cache-Control: public, max-age=14400, stale-while-revalidate=3600` (4h fresh, 1h stale)
- `ETag` — Weak content hash for conditional GET (send `If-None-Match` to receive 304)
- `Last-Modified` — Timestamp for conditional requests
## Read-only MCP

Fabric GPS serves an official-SDK Streamable HTTP endpoint at `/mcp` automatically
in web mode, alongside all existing Flask routes on the same managed listener.
It exposes only public roadmap search, detail, history and filter discovery.
See [MCP deployment and client guide](MCP.md) for the supported protocol/SDK,
strict limits, client examples, Front Door configuration, and deployment checks.
No feature flag or new required environment setting is needed: Host/Origin
allowlists derive from the existing `CANONICAL_HOST` or `BASE_URL`. Normal
`gunicorn -c gunicorn.conf.py` startup uses the supported ASGI worker; refresh
mode keeps its existing sequential pipeline.
