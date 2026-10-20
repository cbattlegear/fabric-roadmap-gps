# Fabric GPS server

Small Flask server that exposes an RSS 2.0 feed and basic REST API for the Fabric Roadmap

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

## Roadmap Insights

`/insights` is a compact, chart-first Analytics Dashboard: stacked product
activity, a daily status trend, target-change share, and upcoming-quarter bars.
Charts are server-rendered; accessible data tables and event evidence live in
keyboard-operable disclosures rather than filling the overview. Filter by `product_name` and
`days=30` or `days=90`. Counts link to exact, paginated evidence rather than the
changelog's latest-change-per-feature view. No login or JavaScript is required.

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
The About-page status remains Planned until merge and this rollout gate pass.

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

## Caching

Caching is handled by Azure Front Door at the CDN edge. The server sets HTTP cache headers on all API and RSS responses:

- `Cache-Control: public, max-age=14400, stale-while-revalidate=3600` (4h fresh, 1h stale)
- `ETag` — Weak content hash for conditional GET (send `If-None-Match` to receive 304)
- `Last-Modified` — Timestamp for conditional requests