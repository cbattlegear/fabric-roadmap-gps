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
Node.js 22+ is also required: pytest invokes the dependency-free browser-logic
checks with `node --test tests/checkpoint.test.cjs`.

## Browser-local changelog catch-up

The `/changelog` catch-up panel stores a versioned UTC calendar day in
`localStorage` only after **Start from today** or **Mark all changes as seen**.
It snapshots the prior baseline in per-tab `sessionStorage`; reloads and
acknowledgements in another tab cannot move an existing tab's baseline.
Closing the tab starts a fresh session. Reset clears this tab's snapshot and
the browser's saved checkpoint; other open tabs retain their snapshots.

The baseline day is **inclusive**. This intentionally repeats same-day changes:
`last_modified` is a date, not an exact timestamp, and later hourly refreshes
on the same day must not be silently skipped. Catch-up retrieves the complete
range before enabling acknowledgement. Product/type/status filters change
what is displayed, not what **Mark all changes as seen** acknowledges.
Failed loads, incomplete responses and browsing alone never save a checkpoint.
First visits do not label the whole roadmap as new. Invalid, future-dated or
expired checkpoints require reset; unavailable storage leaves the normal
changelog usable.

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

Catch-up responses are `private, no-store` with no date-only conditional
validator. Personal checkpoints never enter server-rendered HTML, cookies or
server-side visitor records. The normal changelog and permanent daily links
retain their existing caching and read paths.

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