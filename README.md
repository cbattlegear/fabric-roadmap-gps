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

Frontend behavior checks use Node.js 22's built-in test runner, launched by
pytest (`tests/test_saved_views.py`). No npm dependencies, network, browser, or
credentials are required. Install Node.js 22+ alongside Python to run them;
missing Node fails with an explicit setup message.

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