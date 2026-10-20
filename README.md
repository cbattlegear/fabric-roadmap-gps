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

## Related-article feedback

Roadmap cards and release details offer **Helpful / Not related** for a valid
related article. `POST /api/blog-feedback` accepts only
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

The bottom of `/about` shows the three most recently published stable
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
