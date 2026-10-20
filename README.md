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