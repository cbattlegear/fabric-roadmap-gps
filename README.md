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

## Caching

Caching is handled by Azure Front Door at the CDN edge. The server sets HTTP cache headers on all API and RSS responses:

- `Cache-Control: public, max-age=14400, stale-while-revalidate=3600` (4h fresh, 1h stale)
- `ETag` — Weak content hash for conditional GET (send `If-None-Match` to receive 304)
- `Last-Modified` — Timestamp for conditional requests