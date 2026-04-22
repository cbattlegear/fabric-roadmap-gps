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