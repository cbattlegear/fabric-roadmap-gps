# Copilot Instructions — Fabric GPS

## Architecture

Fabric GPS is a Python/Flask application that tracks Microsoft Fabric roadmap releases and matches them with related blog posts using vector embeddings. It runs as a single Docker image with three operational modes controlled by the `APP_MODE` environment variable:

- **`web`** — Flask app served via Gunicorn (`server.py`). Exposes an RSS 2.0 feed, a REST API (`/api/releases`), HTML pages, and email subscription management endpoints.
- **`fetch`** — Data pipeline that runs sequentially: `get_current_releases.py` → `scrape_fabric_blog.py --rss` → `vectorize_blog_posts.py` → `match_releases_to_blogs.py`. Scrapes the Fabric roadmap and blog, generates Azure OpenAI embeddings (`text-embedding-3-small`), and matches releases to blog posts via SQL Server `VECTOR_DISTANCE('cosine', ...)`.
- **`email`** — Weekly batch job (`weekly_email_job.py`) that sends personalized digest emails to verified subscribers via Azure Communication Services.

### Key data flow

1. `get_current_releases.py` fetches from `roadmap.fabric.microsoft.com`, normalizes JSON, and upserts into SQL Server via SQLAlchemy. A SHA-256 `row_hash` on each row detects content changes — only changed rows get a new `last_modified` date and have their `release_vector` and blog references nulled out to trigger re-processing.
2. `scrape_fabric_blog.py` scrapes blog posts into the `fabric_blog_posts` table (uses raw `pyodbc`, not SQLAlchemy).
3. `vectorize_blog_posts.py` and `match_releases_to_blogs.py` generate embeddings and run cosine-distance matching between `release_items.release_vector` and `fabric_blog_posts.blog_vector` columns (SQL Server native `VECTOR(1536)` type).

### Database

- **SQL Server** (Azure SQL) with `pyodbc` + ODBC Driver 18. Two access patterns coexist:
  - `db/db_sqlserver.py` — SQLAlchemy ORM for `release_items`, `email_subscriptions`, `email_verifications` tables. All public functions use the `@retry_on_transient_errors()` decorator for Azure SQL resilience.
  - Standalone scripts (`scrape_fabric_blog.py`, `vectorize_blog_posts.py`, `match_releases_to_blogs.py`) use raw `pyodbc` connections directly for the `fabric_blog_posts` table.
- **Alembic** manages schema migrations (`migrations/`). The connection string comes from the `SQLSERVER_CONN` env var. Migrations run automatically on startup via `alembic upgrade head` in `start.sh`.

### Caching

Caching is handled by Azure Front Door at the CDN edge. The server sets HTTP cache headers (`Cache-Control`, `ETag`, `Last-Modified`) on all API and RSS responses. `Cache-Control: public, max-age=3600` provides a 1-hour fresh window.

### Observability

Azure Monitor / OpenTelemetry is wired into Flask and SQLAlchemy via `azure-monitor-opentelemetry` and the `opentelemetry-instrumentation-*` packages. Each mode sets its own `OTEL_SERVICE_NAME`. Telemetry is disabled when `CURRENT_ENVIRONMENT=development`.

## Build & Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run the web server locally (requires SQLSERVER_CONN env var)
CURRENT_ENVIRONMENT=development python server.py

# Run via Gunicorn
gunicorn -c gunicorn.conf.py

# Docker build & run
docker build -t fabric-gps .
docker run -e APP_MODE=web -e SQLSERVER_CONN="..." -p 8000:8000 fabric-gps

# Run the data fetch pipeline
APP_MODE=fetch bash start.sh

# Run a single pipeline step
python get_current_releases.py
python scrape_fabric_blog.py --rss          # delta via RSS
python scrape_fabric_blog.py --start-page 1 --end-page 10  # full scrape
python vectorize_blog_posts.py
python match_releases_to_blogs.py

# Run Alembic migrations
alembic upgrade head
alembic revision --autogenerate -m "description"
```

## Conventions

- **Dual data access patterns**: `db/db_sqlserver.py` uses SQLAlchemy ORM; blog/vectorization scripts use raw `pyodbc`. Don't mix these in the same module.
- **Transient error handling**: All SQLAlchemy DB functions exposed from `db/db_sqlserver.py` must be decorated with `@retry_on_transient_errors()` for Azure SQL resilience (exponential backoff with jitter).
- **Row-hash change detection**: `release_items` uses a SHA-256 hash (`row_hash`) computed from normalized content fields. Only insert/update when the hash changes, and null out `release_vector`, `blog_title`, `blog_url` on content change to trigger downstream re-processing.
- **`ReleaseItem` dataclass** (`lib/release_item.py`): Used by `get_current_releases.py` for parsing API JSON. Field mapping uses PascalCase keys from the Fabric API (e.g., `FeatureName`, `ReleaseType`) mapped to snake_case model attributes.
- **Templates**: Jinja2 HTML templates in `templates/`, static assets in `static/`. The frontend is server-rendered.
- **Environment variables**: All configuration is via env vars — see `.env.example`. Key vars: `SQLSERVER_CONN`, `AZURE_COMMUNICATION_CONNECTION_STRING`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_KEY`, `APP_MODE`, `CURRENT_ENVIRONMENT`, `BASE_URL`.
- **Naming convention for SQLAlchemy constraints**: Defined in `db/db_sqlserver.py` metadata (`ix_`, `uq_`, `ck_`, `fk_`, `pk_` prefixes). Alembic's `env.py` references `Base.metadata` from this module.
