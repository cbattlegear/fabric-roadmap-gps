# Copilot Instructions — Fabric GPS

## Architecture

Fabric GPS is a Python/Flask application that tracks Microsoft Fabric roadmap releases and matches them with related blog posts using vector embeddings. It runs as a single Docker image with two operational modes controlled by the `APP_MODE` environment variable:

- **`web`** — Flask app served via Gunicorn (`server.py`). Exposes an RSS 2.0 feed, a REST API (`/api/releases`), HTML pages, and email subscription management endpoints.
- **`refresh`** — Hourly batch job that runs the data pipeline followed by the email sender, sequentially: `get_current_releases.py` → `scrape_fabric_blog.py --rss` → `vectorize_blog_posts.py` → `match_releases_to_blogs.py` → `weekly_email_job.py`. Scrapes the Fabric roadmap and blog, generates Azure OpenAI embeddings (`text-embedding-3-small`), matches releases to blog posts via SQL Server `VECTOR_DISTANCE('cosine', ...)`, and then sends digest and watch-alert emails to verified subscribers via Azure Communication Services.

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

# Run the data refresh + email pipeline
APP_MODE=refresh bash start.sh

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

## Workflow

- **Test coverage is required for all code changes.** Every PR that adds or modifies behavior must include corresponding pytest coverage in `tests/`. This applies to bug fixes (add a regression test that fails before the fix and passes after), refactors (existing tests must still cover the touched paths — add coverage if they don't), and new features (cover the happy path plus error/edge cases). The bar is high-leverage tests, not coverage-for-its-own-sake; if a change genuinely has no testable behavior (pure renames, doc-only changes, dependency bumps, formatting, generated migrations), call that out in the PR body so reviewers can confirm. The CI workflow at `.github/workflows/tests.yml` runs `pytest` on every push and PR — keep it green.
- **Where tests live**: `tests/` at the repo root, with `tests/conftest.py` adding the project root to `sys.path` so absolute imports like `from lib.db_retry import …` work. Test modules are named `test_*.py`. Tests must run with no live SQL Server, no network, and no secrets — mock external dependencies (use `unittest.mock`, `freezegun` for time, etc.). For code that's hard to test in its current shape, prefer extracting the testable logic into a pure helper (see `WeeklyEmailSender._filter_by_cadence` for an example) over writing brittle integration tests.
- **Starting new work**: When the user asks to start a new branch (phrases like "jump to main", "pop up to main, new branch for X", "let's start a branch"), run `git checkout main && git pull` first, then create the branch. Use descriptive prefixes matching the task type: `fix/`, `chore/`, `feature/`, `hotfix/`.
- **Pre-push code review**: Before pushing a branch intended for a PR (user says "push and PR", "let's ship this", "send it up", etc.), first invoke the `code-review` sub-agent via the `task` tool on the pending diff. Summarize any high-signal findings (bugs, logic errors, security issues) to the user and wait for their approval or fixes before proceeding to push. Skip this for trivial doc-only changes when the user asks for a fast path.
- **Creating PRs with `gh`**: This repo is developed on Windows/PowerShell. Always write the PR body to a file and pass `--body-file <path>` to `gh pr create` and `gh pr edit`. Never use inline `--body "..."` — PowerShell strips backticks and interprets `\r`/`\f`/`\v`/`\e`/`\n` sequences, which garbles filenames and variable names in the rendered markdown (e.g., `fetch` → `etch`, `refresh` → `\r` + `efresh`). A temp file under the session workspace works well.
