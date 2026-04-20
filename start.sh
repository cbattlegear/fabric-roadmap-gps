#!/usr/bin/env bash
set -euo pipefail

MODE="${APP_MODE:-web}"

alembic upgrade head

if [[ "$MODE" == "web" ]]; then
  # Run the Flask app with Gunicorn
  exec gunicorn -c gunicorn.conf.py
elif [[ "$MODE" == "refresh" ]]; then
  # Run the data fetcher pipeline followed by the email sending job
  python get_current_releases.py \
    && python scrape_fabric_blog.py --rss \
    && python vectorize_blog_posts.py \
    && python match_releases_to_blogs.py \
    && exec python weekly_email_job.py
else
  echo "Unknown APP_MODE: $MODE (expected 'web' or 'refresh')" >&2
  exit 1
fi