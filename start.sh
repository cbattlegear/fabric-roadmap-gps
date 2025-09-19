#!/usr/bin/env bash
set -euo pipefail

MODE="${APP_MODE:-web}"

alembic upgrade head

if [[ "$MODE" == "web" ]]; then
  # Run the Flask app with Gunicorn
  exec gunicorn -c gunicorn.conf.py
elif [[ "$MODE" == "fetch" ]]; then
  # Run the data fetcher script
  exec python get_current_releases.py
elif [[ "$MODE" == "email" ]]; then
  # Run the email sending script
  exec python weekly_email_job.py
else
  echo "Unknown APP_MODE: $MODE (expected 'web', 'fetch', or 'email')" >&2
  exit 1
fi
