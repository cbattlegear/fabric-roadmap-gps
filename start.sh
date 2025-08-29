#!/usr/bin/env bash
set -euo pipefail

MODE="${APP_MODE:-web}"

if [[ "$MODE" == "web" ]]; then
  # Run the Flask app with Gunicorn
  exec gunicorn -c gunicorn.conf.py
elif [[ "$MODE" == "fetch" ]]; then
  # Run the data fetcher script
  exec python get_current_releases.py
else
  echo "Unknown APP_MODE: $MODE (expected 'web' or 'fetch')" >&2
  exit 1
fi
