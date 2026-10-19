"""Opt-in ASGI hosting; normal web and refresh modes retain their entrypoints."""

import os

bind = os.environ.get("GUNICORN_BIND", "0.0.0.0:8000")
workers = int(os.environ.get("GUNICORN_WORKERS", "2"))
worker_class = "uvicorn_worker.UvicornWorker"
wsgi_app = "mcp_app:production_app()"
timeout = 60
keepalive = 5
# No URL/query logging. Tool telemetry records operation/outcome/duration only.
accesslog = None
errorlog = "-"
loglevel = os.environ.get("GUNICORN_LOGLEVEL", "info")
