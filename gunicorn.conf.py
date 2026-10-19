import os

# Bind and workers
bind = os.environ.get('GUNICORN_BIND', '0.0.0.0:8000')
# Keep the anonymous MCP budget bounded independently of host CPU count.
workers = int(os.environ.get('GUNICORN_WORKERS', '2'))
worker_class = 'uvicorn_worker.UvicornWorker'

# Timeouts
timeout = int(os.environ.get('GUNICORN_TIMEOUT', '60'))
keepalive = int(os.environ.get('GUNICORN_KEEPALIVE', '2'))

# Logging
# Avoid logging private query/token URLs on either Flask or MCP requests.
accesslog = None
errorlog = os.environ.get('GUNICORN_ERRORLOG', '-')
loglevel = os.environ.get('GUNICORN_LOGLEVEL', 'info')

# App entrypoint
wsgi_app = 'mcp_app:production_app()'
