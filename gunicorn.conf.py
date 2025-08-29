import os
import multiprocessing

# Bind and workers
bind = os.environ.get('GUNICORN_BIND', '0.0.0.0:8000')
# Default workers = (2 * cores) + 1, overridable via GUNICORN_WORKERS
_cores = multiprocessing.cpu_count()
workers = int(os.environ.get('GUNICORN_WORKERS', str((_cores * 2) + 1)))
threads = int(os.environ.get('GUNICORN_THREADS', '2'))
worker_class = os.environ.get('GUNICORN_WORKER_CLASS', 'gthread')

# Timeouts
timeout = int(os.environ.get('GUNICORN_TIMEOUT', '60'))
keepalive = int(os.environ.get('GUNICORN_KEEPALIVE', '2'))

# Logging
accesslog = os.environ.get('GUNICORN_ACCESSLOG', '-')
errorlog = os.environ.get('GUNICORN_ERRORLOG', '-')
loglevel = os.environ.get('GUNICORN_LOGLEVEL', 'info')

# App entrypoint
wsgi_app = 'server:app'
