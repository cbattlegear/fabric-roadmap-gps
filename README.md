# Fabric GPS server

Small Flask server that exposes an RSS 2.0 feed and basic REST API for the Fabric Roadmap

## Redis-backed caching

This app uses Redis for response caching (RSS, API, and index) with stale-while-revalidate.

- Fresh window: 24 hours
- Stale-while-revalidate window: additional 24 hours
- ETag and Last-Modified headers are set; 304 responses are supported.

Enable Redis via environment variables (any of the following):

- REDIS_URL (e.g. `redis://:password@hostname:6379/0`)
- REDIS_HOST (default `localhost`)
- REDIS_PORT (default `6379`)
- REDIS_DB (default `0`)
- REDIS_PASSWORD (optional)
- APP_CACHE_NS (optional key namespace, default `fabric-gps`)

If Redis is not configured/reachable, the app gracefully falls back to no-cache for that request.

In Docker/Kubernetes, point `REDIS_URL` at your Redis service, e.g. `REDIS_URL=redis://redis:6379/0`.