# Fabric GPS server

Small Flask server that exposes an RSS 2.0 feed and basic REST API for the Fabric Roadmap

## Caching

Caching is handled by Azure Front Door at the CDN edge. The server sets HTTP cache headers on all API and RSS responses:

- `Cache-Control: public, max-age=1800, stale-while-revalidate=900` (30 min fresh, 15 min stale)
- `ETag` — Weak content hash for conditional GET (send `If-None-Match` to receive 304)
- `Last-Modified` — Timestamp for conditional requests