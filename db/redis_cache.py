import time as _time
import hashlib
import os
import json

from datetime import datetime, timezone
from typing import Optional

try:
    import redis as _redis
except Exception:
    _redis = None

# Module-level singleton for Redis client used by RedisCache
REDIS = None

class RedisCache:
    def __init__(self, ttl_seconds: int, lock_ttl_seconds: int, stale_ttl_seconds: Optional[int] = None, namespace: Optional[str] = None):
        """Redis-backed cache with stale-while-revalidate.

        - ttl_seconds: fresh window
        - stale_ttl_seconds: stale-while-revalidate window (defaults to ttl_seconds)
        - lock_ttl_seconds: TTL for refresh locks
        - namespace: key namespace prefix (defaults to env APP_CACHE_NS or 'fabric-gps')
        """
        self.client = self.get_redis()
        self._TTL_SECONDS = int(ttl_seconds)
        self._STALE_TTL_SECONDS = int(stale_ttl_seconds if stale_ttl_seconds is not None else ttl_seconds)
        self._LOCK_TTL_SECONDS = int(lock_ttl_seconds)
        self._NS = (namespace or os.getenv("APP_CACHE_NS") or "fabric-gps").strip()



    def get_redis(self):
        """Return a Redis client, configured from env.

        Env options (in priority order):
        - REDIS_URL (e.g., redis://:pass@host:6379/0)
        - REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
        """
        global REDIS
        if REDIS is None:
            if _redis is None:
                return None
            url = os.getenv("REDIS_URL")
            if url:
                REDIS = _redis.Redis.from_url(url, decode_responses=True)
            else:
                host = os.getenv("REDIS_HOST", "localhost")
                port = int(os.getenv("REDIS_PORT", "6379"))
                db = int(os.getenv("REDIS_DB", "0"))
                password = os.getenv("REDIS_PASSWORD")
                REDIS = _redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)
        return REDIS


    def _key_id(self, parts: tuple[str, ...]) -> str:
        """Create a stable short key id from tuple of parts."""
        joined = "\u0001".join([p or "" for p in parts])
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()


    def _cache_key(self, prefix: str, parts: tuple[str, ...]) -> tuple[str, str]:
        """Return (data_key, lock_key) for a given prefix and parameter tuple."""
        kid = self._key_id(parts)
        return f"{self._NS}:cache:{prefix}:{kid}", f"{self._NS}:lock:{prefix}:{kid}"


    def _cache_get(self, prefix: str, parts: tuple[str, ...]) -> Optional[dict]:
        """Get cached entry JSON from Redis. Returns dict or None.
        Entry schema: { body, etag, built_iso, fresh_until_ts, stale_until_ts }
        """
        try:
            r = self.client
            key, _ = self._cache_key(prefix, parts)
            raw = r.get(key)
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None


    def _cache_set(self, prefix: str, parts: tuple[str, ...], body: str, etag: str, built_dt: datetime):
        now = _time.time()
        fresh_until = now + self._TTL_SECONDS
        stale_until = fresh_until + self._STALE_TTL_SECONDS
        entry = {
            "body": body,
            "etag": etag,
            "built_iso": built_dt.astimezone(timezone.utc).isoformat(),
            "fresh_until_ts": fresh_until,
            "stale_until_ts": stale_until,
        }
        ttl = int(self._TTL_SECONDS + self._STALE_TTL_SECONDS)
        try:
            r = self.client
            key, _ = self._cache_key(prefix, parts)
            r.setex(key, ttl, json.dumps(entry, separators=(",", ":")))
        except Exception:
            # Best-effort; if Redis is unavailable we simply don't cache
            pass


    def _try_acquire_lock(self, prefix: str, parts: tuple[str, ...]) -> bool:
        try:
            r = self.client
            _, lkey = self._cache_key(prefix, parts)
            # NX + EX for a simple, safe lock
            return bool(r.set(lkey, "1", nx=True, ex=self._LOCK_TTL_SECONDS))
        except Exception:
            return False


    def _release_lock(self, prefix: str, parts: tuple[str, ...]):
        try:
            r = self.client
            _, lkey = self._cache_key(prefix, parts)
            r.delete(lkey)
        except Exception:
            pass

    # Public wrappers
    def get(self, prefix: str, parts: tuple[str, ...]) -> Optional[dict]:
        return self._cache_get(prefix, parts)

    def set(self, prefix: str, parts: tuple[str, ...], body: str, etag: str, built_dt: datetime):
        return self._cache_set(prefix, parts, body, etag, built_dt)

    def try_acquire_lock(self, prefix: str, parts: tuple[str, ...]) -> bool:
        return self._try_acquire_lock(prefix, parts)

    def release_lock(self, prefix: str, parts: tuple[str, ...]):
        return self._release_lock(prefix, parts)
    
    def flush(self):
        try:
            r = self.client
            r.flushdb()
        except Exception:
            pass

    # Health helpers
    def enabled(self) -> bool:
        return self.client is not None

    def ping(self) -> bool:
        try:
            return bool(self.client.ping()) if self.client is not None else False
        except Exception:
            return False