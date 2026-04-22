"""Generic in-process sliding-window rate limiter.

This module is intentionally decoupled from any specific transport (HTTP,
SMTP, Azure Communication Services, ...). Callers wrap each outbound
operation in a ``wait_for_capacity()`` / ``record()`` pair:

    limiter = SlidingWindowLimiter(max_calls=30, window_seconds=60)
    for url in urls:
        limiter.wait_for_capacity()
        do_request(url)
        limiter.record()

The ``sleep`` and ``monotonic`` callables are injectable so tests can
drive the clock deterministically without ``time.sleep`` actually pausing.

This is the generic primitive intended for reuse across the codebase.
``lib/acs_rate_limit.py`` is a thin adapter on top of this module that
carries ACS-specific defaults and the legacy
``wait_for_capacity`` / ``record_send`` API used by the email job.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Callable, Deque, Optional


class QuotaExceeded(RuntimeError):
    """Raised when ``wait_for_capacity`` exceeds its caller-supplied timeout."""


class SlidingWindowLimiter:
    """True rolling sliding-window rate limiter.

    Unlike a fixed-window counter (which permits bursts at the window
    boundary), this implementation tracks the timestamp of every recorded
    call inside the current window. Capacity is recovered as soon as the
    oldest call ages past ``window_seconds`` ago.

    A separate ``min_interval_seconds`` enforces a minimum gap between
    consecutive calls and is checked after the window quota.
    """

    def __init__(
        self,
        *,
        max_calls: int,
        window_seconds: float,
        min_interval_seconds: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if max_calls <= 0:
            raise ValueError("max_calls must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds must be non-negative")

        self._max_calls = max_calls
        self._window_seconds = float(window_seconds)
        self._min_interval = float(min_interval_seconds)
        self._sleep = sleep
        self._monotonic = monotonic
        self._logger = logger or logging.getLogger(__name__)

        self._calls: Deque[float] = deque()
        self._last_call_time: float = float("-inf")

    # ------------------------------------------------------------------
    # Introspection helpers (mostly for tests / metrics)
    # ------------------------------------------------------------------
    @property
    def max_calls(self) -> int:
        return self._max_calls

    @property
    def window_seconds(self) -> float:
        return self._window_seconds

    @property
    def min_interval_seconds(self) -> float:
        return self._min_interval

    def current_load(self) -> int:
        """Return the number of calls counted inside the current window."""
        self._evict_expired(self._monotonic())
        return len(self._calls)

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------
    def wait_for_capacity(self, *, timeout: Optional[float] = None) -> None:
        """Block until the next call would respect both quotas.

        Args:
            timeout: Maximum time, in seconds, to wait. ``None`` waits
                indefinitely. If the cumulative wait would exceed
                ``timeout``, raises :class:`QuotaExceeded` without
                sleeping further.
        """
        deadline = None if timeout is None else self._monotonic() + timeout

        while True:
            now = self._monotonic()
            self._evict_expired(now)

            wait = 0.0
            if len(self._calls) >= self._max_calls:
                # Window full: wait until the oldest call ages out.
                wait = max(wait, self._calls[0] + self._window_seconds - now)

            if self._min_interval > 0 and self._last_call_time > float("-inf"):
                gap = now - self._last_call_time
                if gap < self._min_interval:
                    wait = max(wait, self._min_interval - gap)

            if wait <= 0:
                return

            if deadline is not None and now + wait > deadline:
                raise QuotaExceeded(
                    f"Rate limit wait of {wait:.2f}s exceeds timeout"
                )

            self._logger.debug("rate-limit sleeping %.3fs", wait)
            self._sleep(wait)

    def record(self) -> None:
        """Record that one call just completed."""
        now = self._monotonic()
        self._calls.append(now)
        self._last_call_time = now

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _evict_expired(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._calls and self._calls[0] <= cutoff:
            self._calls.popleft()
