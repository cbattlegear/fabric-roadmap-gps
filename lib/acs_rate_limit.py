"""Sliding-window rate limiter for outbound API calls.

Originally extracted from ``weekly_email_job.py`` where it was tangled into
the digest-send loop. The implementation is intentionally generic — it
knows nothing about Azure Communication Services beyond an
``acs_default_config()`` factory that captures the current production
quota. Issue #77 (M15) is expected to reuse this for the scraper, at
which point this module can be promoted to ``lib/rate_limit.py`` without
behaviour changes.

Two independent constraints are enforced:

* A minimum interval between sends (per-second / per-minute throttling).
* A maximum number of sends per rolling window (e.g. 800 / hour).

The limiter is *blocking*: callers invoke :meth:`wait_for_capacity`
before each send and :meth:`record_send` after a successful send. The
``sleep`` and ``monotonic`` callables are injectable so tests can drive
the clock deterministically without ``time.sleep`` actually pausing.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class RateLimitConfig:
    """Configuration for :class:`SlidingWindowRateLimiter`.

    Attributes:
        min_interval_seconds: Minimum gap between two consecutive sends.
        max_per_window: Maximum number of sends allowed inside a single
            ``window_seconds`` rolling window.
        window_seconds: Length of the rolling window in seconds.
    """

    min_interval_seconds: float
    max_per_window: int
    window_seconds: float = 3600.0


def acs_default_config() -> RateLimitConfig:
    """Return the production Azure Communication Services config.

    ACS quota is 100 emails/min and 1000 emails/hr. We reserve ~20%
    headroom for transactional emails (verification, preferences links)
    sent by the web server from the same quota:

    * ``min_interval_seconds = 0.75`` — ~80 sends/min.
    * ``max_per_window = 800`` per ``3600`` seconds — leaves 200/hr for
      transactional.
    """
    return RateLimitConfig(
        min_interval_seconds=0.75,
        max_per_window=800,
        window_seconds=3600.0,
    )


class SlidingWindowRateLimiter:
    """Blocking sliding-window rate limiter.

    The window is *not* a true sliding window in the strict sense: it is
    a fixed-length window that resets to zero once ``window_seconds`` has
    elapsed since the window started. This matches the original
    ``weekly_email_job.py`` semantics exactly.
    """

    def __init__(
        self,
        config: RateLimitConfig,
        *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._config = config
        self._sleep = sleep
        self._monotonic = monotonic
        self._logger = logger or logging.getLogger(__name__)

        self._last_send_time: float = 0.0
        self._window_sent: int = 0
        self._window_start: float = monotonic()

    @property
    def config(self) -> RateLimitConfig:
        return self._config

    @property
    def window_sent(self) -> int:
        return self._window_sent

    def wait_for_capacity(self) -> None:
        """Block until the next send would respect both quotas.

        Order matters: the per-window check runs first (and may sleep
        for a long time, possibly minutes), then the per-send minimum
        interval is enforced.
        """
        now = self._monotonic()
        elapsed_in_window = now - self._window_start
        if elapsed_in_window >= self._config.window_seconds:
            self._window_sent = 0
            self._window_start = now
        elif self._window_sent >= self._config.max_per_window:
            wait_time = self._config.window_seconds - elapsed_in_window
            self._logger.info(
                "Hourly quota reached (%d), pausing %.0fs",
                self._config.max_per_window,
                wait_time,
            )
            if wait_time > 0:
                self._sleep(wait_time)
            self._window_sent = 0
            self._window_start = self._monotonic()

        elapsed = self._monotonic() - self._last_send_time
        if elapsed < self._config.min_interval_seconds:
            self._sleep(self._config.min_interval_seconds - elapsed)

    def record_send(self) -> None:
        """Record that a send completed successfully.

        Only successful sends count against the quota — failed sends
        leave the counters untouched so a flapping ACS endpoint doesn't
        falsely consume budget.
        """
        self._window_sent += 1
        self._last_send_time = self._monotonic()
