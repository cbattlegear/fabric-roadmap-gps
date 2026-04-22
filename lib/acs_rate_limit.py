"""ACS-flavoured adapter over :mod:`lib.rate_limit`.

This module used to carry its own sliding-window implementation
(extracted from ``weekly_email_job.py`` in PR #80). PR #81 introduced
the generic :class:`lib.rate_limit.SlidingWindowLimiter`, which already
supports the two constraints we need:

* ``min_interval_seconds`` — minimum gap between consecutive sends.
* ``max_calls`` per ``window_seconds`` — rolling-window quota.

So this module is now just a thin shim that:

1. Captures the production ACS quota in :func:`acs_default_config` and
   :class:`RateLimitConfig` (kept as a frozen dataclass for stability /
   readability at call sites).
2. Wraps a :class:`SlidingWindowLimiter` behind the original
   ``wait_for_capacity()`` / ``record_send()`` / ``window_sent`` API
   that ``weekly_email_job.py`` was written against.

The generic limiter's sliding-window semantics are a strict superset
of the previous fixed-window behaviour (it is at least as conservative
at every point in time), so existing ACS quota guarantees are preserved.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

from lib.rate_limit import SlidingWindowLimiter


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
    """Blocking sliding-window rate limiter for ACS sends.

    Thin wrapper over :class:`lib.rate_limit.SlidingWindowLimiter` that
    preserves the original ``wait_for_capacity`` / ``record_send`` API
    used by ``weekly_email_job.py`` and adds the ``window_sent`` /
    ``config`` introspection properties the existing tests rely on.
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
        self._limiter = SlidingWindowLimiter(
            max_calls=config.max_per_window,
            window_seconds=config.window_seconds,
            min_interval_seconds=config.min_interval_seconds,
            sleep=sleep,
            monotonic=monotonic,
            logger=logger,
        )

    @property
    def config(self) -> RateLimitConfig:
        return self._config

    @property
    def window_sent(self) -> int:
        """Number of sends currently counted inside the rolling window."""
        return self._limiter.current_load()

    def wait_for_capacity(self) -> None:
        """Block until the next send would respect both quotas."""
        self._limiter.wait_for_capacity()

    def record_send(self) -> None:
        """Record that a send completed successfully.

        Only successful sends count against the quota — failed sends
        leave the counters untouched so a flapping ACS endpoint doesn't
        falsely consume budget.
        """
        self._limiter.record()
