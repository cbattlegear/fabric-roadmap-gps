"""Tests for ``lib.acs_rate_limit`` (issue #74 / M5).

Drives the rate limiter with injected ``sleep`` and ``monotonic``
callables so the tests are deterministic and don't actually sleep.
"""

from __future__ import annotations

import pytest

from lib.acs_rate_limit import (
    RateLimitConfig,
    SlidingWindowRateLimiter,
    acs_default_config,
)


class FakeClock:
    """Monotonic-time fake with explicit advancement."""

    def __init__(self, start: float = 1000.0) -> None:
        self._t = start
        self.sleeps: list = []

    def monotonic(self) -> float:
        return self._t

    def sleep(self, secs: float) -> None:
        self.sleeps.append(secs)
        self._t += secs

    def advance(self, secs: float) -> None:
        self._t += secs


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def fast_config() -> RateLimitConfig:
    # Tiny windows for unit tests
    return RateLimitConfig(min_interval_seconds=0.5, max_per_window=3, window_seconds=10.0)


def _make(config, clock):
    return SlidingWindowRateLimiter(
        config, sleep=clock.sleep, monotonic=clock.monotonic,
    )


class TestAcsDefaultConfig:
    def test_matches_documented_quotas(self):
        cfg = acs_default_config()
        assert cfg.min_interval_seconds == 0.75
        assert cfg.max_per_window == 800
        assert cfg.window_seconds == 3600.0


class TestMinInterval:
    def test_first_send_does_not_sleep(self, fast_config, clock):
        lim = _make(fast_config, clock)
        lim.wait_for_capacity()
        assert clock.sleeps == []

    def test_consecutive_sends_sleep_for_remaining_interval(self, fast_config, clock):
        lim = _make(fast_config, clock)
        lim.wait_for_capacity()
        lim.record_send()
        # No advance: zero elapsed since record_send -> should sleep full interval
        lim.wait_for_capacity()
        assert clock.sleeps == [0.5]

    def test_sleep_is_only_the_remaining_time(self, fast_config, clock):
        lim = _make(fast_config, clock)
        lim.wait_for_capacity()
        lim.record_send()
        clock.advance(0.2)  # 0.3s remaining of 0.5s interval
        lim.wait_for_capacity()
        assert clock.sleeps == [pytest.approx(0.3)]

    def test_no_sleep_when_interval_already_elapsed(self, fast_config, clock):
        lim = _make(fast_config, clock)
        lim.wait_for_capacity()
        lim.record_send()
        clock.advance(1.0)
        lim.wait_for_capacity()
        assert clock.sleeps == []


class TestWindowQuota:
    def test_exhausting_window_sleeps_until_window_resets(self, fast_config, clock):
        lim = _make(fast_config, clock)
        # Fill the window without min-interval sleeps interfering: advance
        # past min_interval each time.
        for _ in range(3):
            lim.wait_for_capacity()
            lim.record_send()
            clock.advance(1.0)
        # Window: three sends at t=1000, 1001, 1002, now t=1003.
        # Next call: window full -> sleep until oldest (t=1000) ages out
        # at t=1010, i.e. 7s. After the sleep only the t=1000 call has
        # expired; t=1001 and t=1002 are still inside the rolling window.
        clock.sleeps.clear()
        lim.wait_for_capacity()
        assert lim.window_sent == 2
        assert clock.sleeps == [pytest.approx(7.0)]

    def test_window_resets_naturally_when_window_seconds_elapse(self, fast_config, clock):
        lim = _make(fast_config, clock)
        for _ in range(3):
            lim.wait_for_capacity()
            lim.record_send()
            clock.advance(1.0)
        clock.advance(20.0)  # well past window
        clock.sleeps.clear()
        lim.wait_for_capacity()
        assert lim.window_sent == 0
        # Window reset path doesn't sleep.
        assert clock.sleeps == []

    def test_failed_send_does_not_count_against_window(self, fast_config, clock):
        lim = _make(fast_config, clock)
        for _ in range(5):
            lim.wait_for_capacity()
            # Don't record_send -> simulates a failed ACS call
            clock.advance(1.0)
        assert lim.window_sent == 0


class TestRecordSend:
    def test_record_send_increments_window_count(self, fast_config, clock):
        lim = _make(fast_config, clock)
        lim.wait_for_capacity()
        lim.record_send()
        assert lim.window_sent == 1

    def test_record_send_updates_last_send_time(self, fast_config, clock):
        lim = _make(fast_config, clock)
        lim.wait_for_capacity()
        lim.record_send()
        clock.advance(0.1)
        # Next wait should sleep ~0.4 (0.5 - 0.1)
        lim.wait_for_capacity()
        assert clock.sleeps == [pytest.approx(0.4)]


class TestEdgeCases:
    def test_zero_min_interval_never_sleeps_for_pacing(self, clock):
        cfg = RateLimitConfig(min_interval_seconds=0.0, max_per_window=10, window_seconds=10.0)
        lim = _make(cfg, clock)
        for _ in range(5):
            lim.wait_for_capacity()
            lim.record_send()
        assert clock.sleeps == []
