"""Tests for ``lib/rate_limit.py``.

Uses a fake clock and fake ``sleep`` (both injected via the limiter
constructor) so the tests are fully deterministic and don't touch real
time. ``freezegun`` isn't needed because the limiter only reads
``monotonic`` through the injected callable.
"""

from __future__ import annotations

import logging

import pytest

from lib.rate_limit import QuotaExceeded, SlidingWindowLimiter


class FakeClock:
    """Monotonic clock + sleep that simply advance an internal counter."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        # Record what was requested then advance the clock.
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _make(clock: FakeClock, **kwargs) -> SlidingWindowLimiter:
    defaults = dict(max_calls=3, window_seconds=60.0)
    defaults.update(kwargs)
    return SlidingWindowLimiter(
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        **defaults,
    )


# ---------------------------------------------------------------------------
# Construction / validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "kwargs",
    [
        dict(max_calls=0, window_seconds=60),
        dict(max_calls=-1, window_seconds=60),
        dict(max_calls=10, window_seconds=0),
        dict(max_calls=10, window_seconds=-1),
        dict(max_calls=10, window_seconds=60, min_interval_seconds=-0.1),
    ],
)
def test_invalid_construction_raises(kwargs):
    with pytest.raises(ValueError):
        SlidingWindowLimiter(**kwargs)


def test_properties_expose_config():
    clock = FakeClock()
    limiter = _make(clock, max_calls=5, window_seconds=30, min_interval_seconds=0.5)
    assert limiter.max_calls == 5
    assert limiter.window_seconds == 30.0
    assert limiter.min_interval_seconds == 0.5
    assert limiter.current_load() == 0


# ---------------------------------------------------------------------------
# Basic quota behavior
# ---------------------------------------------------------------------------
def test_under_quota_does_not_sleep():
    clock = FakeClock()
    limiter = _make(clock, max_calls=3, window_seconds=60)

    for _ in range(3):
        limiter.wait_for_capacity()
        limiter.record()

    assert clock.sleeps == []
    assert limiter.current_load() == 3


def test_burst_then_block_until_oldest_ages_out():
    clock = FakeClock()
    limiter = _make(clock, max_calls=3, window_seconds=60)

    # Three calls at t=0, 1, 2 fill the window.
    for i in range(3):
        limiter.wait_for_capacity()
        limiter.record()
        clock.advance(1.0)
    # clock now at t=3.

    # 4th request must wait until the oldest call (t=0) is >60s old,
    # i.e. until t=60. Currently t=3 → sleep for 57s.
    limiter.wait_for_capacity()
    assert clock.sleeps == [pytest.approx(57.0)]


def test_capacity_recovers_as_calls_age_out():
    """Sliding window: calls expire individually, not in a batch reset."""
    clock = FakeClock()
    limiter = _make(clock, max_calls=2, window_seconds=10)

    limiter.wait_for_capacity(); limiter.record()  # t=0
    clock.advance(5)
    limiter.wait_for_capacity(); limiter.record()  # t=5
    assert limiter.current_load() == 2

    # Advance to t=11: first call (t=0) has expired (>10s), second
    # (t=5) has not. We should be able to fire one more without sleeping.
    clock.advance(6)  # t=11
    assert limiter.current_load() == 1
    limiter.wait_for_capacity()
    assert clock.sleeps == []  # no sleep needed


def test_min_interval_enforced_between_calls():
    clock = FakeClock()
    limiter = _make(clock, max_calls=100, window_seconds=60, min_interval_seconds=2.0)

    limiter.wait_for_capacity(); limiter.record()  # t=0
    # No advance: next call should wait the full 2s.
    limiter.wait_for_capacity()
    assert clock.sleeps == [pytest.approx(2.0)]


def test_min_interval_satisfied_when_enough_gap_already_elapsed():
    clock = FakeClock()
    limiter = _make(clock, max_calls=100, window_seconds=60, min_interval_seconds=2.0)

    limiter.wait_for_capacity(); limiter.record()  # t=0
    clock.advance(3.0)
    limiter.wait_for_capacity()
    assert clock.sleeps == []


def test_min_interval_and_window_both_respected_picks_larger_wait():
    clock = FakeClock()
    limiter = _make(clock, max_calls=2, window_seconds=10, min_interval_seconds=1.0)

    limiter.wait_for_capacity(); limiter.record()  # t=0
    limiter.wait_for_capacity(); limiter.record()  # t=1 (slept 1.0)
    # Window full. Oldest at t=0 ages out at t=10 → need to wait 9s
    # from current t=1. min_interval would only ask for 1s.
    limiter.wait_for_capacity()
    assert clock.sleeps == [pytest.approx(1.0), pytest.approx(9.0)]


# ---------------------------------------------------------------------------
# Window-boundary semantics (the meat of "sliding")
# ---------------------------------------------------------------------------
def test_sustained_throughput_at_quota_no_extra_sleeps():
    """Calling at exactly the quota rate should never block."""
    clock = FakeClock()
    limiter = _make(clock, max_calls=10, window_seconds=60)

    # 10 calls in the first second.
    for _ in range(10):
        limiter.wait_for_capacity(); limiter.record()
        clock.advance(0.1)

    # Wait one full window past the first call, then 10 more.
    clock.advance(60)
    sleeps_before = list(clock.sleeps)
    for _ in range(10):
        limiter.wait_for_capacity(); limiter.record()
        clock.advance(0.1)

    assert clock.sleeps == sleeps_before  # no new sleeps


# ---------------------------------------------------------------------------
# Timeout / QuotaExceeded
# ---------------------------------------------------------------------------
def test_timeout_raises_quota_exceeded_without_sleeping():
    clock = FakeClock()
    limiter = _make(clock, max_calls=1, window_seconds=60)

    limiter.wait_for_capacity(); limiter.record()  # t=0, fills window

    with pytest.raises(QuotaExceeded):
        # Next call would need to wait ~60s, but we only allow 5s.
        limiter.wait_for_capacity(timeout=5.0)
    # No sleep should have been performed.
    assert clock.sleeps == []


def test_timeout_none_blocks_indefinitely():
    clock = FakeClock()
    limiter = _make(clock, max_calls=1, window_seconds=10)
    limiter.wait_for_capacity(); limiter.record()
    # Should not raise; should sleep ~10s.
    limiter.wait_for_capacity(timeout=None)
    assert clock.sleeps == [pytest.approx(10.0)]


# ---------------------------------------------------------------------------
# Logger plumbing
# ---------------------------------------------------------------------------
def test_custom_logger_is_used(caplog):
    clock = FakeClock()
    custom = logging.getLogger("rate_limit.test.custom")
    limiter = _make(clock, max_calls=1, window_seconds=10, logger=custom)
    limiter.wait_for_capacity(); limiter.record()

    with caplog.at_level(logging.DEBUG, logger=custom.name):
        limiter.wait_for_capacity()
    assert any("rate-limit sleeping" in r.message for r in caplog.records)
