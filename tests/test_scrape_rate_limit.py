"""Tests for ``scrape_fabric_blog._rate_limited_get``.

We don't exercise the SQL-server side of the scraper here — that needs a
live ODBC driver and a database. Instead we instantiate the scraper with
the connection-string env var stubbed out and a fake limiter, then drive
``_rate_limited_get`` against a mocked ``requests.Session``.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
import requests


@pytest.fixture
def scraper_module():
    os.environ.setdefault("SQLSERVER_CONN", "Driver={Test};Server=test;")
    import scrape_fabric_blog
    return scrape_fabric_blog


@pytest.fixture
def scraper(scraper_module):
    from lib.rate_limit import SlidingWindowLimiter
    s = scraper_module.FabricBlogScraper(
        rate_limiter=SlidingWindowLimiter(max_calls=1000, window_seconds=60)
    )
    s.session = MagicMock()
    s.rate_limiter = MagicMock(wraps=s.rate_limiter)
    s.backoff_base = 0.0  # don't actually sleep in tests
    s.backoff_max = 0.0
    return s


def _resp(status: int, body: bytes = b"<html/>", headers=None) -> MagicMock:
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.content = body
    r.headers = headers or {}
    if 400 <= status < 600:
        r.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}", response=r)
    else:
        r.raise_for_status.return_value = None
    return r


def test_happy_path_calls_limiter_then_records(scraper):
    scraper.session.get.return_value = _resp(200)

    response = scraper._rate_limited_get("https://example.test/page")

    assert response.status_code == 200
    scraper.rate_limiter.wait_for_capacity.assert_called_once()
    scraper.rate_limiter.record.assert_called_once()
    scraper.session.get.assert_called_once_with("https://example.test/page", timeout=30)


def test_429_then_200_retries_with_backoff(scraper):
    scraper.session.get.side_effect = [_resp(429), _resp(200)]

    with patch("scrape_fabric_blog.time.sleep") as mock_sleep:
        response = scraper._rate_limited_get("https://example.test/page")

    assert response.status_code == 200
    assert scraper.session.get.call_count == 2
    assert scraper.rate_limiter.wait_for_capacity.call_count == 2
    # Failed (429) attempts do not consume client-side quota; only the
    # final successful response is recorded.
    assert scraper.rate_limiter.record.call_count == 1
    # One backoff sleep between the two attempts.
    assert mock_sleep.call_count == 1


def test_503_also_triggers_backoff_retry(scraper):
    scraper.session.get.side_effect = [_resp(503), _resp(200)]

    with patch("scrape_fabric_blog.time.sleep"):
        response = scraper._rate_limited_get("https://example.test/page")

    assert response.status_code == 200
    assert scraper.session.get.call_count == 2


def test_retry_after_header_overrides_backoff(scraper):
    scraper.backoff_base = 1.0
    scraper.backoff_max = 1.0
    scraper.session.get.side_effect = [
        _resp(429, headers={"Retry-After": "7"}),
        _resp(200),
    ]

    with patch("scrape_fabric_blog.time.sleep") as mock_sleep:
        scraper._rate_limited_get("https://example.test/page")

    # backoff floor is min(max=1.0, base*2**0=1.0)=1.0, but Retry-After
    # bumps it to max(1.0, 7.0) = 7.0.
    mock_sleep.assert_called_once_with(7.0)


def test_429_exhausts_retries_then_raises(scraper):
    scraper.max_retries = 2
    scraper.session.get.return_value = _resp(429)

    with patch("scrape_fabric_blog.time.sleep"):
        with pytest.raises(requests.HTTPError):
            scraper._rate_limited_get("https://example.test/page")

    # Initial attempt + 2 retries = 3 GETs.
    assert scraper.session.get.call_count == 3
    # Only the terminal give-up call records against the limiter; the
    # intermediate 429s do not.
    assert scraper.rate_limiter.record.call_count == 1


def test_non_retryable_4xx_does_not_retry(scraper):
    scraper.session.get.return_value = _resp(404)

    with pytest.raises(requests.HTTPError):
        scraper._rate_limited_get("https://example.test/page")

    assert scraper.session.get.call_count == 1
    # Limiter should still have counted the call.
    scraper.rate_limiter.wait_for_capacity.assert_called_once()
    scraper.rate_limiter.record.assert_called_once()


def test_fetch_page_uses_rate_limited_get(scraper):
    scraper.session.get.return_value = _resp(
        200, body=b"<html><body><article class='post'></article></body></html>"
    )
    soup = scraper.fetch_page(7)

    assert soup is not None
    scraper.rate_limiter.wait_for_capacity.assert_called_once()
    scraper.rate_limiter.record.assert_called_once()
    args, kwargs = scraper.session.get.call_args
    assert "page=7" in args[0]


def test_default_limiter_constructed_from_env(monkeypatch, scraper_module):
    monkeypatch.setenv("BLOG_SCRAPER_MAX_REQUESTS_PER_MINUTE", "42")
    limiter = scraper_module._build_default_limiter()
    assert limiter.max_calls == 42
    assert limiter.window_seconds == 60.0
