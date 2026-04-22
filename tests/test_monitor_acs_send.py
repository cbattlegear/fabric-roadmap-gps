"""Tests for ``server._monitor_acs_send``.

The helper runs on a background thread and converts an ACS poller result
into a structured log line. Real ACS results in current SDK versions are
``SendStatusResult``-style objects with a ``.status`` attribute, but
older / mocked results may be dicts or other mapping-shaped objects.

We verify all three shapes plus the failure-mode log paths so the three
call sites can no longer drift.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import server


@pytest.fixture
def captured_logs(monkeypatch):
    """Capture otelLogger.info / .warning / .error calls."""
    calls = {"info": [], "warning": [], "error": []}

    def make_recorder(level):
        def _rec(msg, *args, **kwargs):
            # Mirror %-formatting if args are passed (otelLogger usage is
            # mostly f-string-based, but be safe).
            calls[level].append(msg % args if args else msg)
        return _rec

    monkeypatch.setattr(server.otelLogger, "info", make_recorder("info"))
    monkeypatch.setattr(server.otelLogger, "warning", make_recorder("warning"))
    monkeypatch.setattr(server.otelLogger, "error", make_recorder("error"))
    return calls


def _make_poller(result):
    poller = MagicMock()
    poller.result.return_value = result
    return poller


def test_succeeded_status_attribute_logs_info(captured_logs):
    """Real ACS SendStatusResult-style: ``.status`` attribute."""
    poller = _make_poller(SimpleNamespace(status="Succeeded"))
    server._monitor_acs_send(poller, recipient="alice@example.com", kind="verification")
    assert captured_logs["info"] == ["Verification email sent to alice@example.com"]
    assert captured_logs["warning"] == []
    assert captured_logs["error"] == []


def test_succeeded_status_dict_logs_info(captured_logs):
    """Dict-shaped result, ``status`` key."""
    poller = _make_poller({"status": "Succeeded"})
    server._monitor_acs_send(poller, recipient="bob@example.com", kind="goodbye")
    assert captured_logs["info"] == ["Goodbye email sent to bob@example.com"]


def test_succeeded_status_via_get_method(captured_logs):
    """Mapping-like object with ``.get`` but no ``.status`` and not a dict."""
    class FakeMapping:
        def get(self, key):
            return "Succeeded" if key == "status" else None

    poller = _make_poller(FakeMapping())
    server._monitor_acs_send(poller, recipient="carol@example.com", kind="preferences link")
    assert captured_logs["info"] == ["Preferences link email sent to carol@example.com"]


def test_non_success_status_logs_warning(captured_logs):
    poller = _make_poller(SimpleNamespace(status="Failed"))
    server._monitor_acs_send(poller, recipient="dave@example.com", kind="verification")
    assert captured_logs["warning"] == ["Verification email status for dave@example.com: Failed"]
    assert captured_logs["info"] == []


def test_unknown_shape_logs_warning_with_none(captured_logs):
    """Result has neither ``.status`` nor dict shape nor ``.get`` — log warning."""
    class Opaque:
        pass

    poller = _make_poller(Opaque())
    server._monitor_acs_send(poller, recipient="eve@example.com", kind="goodbye")
    assert captured_logs["warning"] == ["Goodbye email status for eve@example.com: None"]


def test_poller_exception_logs_error(captured_logs):
    poller = MagicMock()
    poller.result.side_effect = RuntimeError("boom")
    server._monitor_acs_send(poller, recipient="frank@example.com", kind="verification")
    assert len(captured_logs["error"]) == 1
    assert "frank@example.com" in captured_logs["error"][0]
    assert "boom" in captured_logs["error"][0]
    assert captured_logs["info"] == []
    assert captured_logs["warning"] == []


def test_kind_label_is_capitalized_in_log(captured_logs):
    """``kind`` only affects log text; verify the capitalize() formatting."""
    poller = _make_poller(SimpleNamespace(status="Succeeded"))
    server._monitor_acs_send(poller, recipient="x@example.com", kind="preferences link")
    assert captured_logs["info"] == ["Preferences link email sent to x@example.com"]
