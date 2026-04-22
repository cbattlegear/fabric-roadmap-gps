"""Tests for ``lib.telemetry`` — Azure Monitor wiring with safe defaults."""

import logging
import os
import sys

import pytest

from lib import telemetry


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Tests should not depend on the developer's actual env vars."""
    monkeypatch.delenv("APPLICATIONINSIGHTS_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
    monkeypatch.delenv("CURRENT_ENVIRONMENT", raising=False)
    yield


@pytest.fixture(autouse=True)
def _strip_stream_handlers():
    """``init_telemetry`` adds a single StreamHandler per logger and is
    careful not to duplicate, but tests create loggers under several names
    and we want each test to start from a clean slate."""
    yield
    for name in list(logging.Logger.manager.loggerDict.keys()):
        if "fabric-gps-test" in name or name.startswith("svc") or name == "custom.name":
            lg = logging.getLogger(name)
            lg.handlers.clear()


def test_init_telemetry_sets_service_name():
    telemetry.init_telemetry("fabric-gps-test")

    assert os.environ["OTEL_SERVICE_NAME"] == "fabric-gps-test"


def test_init_telemetry_returns_logger_with_stream_handler():
    logger = telemetry.init_telemetry("fabric-gps-test")

    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


def test_init_telemetry_default_logger_name():
    logger = telemetry.init_telemetry("fabric-gps-test")

    assert logger.name == "fabric-gps-test.opentelemetry"


def test_init_telemetry_custom_logger_name():
    logger = telemetry.init_telemetry("fabric-gps-test", logger_name="custom.name")

    assert logger.name == "custom.name"


def test_init_telemetry_skips_azure_monitor_when_no_connection_string(monkeypatch):
    """Without APPLICATIONINSIGHTS_CONNECTION_STRING set, Azure Monitor must
    not be configured — otherwise local dev runs would fail trying to ship
    spans to a non-existent endpoint."""
    monkeypatch.setattr(telemetry, "_should_configure_azure_monitor", lambda: False)

    # If gating were broken and the lazy import ran, monkey-injecting a
    # poison fake_configure would surface the bug.
    fake_module = type(sys)("azure.monitor.opentelemetry")

    def explode(**kwargs):
        raise AssertionError("configure_azure_monitor must NOT be called when gated off")

    fake_module.configure_azure_monitor = explode
    monkeypatch.setitem(sys.modules, "azure.monitor.opentelemetry", fake_module)

    telemetry.init_telemetry("svc")  # must not raise


def test_init_telemetry_calls_azure_monitor_when_gated_on(monkeypatch):
    """When the gate returns True, ``configure_azure_monitor`` must be
    invoked with the resolved logger name and live-metrics flag."""
    monkeypatch.setattr(telemetry, "_should_configure_azure_monitor", lambda: True)

    captured = {}

    def fake_configure(**kwargs):
        captured.update(kwargs)

    fake_module = type(sys)("azure.monitor.opentelemetry")
    fake_module.configure_azure_monitor = fake_configure
    monkeypatch.setitem(sys.modules, "azure.monitor.opentelemetry", fake_module)

    telemetry.init_telemetry("svc", enable_live_metrics=False)

    assert captured == {
        "logger_name": "svc.opentelemetry",
        "enable_live_metrics": False,
    }


def test_should_configure_returns_false_in_development(monkeypatch):
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "Endpoint=https://x")
    monkeypatch.setenv("CURRENT_ENVIRONMENT", "development")

    assert telemetry._should_configure_azure_monitor() is False


def test_should_configure_returns_false_without_connection_string(monkeypatch):
    monkeypatch.setenv("CURRENT_ENVIRONMENT", "production")

    assert telemetry._should_configure_azure_monitor() is False


def test_should_configure_returns_true_in_production_with_connection_string(monkeypatch):
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "Endpoint=https://x")
    monkeypatch.setenv("CURRENT_ENVIRONMENT", "production")

    assert telemetry._should_configure_azure_monitor() is True


def test_init_telemetry_does_not_duplicate_stream_handler():
    """Calling init_telemetry twice with the same logger_name must not stack
    StreamHandlers — otherwise log lines would print N times in long-running
    processes."""
    logger = telemetry.init_telemetry("svc-a")
    telemetry.init_telemetry("svc-a")

    stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
    assert len(stream_handlers) == 1
