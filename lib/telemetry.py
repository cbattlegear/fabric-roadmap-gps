"""Shared OpenTelemetry / Azure Monitor wiring for all pipeline + web entrypoints.

Half of the pipeline scripts had `configure_azure_monitor` set up inline at
module import; the other half (vectorize, match, scrape) had nothing, so they
emitted no traces or logs to App Insights. That meant a "the vectorize step
took 4 minutes longer this week" question had no answer.

Centralizing here means:

- One place to bump SDK config (sampling, live metrics, exporters).
- Every script gets the same telemetry-disabled-in-development behavior.
- Tests can trivially exercise the gating logic without touching the network.

Usage at the top of each entrypoint::

    from lib.telemetry import init_telemetry

    logger = init_telemetry("fabric-gps-vectorize")

The function is a no-op (apart from setting the service name and creating
the logger) when ``APPLICATIONINSIGHTS_CONNECTION_STRING`` is unset or
``CURRENT_ENVIRONMENT == 'development'``.
"""

import logging
import os
from typing import Optional


def _should_configure_azure_monitor() -> bool:
    """Return True only when telemetry is wired in production.

    Mirrors the gating used by ``server.py`` and ``get_current_releases.py``
    so the behavior is identical across entrypoints.
    """
    if not os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"):
        return False
    if os.getenv("CURRENT_ENVIRONMENT") == "development":
        return False
    return True


def init_telemetry(
    service_name: str,
    logger_name: Optional[str] = None,
    *,
    enable_live_metrics: bool = True,
) -> logging.Logger:
    """Configure Azure Monitor + return a stream-handled OTel logger.

    Args:
        service_name: Value to set for ``OTEL_SERVICE_NAME``. Each pipeline
            entrypoint should pick a distinct name (e.g.
            ``"fabric-gps-vectorize"``) so traces partition cleanly in
            App Insights.
        logger_name: Name to use for the returned logger. Defaults to
            ``f"{service_name}.opentelemetry"`` to match the pre-existing
            convention in ``server.py`` and ``get_current_releases.py``.
        enable_live_metrics: Forwarded to ``configure_azure_monitor``.

    Returns:
        A ``logging.Logger`` with a ``StreamHandler`` attached and INFO
        level set. Safe to use whether or not Azure Monitor was actually
        configured (in development the same logger still prints to stderr).
    """
    os.environ["OTEL_SERVICE_NAME"] = service_name

    name = logger_name or f"{service_name}.opentelemetry"

    if _should_configure_azure_monitor():
        # Imported lazily so unit tests / dev environments without the
        # azure-monitor-opentelemetry distribution can still import this
        # module to check the gating logic.
        from azure.monitor.opentelemetry import configure_azure_monitor

        configure_azure_monitor(
            logger_name=name,
            enable_live_metrics=enable_live_metrics,
        )

    logger = logging.getLogger(name)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    return logger
