"""Content-free MCP observations using the web process's existing OTel provider."""

import asyncio
import json
import logging
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.shared.exceptions import MCPError
from mcp.types import CallToolResult
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from pydantic import ValidationError

logger = logging.getLogger("server.opentelemetry.mcp")
tracer = trace.get_tracer("fabric-gps.mcp")
METHODS = frozenset({
    "initialize", "server/discover", "ping", "tools/list", "tools/call",
    "resources/list", "resources/templates/list", "prompts/list",
    "notifications/initialized", "notifications/cancelled",
    "notifications/progress", "notifications/roots/list_changed",
    "subscriptions/listen",
})
TOOLS = frozenset({
    "search_releases", "get_release", "get_release_history", "get_filter_options",
})


@dataclass
class TransportLifecycle:
    started: float = 0
    response_start: float | None = None
    response_complete: float | None = None
    response_format: str = "none"
    disconnected: bool = False
    cleanup_outcome: str = "ok"

    def sent(self, message):
        if message["type"] == "http.response.start":
            self.response_start = time.monotonic()
            self.response_format = "other"
            for name, value in message.get("headers", []):
                if name.lower() == b"content-type":
                    media_type = value.split(b";", 1)[0].strip().lower()
                    self.response_format = {
                        b"application/json": "json", b"text/event-stream": "eventstream",
                    }.get(media_type, "other")
                    break
        elif (
            message["type"] == "http.response.body"
            and not message.get("more_body", False)
            and self.response_start is not None
            and self.response_complete is None
        ):
            self.response_complete = time.monotonic()

    def attributes(self, finished):
        attributes = {
            "mcp.response_started": self.response_start is not None,
            "mcp.response_completed": self.response_complete is not None,
            "mcp.response_format": self.response_format,
            "mcp.disconnected": self.disconnected,
            "mcp.cleanup_outcome": self.cleanup_outcome,
        }
        if self.response_start is not None:
            attributes["mcp.response_start_ms"] = (self.response_start - self.started) * 1000
        if self.response_complete is not None:
            attributes["mcp.response_complete_ms"] = (self.response_complete - self.started) * 1000
            attributes["mcp.cleanup_duration_ms"] = (finished - self.response_complete) * 1000
        return attributes


@dataclass
class Observation:
    method: str = "unknown"
    tool: str = "none"
    outcome: str = "ok"
    executed: bool = False
    status: int = 0
    transport: TransportLifecycle | None = None


request_observation: ContextVar[Observation | None] = ContextVar("mcp_request", default=None)
tool_observation: ContextVar[Observation | None] = ContextVar("mcp_tool", default=None)


def identify_message(body, observation):
    # Notifications can be acknowledged before SDK middleware runs. This only
    # labels an already-bounded body; the SDK still owns protocol validation.
    try:
        message = json.loads(body)
    except (ValueError, RecursionError):
        return
    if not isinstance(message, dict):
        return
    method = message.get("method")
    observation.method = method if isinstance(method, str) and method in METHODS else "unknown"
    params = message.get("params")
    if observation.method == "tools/call" and isinstance(params, dict):
        name = params.get("name")
        observation.tool = name if isinstance(name, str) and name in TOOLS else "unknown"


@contextmanager
def observe(kind, observation):
    started = time.monotonic()
    transport = observation.transport
    if transport is not None:
        transport.started = started
    with tracer.start_as_current_span(
        f"mcp.{kind}", kind=SpanKind.SERVER if kind == "request" else SpanKind.INTERNAL,
        record_exception=False, set_status_on_exception=False,
    ) as span:
        try:
            yield observation
        except asyncio.CancelledError:
            if transport is not None and transport.response_complete is not None:
                transport.cleanup_outcome = "cancelled"
            else:
                observation.outcome = "cancelled"
            raise
        except BaseException:
            if transport is not None and transport.response_complete is not None:
                transport.cleanup_outcome = "error"
            elif observation.outcome == "ok":
                observation.outcome = "error"
            raise
        finally:
            finished = time.monotonic()
            if transport is not None and observation.outcome == "ok":
                if transport.response_complete is None:
                    observation.outcome = "disconnected" if transport.disconnected else "incomplete"
            attributes = {
                "mcp.kind": kind,
                "mcp.method": observation.method,
                "mcp.tool": observation.tool,
                "mcp.outcome": observation.outcome,
                "mcp.executed": observation.executed,
                "mcp.duration_ms": (finished - started) * 1000,
            }
            if kind == "request":
                attributes["http.response.status_code"] = observation.status
                attributes.update(transport.attributes(finished) if transport is not None else {})
            span.set_attributes(attributes)
            if observation.outcome != "ok":
                span.set_status(StatusCode.ERROR)
            logger.info("mcp_%s", kind, extra=attributes)


class UsageMiddleware:
    """Replace SDK telemetry, not its parsing, validation or error handling."""

    async def __call__(
        self, ctx: ServerRequestContext, call_next: CallNext,
    ) -> HandlerResult:
        method = ctx.method if ctx.method in METHODS else "unknown"
        name = ctx.params.get("name") if ctx.params else None
        tool = name if isinstance(name, str) and name in TOOLS else "unknown"
        request = request_observation.get()
        if request is not None:
            request.method = method
            request.tool = tool if method == "tools/call" else "none"
        observation = Observation(method=method, tool=tool)

        async def dispatch():
            try:
                result = await call_next(ctx)
            except ValidationError:
                observation.outcome = "validation_error"
                raise
            except MCPError:
                observation.outcome = "protocol_error"
                raise
            else:
                if (
                    isinstance(result, CallToolResult) and result.is_error
                    or isinstance(result, dict) and result.get("isError") is True
                ) and observation.outcome == "ok":
                    observation.outcome = (
                        "tool_error" if observation.executed else "validation_error"
                    )
                return result
            finally:
                if request is not None:
                    request.outcome = observation.outcome
                    request.executed = observation.executed

        if method != "tools/call" or ctx.request_id is None:
            return await dispatch()
        token = tool_observation.set(observation)
        try:
            with observe("tool", observation):
                return await dispatch()
        finally:
            if request is not None:
                request.outcome = observation.outcome
            tool_observation.reset(token)
