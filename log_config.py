"""Logging configuration with context-aware structured output.

Uses asyncio-safe ``contextvars`` so that ``saga_id`` and ``step_name``
are automatically attached to every log record within the current execution
scope, without needing to pass them through every function call.
"""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar

_saga_context: ContextVar[dict[str, str]] = ContextVar(
    "saga_context", default={}
)


class SagaContextFilter(logging.Filter):
    """Injects ``saga_id`` and ``step_name`` from the context into log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _saga_context.get()
        record.saga_id = ctx.get("saga_id", "-")
        record.step_name = ctx.get("step_name", "-")
        return True


class StructuredFormatter(logging.Formatter):
    """Formats log records as structured, searchable key=value lines."""

    _LEVEL_COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    _RESET = "\033[0m"
    _BOLD = "\033[1m"

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        level = record.levelname
        color = self._LEVEL_COLORS.get(level, "")
        name = record.name

        saga_id = getattr(record, "saga_id", "-")
        step = getattr(record, "step_name", "-")

        base = f"{ts}.{record.msecs:03.0f} {color}{level:<8}{self._RESET} {self._BOLD}{name}{self._RESET} saga={saga_id} step={step}"
        msg = record.getMessage()

        if record.exc_info:
            exc = self.formatException(record.exc_info)
            return f"{base} | {msg}\n{exc}"

        return f"{base} | {msg}"


def set_saga_context(saga_id: str | None = None, step_name: str | None = None) -> None:
    """Update the current async-log context with saga identifiers.

    Call without arguments to clear the context (e.g. after a saga completes).
    """
    ctx = _saga_context.get()
    new_ctx = dict(ctx)
    if saga_id is not None:
        new_ctx["saga_id"] = saga_id
    if step_name is not None:
        new_ctx["step_name"] = step_name
    _saga_context.set(new_ctx)


def clear_saga_context() -> None:
    _saga_context.set({})


def setup_logging(level: int = logging.INFO) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredFormatter())
    handler.addFilter(SagaContextFilter())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)

    logging.getLogger("aiosqlite").setLevel(logging.WARNING)


__all__ = [
    "setup_logging",
    "set_saga_context",
    "clear_saga_context",
    "SagaContextFilter",
    "StructuredFormatter",
]
