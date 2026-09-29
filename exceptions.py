"""Custom exception hierarchy for the Saga Orchestrator."""

from __future__ import annotations


class SagaError(Exception):
    """Base exception for all saga-related errors."""

    def __init__(self, message: str, *, saga_id: str | None = None) -> None:
        super().__init__(message)
        self.saga_id = saga_id
        self.message = message

    def __str__(self) -> str:
        prefix = f"[{self.saga_id}] " if self.saga_id else ""
        return f"{prefix}{self.message}"


class WALWriteError(SagaError):
    """Raised when the Write-Ahead Log cannot persist an event."""


class StateTransitionError(SagaError):
    """Raised when a saga attempts an invalid or non-idempotent state transition."""


class SagaExecutionError(SagaError):
    """Raised when a saga step or its compensation fails during execution."""

    def __init__(
        self,
        message: str,
        *,
        saga_id: str | None = None,
        step_name: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message, saga_id=saga_id)
        self.step_name = step_name
        self.__cause__ = cause


class ServiceError(SagaError):
    """Raised when a downstream microservice call fails (non-timeout)."""

    def __init__(
        self,
        message: str,
        *,
        saga_id: str | None = None,
        step_name: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message, saga_id=saga_id)
        self.step_name = step_name
        self.__cause__ = cause


class SagaTimeoutError(SagaExecutionError):
    """Raised when a saga step exceeds its configured timeout."""
