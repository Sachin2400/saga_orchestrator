"""Domain models: enums, dataclasses, and type aliases."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable


class SagaState(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"


class StepState(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"


class EventType(str, enum.Enum):
    SAGA_CREATED = "SAGA_CREATED"
    SAGA_STARTED = "SAGA_STARTED"
    STEP_STARTED = "STEP_STARTED"
    STEP_COMPLETED = "STEP_COMPLETED"
    STEP_FAILED = "STEP_FAILED"
    SAGA_COMPLETED = "SAGA_COMPLETED"
    SAGA_FAILED = "SAGA_FAILED"
    ROLLBACK_STARTED = "ROLLBACK_STARTED"
    COMPENSATING = "COMPENSATING"
    COMPENSATION_COMPLETED = "COMPENSATION_COMPLETED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"
    SAGA_COMPENSATED = "SAGA_COMPENSATED"
    SAGA_RESUMED = "SAGA_RESUMED"


SagaFunc = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class SagaStepConfig:
    name: str
    execute: SagaFunc
    compensate: SagaFunc
    timeout: float = 10.0
    required: bool = True


@dataclass
class SagaStepResult:
    step_name: str
    state: StepState
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass
class SagaStepRecord:
    saga_id: str
    step_name: str
    step_order: int
    state: StepState
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass
class SagaInstance:
    saga_id: str
    name: str
    state: SagaState
    definition: "SagaDefinition | None" = None
    steps: list[SagaStepRecord] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class SagaDefinition:
    name: str
    steps: tuple[SagaStepConfig, ...]
    initial_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class SagaEvent:
    saga_id: str
    event_type: EventType
    step_name: str | None
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime | None = None
    error: str | None = None


@dataclass
class StepPayload:
    saga_id: str = ""
    order: dict[str, Any] = field(default_factory=dict)
    reservation: dict[str, Any] = field(default_factory=dict)
    payment: dict[str, Any] = field(default_factory=dict)
    dispatch: dict[str, Any] = field(default_factory=dict)


__all__ = [
    "SagaState",
    "StepState",
    "EventType",
    "SagaFunc",
    "SagaStepConfig",
    "SagaStepResult",
    "SagaStepRecord",
    "SagaInstance",
    "SagaDefinition",
    "SagaEvent",
    "StepPayload",
]
