"""Write-Ahead Log: durable persistence layer for saga state.

All state transitions are written as an immutable event log. Materialized
'sagas' and 'saga_steps' tables are maintained in the same transaction so
that recovery can query current state without replaying the entire event
stream every time.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator

import aiosqlite
from aiosqlite import Connection

from models import (
    EventType,
    SagaEvent,
    SagaInstance,
    SagaState,
    SagaStepRecord,
    StepState,
)

logger = logging.getLogger("saga.wal")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_payload(payload: dict) -> str:
    """SQLite has no native JSON column; use TEXT with an index-friendly representation."""
    return json.dumps(payload, default=str)


def _deserialize_payload(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Corrupt payload in WAL, returning empty dict: %s", raw)
        return {}


class WALManager:
    """Manages the SQLite-backed Write-Ahead Log.

    Every public mutation method opens its own transaction that writes the
    event row *and* updates the materialized state tables atomically. This
    guarantees that a crash between the event log and the materialized view
    cannot leave them inconsistent.
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: Connection | None = None

    # ------------------------------------------------------------------ #
    # Connection / schema lifecycle
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        self._conn = await aiosqlite.connect(self._db_path)
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()
        logger.info("WAL initialized at %s", self._db_path)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Connection]:
        if self._conn is None:
            raise RuntimeError("WALManager not initialized; call initialize() first")
        try:
            yield self._conn
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    # ------------------------------------------------------------------ #
    # Saga-level operations
    # ------------------------------------------------------------------ #

    async def create_saga(
        self, saga_id: str, name: str, initial_payload: dict
    ) -> None:
        event_payload = {"definition_name": name, "initial_payload": initial_payload}
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.SAGA_CREATED,
                step_name=None,
                payload=event_payload,
                timestamp=_utcnow(),
            ),
            saga_state=SagaState.PENDING,
        )
        logger.info("Saga created: id=%s name=%s", saga_id, name)

    async def start_saga(self, saga_id: str) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.SAGA_STARTED,
                step_name=None,
                payload={},
                timestamp=_utcnow(),
            ),
            saga_state=SagaState.RUNNING,
        )
        logger.info("Saga started: id=%s", saga_id)

    async def complete_saga(self, saga_id: str) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.SAGA_COMPLETED,
                step_name=None,
                payload={},
                timestamp=_utcnow(),
            ),
            saga_state=SagaState.COMPLETED,
        )
        logger.info("Saga completed: id=%s", saga_id)

    async def fail_saga(self, saga_id: str, error: str) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.SAGA_FAILED,
                step_name=None,
                payload={"error": error},
                timestamp=_utcnow(),
            ),
            saga_state=SagaState.FAILED,
        )
        logger.info("Saga failed: id=%s error=%s", saga_id, error)

    async def start_compensation(self, saga_id: str) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.ROLLBACK_STARTED,
                step_name=None,
                payload={},
                timestamp=_utcnow(),
            ),
            saga_state=SagaState.COMPENSATING,
        )
        logger.info("Rollback started: id=%s", saga_id)

    async def complete_compensation(self, saga_id: str) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.SAGA_COMPENSATED,
                step_name=None,
                payload={},
                timestamp=_utcnow(),
            ),
            saga_state=SagaState.COMPENSATED,
        )
        logger.info("Saga fully compensated: id=%s", saga_id)

    # ------------------------------------------------------------------ #
    # Step-level operations
    # ------------------------------------------------------------------ #

    async def record_step_started(
        self, saga_id: str, step_name: str, step_order: int, payload: dict
    ) -> None:
        result_payload = {"input": payload}
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.STEP_STARTED,
                step_name=step_name,
                payload=result_payload,
                timestamp=_utcnow(),
            ),
            saga_state=None,
            step_state=StepState.RUNNING,
            step_name=step_name,
            step_order=step_order,
            step_payload=payload,
        )
        logger.info("Step started: id=%s step=%s order=%d", saga_id, step_name, step_order)

    async def record_step_completed(
        self, saga_id: str, step_name: str, step_order: int, result: dict
    ) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.STEP_COMPLETED,
                step_name=step_name,
                payload={"result": result},
                timestamp=_utcnow(),
            ),
            saga_state=None,
            step_state=StepState.COMPLETED,
            step_name=step_name,
            step_order=step_order,
            step_payload=result,
        )
        logger.info("Step completed: id=%s step=%s order=%d", saga_id, step_name, step_order)

    async def record_step_failed(
        self,
        saga_id: str,
        step_name: str,
        step_order: int,
        error: str,
    ) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.STEP_FAILED,
                step_name=step_name,
                payload={"error": error},
                timestamp=_utcnow(),
                error=error,
            ),
            saga_state=None,
            step_state=StepState.FAILED,
            step_name=step_name,
            step_order=step_order,
            step_payload={},
            step_error=error,
        )
        logger.info("Step failed: id=%s step=%s order=%d error=%s", saga_id, step_name, step_order, error)

    async def record_compensating(
        self, saga_id: str, step_name: str, step_order: int, reason: str
    ) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.COMPENSATING,
                step_name=step_name,
                payload={"reason": reason},
                timestamp=_utcnow(),
            ),
            saga_state=None,
            step_state=StepState.COMPENSATING,
            step_name=step_name,
            step_order=step_order,
        )
        logger.info("Compensating: id=%s step=%s order=%d reason=%s", saga_id, step_name, step_order, reason)

    async def record_compensation_completed(
        self, saga_id: str, step_name: str, step_order: int, result: dict
    ) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.COMPENSATION_COMPLETED,
                step_name=step_name,
                payload={"result": result},
                timestamp=_utcnow(),
            ),
            saga_state=None,
            step_state=StepState.COMPENSATED,
            step_name=step_name,
            step_order=step_order,
        )
        logger.info("Compensation completed: id=%s step=%s order=%d", saga_id, step_name, step_order)

    async def record_compensation_failed(
        self,
        saga_id: str,
        step_name: str,
        step_order: int,
        error: str,
    ) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.COMPENSATION_FAILED,
                step_name=step_name,
                payload={"error": error},
                timestamp=_utcnow(),
                error=error,
            ),
            saga_state=None,
            step_state=StepState.FAILED,
            step_name=step_name,
            step_order=step_order,
            step_error=error,
        )
        logger.warning("Compensation failed: id=%s step=%s order=%d error=%s", saga_id, step_name, step_order, error)

    # ------------------------------------------------------------------ #
    # Query operations (used during recovery)
    # ------------------------------------------------------------------ #

    async def get_saga_state(self, saga_id: str) -> SagaState | None:
        async with self._transaction() as conn:
            cursor = await conn.execute(
                "SELECT state FROM sagas WHERE saga_id = ?",
                (saga_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
        if row is None:
            return None
        return SagaState(row[0])

    async def get_saga_instance(self, saga_id: str) -> SagaInstance | None:
        async with self._transaction() as conn:
            cursor = await conn.execute(
                "SELECT saga_id, name, state, created_at, updated_at "
                "FROM sagas WHERE saga_id = ?",
                (saga_id,),
            )
            saga_row = await cursor.fetchone()
            await cursor.close()
            if saga_row is None:
                return None

            step_rows = await conn.execute_fetchall(
                "SELECT saga_id, step_name, step_order, state, payload, error, "
                "started_at, completed_at FROM saga_steps WHERE saga_id = ? "
                "ORDER BY step_order",
                (saga_id,),
            )

        steps = [
            SagaStepRecord(
                saga_id=r[0],
                step_name=r[1],
                step_order=r[2],
                state=StepState(r[3]),
                payload=_deserialize_payload(r[4]),
                error=r[5],
                started_at=self._parse_ts(r[6]),
                completed_at=self._parse_ts(r[7]),
            )
            for r in step_rows
        ]

        return SagaInstance(
            saga_id=saga_row[0],
            name=saga_row[1],
            state=SagaState(saga_row[2]),
            steps=steps,
            created_at=self._parse_ts(saga_row[3]),
            updated_at=self._parse_ts(saga_row[4]),
        )

    async def get_incomplete_sagas(self) -> list[SagaInstance]:
        """Return all sagas that are not in a terminal state.

        Used during recovery to find sagas that need resuming or rollback.
        """
        terminal_states = (
            SagaState.COMPLETED.value,
            SagaState.FAILED.value,
            SagaState.COMPENSATED.value,
        )
        async with self._transaction() as conn:
            saga_rows = await conn.execute_fetchall(
                "SELECT saga_id, name, state, created_at, updated_at "
                "FROM sagas WHERE state NOT IN (?, ?, ?) "
                "ORDER BY created_at",
                (*terminal_states,),
            )

            instances: list[SagaInstance] = []
            for saga_row in saga_rows:
                step_rows = await conn.execute_fetchall(
                    "SELECT saga_id, step_name, step_order, state, payload, error, "
                    "started_at, completed_at FROM saga_steps WHERE saga_id = ? "
                    "ORDER BY step_order",
                    (saga_row[0],),
                )
                steps = [
                    SagaStepRecord(
                        saga_id=r[0],
                        step_name=r[1],
                        step_order=r[2],
                        state=StepState(r[3]),
                        payload=_deserialize_payload(r[4]),
                        error=r[5],
                        started_at=self._parse_ts(r[6]),
                        completed_at=self._parse_ts(r[7]),
                    )
                    for r in step_rows
                ]
                instances.append(
                    SagaInstance(
                        saga_id=saga_row[0],
                        name=saga_row[1],
                        state=SagaState(saga_row[2]),
                        steps=steps,
                        created_at=self._parse_ts(saga_row[3]),
                        updated_at=self._parse_ts(saga_row[4]),
                    )
                )
        return instances

    async def get_saga_initial_payload(self, saga_id: str) -> dict:
        """Retrieve the initial payload from the SAGA_CREATED event.

        Used during recovery to reconstruct the input for re-execution.
        """
        async with self._transaction() as conn:
            cursor = await conn.execute(
                "SELECT payload FROM saga_events WHERE saga_id = ? AND event_type = ? "
                "ORDER BY event_id DESC LIMIT 1",
                (saga_id, EventType.SAGA_CREATED.value),
            )
            row = await cursor.fetchone()
            await cursor.close()
        if row is None:
            return {}
        data = _deserialize_payload(row[0])
        return data.get("initial_payload", {})

    async def resume_saga(self, saga_id: str) -> None:
        await self._record_event(
            SagaEvent(
                saga_id=saga_id,
                event_type=EventType.SAGA_RESUMED,
                step_name=None,
                payload={},
                timestamp=_utcnow(),
            ),
            saga_state=None,
        )
        logger.info("Saga resumed: id=%s", saga_id)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    async def _record_event(
        self,
        event: SagaEvent,
        *,
        saga_state: SagaState | None = None,
        step_state: StepState | None = None,
        step_name: str | None = None,
        step_order: int | None = None,
        step_payload: dict | None = None,
        step_error: str | None = None,
    ) -> None:
        async with self._transaction() as conn:
            now = event.timestamp or _utcnow()
            iso_ts = now.isoformat()

            await conn.execute(
                "INSERT INTO saga_events (saga_id, event_type, step_name, payload, timestamp, error) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event.saga_id,
                    event.event_type.value,
                    event.step_name,
                    _serialize_payload(event.payload),
                    iso_ts,
                    event.error,
                ),
            )

            if saga_state is not None:
                await conn.execute(
                    "INSERT INTO sagas (saga_id, name, state, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(saga_id) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at",
                    (
                        event.saga_id,
                        event.payload.get("definition_name", event.saga_id),
                        saga_state.value,
                        iso_ts,
                        iso_ts,
                    ),
                )

            if step_state is not None:
                await conn.execute(
                    "INSERT INTO saga_steps (saga_id, step_name, step_order, state, payload, error, started_at, completed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(saga_id, step_name) DO UPDATE SET "
                    "state = excluded.state, payload = excluded.payload, error = excluded.error, "
                    "started_at = COALESCE(saga_steps.started_at, excluded.started_at), "
                    "completed_at = excluded.completed_at",
                    (
                        event.saga_id,
                        step_name,
                        step_order,
                        step_state.value,
                        _serialize_payload(step_payload or {}),
                        step_error,
                        iso_ts if step_state in (StepState.RUNNING, StepState.COMPENSATING, StepState.FAILED) else None,
                        iso_ts if step_state in (StepState.COMPLETED, StepState.COMPENSATED, StepState.FAILED) else None,
                    ),
                )

    @staticmethod
    def _parse_ts(raw: str | None) -> datetime | None:
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw)
        except (ValueError, TypeError):
            return None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS saga_events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    saga_id     TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    step_name   TEXT,
    payload     TEXT,
    error       TEXT,
    timestamp   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saga_events_saga_id ON saga_events(saga_id);

CREATE TABLE IF NOT EXISTS sagas (
    saga_id     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    state       TEXT NOT NULL,
    created_at  TEXT,
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS saga_steps (
    saga_id     TEXT NOT NULL,
    step_name   TEXT NOT NULL,
    step_order  INTEGER NOT NULL,
    state       TEXT NOT NULL,
    payload     TEXT,
    error       TEXT,
    started_at  TEXT,
    completed_at TEXT,
    PRIMARY KEY (saga_id, step_name),
    FOREIGN KEY (saga_id) REFERENCES sagas(saga_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_saga_steps_saga_id ON saga_steps(saga_id);
"""
