"""Core Saga orchestration engine.

Implements forward execution and backward compensation for distributed
transactions. Every WAL write happens before the corresponding service
call, so a crash at any point leaves the system in a recoverable state.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from log_config import clear_saga_context, set_saga_context
from exceptions import (
    SagaExecutionError,
    SagaTimeoutError,
    ServiceError,
)
from models import (
    SagaDefinition,
    SagaInstance,
    SagaState,
    SagaStepConfig,
    SagaStepRecord,
    StepState,
)
from services import ServiceFailure
from wal import WALManager

logger = logging.getLogger("saga.orchestrator")


class SagaOrchestrator:
    """Coordinates multi-step distributed transactions with compensating rollbacks.

    The orchestrator maintains an in-process definition registry so that
    ``recover()`` can look up step configurations (including the compensate
    callbacks) by saga name when resuming an incomplete saga from the WAL.
    """

    def __init__(self, wal: WALManager):
        self._wal = wal
        self._definitions: dict[str, SagaDefinition] = {}

    def register_definition(self, definition: SagaDefinition) -> None:
        self._definitions[definition.name] = definition

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def run(
        self,
        definition: SagaDefinition,
        saga_id: str | None = None,
        initial_payload: dict[str, Any] | None = None,
    ) -> SagaInstance:
        """Start a new saga, execute it to completion or rollback."""
        saga_id = saga_id or self._generate_saga_id()
        payload = {**definition.initial_payload, **(initial_payload or {})}
        payload["saga_id"] = saga_id

        # Register locally so recovery (if it happens in this process)
        # can resolve the definition.
        self._definitions[definition.name] = definition

        await self._wal.create_saga(saga_id, definition.name, payload)
        await self._wal.start_saga(saga_id)

        set_saga_context(saga_id=saga_id)
        logger.info(
            "Saga started saga_id=%s definition=%s steps=%d",
            saga_id,
            definition.name,
            len(definition.steps),
        )

        return await self._execute_forward(saga_id, definition, payload)

    async def recover(self) -> list[SagaInstance]:
        """Scan the WAL for incomplete sagas and resume them."""
        incomplete = await self._wal.get_incomplete_sagas()
        results: list[SagaInstance] = []

        for instance in incomplete:
            definition = self._definitions.get(instance.name)
            if definition is None:
                logger.warning(
                    "No registered definition for saga name=%s id=%s; skipping",
                    instance.name,
                    instance.saga_id,
                )
                results.append(instance)
                continue

            try:
                instance = await self._resume(instance, definition)
            except Exception:
                logger.error(
                    "Recovery error saga_id=%s", instance.saga_id, exc_info=True
                )
            results.append(instance)

        return results

    # ------------------------------------------------------------------ #
    # Forward execution
    # ------------------------------------------------------------------ #

    async def _execute_forward(
        self,
        saga_id: str,
        definition: SagaDefinition,
        payload: dict[str, Any],
    ) -> SagaInstance:
        results: dict[str, Any] = dict(payload)

        try:
            for order, step in enumerate(definition.steps):
                step_result = await self._execute_step(saga_id, step, order, results)
                results[step.name] = step_result
                results.update(
                    {k: v for k, v in step_result.items() if k != "status"}
                )

            await self._wal.complete_saga(saga_id)
            logger.info("Saga completed successfully saga_id=%s", saga_id)

        except (SagaExecutionError, ServiceError, SagaTimeoutError) as exc:
            logger.error(
                "Saga execution halted saga_id=%s step=%s",
                saga_id,
                getattr(exc, "step_name", None),
            )
            await self._rollback(saga_id, definition, results)

        except Exception:
            logger.exception("Unexpected error during saga execution saga_id=%s", saga_id)
            await self._rollback(saga_id, definition, results)
            raise
        finally:
            clear_saga_context()

        return await self._wal.get_saga_instance(saga_id)

    async def _execute_step(
        self,
        saga_id: str,
        step: SagaStepConfig,
        order: int,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        set_saga_context(saga_id=saga_id, step_name=step.name)

        # The WAL write precedes the service call. If the orchestrator
        # crashes between the write and the service response, recovery
        # sees a RUNNING step and re-executes (the service is idempotent).
        await self._wal.record_step_started(saga_id, step.name, order, payload)

        try:
            result = await asyncio.wait_for(
                step.execute(dict(payload)), timeout=step.timeout
            )
        except asyncio.TimeoutError as exc:
            await self._wal.record_step_failed(
                saga_id, step.name, order, f"Timeout after {step.timeout}s"
            )
            logger.warning(
                "Step timed out saga_id=%s step=%s timeout=%ss",
                saga_id,
                step.name,
                step.timeout,
            )
            raise SagaTimeoutError(
                f"Step {step.name} timed out after {step.timeout}s",
                saga_id=saga_id,
                step_name=step.name,
                cause=exc,
            ) from exc

        except ServiceFailure as exc:
            await self._wal.record_step_failed(saga_id, step.name, order, str(exc))
            logger.error(
                "Step service failure saga_id=%s step=%s service=%s",
                saga_id,
                step.name,
                exc.service,
            )
            raise ServiceError(
                f"Step {step.name} failed: {exc.message}",
                saga_id=saga_id,
                step_name=step.name,
                cause=exc,
            ) from exc

        except Exception as exc:
            await self._wal.record_step_failed(saga_id, step.name, order, str(exc))
            logger.error(
                "Step failed saga_id=%s step=%s error=%s",
                saga_id,
                step.name,
                exc,
            )
            raise SagaExecutionError(
                f"Step {step.name} failed: {exc}",
                saga_id=saga_id,
                step_name=step.name,
                cause=exc,
            ) from exc

        await self._wal.record_step_completed(saga_id, step.name, order, result)
        return result

    # ------------------------------------------------------------------ #
    # Backward compensation (rollback)
    # ------------------------------------------------------------------ #

    async def _rollback(
        self,
        saga_id: str,
        definition: SagaDefinition,
        results: dict[str, Any],
    ) -> None:
        await self._wal.start_compensation(saga_id)

        instance = await self._wal.get_saga_instance(saga_id)
        completed_steps = [
            s for s in instance.steps if s.state == StepState.COMPLETED
        ]
        completed_steps.reverse()

        compensation_errors: list[str] = []

        for step_record in completed_steps:
            step = self._find_step_by_name(definition, step_record.step_name)
            if step is None:
                logger.warning(
                    "No compensation config for step=%s saga_id=%s",
                    step_record.step_name,
                    saga_id,
                )
                continue

            # Build the compensation payload from the initial payload
            # merged with the step's own execution result (stored in WAL).
            initial_payload = await self._wal.get_saga_initial_payload(saga_id)
            comp_payload = {**initial_payload, "saga_id": saga_id}
            comp_payload.update(step_record.payload)

            try:
                await self._compensate_step(
                    saga_id, step, step_record, comp_payload
                )
            except Exception as exc:
                compensation_errors.append(
                    f"Compensation failed for {step_record.step_name}: {exc}"
                )
                logger.error(
                    "Compensation failed saga_id=%s step=%s error=%s",
                    saga_id,
                    step_record.step_name,
                    exc,
                )

        if compensation_errors:
            logger.warning(
                "Saga partially compensated saga_id=%s errors=%d",
                saga_id,
                len(compensation_errors),
            )

        await self._wal.complete_compensation(saga_id)
        logger.info("Saga compensated saga_id=%s", saga_id)

    async def _compensate_step(
        self,
        saga_id: str,
        step: SagaStepConfig,
        record: SagaStepRecord,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        set_saga_context(saga_id=saga_id, step_name=step.name)
        await self._wal.record_compensating(
            saga_id,
            step.name,
            record.step_order,
            "step failed or saga rolled back",
        )

        try:
            result = await asyncio.wait_for(
                step.compensate(dict(payload)), timeout=step.timeout
            )
        except asyncio.TimeoutError as exc:
            await self._wal.record_compensation_failed(
                saga_id, step.name, record.step_order, f"Timeout after {step.timeout}s"
            )
            raise SagaTimeoutError(
                f"Compensation for {step.name} timed out after {step.timeout}s",
                saga_id=saga_id,
                step_name=step.name,
                cause=exc,
            ) from exc

        except ServiceFailure as exc:
            await self._wal.record_compensation_failed(
                saga_id, step.name, record.step_order, str(exc)
            )
            raise ServiceError(
                f"Compensation for {step.name} failed: {exc.message}",
                saga_id=saga_id,
                step_name=step.name,
                cause=exc,
            ) from exc

        except Exception as exc:
            await self._wal.record_compensation_failed(
                saga_id, step.name, record.step_order, str(exc)
            )
            raise SagaExecutionError(
                f"Compensation for {step.name} failed: {exc}",
                saga_id=saga_id,
                step_name=step.name,
                cause=exc,
            ) from exc

        await self._wal.record_compensation_completed(
            saga_id, step.name, record.step_order, result
        )
        return result

    # ------------------------------------------------------------------ #
    # Recovery
    # ------------------------------------------------------------------ #

    async def _resume(self, instance: SagaInstance, definition: SagaDefinition) -> SagaInstance:
        """Resume a saga from its persisted WAL state.

        RUNNING: skip COMPLETED steps, re-execute the first non-terminal
        step (idempotent replay). If a step already FAILED, transition
        to rollback.

        COMPENSATING: skip COMPENSATED steps, re-execute compensation
        for the first non-terminal step.
        """
        await self._wal.resume_saga(instance.saga_id)

        state = instance.state

        if state in (SagaState.COMPLETED, SagaState.FAILED, SagaState.COMPENSATED):
            logger.info(
                "Saga already terminal saga_id=%s state=%s", instance.saga_id, state
            )
            return instance

        # Reconstruct the payload from the initial state + completed steps.
        payload = await self._reconstruct_payload(instance)

        if state == SagaState.RUNNING:
            # If any step FAILED, start rollback.
            failed = [s for s in instance.steps if s.state == StepState.FAILED]
            if failed:
                logger.info(
                    "Detected failed step during recovery -> rollback saga_id=%s step=%s",
                    instance.saga_id,
                    failed[0].step_name,
                )
                await self._wal.fail_saga(
                    instance.saga_id, failed[0].error or "unknown"
                )
                await self._wal.start_compensation(instance.saga_id)
                await self._resume_compensating(instance, definition)
                return await self._wal.get_saga_instance(instance.saga_id)

            # Find the first step that is not COMPLETED.
            step_map = {s.step_name: s for s in instance.steps}
            for order, step in enumerate(definition.steps):
                record = step_map.get(step.name)
                if record is None:
                    # Step doesn't exist in WAL -> never started.
                    step_result = await self._execute_step(instance.saga_id, step, order, payload)
                    payload[step.name] = step_result
                    payload.update({k: v for k, v in step_result.items() if k != "status"})
                    continue

                if record.state == StepState.COMPLETED:
                    # Re-merge completed step result into payload.
                    payload.update(record.payload)
                    continue

                if record.state == StepState.RUNNING:
                    # Crashed mid-execution. Re-execute (idempotent).
                    step_result = await self._execute_step(instance.saga_id, step, order, payload)
                    payload[step.name] = step_result
                    payload.update({k: v for k, v in step_result.items() if k != "status"})
                    continue

                # PENDING or any other non-terminal state -> execute.
                step_result = await self._execute_step(instance.saga_id, step, order, payload)
                payload[step.name] = step_result
                payload.update({k: v for k, v in step_result.items() if k != "status"})

            await self._wal.complete_saga(instance.saga_id)
            logger.info("Saga completed during recovery saga_id=%s", instance.saga_id)

        elif state == SagaState.COMPENSATING:
            await self._resume_compensating(instance, definition)

        return await self._wal.get_saga_instance(instance.saga_id)

    async def _resume_compensating(
        self, instance: SagaInstance, definition: SagaDefinition
    ) -> None:
        """Re-execute compensations for steps not yet COMPENSATED."""
        completed_steps = [
            s for s in instance.steps if s.state == StepState.COMPLETED
        ]
        completed_steps.reverse()

        name_to_step = {s.name: s for s in definition.steps}

        for step_record in completed_steps:
            if step_record.state == StepState.COMPENSATED:
                continue

            step = name_to_step.get(step_record.step_name)
            if step is None:
                logger.warning(
                    "No compensation config for step=%s during recovery saga_id=%s",
                    step_record.step_name,
                    instance.saga_id,
                )
                continue

            initial_payload = await self._wal.get_saga_initial_payload(instance.saga_id)
            comp_payload = {**initial_payload, "saga_id": instance.saga_id}
            comp_payload.update(step_record.payload)

            try:
                if step_record.state == StepState.COMPENSATING:
                    # Crashed mid-compensation -> re-execute.
                    await self._compensate_step(
                        instance.saga_id, step, step_record, comp_payload
                    )
                else:
                    # COMPLETED -> just compensate.
                    await self._compensate_step(
                        instance.saga_id, step, step_record, comp_payload
                    )
            except Exception as exc:
                logger.error(
                    "Compensation failed during recovery saga_id=%s step=%s error=%s",
                    instance.saga_id,
                    step_record.step_name,
                    exc,
                )

        await self._wal.complete_compensation(instance.saga_id)
        logger.info(
            "Saga compensation completed during recovery saga_id=%s",
            instance.saga_id,
        )

    async def _reconstruct_payload(self, instance: SagaInstance) -> dict[str, Any]:
        """Rebuild the execution payload from the initial payload + completed step results."""
        payload = await self._wal.get_saga_initial_payload(instance.saga_id)
        payload = dict(payload)
        payload["saga_id"] = instance.saga_id

        for step in instance.steps:
            if step.state == StepState.COMPLETED:
                payload.update(step.payload)

        return payload

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _generate_saga_id() -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"saga-{ts}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _find_step_by_name(
        definition: SagaDefinition, name: str
    ) -> SagaStepConfig | None:
        for step in definition.steps:
            if step.name == name:
                return step
        return None
