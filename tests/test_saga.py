"""Test suite for the Saga Orchestrator Engine.

Tests cover: WAL schema integrity, saga state transitions, successful
execution, failure + compensation rollback, crash recovery, idempotency,
and timeout handling.
"""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from exceptions import (  # noqa: E402
    SagaError,
    SagaExecutionError,
    SagaTimeoutError,
    ServiceError,
    StateTransitionError,
    WALWriteError,
)
from models import SagaDefinition, SagaState, SagaStepConfig, StepState  # noqa: E402
from orchestrator import SagaOrchestrator  # noqa: E402
from services import (  # noqa: E402
    cancel_dispatch,
    charge_payment,
    clear_failure_overrides,
    dispatch_order,
    inject_failure,
    release_inventory,
    refund_payment,
    reserve_inventory,
)
from wal import WALManager  # noqa: E402


def build_definition():
    """Build a fresh E-CommerceOrderSaga definition for testing."""
    return SagaDefinition(
        name="ECommerceOrderSaga",
        steps=(
            SagaStepConfig(
                name="Reserve_Inventory",
                execute=reserve_inventory,
                compensate=release_inventory,
                timeout=5.0,
            ),
            SagaStepConfig(
                name="Charge_Payment",
                execute=charge_payment,
                compensate=refund_payment,
                timeout=5.0,
            ),
            SagaStepConfig(
                name="Dispatch_Order",
                execute=dispatch_order,
                compensate=cancel_dispatch,
                timeout=5.0,
            ),
        ),
        initial_payload={
            "product_id": "PROD-TEST",
            "quantity": 1,
            "amount": 49.99,
            "currency": "USD",
            "order_id": "ORD-TEST",
        },
    )


# --------------------------------------------------------------------------- #
# WAL / Schema tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_wal_schema_is_created(db_path):
    """The WAL manager must create all three tables on initialization."""
    import aiosqlite

    wal = WALManager(db_path)
    await wal.initialize()

    conn = await aiosqlite.connect(db_path)
    cursor = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [r[0] for r in await cursor.fetchall()]
    await cursor.close()
    await conn.close()

    assert "saga_events" in tables
    assert "sagas" in tables
    assert "saga_steps" in tables

    await wal.close()


@pytest.mark.asyncio
async def test_wal_starts_empty(db_path):
    """A freshly initialized WAL should have no sagas."""
    wal = WALManager(db_path)
    await wal.initialize()
    instances = await wal.get_incomplete_sagas()
    assert len(instances) == 0
    await wal.close()


@pytest.mark.asyncio
async def test_saga_state_transitions(db_path):
    """create -> start -> complete should move saga through all states."""
    wal = WALManager(db_path)
    await wal.initialize()

    saga_id = "test-state-transitions"

    await wal.create_saga(saga_id, "TestSaga", {"foo": "bar"})
    assert await wal.get_saga_state(saga_id) == SagaState.PENDING

    await wal.start_saga(saga_id)
    assert await wal.get_saga_state(saga_id) == SagaState.RUNNING

    await wal.complete_saga(saga_id)
    assert await wal.get_saga_state(saga_id) == SagaState.COMPLETED

    await wal.close()


@pytest.mark.asyncio
async def test_step_state_lifecycle(db_path):
    """Steps should progress: started (RUNNING) -> completed."""
    wal = WALManager(db_path)
    await wal.initialize()

    saga_id = "test-step-lifecycle"
    await wal.create_saga(saga_id, "TestSaga", {})
    await wal.start_saga(saga_id)

    await wal.record_step_started(saga_id, "StepA", 0, {"input": 1})
    instance = await wal.get_saga_instance(saga_id)
    step = next(s for s in instance.steps if s.step_name == "StepA")
    assert step.state == StepState.RUNNING

    await wal.record_step_completed(saga_id, "StepA", 0, {"result": "ok"})
    instance = await wal.get_saga_instance(saga_id)
    step = next(s for s in instance.steps if s.step_name == "StepA")
    assert step.state == StepState.COMPLETED

    await wal.close()


# --------------------------------------------------------------------------- #
# Orchestration tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_successful_saga(db_path):
    """All steps succeed -> saga reaches COMPLETED."""
    wal = WALManager(db_path)
    await wal.initialize()

    orch = SagaOrchestrator(wal)
    definition = build_definition()
    orch.register_definition(definition)

    instance = await orch.run(definition, saga_id="test-success")

    assert instance.state == SagaState.COMPLETED
    assert len(instance.steps) == 3
    for step in instance.steps:
        assert step.state == StepState.COMPLETED

    await wal.close()


@pytest.mark.asyncio
async def test_failure_triggers_compensation(db_path):
    """When Charge_Payment fails, Reserve_Inventory should be compensated."""
    wal = WALManager(db_path)
    await wal.initialize()

    saga_id = "test-fail-compensation"
    inject_failure(saga_id, "payment", force=True)

    orch = SagaOrchestrator(wal)
    definition = build_definition()
    orch.register_definition(definition)

    instance = await orch.run(definition, saga_id=saga_id)

    assert instance.state == SagaState.COMPENSATED

    step_map = {s.step_name: s for s in instance.steps}
    assert step_map["Reserve_Inventory"].state == StepState.COMPENSATED
    assert step_map["Charge_Payment"].state == StepState.FAILED
    assert "Dispatch_Order" not in step_map

    clear_failure_overrides(saga_id)
    await wal.close()


@pytest.mark.asyncio
async def test_compensation_runs_in_reverse_order(db_path):
    """If Dispatch_Order fails, steps 2 and 1 should be compensated."""
    wal = WALManager(db_path)
    await wal.initialize()

    saga_id = "test-reverse-compensation"
    inject_failure(saga_id, "dispatch", force=True)

    orch = SagaOrchestrator(wal)
    definition = build_definition()
    orch.register_definition(definition)

    instance = await orch.run(definition, saga_id=saga_id)

    assert instance.state == SagaState.COMPENSATED

    step_map = {s.step_name: s for s in instance.steps}
    assert step_map["Reserve_Inventory"].state == StepState.COMPENSATED
    assert step_map["Charge_Payment"].state == StepState.COMPENSATED
    assert step_map["Dispatch_Order"].state == StepState.FAILED

    clear_failure_overrides(saga_id)
    await wal.close()


# --------------------------------------------------------------------------- #
# Idempotency tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_service_idempotency(db_path):
    """Calling reserve_inventory twice with the same saga_id returns the same result."""
    wal = WALManager(db_path)
    await wal.initialize()

    saga_id = "test-service-idempotent"
    payload = {"saga_id": saga_id, "product_id": "PROD-1", "quantity": 2}

    result1 = await reserve_inventory(dict(payload))
    result2 = await reserve_inventory(dict(payload))

    assert result1["reservation_id"] == result2["reservation_id"]

    await wal.close()


# --------------------------------------------------------------------------- #
# Recovery tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_crash_recovery_resumes(db_path):
    """If a saga crashes after step 1, recovery should complete remaining steps."""
    wal = WALManager(db_path)
    await wal.initialize()

    saga_id = "test-recovery"
    payload = {
        "product_id": "PROD-REC",
        "quantity": 1,
        "amount": 49.99,
        "currency": "USD",
        "order_id": "ORD-REC",
        "saga_id": saga_id,
    }

    # Manually create saga and complete only step 0 (simulate crash)
    await wal.create_saga(saga_id, "ECommerceOrderSaga", payload)
    await wal.start_saga(saga_id)
    await wal.record_step_started(saga_id, "Reserve_Inventory", 0, payload)
    reserve_result = await reserve_inventory(dict(payload))
    await wal.record_step_completed(saga_id, "Reserve_Inventory", 0, reserve_result)

    await wal.close()

    # Simulate restart
    wal2 = WALManager(db_path)
    await wal2.initialize()

    orch = SagaOrchestrator(wal2)
    definition = build_definition()
    orch.register_definition(definition)

    recovered = await orch.recover()

    found = [i for i in recovered if i.saga_id == saga_id]
    assert len(found) == 1
    assert found[0].state == SagaState.COMPLETED
    assert len(found[0].steps) == 3
    for step in found[0].steps:
        assert step.state == StepState.COMPLETED

    await wal2.close()


# --------------------------------------------------------------------------- #
# Timeout tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_step_timeout():
    """A step that exceeds its timeout should be marked FAILED and the saga compensated."""
    db = tempfile.mkdtemp() + "/timeout_test.db"
    wal = WALManager(db)
    await wal.initialize()

    async def slow_service(payload):
        await asyncio.sleep(2)
        return {"done": True}

    async def noop_compensation(payload):
        return {"compensated": True}

    orch = SagaOrchestrator(wal)
    definition = SagaDefinition(
        name="TimeoutTest",
        steps=(
            SagaStepConfig(
                name="Slow_Step",
                execute=slow_service,
                compensate=noop_compensation,
                timeout=0.5,
            ),
        ),
        initial_payload={},
    )
    orch.register_definition(definition)

    instance = await orch.run(definition, saga_id="test-timeout")

    # The step timed out -> FAILED. No prior steps completed, so there
    # is nothing to compensate, but the saga still reaches COMPENSATED.
    assert instance.state == SagaState.COMPENSATED
    assert instance.steps[0].state == StepState.FAILED

    await wal.close()
    os.remove(db)


# --------------------------------------------------------------------------- #
# Exception hierarchy tests
# --------------------------------------------------------------------------- #


def test_custom_exception_hierarchy():
    """Verify exception classes exist and are properly related."""
    assert issubclass(SagaExecutionError, SagaError)
    assert issubclass(ServiceError, SagaError)
    assert issubclass(SagaTimeoutError, SagaExecutionError)
    assert issubclass(WALWriteError, SagaError)
    assert issubclass(StateTransitionError, SagaError)

    # SagaTimeoutError must carry step_name and cause
    exc = SagaTimeoutError("test", saga_id="s1", step_name="Step1")
    assert exc.step_name == "Step1"
    assert exc.saga_id == "s1"


def test_exception_str_includes_saga_id():
    """SagaError.__str__ should include the saga_id prefix."""
    exc = ServiceError("something broke", saga_id="saga-abc", step_name="Step1")
    assert "saga-abc" in str(exc)


# --------------------------------------------------------------------------- #
# Persistence tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_wal_persists_events(db_path):
    """Events written to the WAL should be queryable after the fact."""
    wal = WALManager(db_path)
    await wal.initialize()

    saga_id = "test-event-persistence"
    await wal.create_saga(saga_id, "TestSaga", {"key": "value"})
    await wal.start_saga(saga_id)
    await wal.record_step_started(saga_id, "Step1", 0, {"input": 1})
    await wal.record_step_completed(saga_id, "Step1", 0, {"result": "ok"})
    await wal.complete_saga(saga_id)

    instance = await wal.get_saga_instance(saga_id)
    assert instance is not None
    assert instance.saga_id == saga_id
    assert instance.state == SagaState.COMPLETED
    assert len(instance.steps) == 1
    assert instance.steps[0].state == StepState.COMPLETED
    assert instance.steps[0].payload == {"result": "ok"}

    await wal.close()
