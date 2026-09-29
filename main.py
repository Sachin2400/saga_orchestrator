"""Entry point for the Distributed Saga Orchestrator Engine.

Demonstrates three scenarios:
1. A successful end-to-end order saga.
2. A failing saga (payment declined) -> automatic compensating rollback.
3.    Recovery after a simulated crash - the orchestrator restarts and
   resumes incomplete sagas from the Write-Ahead Log.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

# Ensure the project directory is importable when run directly.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from log_config import clear_saga_context, setup_logging
from models import SagaDefinition, SagaStepConfig
from orchestrator import SagaOrchestrator
from services import (
    cancel_dispatch,
    charge_payment,
    clear_failure_overrides,
    dispatch_order,
    inject_failure,
    release_inventory,
    refund_payment,
    reserve_inventory,
)
from wal import WALManager

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saga_wal.db")

logger = logging.getLogger("saga.main")


def build_order_saga_definition() -> SagaDefinition:
    """E-commerce order: Reserve -> Charge -> Dispatch, with compensating rollback."""
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
            "product_id": "PROD-001",
            "quantity": 2,
            "amount": 99.99,
            "currency": "USD",
            "order_id": "ORD-1001",
        },
    )


async def run_successful_order(orchestrator: SagaOrchestrator) -> str:
    """Execute a saga where all three steps succeed."""
    definition = build_order_saga_definition()
    instance = await orchestrator.run(definition)
    logger.info(
        "Result: saga_id=%s final_state=%s",
        instance.saga_id,
        instance.state.value,
    )
    return instance.saga_id


async def run_failing_order(orchestrator: SagaOrchestrator) -> str:
    """Execute a saga where Charge_Payment fails, triggering rollback."""
    saga_id = f"saga-fail-{int(time.time() * 1000)}"
    inject_failure(saga_id, "payment", force=True)

    definition = build_order_saga_definition()
    try:
        instance = await orchestrator.run(definition, saga_id=saga_id)
        logger.info(
            "Result: saga_id=%s final_state=%s",
            instance.saga_id,
            instance.state.value,
        )
        for step in instance.steps:
            logger.info(
                "  step=%s state=%s",
                step.step_name,
                step.state.value,
            )
        return instance.saga_id
    finally:
        clear_failure_overrides(saga_id)


async def simulate_crash_and_recover() -> None:
    """Simulate a crash mid-saga, then restart the orchestrator and recover.

    We manually advance the first step in the WAL, then close the connection
    (simulating a hard crash). On restart, ``recover()`` discovers the
    incomplete saga and resumes from step 2.
    """
    wal = WALManager(DB_PATH)
    await wal.initialize()

    saga_id = f"saga-recover-{int(time.time() * 1000)}"
    definition = build_order_saga_definition()
    payload = {**definition.initial_payload}
    payload["saga_id"] = saga_id

    # Create and start the saga, then complete only step 0.
    await wal.create_saga(saga_id, definition.name, payload)
    await wal.start_saga(saga_id)
    await wal.record_step_started(saga_id, "Reserve_Inventory", 0, payload)
    reserve_result = await reserve_inventory(dict(payload))
    await wal.record_step_completed(saga_id, "Reserve_Inventory", 0, reserve_result)

    # Simulate hard crash: close the WAL without completing or rolling back.
    await wal.close()
    logger.warning("Simulated crash -- saga_id=%s step=Reserve_Inventory (completed)", saga_id)

    # ---- Orchestrator restart: new process, fresh in-memory state ---- #
    wal2 = WALManager(DB_PATH)
    await wal2.initialize()

    orchestrator = SagaOrchestrator(wal2)
    orchestrator.register_definition(definition)

    recovered = await orchestrator.recover()

    for instance in recovered:
        if instance.saga_id == saga_id:
            logger.info(
                "Recovered saga: id=%s state=%s",
                instance.saga_id,
                instance.state.value,
            )
            for step in instance.steps:
                logger.info(
                    "  step=%-20s order=%d state=%s",
                    step.step_name,
                    step.step_order,
                    step.state.value,
                )

    await wal2.close()


async def main() -> None:
    setup_logging()

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    wal = WALManager(DB_PATH)
    await wal.initialize()

    orchestrator = SagaOrchestrator(wal)
    orchestrator.register_definition(build_order_saga_definition())

    logger.info("=" * 70)
    logger.info("SCENARIO 1: Successful Order Saga (all steps succeed)")
    logger.info("=" * 70)
    clear_saga_context()
    await run_successful_order(orchestrator)

    logger.info("=" * 70)
    logger.info("SCENARIO 2: Failing Order Saga (payment declined -> rollback)")
    logger.info("=" * 70)
    clear_saga_context()
    await run_failing_order(orchestrator)

    await wal.close()

    logger.info("=" * 70)
    logger.info("SCENARIO 3: Crash Recovery (resume from WAL after restart)")
    logger.info("=" * 70)
    await simulate_crash_and_recover()

    logger.info("=" * 70)
    logger.info("All scenarios complete.")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
