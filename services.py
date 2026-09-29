"""Mocked downstream microservices for the e-commerce saga.

Each service simulates realistic network latency and a configurable failure
rate. The functions are designed to be idempotent: supplying the same
idempotency key returns a deterministic result.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass

logger = logging.getLogger("saga.services")

# In-memory idempotency ledger: (service_name, saga_id) -> result snapshot.
# In a real system this would live in each microservice's own state store.
_ledger: dict[tuple[str, str], dict] = defaultdict(dict)

# Per-saga failure injection flags, set externally to force specific outcomes.
_failure_overrides: dict[str, dict[str, bool]] = defaultdict(dict)


@dataclass
class InventoryReservation:
    reservation_id: str
    product_id: str
    quantity: int
    ttl_seconds: int = 120


@dataclass
class PaymentCharge:
    charge_id: str
    amount: float
    currency: str
    status: str = "charged"


@dataclass
class DispatchRecord:
    shipment_id: str
    tracking_number: str
    carrier: str
    status: str = "dispatched"


class ServiceFailure(Exception):
    """Raised by a mocked microservice to signal a domain-level failure."""

    def __init__(self, service: str, message: str) -> None:
        super().__init__(message)
        self.service = service
        self.message = message

    def __str__(self) -> str:
        return f"[{self.service}] {self.message}"


# Configuration knobs -------------------------------------------------------- #

class FailureProfile:
    """Controls random failure injection for deterministic test runs."""

    DEFAULT = "default"
    CHARGE_FAIL = "charge_fail"
    ALL_FAIL = "all_fail"


# Realistic base latencies in seconds
_BASE_LATENCY = (0.15, 0.45)


def _random_latency() -> float:
    return random.uniform(*_BASE_LATENCY)


def _idempotency_key(saga_id: str, step_name: str) -> str:
    return f"{step_name}:{saga_id}"


def _check_idempotent(service: str, saga_id: str, step_name: str) -> dict | None:
    key = _idempotency_key(saga_id, step_name)
    cached = _ledger.get((service, saga_id))
    if cached:
        logger.debug("Idempotent replay - service=%s saga_id=%s", service, saga_id)
        return cached.get(key)
    return None


def _store_result(service: str, saga_id: str, step_name: str, result: dict) -> None:
    key = _idempotency_key(saga_id, step_name)
    _ledger[(service, saga_id)][key] = result


def inject_failure(saga_id: str, service: str, force: bool = True) -> None:
    """Externally force a service to fail for a specific saga."""
    _failure_overrides[saga_id][service] = force


def clear_failure_overrides(saga_id: str) -> None:
    _failure_overrides.pop(saga_id, None)


def _should_fail(saga_id: str, service: str, override_key: str | None = None) -> bool:
    override = _failure_overrides.get(saga_id, {})
    if override_key and override.get(override_key, False):
        return True
    if override.get(service, False):
        return True
    return False


# --------------------------------------------------------------------------- #
# Step 1: Reserve Inventory
# --------------------------------------------------------------------------- #

async def reserve_inventory(payload: dict) -> dict:
    saga_id = payload.get("saga_id", "unknown")
    step_name = "Reserve_Inventory"

    cached = _check_idempotent("inventory", saga_id, step_name)
    if cached is not None:
        return cached

    if _should_fail(saga_id, "inventory", override_key="inventory"):
        await asyncio.sleep(_random_latency())
        msg = "Inventory service unavailable - could not reserve stock"
        logger.error("reserve_inventory FAILED saga_id=%s", saga_id)
        raise ServiceFailure("inventory", msg)

    await asyncio.sleep(_random_latency())

    product_id = payload.get("product_id", "PROD-001")
    quantity = payload.get("quantity", 1)

    result = {
        "reservation_id": f"res-{uuid.uuid4().hex[:12]}",
        "product_id": product_id,
        "quantity": quantity,
        "status": "reserved",
    }
    _store_result("inventory", saga_id, step_name, result)

    logger.info("reserve_inventory OK saga_id=%s reservation_id=%s", saga_id, result["reservation_id"])
    return result


# --------------------------------------------------------------------------- #
# Compensating Step 1: Release Inventory
# --------------------------------------------------------------------------- #

async def release_inventory(payload: dict) -> dict:
    saga_id = payload.get("saga_id", "unknown")
    step_name = "Release_Inventory"

    cached = _check_idempotent("inventory", saga_id, step_name)
    if cached is not None:
        return cached

    await asyncio.sleep(_random_latency() * 0.5)

    reservation_id = payload.get("reservation_id", "")
    if not reservation_id:
        logger.warning("release_inventory called without reservation_id saga_id=%s", saga_id)

    result = {
        "reservation_id": reservation_id,
        "status": "released",
    }
    _store_result("inventory", saga_id, step_name, result)

    logger.info("release_inventory OK saga_id=%s reservation_id=%s", saga_id, reservation_id)
    return result


# --------------------------------------------------------------------------- #
# Step 2: Charge Payment
# --------------------------------------------------------------------------- #

async def charge_payment(payload: dict) -> dict:
    saga_id = payload.get("saga_id", "unknown")
    step_name = "Charge_Payment"

    cached = _check_idempotent("payment", saga_id, step_name)
    if cached is not None:
        return cached

    if _should_fail(saga_id, "payment", override_key="payment"):
        await asyncio.sleep(_random_latency())
        msg = "Payment gateway declined - insufficient funds"
        logger.error("charge_payment FAILED saga_id=%s", saga_id)
        raise ServiceFailure("payment", msg)

    await asyncio.sleep(_random_latency())

    amount = payload.get("amount", 0.0)
    currency = payload.get("currency", "USD")

    result = {
        "charge_id": f"ch-{uuid.uuid4().hex[:12]}",
        "amount": amount,
        "currency": currency,
        "status": "charged",
    }
    _store_result("payment", saga_id, step_name, result)

    logger.info("charge_payment OK saga_id=%s charge_id=%s", saga_id, result["charge_id"])
    return result


# --------------------------------------------------------------------------- #
# Compensating Step 2: Refund Payment
# --------------------------------------------------------------------------- #

async def refund_payment(payload: dict) -> dict:
    saga_id = payload.get("saga_id", "unknown")
    step_name = "Refund_Payment"

    cached = _check_idempotent("payment", saga_id, step_name)
    if cached is not None:
        return cached

    await asyncio.sleep(_random_latency() * 0.5)

    charge_id = payload.get("charge_id", "")
    amount = payload.get("amount", 0.0)

    result = {
        "charge_id": charge_id,
        "refund_id": f"ref-{uuid.uuid4().hex[:12]}",
        "amount": amount,
        "status": "refunded",
    }
    _store_result("payment", saga_id, step_name, result)

    logger.info("refund_payment OK saga_id=%s charge_id=%s", saga_id, charge_id)
    return result


# --------------------------------------------------------------------------- #
# Step 3: Dispatch Order
# --------------------------------------------------------------------------- #

async def dispatch_order(payload: dict) -> dict:
    saga_id = payload.get("saga_id", "unknown")
    step_name = "Dispatch_Order"

    cached = _check_idempotent("dispatch", saga_id, step_name)
    if cached is not None:
        return cached

    if _should_fail(saga_id, "dispatch", override_key="dispatch"):
        await asyncio.sleep(_random_latency())
        msg = "Dispatch service unavailable - could not create shipment"
        logger.error("dispatch_order FAILED saga_id=%s", saga_id)
        raise ServiceFailure("dispatch", msg)

    await asyncio.sleep(_random_latency())

    order_id = payload.get("order_id", "")

    result = {
        "shipment_id": f"shp-{uuid.uuid4().hex[:12]}",
        "tracking_number": f"TRK-{uuid.uuid4().hex[:10].upper()}",
        "carrier": "FastShip",
        "order_id": order_id,
        "status": "dispatched",
    }
    _store_result("dispatch", saga_id, step_name, result)

    logger.info("dispatch_order OK saga_id=%s shipment_id=%s", saga_id, result["shipment_id"])
    return result


# --------------------------------------------------------------------------- #
# Compensating Step 3: Cancel Dispatch
# --------------------------------------------------------------------------- #

async def cancel_dispatch(payload: dict) -> dict:
    saga_id = payload.get("saga_id", "unknown")
    step_name = "Cancel_Dispatch"

    cached = _check_idempotent("dispatch", saga_id, step_name)
    if cached is not None:
        return cached

    await asyncio.sleep(_random_latency() * 0.5)

    shipment_id = payload.get("shipment_id", "")

    result = {
        "shipment_id": shipment_id,
        "status": "cancelled",
    }
    _store_result("dispatch", saga_id, step_name, result)

    logger.info("cancel_dispatch OK saga_id=%s shipment_id=%s", saga_id, shipment_id)
    return result


__all__ = [
    "reserve_inventory",
    "release_inventory",
    "charge_payment",
    "refund_payment",
    "dispatch_order",
    "cancel_dispatch",
    "inject_failure",
    "clear_failure_overrides",
    "ServiceFailure",
    "FailureProfile",
    "InventoryReservation",
    "PaymentCharge",
    "DispatchRecord",
]
