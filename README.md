# Distributed Saga Orchestrator Engine

A production-grade Python application implementing a distributed saga orchestrator using `asyncio` and `aiosqlite`.

## Overview

The Saga pattern coordinates distributed transactions across multiple services. Each saga is a sequence of local transactions where:
- Each step has an **execute** action and a **compensating** action
- If a step fails, previously completed steps are rolled back in reverse order
- A Write-Ahead Log (WAL) persists all state transitions for crash recovery

## Architecture

```
main.py          -- Entry point: runs success, failure, and recovery demos
orchestrator.py  -- Core Saga engine (forward execution + backward compensation)
wal.py           -- SQLite-backed Write-Ahead Log (aiosqlite)
services.py      -- Mocked microservices with idempotency and failure injection
models.py        -- Enums, dataclasses, and type aliases
exceptions.py    -- Custom exception hierarchy
log_config.py    -- Structured logging with context-aware contextvars
```

## WAL Schema

Three tables maintain durable state:

| Table | Purpose |
|---|---|
| `saga_events` | Immutable event log (all state transitions) |
| `sagas` | Materialized saga state (current view) |
| `saga_steps` | Materialized per-step state |

Every event write and state update happens atomically in the same transaction, ensuring consistency even if the orchestrator crashes mid-write.

## Recovery

On restart, the orchestrator scans the WAL for incomplete sagas and:
1. **RUNNING** sagas: skips `COMPLETED` steps, re-executes the first non-terminal step (idempotent replay)
2. **COMPENSATING** sagas: re-executes compensation for steps not yet `COMPENSATED`
3. **FAILED** step detection: if a step already failed, triggers rollback automatically

## Demo Scenarios

1. **Successful order** — all 3 steps complete
2. **Failing order** — payment declines → inventory released
3. **Crash recovery** — saga interrupted after step 1, resumed on restart

## Requirements

- Python 3.10+
- `aiosqlite`

## Usage

```bash
pip install -r requirements.txt
python main.py
```
