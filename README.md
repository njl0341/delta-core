# delta-core

A lightweight, deterministic reconciliation engine that continuously matches transactional data across streaming and batch financial systems, isolating discrepancies in real time to guarantee absolute data integrity.

---

## The Problem

Modern banking rarely lives entirely in one world. A card authorization may fire on a real-time event stream while the settlement leg arrives hours later from a mainframe batch file. When those two records don't agree — even by a single cent — downstream ledgers fail to close, audit trails break, and operations teams scramble after the fact.

Traditional reconciliation is historical: it runs at midnight, discovers yesterday's errors, and triggers a remediation cycle that can take hours or days. **delta-core shifts that paradigm.** It ingests records from both sources as they arrive, matches them by composite key `(transactionId, sourceSystem, targetSystem)`, and flags every discrepancy within the same processing cycle.

---

## What delta-core Is (and Is Not)

| Is | Is Not |
|---|---|
| A stateless transaction-matching service | A core banking system or ledger |
| Operates on tokenized transaction metadata | Does not store PII or cardholder data |
| Detects discrepancies in real time | Does not calculate running account balances |
| Source-agnostic (stream or batch) | Does not issue corrections or reversals |
| Strictly auditable and deterministic | Does not maintain persistent account state |

---

## Architecture

```
┌─────────────────────┐   POST /api/v1/ingest/primary    ┌──────────────────────────┐
│  Event Stream (Kafka │ ───────────────────────────────► │                          │
│  Flink, Kinesis …)  │                                  │     ReconcilerEngine     │
└─────────────────────┘   POST /api/v1/ingest/secondary  │                          │
┌─────────────────────┐ ───────────────────────────────► │  primary_ledger          │
│  Mainframe Batch    │                                  │  secondary_ledger        │
│  (CSV / JSON drop)  │                                  │  key: (txId, src, tgt)   │
└─────────────────────┘                                  └────────────┬─────────────┘
                                                                   │
                                                    POST /api/v1/reconcile
                                                                   │
                                                                   ▼
                                                      ┌────────────────────────┐
                                                      │  ReconciliationReport  │
                                                      │  run_id, matched_count │
                                                      │  discrepancies[]       │
                                                      └────────────────────────┘
```

Every `TransactionRecord` that enters the system is validated at the boundary: PAN-pattern detection, Decimal precision enforcement, and timestamp normalization all happen before a record touches the ledger.

---

## Project Layout

```
delta-core/
├── src/
│   └── delta_core/
│       ├── __init__.py       # Public exports
│       ├── models.py         # TransactionRecord, DiscrepancyRecord, enums
│       ├── reconciler.py     # ReconcilerEngine — core matching logic
│       ├── ingestion.py      # File loaders: load_json, load_csv
│       ├── config.py         # Settings (env-driven via pydantic-settings)
│       └── api.py            # FastAPI REST layer
├── tests/
│   ├── test_models.py        # Boundary validation, PAN rejection, Decimal rules
│   ├── test_reconciler.py    # Matching logic, all discrepancy types, thresholds
│   ├── test_ingestion.py     # File loaders, error handling
│   └── test_api.py           # HTTP layer, request/response contracts
├── data/
│   ├── stream_sample.json    # Sample streaming transactions
│   └── batch_sample.csv      # Sample mainframe batch file
├── main.py                   # uvicorn entrypoint
├── pyproject.toml
└── .env.example
```

---

## Data Model

### TransactionRecord

The only object that flows through delta-core. All fields are transaction **metadata** — no balances, no PII, no raw PANs.

| Field | Type | Required | Description |
|---|---|---|---|
| `transactionId` | `string` | ✓ | Unique transaction tracking identifier (1–128 chars) |
| `accountToken` | `string` | ✓ | Tokenized account reference (8–64 uppercase alphanumeric) |
| `amount` | `decimal` | ✓ | Positive value, max 2 decimal places |
| `timestamp` | `datetime` | ✓ | ISO 8601; normalized to UTC on ingestion |
| `sourceSystem` | `string` | ✓ | Free-form identifier of the originating system (e.g. `KAFKA-STREAM`) |
| `targetSystem` | `string` | ✓ | Free-form identifier of the destination system (e.g. `CORE-BANKING`) |
| `auxiliaryMetadata` | `object` | — | Non-PII key-value annotations; never read by matching logic |

### Security Constraints Enforced at the Boundary

- **PAN rejection** — any value matching a 13–19 digit sequence in `transactionId` or `accountToken` is rejected with a `422`.
- **Decimal precision** — amounts with more than 2 decimal places are rejected to prevent sub-cent ambiguity.
- **Token format** — `accountToken` must be 8–64 uppercase alphanumeric characters (`A–Z`, `0–9`, `_`, `-`).
- **UTC normalization** — naive timestamps are attached UTC; timezone-aware timestamps are converted to UTC. All skew comparisons are therefore deterministic.

### Matching Algorithm

A secondary record is a candidate for parity comparison only when all three fields match a primary row exactly:

1. **Locate candidate** — search the primary ledger for a row where `transactionId`, `sourceSystem`, and `targetSystem` are all identical.
2. **Evaluate parity** — no match → `MISSING_FROM_PRIMARY`; match → compare `amount` only.
3. **Resolve** — perfect match → purge both rows (hard-delete). Amount delta → `AMOUNT_MISMATCH` + attach `auxiliaryMetadata` blob to the discrepancy record.

### Discrepancy Types

| Type | Meaning |
|---|---|
| `AMOUNT_MISMATCH` | Composite-key match found; primary and secondary amounts disagree |
| `MISSING_FROM_PRIMARY` | Secondary record has no matching primary row (composite key not found) |
| `MISSING_FROM_SECONDARY` | Primary record has no matching secondary row after reconciliation |
| `DUPLICATE_IN_PRIMARY` | Composite key ingested more than once on the primary side |
| `DUPLICATE_IN_SECONDARY` | Composite key ingested more than once on the secondary side |

---

## REST API

Base path: `http://localhost:8000`

### `GET /health`
Liveness check.

```json
{ "status": "ok", "service": "delta-core", "version": "0.1.0" }
```

---

### `POST /api/v1/ingest/primary`
Ingest one or more records into the **primary** ledger.

Routing to the primary or secondary ledger is determined by the endpoint you call, not by any field in the payload. `sourceSystem` and `targetSystem` are pure business-topology fields — they form part of the composite match key and are shared identically by both sides of the same transaction.

**Request body** — array of `TransactionRecord`:
```json
[
  {
    "transactionId": "TXN-20240115-0001",
    "accountToken": "TOK_A1B2C3D4E5F6",
    "amount": "45.12",
    "timestamp": "2024-01-15T09:01:32",
    "sourceSystem": "PAYMENTS-PROC",
    "targetSystem": "CORE-BANKING"
  }
]
```

**Response** `202 Accepted`:
```json
{
  "accepted": 1,
  "duplicates_detected": 0,
  "duplicate_tx_ids": []
}
```

---

### `POST /api/v1/ingest/secondary`
Ingest one or more records into the **secondary** ledger. Request/response shape is identical to `/ingest/primary`.

---

### `POST /api/v1/reconcile`
Execute a full reconciliation pass over all currently ingested records. Returns a `ReconciliationReport` with a unique `run_id`.

**Response** `200 OK`:
```json
{
  "report": {
    "run_id": "3f2e1a4b-...",
    "generated_at": "2024-01-15T22:05:00Z",
    "primary_system": "KAFKA-STREAM",
    "secondary_system": "NIGHTLY-BATCH",
    "primary_count": 5,
    "secondary_count": 5,
    "matched_count": 3,
    "discrepancy_count": 2,
    "discrepancies": [
      {
        "transaction_id": "TXN-20240115-0004",
        "discrepancy_type": "AMOUNT_MISMATCH",
        "primary_value": "320.50",
        "secondary_value": "320.51",
        "detail": "Amount delta: 0.01",
        "detected_at": "2024-01-15T22:05:01Z"
      }
    ]
  }
}
```

---

### `GET /api/v1/summary`
Return current ledger sizes without running a reconciliation pass.

```json
{ "primary_system": "KAFKA-STREAM", "secondary_system": "NIGHTLY-BATCH", "primary_count": 5, "secondary_count": 5 }
```

---

### `DELETE /api/v1/reset`
Clear all ingested records and discrepancies. Use between reconciliation cycles or test runs. Returns `204 No Content`.

---

## Setup

### Prerequisites

- Python 3.11+

### Install

```bash
# Clone and enter the project
git clone <repo-url>
cd delta-core

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Install with development dependencies
pip install -e ".[dev]"
```

### Environment Configuration

```bash
cp .env.example .env
```

All settings are optional — the defaults are production-safe. Edit `.env` only to override:

| Variable | Default | Description |
|---|---|---|
| `DELTA_CORE_PRIMARY_SYSTEM` | `STREAM` | Label for the primary ledger (informational) |
| `DELTA_CORE_SECONDARY_SYSTEM` | `BATCH` | Label for the secondary ledger (informational) |
| `DELTA_CORE_AMOUNT_TOLERANCE` | `0.00` | Amount comparison tolerance (keep at `0.00`) |
| `DELTA_CORE_DEBUG` | `false` | FastAPI debug mode — never enable in production |

---

## Running the Service

```bash
source .venv/bin/activate
python main.py
```

The API will be available at `http://localhost:8000`. Interactive docs are served automatically at:

- **Swagger UI** — `http://localhost:8000/docs`
- **ReDoc** — `http://localhost:8000/redoc`

---

## Testing

The full test suite covers model validation, reconciliation logic, file ingestion, and the HTTP layer — 55 tests, zero dependencies on external services.

### Run All Tests

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

### Run a Specific Test Module

```bash
# Model boundary validation and PAN-rejection rules
python -m pytest tests/test_models.py -v

# ReconcilerEngine — matching logic and all discrepancy types
python -m pytest tests/test_reconciler.py -v

# File ingestion (JSON / CSV loaders)
python -m pytest tests/test_ingestion.py -v

# FastAPI HTTP layer
python -m pytest tests/test_api.py -v
```

### Run Tests Matching a Keyword

```bash
# All tests related to amount handling
python -m pytest tests/ -v -k "amount"

# All tests related to missing records
python -m pytest tests/ -v -k "missing"
```

### What Each Suite Tests

#### `test_models.py` (17 tests)
- Valid `TransactionRecord` construction
- PAN-pattern rejection on `accountToken` and `transactionId`
- Negative and zero amount rejection
- Sub-cent amount rejection (> 2 decimal places)
- Naive timestamp normalization to UTC
- Timezone-aware timestamp conversion to UTC
- camelCase alias acceptance (`transactionId`, `sourceSystem`, etc.)
- `DiscrepancyRecord` and `ReconciliationReport` field defaults

#### `test_reconciler.py` (18 tests)
- Clean composite-key match produces zero discrepancies
- Correct `primary_count` / `secondary_count` / `matched_count` in report
- Report carries `primary_system` and `secondary_system` names
- `AMOUNT_MISMATCH` — detection and delta value in `detail`
- `MISSING_FROM_PRIMARY` — secondary record with no matching primary composite key
- `MISSING_FROM_SECONDARY` — primary record unmatched after full pass
- Payload attached to missing-record discrepancies
- `DUPLICATE_IN_PRIMARY` / `DUPLICATE_IN_SECONDARY` — surfaced at ingestion time
- Duplicate does not overwrite the original ledger entry
- Different `targetSystem` on same `transactionId` → treated as two separate composite keys (both flagged MISSING)
- Amount is the sole parity field — other field differences after key match are ignored
- Custom `amount_tolerance` allows a configured delta to pass
- Unique `run_id` generated per `reconcile()` call
- `reset()` clears all ledger state
- `ingest_many_primary()` returns only duplicate discrepancies

#### `test_ingestion.py` (12 tests)
- `load_json` — record count, `source_system`, `target_system`, Decimal precision
- `load_csv` — record count, `source_system`, Decimal precision
- Error raised on missing required fields
- Error raised on non-numeric amount
- Error raised on PAN-pattern in `accountToken`
- Error raised when JSON root is an object (not array)

#### `test_api.py` (12 tests)
- `GET /health` returns `200 ok`
- Single record ingested to `/ingest/primary` successfully (`202`)
- Record ingested to `/ingest/primary` and matching record to `/ingest/secondary` (`202` each)
- Empty request body returns `400`
- Duplicate composite key flagged in `duplicate_tx_ids`
- PAN in `accountToken` returns `422`
- Clean match reconciled with `matched_count=1`, `discrepancy_count=0`
- Amount mismatch surfaced in reconciliation report via HTTP
- Missing-from-secondary discrepancy surfaced via HTTP
- Report contains a `run_id`
- `GET /api/v1/summary` reflects `primary_count` / `secondary_count`
- `DELETE /api/v1/reset` clears state (`204`), verified by summary

---

## End-to-End Smoke Test with the Sample Data

The `data/` directory contains a paired stream/batch dataset that deliberately includes several discrepancies for manual inspection.

```bash
source .venv/bin/activate

# Start the service (leave running in this terminal)
python main.py
```

In a second terminal:

```bash
# 1. Ingest the primary (stream) records
curl -s -X POST http://localhost:8000/api/v1/ingest/primary \
  -H "Content-Type: application/json" \
  -d @data/stream_sample.json | python3 -m json.tool

# 2. Convert and ingest the secondary (batch) CSV via Python helper
python3 - <<'EOF'
import json, csv, httpx
rows = list(csv.DictReader(open("data/batch_sample.csv")))
resp = httpx.post("http://localhost:8000/api/v1/ingest/secondary", json=rows)
print(resp.json())
EOF

# 3. Run reconciliation and review the report
curl -s -X POST http://localhost:8000/api/v1/reconcile | python3 -m json.tool
```

Expected discrepancies from the sample data:

| `transactionId` | Discrepancy |
|---|---|
| `TXN-20240115-0004` | `AMOUNT_MISMATCH` — primary: `320.50`, secondary: `320.51` |
| `TXN-20240115-0005` | `MISSING_FROM_SECONDARY` — record present in primary, absent from secondary |
| `TXN-20240115-0006` | `MISSING_FROM_PRIMARY` — secondary record has no primary counterpart |

---

## Configuration Reference

All settings are read from environment variables prefixed `DELTA_CORE_` or from a `.env` file in the project root.

```bash
# Strict zero-tolerance amount matching (default and recommended)
DELTA_CORE_AMOUNT_TOLERANCE=0.00
```

The `ReconcilerEngine` also accepts these values directly for programmatic use:

```python
from delta_core import ReconcilerEngine
from decimal import Decimal

engine = ReconcilerEngine(
    primary_system="PAYMENTS-PROC",
    secondary_system="CORE-BANKING",
    amount_tolerance=Decimal("0.00"),
)
```
