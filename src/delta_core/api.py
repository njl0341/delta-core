from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from .config import settings
from .models import ReconciliationReport, TransactionRecord
from .reconciler import ReconcilerEngine

logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Stateless transaction reconciliation engine for hybrid "
        "stream/batch banking architectures. Operates exclusively on "
        "tokenized transaction metadata — no PII or account state."
    ),
)

# Single engine instance per process.
# In a distributed deployment, back the ledgers with a shared store (e.g. Redis).
_engine = ReconcilerEngine(
    primary_system=settings.primary_system,
    secondary_system=settings.secondary_system,
    amount_tolerance=settings.amount_tolerance,
)


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class IngestResponse(BaseModel):
    accepted: int
    duplicates_detected: int
    duplicate_tx_ids: list[str]


class ReconcileResponse(BaseModel):
    report: ReconciliationReport


class SummaryResponse(BaseModel):
    primary_system: str
    secondary_system: str
    primary_count: int
    secondary_count: int


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Ops"])
def health_check() -> dict:
    return {
        "status": "ok",
        "service": settings.app_name,
        "version": settings.app_version,
    }


@app.post(
    "/api/v1/ingest/primary",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Ingestion"],
)
def ingest_primary(records: list[TransactionRecord]) -> IngestResponse:
    """
    Ingest one or more records into the **primary** ledger.

    Records are keyed by ``(transactionId, sourceSystem, targetSystem)``.
    A duplicate composite key on this side is flagged immediately and the
    original row is preserved.
    """
    if not records:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body must contain at least one transaction record.",
        )
    duplicates = _engine.ingest_many_primary(records)
    return IngestResponse(
        accepted=len(records) - len(duplicates),
        duplicates_detected=len(duplicates),
        duplicate_tx_ids=[d.transaction_id for d in duplicates],
    )


@app.post(
    "/api/v1/ingest/secondary",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Ingestion"],
)
def ingest_secondary(records: list[TransactionRecord]) -> IngestResponse:
    """
    Ingest one or more records into the **secondary** ledger.

    Records are keyed by ``(transactionId, sourceSystem, targetSystem)``.
    A duplicate composite key on this side is flagged immediately and the
    original row is preserved.
    """
    if not records:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body must contain at least one transaction record.",
        )
    duplicates = _engine.ingest_many_secondary(records)
    return IngestResponse(
        accepted=len(records) - len(duplicates),
        duplicates_detected=len(duplicates),
        duplicate_tx_ids=[d.transaction_id for d in duplicates],
    )


@app.post("/api/v1/reconcile", response_model=ReconcileResponse, tags=["Reconciliation"])
def run_reconciliation() -> ReconcileResponse:
    """
    Execute a full reconciliation pass over all currently ingested records.

    Returns a ``ReconciliationReport`` containing a unique ``run_id``,
    match counts, and the full list of discrepancies found.
    """
    report = _engine.reconcile()
    return ReconcileResponse(report=report)


@app.get("/api/v1/summary", response_model=SummaryResponse, tags=["Reconciliation"])
def get_summary() -> SummaryResponse:
    """Return current ledger sizes without running a reconciliation pass."""
    return SummaryResponse(
        primary_system=settings.primary_system,
        secondary_system=settings.secondary_system,
        primary_count=_engine.primary_count,
        secondary_count=_engine.secondary_count,
    )


@app.delete("/api/v1/reset", status_code=status.HTTP_204_NO_CONTENT, tags=["Ops"])
def reset_engine() -> None:
    """
    Reset the engine — clears all ingested records and prior discrepancies.

    Use between reconciliation cycles or automated test runs.
    """
    _engine.reset()
