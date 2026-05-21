"""
Tests for the FastAPI REST layer.

Uses FastAPI's TestClient (backed by httpx) — no live server required.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.delta_core.api import app, _engine


@pytest.fixture(autouse=True)
def reset_engine():
    """Ensure a clean engine state before every test."""
    _engine.reset()
    yield
    _engine.reset()


client = TestClient(app)


def _primary_payload(tx_id: str, amount: str = "75.00") -> dict:
    return {
        "transactionId": tx_id,
        "accountToken": "TOK_APIX1234",
        "amount": amount,
        "timestamp": "2024-01-15T10:00:00",
        "sourceSystem": "PAYMENTS-PROC",
        "targetSystem": "CORE-BANKING",
    }


# Both sides of the same transaction share identical topology fields.
# Routing to primary vs secondary ledger is determined by the endpoint, not the payload.
_secondary_payload = _primary_payload


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health_returns_ok():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def test_ingest_single_primary_record():
    r = client.post("/api/v1/ingest/primary", json=[_primary_payload("API-TX-001")])
    assert r.status_code == 202
    data = r.json()
    assert data["accepted"] == 1
    assert data["duplicates_detected"] == 0


def test_ingest_primary_and_secondary_record():
    r1 = client.post("/api/v1/ingest/primary", json=[_primary_payload("API-TX-002")])
    r2 = client.post("/api/v1/ingest/secondary", json=[_secondary_payload("API-TX-002")])
    assert r1.status_code == 202
    assert r2.status_code == 202
    assert r1.json()["accepted"] == 1
    assert r2.json()["accepted"] == 1


def test_ingest_empty_body_returns_400():
    r = client.post("/api/v1/ingest/primary", json=[])
    assert r.status_code == 400


def test_ingest_duplicate_flagged():
    client.post("/api/v1/ingest/primary", json=[_primary_payload("API-TX-003")])
    r = client.post("/api/v1/ingest/primary", json=[_primary_payload("API-TX-003")])
    data = r.json()
    assert data["duplicates_detected"] == 1
    assert "API-TX-003" in data["duplicate_tx_ids"]


def test_ingest_rejects_pan_in_token():
    payload = _primary_payload("API-TX-004")
    payload["accountToken"] = "4111111111111111"
    r = client.post("/api/v1/ingest/primary", json=[payload])
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Reconcile
# ---------------------------------------------------------------------------

def test_reconcile_clean_match():
    client.post("/api/v1/ingest/primary", json=[_primary_payload("REC-TX-001")])
    client.post("/api/v1/ingest/secondary", json=[_secondary_payload("REC-TX-001")])
    r = client.post("/api/v1/reconcile")
    assert r.status_code == 200
    report = r.json()["report"]
    assert report["matched_count"] == 1
    assert report["discrepancy_count"] == 0


def test_reconcile_amount_mismatch():
    client.post("/api/v1/ingest/primary", json=[_primary_payload("REC-TX-002", "100.00")])
    client.post("/api/v1/ingest/secondary", json=[_secondary_payload("REC-TX-002", "100.01")])
    r = client.post("/api/v1/reconcile")
    report = r.json()["report"]
    assert report["discrepancy_count"] == 1
    assert report["discrepancies"][0]["discrepancy_type"] == "AMOUNT_MISMATCH"


def test_reconcile_missing_from_secondary():
    client.post("/api/v1/ingest/primary", json=[_primary_payload("REC-TX-003")])
    r = client.post("/api/v1/reconcile")
    report = r.json()["report"]
    assert report["discrepancies"][0]["discrepancy_type"] == "MISSING_FROM_SECONDARY"


def test_reconcile_report_has_run_id():
    r = client.post("/api/v1/reconcile")
    report = r.json()["report"]
    assert report["run_id"]


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def test_summary_reflects_ingested_counts():
    client.post("/api/v1/ingest/primary", json=[_primary_payload("SUM-TX-001")])
    r = client.get("/api/v1/summary")
    assert r.status_code == 200
    data = r.json()
    assert data["primary_count"] == 1
    assert data["secondary_count"] == 0


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

def test_reset_clears_ledger():
    client.post("/api/v1/ingest/primary", json=[_primary_payload("RST-TX-001")])
    r = client.delete("/api/v1/reset")
    assert r.status_code == 204
    r = client.get("/api/v1/summary")
    assert r.json()["primary_count"] == 0

