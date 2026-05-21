"""
Tests for ReconcilerEngine.

Covers: clean matches, all discrepancy types, duplicate detection,
multi-discrepancy records, custom thresholds, and state management.
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone
from decimal import Decimal

from src.delta_core.models import (
    DiscrepancyType,
    TransactionRecord,
)
from src.delta_core.reconciler import ReconcilerEngine


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _record(
    tx_id: str,
    *,
    source_system: str = "PAYMENTS-PROC",
    amount: Decimal = Decimal("100.00"),
    target_system: str = "CORE-BANKING",
    timestamp: datetime | None = None,
) -> TransactionRecord:
    return TransactionRecord(
        transaction_id=tx_id,
        account_token="TOK_ABCD1234",
        amount=amount,
        timestamp=timestamp or datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        source_system=source_system,
        target_system=target_system,
    )


@pytest.fixture()
def engine():
    e = ReconcilerEngine(primary_system="STREAM", secondary_system="BATCH")
    yield e
    e.reset()


# ---------------------------------------------------------------------------
# Clean match
# ---------------------------------------------------------------------------

def test_clean_match_produces_no_discrepancies(engine):
    engine.ingest_primary(_record("TX001"))
    engine.ingest_secondary(_record("TX001"))
    report = engine.reconcile()
    assert report.matched_count == 1
    assert report.discrepancy_count == 0
    assert report.discrepancies == []


def test_report_counts_reflect_ingested_totals(engine):
    for i in range(3):
        engine.ingest_primary(_record(f"TX{i:03d}"))
        engine.ingest_secondary(_record(f"TX{i:03d}"))
    report = engine.reconcile()
    assert report.primary_count == 3
    assert report.secondary_count == 3
    assert report.matched_count == 3


def test_report_includes_system_names(engine):
    report = engine.reconcile()
    assert report.primary_system == "STREAM"
    assert report.secondary_system == "BATCH"


# ---------------------------------------------------------------------------
# Amount mismatch
# ---------------------------------------------------------------------------

def test_amount_mismatch_detected(engine):
    engine.ingest_primary(_record("TX002", amount=Decimal("100.00")))
    engine.ingest_secondary(_record("TX002", amount=Decimal("100.01")))
    report = engine.reconcile()
    assert report.discrepancy_count == 1
    assert report.discrepancies[0].discrepancy_type == DiscrepancyType.AMOUNT_MISMATCH


def test_amount_mismatch_carries_delta_detail(engine):
    engine.ingest_primary(_record("TX003", amount=Decimal("50.00")))
    engine.ingest_secondary(_record("TX003", amount=Decimal("49.99")))
    report = engine.reconcile()
    d = report.discrepancies[0]
    assert "0.01" in d.detail


# ---------------------------------------------------------------------------
# Missing records
# ---------------------------------------------------------------------------

def test_missing_from_primary(engine):
    engine.ingest_secondary(_record("TX004"))
    report = engine.reconcile()
    assert report.discrepancy_count == 1
    assert report.discrepancies[0].discrepancy_type == DiscrepancyType.MISSING_FROM_PRIMARY


def test_missing_from_secondary(engine):
    engine.ingest_primary(_record("TX005"))
    report = engine.reconcile()
    assert report.discrepancy_count == 1
    assert report.discrepancies[0].discrepancy_type == DiscrepancyType.MISSING_FROM_SECONDARY


def test_missing_record_carries_payload(engine):
    engine.ingest_primary(_record("TX006"))
    report = engine.reconcile()
    d = report.discrepancies[0]
    assert d.primary_value is not None
    assert d.primary_value["transaction_id"] == "TX006"


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def test_duplicate_primary_returns_discrepancy(engine):
    first = engine.ingest_primary(_record("TX007"))
    second = engine.ingest_primary(_record("TX007"))
    assert first is None
    assert second is not None
    assert second.discrepancy_type == DiscrepancyType.DUPLICATE_IN_PRIMARY


def test_duplicate_secondary_returns_discrepancy(engine):
    engine.ingest_secondary(_record("TX008"))
    d = engine.ingest_secondary(_record("TX008"))
    assert d.discrepancy_type == DiscrepancyType.DUPLICATE_IN_SECONDARY


def test_duplicate_does_not_overwrite_original(engine):
    original = _record("TX009", amount=Decimal("100.00"))
    duplicate = _record("TX009", amount=Decimal("999.99"))
    engine.ingest_primary(original)
    engine.ingest_primary(duplicate)
    engine.ingest_secondary(_record("TX009", amount=Decimal("100.00")))
    report = engine.reconcile()
    amount_mismatches = [
        d for d in report.discrepancies
        if d.discrepancy_type == DiscrepancyType.AMOUNT_MISMATCH
    ]
    assert len(amount_mismatches) == 0


# ---------------------------------------------------------------------------
# Topology routing — composite key: transactionId + sourceSystem + targetSystem
# ---------------------------------------------------------------------------

def test_topology_key_must_match_for_pairing(engine):
    """Different targetSystem = different composite key = no candidate row found."""
    engine.ingest_primary(_record("TX010", target_system="CORE-BANKING"))
    engine.ingest_secondary(_record("TX010", target_system="ACH-RAIL"))
    report = engine.reconcile()
    types = {d.discrepancy_type for d in report.discrepancies}
    assert DiscrepancyType.MISSING_FROM_PRIMARY in types
    assert DiscrepancyType.MISSING_FROM_SECONDARY in types
    assert report.matched_count == 0


# ---------------------------------------------------------------------------
# Amount is the sole parity field
# ---------------------------------------------------------------------------

def test_only_amount_mismatch_flagged_when_other_fields_differ(engine):
    """After topology key match, amount is the only field evaluated for parity."""
    engine.ingest_primary(_record("TX014", amount=Decimal("50.00")))
    engine.ingest_secondary(_record("TX014", amount=Decimal("50.01")))
    report = engine.reconcile()
    assert report.discrepancy_count == 1
    assert report.discrepancies[0].discrepancy_type == DiscrepancyType.AMOUNT_MISMATCH


# ---------------------------------------------------------------------------
# Unknown source system
# ---------------------------------------------------------------------------

# Removed: routing is now explicit (ingest_primary / ingest_secondary).
# There is no unknown-system code path in the engine.

# ---------------------------------------------------------------------------
# Custom thresholds
# ---------------------------------------------------------------------------

def test_custom_amount_tolerance_allows_small_delta():
    tolerant_engine = ReconcilerEngine(
        primary_system="STREAM",
        secondary_system="BATCH",
        amount_tolerance=Decimal("0.01"),
    )
    tolerant_engine.ingest_primary(_record("TX016", amount=Decimal("100.00")))
    tolerant_engine.ingest_secondary(_record("TX016", amount=Decimal("100.01")))
    report = tolerant_engine.reconcile()
    assert report.matched_count == 1
    assert report.discrepancy_count == 0


# ---------------------------------------------------------------------------
# Report metadata
# ---------------------------------------------------------------------------

def test_report_has_unique_run_id_per_call(engine):
    engine.ingest_primary(_record("TX018"))
    engine.ingest_secondary(_record("TX018"))
    r1 = engine.reconcile()
    r2 = engine.reconcile()
    assert r1.run_id != r2.run_id


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def test_reset_clears_all_state(engine):
    engine.ingest_primary(_record("TX019"))
    engine.ingest_secondary(_record("TX019"))
    engine.reset()
    assert engine.primary_count == 0
    assert engine.secondary_count == 0
    report = engine.reconcile()
    assert report.matched_count == 0
    assert report.discrepancy_count == 0


def test_ingest_many_returns_only_duplicates(engine):
    records = [
        _record("TX020"),
        _record("TX021"),
        _record("TX020"),  # duplicate composite key
    ]
    dupes = engine.ingest_many_primary(records)
    assert len(dupes) == 1
    assert dupes[0].transaction_id == "TX020"
    assert engine.primary_count == 2

