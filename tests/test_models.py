"""
Tests for TransactionRecord and ancillary models.

Focus: field validation, PAN-pattern rejection, and Decimal precision rules
that enforce data integrity at the system boundary.
"""
from __future__ import annotations

import pytest
from decimal import Decimal
from datetime import datetime, timezone

from pydantic import ValidationError

from src.delta_core.models import (
    DiscrepancyRecord,
    DiscrepancyType,
    ReconciliationReport,
    TransactionRecord,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_payload(**overrides) -> dict:
    base = {
        "transaction_id": "TXN-2024-001",
        "account_token": "TOK_A1B2C3D4",
        "amount": Decimal("45.12"),
        "timestamp": datetime(2024, 1, 15, 12, 0, 0),
        "source_system": "KAFKA-STREAM",
        "target_system": "CORE-BANKING",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Happy-path
# ---------------------------------------------------------------------------

def test_valid_transaction_parses():
    record = TransactionRecord(**_valid_payload())
    assert record.transaction_id == "TXN-2024-001"
    assert record.amount == Decimal("45.12")
    assert record.source_system == "KAFKA-STREAM"
    assert record.target_system == "CORE-BANKING"


def test_auxiliary_metadata_accepted():
    record = TransactionRecord(**_valid_payload(auxiliary_metadata={"channel": "POS", "region": "US-EAST"}))
    assert record.auxiliary_metadata["channel"] == "POS"


def test_auxiliary_metadata_defaults_none():
    record = TransactionRecord(**_valid_payload())
    assert record.auxiliary_metadata is None


def test_naive_timestamp_normalized_to_utc():
    record = TransactionRecord(**_valid_payload(timestamp=datetime(2024, 1, 15, 12, 0, 0)))
    assert record.timestamp.tzinfo is not None
    assert record.timestamp.tzinfo == timezone.utc


def test_tz_aware_timestamp_converted_to_utc():
    from datetime import timezone as tz, timedelta

    eastern = tz(timedelta(hours=-5))
    ts = datetime(2024, 1, 15, 7, 0, 0, tzinfo=eastern)
    record = TransactionRecord(**_valid_payload(timestamp=ts))
    assert record.timestamp.hour == 12  # 07:00 EST == 12:00 UTC


def test_camelcase_keys_accepted():
    """JSON payloads use camelCase aliases; Pydantic must accept them."""
    record = TransactionRecord.model_validate({
        "transactionId": "TXN-CAMEL-001",
        "accountToken": "TOK_CAMEL123",
        "amount": "99.99",
        "timestamp": "2024-01-15T12:00:00",
        "sourceSystem": "KAFKA-STREAM",
        "targetSystem": "CORE-BANKING",
    })
    assert record.transaction_id == "TXN-CAMEL-001"
    assert record.source_system == "KAFKA-STREAM"


# ---------------------------------------------------------------------------
# PAN / PII rejection
# ---------------------------------------------------------------------------

def test_rejects_pan_in_account_token():
    with pytest.raises(ValidationError, match="accountToken"):
        TransactionRecord(**_valid_payload(account_token="4111111111111111"))


def test_rejects_pan_in_transaction_id():
    with pytest.raises(ValidationError):
        TransactionRecord(**_valid_payload(transaction_id="4111111111111111"))


def test_rejects_short_token():
    with pytest.raises(ValidationError, match="accountToken"):
        TransactionRecord(**_valid_payload(account_token="SHORT"))


# ---------------------------------------------------------------------------
# Amount validation
# ---------------------------------------------------------------------------

def test_rejects_negative_amount():
    with pytest.raises(ValidationError):
        TransactionRecord(**_valid_payload(amount=Decimal("-1.00")))


def test_rejects_zero_amount():
    with pytest.raises(ValidationError):
        TransactionRecord(**_valid_payload(amount=Decimal("0.00")))


def test_rejects_sub_cent_amount():
    with pytest.raises(ValidationError, match="decimal places"):
        TransactionRecord(**_valid_payload(amount=Decimal("45.123")))


def test_accepts_whole_dollar_amount():
    record = TransactionRecord(**_valid_payload(amount=Decimal("100")))
    assert record.amount == Decimal("100")


# ---------------------------------------------------------------------------
# DiscrepancyRecord and ReconciliationReport
# ---------------------------------------------------------------------------

def test_discrepancy_record_has_detected_at():
    d = DiscrepancyRecord(
        transaction_id="TX001",
        discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
    )
    assert d.detected_at is not None


def test_reconciliation_report_has_run_id():
    report = ReconciliationReport(
        primary_system="STREAM",
        secondary_system="BATCH",
        primary_count=1,
        secondary_count=1,
        matched_count=1,
        discrepancy_count=0,
        discrepancies=[],
    )
    assert report.run_id
    assert report.generated_at is not None
    assert report.primary_system == "STREAM"
    assert report.secondary_system == "BATCH"

