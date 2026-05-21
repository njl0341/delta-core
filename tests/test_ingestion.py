"""
Tests for ingestion helpers (load_json, load_csv).
"""
from __future__ import annotations

import csv
import json
import pytest
from decimal import Decimal
from pathlib import Path

from src.delta_core.ingestion import load_csv, load_json


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_ROWS = [
    {
        "transactionId": "TXN-001",
        "accountToken": "TOK_ABCD1234",
        "amount": "100.00",
        "timestamp": "2024-01-15T12:00:00",
        "sourceSystem": "KAFKA-STREAM",
        "targetSystem": "CORE-BANKING",
    },
    {
        "transactionId": "TXN-002",
        "accountToken": "TOK_EFGH5678",
        "amount": "250.75",
        "timestamp": "2024-01-15T12:05:00",
        "sourceSystem": "KAFKA-STREAM",
        "targetSystem": "CORE-BANKING",
    },
]


@pytest.fixture()
def json_file(tmp_path: Path) -> Path:
    f = tmp_path / "records.json"
    f.write_text(json.dumps(SAMPLE_ROWS), encoding="utf-8")
    return f


@pytest.fixture()
def csv_file(tmp_path: Path) -> Path:
    f = tmp_path / "records.csv"
    with f.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(SAMPLE_ROWS[0].keys()))
        writer.writeheader()
        writer.writerows(SAMPLE_ROWS)
    return f


# ---------------------------------------------------------------------------
# load_json
# ---------------------------------------------------------------------------

def test_load_json_count(json_file):
    records = load_json(json_file)
    assert len(records) == 2


def test_load_json_source_system(json_file):
    records = load_json(json_file)
    assert all(r.source_system == "KAFKA-STREAM" for r in records)


def test_load_json_amount_precision(json_file):
    records = load_json(json_file)
    assert records[0].amount == Decimal("100.00")
    assert records[1].amount == Decimal("250.75")


def test_load_json_target_system(json_file):
    records = load_json(json_file)
    assert all(r.target_system == "CORE-BANKING" for r in records)


# ---------------------------------------------------------------------------
# load_csv
# ---------------------------------------------------------------------------

def test_load_csv_count(csv_file):
    records = load_csv(csv_file)
    assert len(records) == 2


def test_load_csv_source_system(csv_file):
    records = load_csv(csv_file)
    assert all(r.source_system == "KAFKA-STREAM" for r in records)


def test_load_csv_amount(csv_file):
    records = load_csv(csv_file)
    assert records[0].amount == Decimal("100.00")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_raises_on_missing_required_field(tmp_path):
    bad = [{"transactionId": "BAD-001", "amount": "100.00"}]  # missing most fields
    f = tmp_path / "bad.json"
    f.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ValueError):
        load_json(f)


def test_raises_on_non_numeric_amount(tmp_path):
    bad = [{**SAMPLE_ROWS[0], "amount": "not-a-number"}]
    f = tmp_path / "bad_amount.json"
    f.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ValueError):
        load_json(f)


def test_raises_on_pan_in_token(tmp_path):
    bad = [{**SAMPLE_ROWS[0], "accountToken": "4111111111111111"}]
    f = tmp_path / "bad_pan.json"
    f.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ValueError):
        load_json(f)


def test_raises_on_non_array_json(tmp_path):
    f = tmp_path / "object.json"
    f.write_text(json.dumps({"transactionId": "TX001"}), encoding="utf-8")
    with pytest.raises(ValueError, match="JSON array"):
        load_json(f)
