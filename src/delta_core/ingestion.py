from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Union

from pydantic import ValidationError

from .models import TransactionRecord


def _parse_row(row: dict) -> TransactionRecord:
    """
    Parse a raw dict into a validated TransactionRecord.

    Accepts both camelCase keys (e.g. ``transactionId``) and snake_case keys
    (e.g. ``transaction_id``) because Pydantic's alias generator handles both
    when ``populate_by_name=True`` is set on the model.

    Raises
    ------
    ValueError
        If a required field is missing, has an invalid format, or fails model
        validation (including PAN-pattern rejection).
    """
    try:
        return TransactionRecord.model_validate(row)
    except ValidationError as exc:
        tx_id = row.get("transactionId") or row.get("transaction_id", "UNKNOWN")
        raise ValueError(
            f"Invalid transaction record for transactionId='{tx_id}': {exc}"
        ) from exc


def load_json(filepath: Union[str, Path]) -> list[TransactionRecord]:
    """Load transactions from a JSON file (top-level array of transaction objects)."""
    path = Path(filepath)
    with path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise ValueError(f"Expected a top-level JSON array in {filepath}")
    return [_parse_row(row) for row in records]


def load_csv(filepath: Union[str, Path]) -> list[TransactionRecord]:
    """Load transactions from a CSV file (header row required, camelCase column names)."""
    path = Path(filepath)
    records: list[TransactionRecord] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(_parse_row(dict(row)))
    return records


# Convenience aliases for callers migrating from the previous API
load_stream_json = load_json
load_batch_json = load_json
load_batch_csv = load_csv
