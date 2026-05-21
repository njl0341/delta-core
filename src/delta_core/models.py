from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel


# ---------------------------------------------------------------------------
# Security guards — reject any value that looks like a raw PAN or account number
# ---------------------------------------------------------------------------
_PAN_PATTERN = re.compile(r"\b\d{13,19}\b")
_TOKEN_PATTERN = re.compile(r"^[A-Z0-9_\-]{8,64}$")


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class DiscrepancyType(str, Enum):
    # Parity check — the only field-level comparison the engine performs
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    # Structural — record present in one ledger but absent from the other
    MISSING_FROM_PRIMARY = "MISSING_FROM_PRIMARY"
    MISSING_FROM_SECONDARY = "MISSING_FROM_SECONDARY"
    # Integrity — same transactionId ingested more than once on one side
    DUPLICATE_IN_PRIMARY = "DUPLICATE_IN_PRIMARY"
    DUPLICATE_IN_SECONDARY = "DUPLICATE_IN_SECONDARY"


# ---------------------------------------------------------------------------
# Core transaction record — the only object that flows through delta-core
# ---------------------------------------------------------------------------

class TransactionRecord(BaseModel):
    """
    Canonical transaction metadata payload.

    Uses camelCase aliases so JSON payloads match the spec exactly.
    Python code may use either snake_case field names or camelCase aliases
    (populate_by_name=True).

    Only tokenized/masked identifiers are accepted. PII, raw PANs, account
    balances, and cardholder state are explicitly out of scope.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )

    transaction_id: str = Field(
        ..., min_length=1, max_length=128,
        description="Unique transaction tracking identifier (primary key).",
    )
    amount: Decimal = Field(
        ..., gt=0,
        description="Exact financial value. Positive, max 2 decimal places.",
    )
    timestamp: datetime = Field(
        ...,
        description="ISO 8601 event timestamp. Normalized to UTC on ingestion.",
    )
    source_system: str = Field(
        ..., min_length=1, max_length=128,
        description="Origin system identifier (e.g. 'KAFKA-STREAM', 'MAINFRAME-BATCH').",
    )
    target_system: str = Field(
        ..., min_length=1, max_length=128,
        description="Destination system identifier (e.g. 'CORE-BANKING', 'ACH-RAIL').",
    )
    account_token: str = Field(
        ...,
        description="Masked or hashed account index key — NOT a raw PAN or account number.",
    )
    auxiliary_metadata: Optional[dict[str, str]] = Field(
        default=None,
        description="Optional pass-through data. Ignored by matching logic.",
    )

    @field_validator("account_token")
    @classmethod
    def validate_token_not_pan(cls, v: str) -> str:
        if _PAN_PATTERN.search(v):
            raise ValueError(
                "accountToken appears to contain a raw PAN or account number. "
                "Only masked or hashed identifiers are accepted."
            )
        if not _TOKEN_PATTERN.match(v.upper()):
            raise ValueError(
                "accountToken must be 8–64 characters (A–Z, 0–9, _, -)."
            )
        return v.upper()

    @field_validator("transaction_id")
    @classmethod
    def validate_transaction_id_no_pan(cls, v: str) -> str:
        if _PAN_PATTERN.search(v):
            raise ValueError("transactionId must not contain PAN-like numeric sequences.")
        return v

    @field_validator("amount")
    @classmethod
    def validate_amount_precision(cls, v: Decimal) -> Decimal:
        if v.as_tuple().exponent < -2:
            raise ValueError("amount must have at most 2 decimal places.")
        return v

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp_to_utc(cls, v: datetime) -> datetime:
        """Attach UTC if naive so all skew comparisons are timezone-aware."""
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Reconciliation output models
# ---------------------------------------------------------------------------

class DiscrepancyRecord(BaseModel):
    transaction_id: str
    discrepancy_type: DiscrepancyType
    primary_value: Optional[Any] = None
    secondary_value: Optional[Any] = None
    detected_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    detail: Optional[str] = None
    auxiliary_metadata_blob: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Exact, untouched auxiliaryMetadata blob from the discrepant record. "
            "Stored opaquely and never queried by matching logic. "
            "Forwarded as-is to the Discrepancy Queue alert payload."
        ),
    )


class ReconciliationReport(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    generated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    primary_system: str
    secondary_system: str
    primary_count: int
    secondary_count: int
    matched_count: int
    discrepancy_count: int
    discrepancies: list[DiscrepancyRecord]
