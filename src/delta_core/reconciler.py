from __future__ import annotations

from decimal import Decimal
from typing import Optional

from .models import (
    DiscrepancyRecord,
    DiscrepancyType,
    ReconciliationReport,
    TransactionRecord,
)


# ---------------------------------------------------------------------------
# Configurable thresholds — overridden via Settings in production
# ---------------------------------------------------------------------------
_DEFAULT_AMOUNT_TOLERANCE: Decimal = Decimal("0.00")

# Composite ledger key: a record is a candidate for parity comparison only when
# all three fields are identical on both sides.  sourceSystem and targetSystem
# are treated as topology identifiers — they must agree for a row to be found.
_LedgerKey = tuple[str, str, str]  # (transaction_id, source_system, target_system)


class ReconcilerEngine:
    """
    Generic multi-system transaction routing and matching fabric.

    Routing
    -------
    Callers explicitly designate which ledger a record belongs to by using
    ``ingest_primary`` or ``ingest_secondary``.  ``sourceSystem`` and
    ``targetSystem`` are pure business-topology fields shared by both sides
    of the same transaction — they are NOT used to determine which ledger
    a record enters.

    Matching algorithm
    ------------------
    1. Locate candidate row — search the store for a primary record where
       the composite key equals that of the incoming secondary record::

           transactionId (secondary) == transactionId (primary)
           sourceSystem  (secondary) == sourceSystem  (primary)
           targetSystem  (secondary) == targetSystem  (primary)

    2. Evaluate parity — if no row matches all three keys, flag as
       ``MISSING_FROM_PRIMARY``.  If a row is found, compare ``amount``.
       A perfect match purges the row from both ledgers; an amount delta
       flags ``AMOUNT_MISMATCH`` and ships ``auxiliaryMetadata`` to the
       Discrepancy Queue payload.

    Constraints
    -----------
    - No PII, account state, or running balances are retained.
    - ``auxiliary_metadata`` is never read by matching logic; it is
      forwarded opaquely to discrepancy alert payloads only.
    - Amount comparison uses ``Decimal`` arithmetic; tolerance is zero by
      default to enforce absolute financial fidelity.
    """

    def __init__(
        self,
        primary_system: str,
        secondary_system: str,
        amount_tolerance: Decimal = _DEFAULT_AMOUNT_TOLERANCE,
    ) -> None:
        self._primary_system = primary_system
        self._secondary_system = secondary_system
        self._amount_tolerance = amount_tolerance

        # Keyed by (transaction_id, source_system, target_system) for O(1) lookup.
        # Both sides of the same business transaction share identical key values.
        self._primary_ledger: dict[_LedgerKey, TransactionRecord] = {}
        self._secondary_ledger: dict[_LedgerKey, TransactionRecord] = {}

        # Discrepancies surfaced at ingestion time (duplicates)
        self._ingestion_discrepancies: list[DiscrepancyRecord] = []

    # ------------------------------------------------------------------
    # Ingestion API
    # ------------------------------------------------------------------

    @staticmethod
    def _make_key(record: TransactionRecord) -> _LedgerKey:
        """Derive the composite lookup key from a record's topology fields."""
        return (record.transaction_id, record.source_system, record.target_system)

    def ingest_primary(self, record: TransactionRecord) -> Optional[DiscrepancyRecord]:
        """Add *record* to the primary ledger.

        Returns a ``DUPLICATE_IN_PRIMARY`` :class:`DiscrepancyRecord` immediately
        if the composite key ``(transactionId, sourceSystem, targetSystem)`` already
        exists in the primary ledger; otherwise returns ``None``.
        """
        return self._ingest_to_ledger(
            record, self._primary_ledger, DiscrepancyType.DUPLICATE_IN_PRIMARY,
            self._primary_system,
        )

    def ingest_secondary(self, record: TransactionRecord) -> Optional[DiscrepancyRecord]:
        """Add *record* to the secondary ledger.

        Returns a ``DUPLICATE_IN_SECONDARY`` :class:`DiscrepancyRecord` immediately
        if the composite key already exists in the secondary ledger; otherwise ``None``.
        """
        return self._ingest_to_ledger(
            record, self._secondary_ledger, DiscrepancyType.DUPLICATE_IN_SECONDARY,
            self._secondary_system,
        )

    def ingest_many_primary(
        self, records: list[TransactionRecord]
    ) -> list[DiscrepancyRecord]:
        """Ingest a batch of records into the primary ledger; returns duplicate discrepancies."""
        duplicates: list[DiscrepancyRecord] = []
        for record in records:
            result = self.ingest_primary(record)
            if result is not None:
                duplicates.append(result)
        return duplicates

    def ingest_many_secondary(
        self, records: list[TransactionRecord]
    ) -> list[DiscrepancyRecord]:
        """Ingest a batch of records into the secondary ledger; returns duplicate discrepancies."""
        duplicates: list[DiscrepancyRecord] = []
        for record in records:
            result = self.ingest_secondary(record)
            if result is not None:
                duplicates.append(result)
        return duplicates

    def _ingest_to_ledger(
        self,
        record: TransactionRecord,
        ledger: dict[_LedgerKey, TransactionRecord],
        duplicate_type: DiscrepancyType,
        ledger_label: str,
    ) -> Optional[DiscrepancyRecord]:
        key = self._make_key(record)
        if key in ledger:
            discrepancy = DiscrepancyRecord(
                transaction_id=record.transaction_id,
                discrepancy_type=duplicate_type,
                auxiliary_metadata_blob=record.auxiliary_metadata,
                detail=(
                    f"Composite key (transactionId={record.transaction_id!r}, "
                    f"sourceSystem={record.source_system!r}, "
                    f"targetSystem={record.target_system!r}) already exists "
                    f"in the {ledger_label} ledger."
                ),
            )
            self._ingestion_discrepancies.append(discrepancy)
            return discrepancy
        ledger[key] = record
        return None

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    def reconcile(self) -> ReconciliationReport:
        """
        Execute a full reconciliation pass over all ingested records.

        Matching strategy and flow
        -----------------
        1. Walk every secondary record; look up the primary ledger for a row
           whose composite key ``(transactionId, sourceSystem, targetSystem)``
           is identical.  No match → ``MISSING_FROM_PRIMARY``.
        2. On a key hit, compare ``amount`` only (the sole parity field).
           Perfect match → purge both rows from the store.
           Amount delta   → ``AMOUNT_MISMATCH`` + ship ``auxiliaryMetadata`` blob.
        3. Primary records never matched by a secondary row → ``MISSING_FROM_SECONDARY``.
        """
        run_discrepancies: list[DiscrepancyRecord] = list(self._ingestion_discrepancies)
        matched = 0
        matched_keys: set[_LedgerKey] = set()
        seen_keys: set[_LedgerKey] = set()

        # Pass 1: Walk secondary ledger, look up candidate primary row
        for key, secondary_record in self._secondary_ledger.items():
            seen_keys.add(key)
            tx_id, src_sys, tgt_sys = key

            if key not in self._primary_ledger:
                run_discrepancies.append(
                    DiscrepancyRecord(
                        transaction_id=tx_id,
                        discrepancy_type=DiscrepancyType.MISSING_FROM_PRIMARY,
                        secondary_value=secondary_record.model_dump(mode="json"),
                        auxiliary_metadata_blob=secondary_record.auxiliary_metadata,
                        detail=(
                            f"No primary record for "
                            f"(transactionId={tx_id!r}, sourceSystem={src_sys!r}, "
                            f"targetSystem={tgt_sys!r})."
                        ),
                    )
                )
                continue

            field_deltas = self._compare(self._primary_ledger[key], secondary_record)
            if field_deltas:
                run_discrepancies.extend(field_deltas)
            else:
                matched += 1
                matched_keys.add(key)

        # Pass 2: Primary records never matched by a secondary row
        for key, primary_record in self._primary_ledger.items():
            if key not in seen_keys:
                tx_id, src_sys, tgt_sys = key
                run_discrepancies.append(
                    DiscrepancyRecord(
                        transaction_id=tx_id,
                        discrepancy_type=DiscrepancyType.MISSING_FROM_SECONDARY,
                        primary_value=primary_record.model_dump(mode="json"),
                        auxiliary_metadata_blob=primary_record.auxiliary_metadata,
                        detail=(
                            f"No secondary record for "
                            f"(transactionId={tx_id!r}, sourceSystem={src_sys!r}, "
                            f"targetSystem={tgt_sys!r})."
                        ),
                    )
                )

        report = ReconciliationReport(
            primary_system=self._primary_system,
            secondary_system=self._secondary_system,
            primary_count=len(self._primary_ledger),
            secondary_count=len(self._secondary_ledger),
            matched_count=matched,
            discrepancy_count=len(run_discrepancies),
            discrepancies=run_discrepancies,
        )

        # Hard-delete perfectly matched records — both sides purged atomically.
        # Discrepant records are retained in the ledger for the queue consumer.
        for key in matched_keys:
            self._primary_ledger.pop(key, None)
            self._secondary_ledger.pop(key, None)

        return report

    def _compare(
        self, primary: TransactionRecord, secondary: TransactionRecord
    ) -> list[DiscrepancyRecord]:
        """
        Parity check: candidate row already located via composite key.
        Validates strictly on ``amount``.  No other field is read.
        ``auxiliary_metadata`` is forwarded opaquely to the alert payload.
        """
        deltas: list[DiscrepancyRecord] = []

        amount_delta = abs(primary.amount - secondary.amount)
        if amount_delta > self._amount_tolerance:
            deltas.append(
                DiscrepancyRecord(
                    transaction_id=primary.transaction_id,
                    discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
                    primary_value=str(primary.amount),
                    secondary_value=str(secondary.amount),
                    auxiliary_metadata_blob=primary.auxiliary_metadata,
                    detail=f"Amount delta: {amount_delta}",
                )
            )

        return deltas

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear all ledger state and discrepancies between reconciliation cycles."""
        self._primary_ledger.clear()
        self._secondary_ledger.clear()
        self._ingestion_discrepancies.clear()

    @property
    def primary_count(self) -> int:
        return len(self._primary_ledger)

    @property
    def secondary_count(self) -> int:
        return len(self._secondary_ledger)

