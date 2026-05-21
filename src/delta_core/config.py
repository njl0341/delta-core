from __future__ import annotations

from decimal import Decimal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DELTA_CORE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    app_name: str = "delta-core"
    app_version: str = "0.1.0"
    debug: bool = False

    # The two source systems being reconciled.
    # Records whose sourceSystem matches primary_system route to the primary ledger;
    # records matching secondary_system route to the secondary ledger.
    # Override via DELTA_CORE_PRIMARY_SYSTEM and DELTA_CORE_SECONDARY_SYSTEM.
    primary_system: str = "STREAM"
    secondary_system: str = "BATCH"

    # Amount comparison tolerance.  Zero enforces absolute financial fidelity.
    # Raise only with explicit business justification — "close enough" doesn't
    # exist in core banking.
    amount_tolerance: Decimal = Decimal("0.00")


settings = Settings()
