"""Configuration management for Drift."""

import tomllib
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSettings(BaseSettings):
    """Settings for Drift's own storage database."""

    model_config = SettingsConfigDict(env_prefix="DRIFT_STORAGE_")

    dsn: str = "postgresql+psycopg://drift:drift@localhost:5433/drift"


class CollectorSettings(BaseSettings):
    """Settings for the stats collector."""

    model_config = SettingsConfigDict(env_prefix="DRIFT_COLLECTOR_")

    interval_seconds: int = 60
    snapshot_timeout_seconds: int = 10


class RetentionSettings(BaseSettings):
    """Settings for data retention."""

    model_config = SettingsConfigDict(env_prefix="DRIFT_RETENTION_")

    raw_days: int = 7
    hourly_days: int = 90


class DriftConfig(BaseSettings):
    """Main configuration for Drift."""

    model_config = SettingsConfigDict(env_prefix="DRIFT_")

    storage: StorageSettings = Field(default_factory=StorageSettings)
    collector: CollectorSettings = Field(default_factory=CollectorSettings)
    retention: RetentionSettings = Field(default_factory=RetentionSettings)


def load_config(config_path: Path | None = None) -> DriftConfig:
    """Load configuration from file and/or environment variables.

    Priority (highest to lowest):
    1. Environment variables
    2. Config file (drift.toml)
    3. Default values
    """
    config_data: dict = {}

    # Try to load from file
    if config_path is None:
        config_path = Path("drift.toml")

    if config_path.exists():
        with open(config_path, "rb") as f:
            config_data = tomllib.load(f)

    # Build nested settings from file data
    storage = StorageSettings(**config_data.get("storage", {}))
    collector = CollectorSettings(**config_data.get("collector", {}))
    retention = RetentionSettings(**config_data.get("retention", {}))

    return DriftConfig(storage=storage, collector=collector, retention=retention)
