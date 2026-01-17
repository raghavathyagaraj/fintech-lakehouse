# ============================================================
# STEP 1.2.1: Create DEV config
# ============================================================
"""
Development Environment Configuration
=====================================

Why separate DEV config?
- Smaller clusters (cost savings)
- Test data paths
- Less strict DQ (allow failures for testing)
- Different alert channels
"""

from dataclasses import dataclass, field
from typing import Optional
from .base import BaseConfig, BusinessRules, DataQualityThresholds


@dataclass
class DevConfig(BaseConfig):
    """Development environment configuration."""

    # Environment identifier
    ENVIRONMENT: str = "dev"

    # Database names (with _dev suffix)
    BRONZE_DB: str = "fintech_bronze_dev"
    SILVER_DB: str = "fintech_silver_dev"
    GOLD_DB: str = "fintech_gold_dev"
    QUARANTINE_DB: str = "fintech_quarantine_dev"

    # Paths (Databricks FileStore for Community Edition)
    BASE_PATH: str = "/FileStore/fintech_lakehouse_dev"
    RAW_PATH: str = "/FileStore/fintech_lakehouse_dev/raw"
    BRONZE_PATH: str = "/FileStore/fintech_lakehouse_dev/bronze"
    SILVER_PATH: str = "/FileStore/fintech_lakehouse_dev/silver"
    GOLD_PATH: str = "/FileStore/fintech_lakehouse_dev/gold"
    QUARANTINE_PATH: str = "/FileStore/fintech_lakehouse_dev/quarantine"
    CHECKPOINT_PATH: str = "/FileStore/fintech_lakehouse_dev/checkpoints"

    # Compute settings (smaller for DEV)
    MAX_WORKERS: int = 2
    DRIVER_NODE_TYPE: str = "Standard_DS3_v2"
    WORKER_NODE_TYPE: str = "Standard_DS3_v2"

    # Data Quality settings
    DQ_FAIL_ON_CRITICAL: bool = False  # Don't fail pipeline in DEV
    DQ_LOG_WARNINGS: bool = True

    # Alerting (DEV channel)
    SLACK_WEBHOOK_URL: Optional[str] = None  # Set via environment variable
    ALERT_CHANNEL: str = "#data-alerts-dev"
    ENABLE_PAGERDUTY: bool = False  # No paging in DEV

    # Logging
    LOG_LEVEL: str = "DEBUG"

    # Processing
    BATCH_SIZE: int = 10000  # Smaller batches for testing
    ENABLE_OPTIMIZATION: bool = False  # Skip OPTIMIZE in DEV

    def get_table_path(self, layer: str, table: str) -> str:
        """Get full path for a table."""
        layer_paths = {
            "bronze": self.BRONZE_PATH,
            "silver": self.SILVER_PATH,
            "gold": self.GOLD_PATH,
            "quarantine": self.QUARANTINE_PATH,
        }
        return f"{layer_paths[layer]}/{table}"

    def get_database(self, layer: str) -> str:
        """Get database name for a layer."""
        databases = {
            "bronze": self.BRONZE_DB,
            "silver": self.SILVER_DB,
            "gold": self.GOLD_DB,
            "quarantine": self.QUARANTINE_DB,
        }
        return databases[layer]
