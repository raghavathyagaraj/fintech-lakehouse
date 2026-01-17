# ============================================================
# STEP 1.2.2: Create PROD config
# ============================================================

"""
Production Environment Configuration
====================================

Why separate PROD config?
- Larger clusters for real data volume
- Production data paths (S3/ADLS in real scenario)
- Strict DQ (fail on critical errors)
- Production alert channels + PagerDuty
"""

from dataclasses import dataclass
from typing import Optional
from .base import BaseConfig


@dataclass
class ProdConfig(BaseConfig):
    """Production environment configuration."""

    # Environment identifier
    ENVIRONMENT: str = "prod"

    # Database names (no suffix for prod)
    BRONZE_DB: str = "fintech_bronze"
    SILVER_DB: str = "fintech_silver"
    GOLD_DB: str = "fintech_gold"
    QUARANTINE_DB: str = "fintech_quarantine"

    # Paths (In real prod, this would be S3/ADLS)
    # For Databricks CE, we use FileStore
    BASE_PATH: str = "/FileStore/fintech_lakehouse"
    RAW_PATH: str = "/FileStore/fintech_lakehouse/raw"
    BRONZE_PATH: str = "/FileStore/fintech_lakehouse/bronze"
    SILVER_PATH: str = "/FileStore/fintech_lakehouse/silver"
    GOLD_PATH: str = "/FileStore/fintech_lakehouse/gold"
    QUARANTINE_PATH: str = "/FileStore/fintech_lakehouse/quarantine"
    CHECKPOINT_PATH: str = "/FileStore/fintech_lakehouse/checkpoints"

    # Compute settings (larger for PROD)
    MAX_WORKERS: int = 8
    DRIVER_NODE_TYPE: str = "Standard_DS4_v2"
    WORKER_NODE_TYPE: str = "Standard_DS4_v2"

    # Data Quality settings
    DQ_FAIL_ON_CRITICAL: bool = True  # FAIL pipeline on critical DQ issues
    DQ_LOG_WARNINGS: bool = True

    # Alerting (PROD channel)
    SLACK_WEBHOOK_URL: Optional[str] = None  # Set via environment variable
    ALERT_CHANNEL: str = "#data-alerts-prod"
    ENABLE_PAGERDUTY: bool = True  # Page on-call for P1
    PAGERDUTY_ROUTING_KEY: Optional[str] = None  # Set via environment variable

    # Logging
    LOG_LEVEL: str = "INFO"

    # Processing
    BATCH_SIZE: int = 100000  # Larger batches for efficiency
    ENABLE_OPTIMIZATION: bool = True  # Run OPTIMIZE and Z-ORDER

    # SLA settings
    PIPELINE_SLA_MINUTES: int = 60  # Pipeline must complete in 60 min
    DATA_FRESHNESS_SLA_HOURS: int = 4  # Data must be <4 hours old

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
