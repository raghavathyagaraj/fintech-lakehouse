"""
Base Configuration
==================
Shared configuration across all environments.

Why base config?
- Business rules that don't change between DEV/PROD
- DRY principle - don't repeat yourself
"""

from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class BusinessRules:
    """Business rules that apply everywhere."""

    # Transaction validation
    MIN_TRANSACTION_AMOUNT: float = 0.01
    MAX_TRANSACTION_AMOUNT: float = 100_000.00

    # Valid enum values
    VALID_TRANSACTION_TYPES: List[str] = field(
        default_factory=lambda: ["PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL"]
    )
    VALID_PAYMENT_METHODS: List[str] = field(
        default_factory=lambda: [
            "CREDIT_CARD",
            "DEBIT_CARD",
            "DIGITAL_WALLET",
            "BANK_TRANSFER",
            "CRYPTO",
        ]
    )
    VALID_CHANNELS: List[str] = field(default_factory=lambda: ["WEB", "MOBILE_APP", "POS", "ATM"])
    VALID_STATUSES: List[str] = field(
        default_factory=lambda: ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
    )
    VALID_KYC_STATUSES: List[str] = field(
        default_factory=lambda: ["VERIFIED", "PENDING", "REJECTED", "EXPIRED"]
    )
    VALID_SEGMENTS: List[str] = field(
        default_factory=lambda: ["HIGH_VALUE", "REGULAR", "OCCASIONAL", "NEW", "CHURNING"]
    )
    VALID_RISK_TIERS: List[str] = field(default_factory=lambda: ["LOW", "MEDIUM", "HIGH"])

    # AML thresholds (regulatory)
    CTR_THRESHOLD: float = 10_000.00  # Currency Transaction Report
    STRUCTURING_THRESHOLD: float = 9_000.00  # Suspicious structuring


@dataclass
class DataQualityThresholds:
    """Data quality thresholds."""

    COMPLETENESS_THRESHOLD: float = 0.95  # 95% non-null
    UNIQUENESS_THRESHOLD: float = 1.00  # 100% unique PKs
    FRESHNESS_HOURS: int = 24  # Max hours data can be stale
    VOLUME_CHANGE_THRESHOLD: float = 0.50  # 50% volume change = anomaly


@dataclass
class BaseConfig:
    """Base configuration inherited by all environments."""

    # Database naming convention
    DATABASE_PREFIX: str = "fintech"

    # Layer suffixes
    BRONZE_SUFFIX: str = "bronze"
    SILVER_SUFFIX: str = "silver"
    GOLD_SUFFIX: str = "gold"
    QUARANTINE_SUFFIX: str = "quarantine"

    # Business rules
    business_rules: BusinessRules = field(default_factory=BusinessRules)

    # DQ thresholds
    dq_thresholds: DataQualityThresholds = field(default_factory=DataQualityThresholds)

    # Tables
    TABLES: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "bronze": ["transactions", "customers", "merchants", "exchange_rates"],
            "silver": ["transactions", "customers", "merchants"],
            "gold": [
                "dim_date",
                "dim_customer",
                "dim_merchant",
                "fact_transactions",
                "agg_daily_metrics",
                "agg_customer_360",
                "agg_merchant_performance",
            ],
        }
    )
