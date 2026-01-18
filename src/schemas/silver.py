"""
Silver Layer Schemas
====================

Why typed schemas in Silver?
- Data has been validated and cleansed
- Proper types enable correct operations (sum decimals, filter dates)
- NOT NULL constraints enforce data quality
- Schema documentation for data catalog
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType,
    DateType,
    BooleanType,
    IntegerType,
    ArrayType,
)


TRANSACTIONS_SCHEMA = StructType(
    [
        # Primary key
        StructField("transaction_id", StringType(), False),  # NOT NULL
        # Foreign keys
        StructField("customer_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        # Money fields - Decimal for precision
        StructField("amount", DecimalType(18, 2), False),
        StructField("currency", StringType(), False),
        StructField("amount_usd", DecimalType(18, 2), True),
        StructField("fee_amount", DecimalType(18, 2), True),
        StructField("net_amount", DecimalType(18, 2), True),
        # Categorical fields
        StructField("transaction_type", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("status", StringType(), False),
        # Geographic
        StructField("merchant_country", StringType(), True),
        StructField("customer_country", StringType(), True),
        StructField("is_cross_border", BooleanType(), True),
        # Fraud
        StructField("is_flagged", BooleanType(), False),
        StructField("fraud_indicators", ArrayType(StringType()), True),
        # Device info
        StructField("ip_address", StringType(), True),
        StructField("device_id", StringType(), True),
        # Timestamps
        StructField("transaction_timestamp", TimestampType(), False),
        StructField("created_at", TimestampType(), True),
        # Derived columns
        StructField("transaction_date", DateType(), False),
        StructField("transaction_hour", IntegerType(), True),
        StructField("transaction_day_of_week", IntegerType(), True),
        # Metadata
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_timestamp", TimestampType(), True),
        StructField("_processing_timestamp", TimestampType(), True),
    ]
)


CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("age", IntegerType(), True),
        StructField("segment", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("kyc_verified_date", DateType(), True),
        StructField("risk_score", IntegerType(), True),
        StructField("is_pep", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_timestamp", TimestampType(), True),
        StructField("_processing_timestamp", TimestampType(), True),
    ]
)


MERCHANTS_SCHEMA = StructType(
    [
        StructField("merchant_id", StringType(), False),
        StructField("merchant_name", StringType(), True),
        StructField("mcc_code", StringType(), True),
        StructField("mcc_category", StringType(), True),
        StructField("country", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("risk_tier", StringType(), True),
        StructField("avg_ticket_size", DecimalType(18, 2), True),
        StructField("monthly_volume", IntegerType(), True),
        StructField("onboarding_date", DateType(), True),
        StructField("days_active", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("fee_rate", DecimalType(8, 4), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_timestamp", TimestampType(), True),
        StructField("_processing_timestamp", TimestampType(), True),
    ]
)
