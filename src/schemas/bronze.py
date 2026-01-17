"""
Bronze Layer Schemas
====================

Why all strings in Bronze?
- Source data can have quality issues
- Type inference can fail or be inconsistent
- We preserve raw data exactly as received
- Type casting happens in Silver layer
"""

from pyspark.sql.types import StructType, StructField, StringType


# All columns as STRING - preserves raw data
TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("amount_usd", StringType(), True),
        StructField("fee_amount", StringType(), True),
        StructField("net_amount", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("merchant_country", StringType(), True),
        StructField("customer_country", StringType(), True),
        StructField("is_cross_border", StringType(), True),
        StructField("is_flagged", StringType(), True),
        StructField("fraud_indicators", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)


CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("kyc_verified_date", StringType(), True),
        StructField("risk_score", StringType(), True),
        StructField("is_pep", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)


MERCHANTS_SCHEMA = StructType(
    [
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("mcc_code", StringType(), True),
        StructField("mcc_category", StringType(), True),
        StructField("country", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("risk_tier", StringType(), True),
        StructField("avg_ticket_size", StringType(), True),
        StructField("monthly_volume", StringType(), True),
        StructField("onboarding_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("fee_rate", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)


EXCHANGE_RATES_SCHEMA = StructType(
    [
        StructField("rate_date", StringType(), True),
        StructField("source_currency", StringType(), True),
        StructField("target_currency", StringType(), True),
        StructField("exchange_rate", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)
