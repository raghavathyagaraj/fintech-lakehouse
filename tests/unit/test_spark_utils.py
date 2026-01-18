"""
Unit Tests for Spark Utilities
==============================

Why test utilities?
- They're used everywhere — bugs here affect everything
- Complex logic that's easy to get wrong
- Prevents regressions when we make changes

How to run:
    pytest tests/unit/test_spark_utils.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Import functions to test
from src.utils.spark_utils import (
    add_ingestion_metadata,
    standardize_string_columns,
    deduplicate_by_key,
    add_surrogate_key,
)


@pytest.fixture(scope="module")
def spark():
    """
    Create a SparkSession for testing.

    Why scope="module"?
    - Creating SparkSession is slow
    - Reuse the same session for all tests in this file
    - Clean up after all tests complete
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("UnitTests")
        .config("spark.sql.shuffle.partitions", "1")  # Faster for small data
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestAddIngestionMetadata:
    """Tests for add_ingestion_metadata function."""

    def test_adds_source_file_column(self, spark):
        """Test that _source_file column is added."""
        # Arrange: Create test data
        data = [("TXN1", 100), ("TXN2", 200)]
        df = spark.createDataFrame(data, ["id", "amount"])

        # Act: Add metadata
        result = add_ingestion_metadata(df)

        # Assert: Check column exists
        assert "_source_file" in result.columns

    def test_adds_ingestion_timestamp_column(self, spark):
        """Test that _ingestion_timestamp column is added."""
        # Arrange
        data = [("TXN1", 100)]
        df = spark.createDataFrame(data, ["id", "amount"])

        # Act
        result = add_ingestion_metadata(df)

        # Assert
        assert "_ingestion_timestamp" in result.columns

    def test_preserves_original_columns(self, spark):
        """Test that original columns are preserved."""
        # Arrange
        data = [("TXN1", 100)]
        df = spark.createDataFrame(data, ["id", "amount"])
        original_columns = df.columns

        # Act
        result = add_ingestion_metadata(df)

        # Assert: Original columns still exist
        for col_name in original_columns:
            assert col_name in result.columns


class TestStandardizeStringColumns:
    """Tests for standardize_string_columns function."""

    def test_uppercase_conversion(self, spark):
        """Test that strings are converted to uppercase."""
        # Arrange
        data = [("usd",), ("Usd",), ("USD",)]
        df = spark.createDataFrame(data, ["currency"])

        # Act
        result = standardize_string_columns(df, ["currency"], case="upper")

        # Assert: All values should be "USD"
        values = [row.currency for row in result.collect()]
        assert all(v == "USD" for v in values)

    def test_lowercase_conversion(self, spark):
        """Test that strings are converted to lowercase."""
        # Arrange
        data = [("TEST@EMAIL.COM",)]
        df = spark.createDataFrame(data, ["email"])

        # Act
        result = standardize_string_columns(df, ["email"], case="lower")

        # Assert
        assert result.collect()[0].email == "test@email.com"

    def test_trim_whitespace(self, spark):
        """Test that whitespace is trimmed."""
        # Arrange
        data = [("  USD  ",), (" USD",), ("USD ")]
        df = spark.createDataFrame(data, ["currency"])

        # Act
        result = standardize_string_columns(df, ["currency"], case="upper", trim_whitespace=True)

        # Assert: All values should be "USD" with no spaces
        values = [row.currency for row in result.collect()]
        assert all(v == "USD" for v in values)

    def test_handles_missing_columns_gracefully(self, spark):
        """Test that missing columns don't cause errors."""
        # Arrange
        data = [("USD",)]
        df = spark.createDataFrame(data, ["currency"])

        # Act: Try to standardize a column that doesn't exist
        result = standardize_string_columns(df, ["currency", "non_existent_column"], case="upper")

        # Assert: Should not raise error, just skip missing column
        assert result.count() == 1


class TestDeduplicateByKey:
    """Tests for deduplicate_by_key function."""

    def test_removes_duplicates(self, spark):
        """Test that duplicates are removed."""
        # Arrange: Same transaction_id appears twice
        data = [
            ("TXN1", 100, 1),  # Older
            ("TXN1", 150, 2),  # Newer (higher timestamp)
            ("TXN2", 200, 1),
        ]
        df = spark.createDataFrame(data, ["txn_id", "amount", "timestamp"])

        # Act: Deduplicate, keeping the one with highest timestamp
        result = deduplicate_by_key(
            df,
            key_columns=["txn_id"],
            order_by_column="timestamp",
            order_ascending=False,  # Keep highest (newest)
        )

        # Assert
        assert result.count() == 2  # TXN1 and TXN2

        # TXN1 should have amount 150 (the newer record)
        txn1_amount = result.filter(col("txn_id") == "TXN1").collect()[0].amount
        assert txn1_amount == 150

    def test_keeps_first_when_ascending(self, spark):
        """Test that first record is kept when order_ascending=True."""
        # Arrange
        data = [
            ("TXN1", 100, 1),  # First
            ("TXN1", 150, 2),  # Second
        ]
        df = spark.createDataFrame(data, ["txn_id", "amount", "timestamp"])

        # Act: Keep the one with lowest timestamp (first)
        result = deduplicate_by_key(
            df,
            key_columns=["txn_id"],
            order_by_column="timestamp",
            order_ascending=True,  # Keep lowest (oldest)
        )

        # Assert: Should keep amount=100 (first record)
        assert result.collect()[0].amount == 100


class TestAddSurrogateKey:
    """Tests for add_surrogate_key function."""

    def test_adds_integer_key(self, spark):
        """Test that surrogate key is added as integer."""
        # Arrange
        data = [("CUST_A",), ("CUST_B",), ("CUST_C",)]
        df = spark.createDataFrame(data, ["customer_id"])

        # Act
        result = add_surrogate_key(df, "customer_key", ["customer_id"])

        # Assert: Key column exists and has integer values
        assert "customer_key" in result.columns
        keys = [row.customer_key for row in result.collect()]
        assert all(isinstance(k, int) for k in keys)

    def test_keys_are_unique(self, spark):
        """Test that surrogate keys are unique."""
        # Arrange
        data = [("A",), ("B",), ("C",), ("D",), ("E",)]
        df = spark.createDataFrame(data, ["id"])

        # Act
        result = add_surrogate_key(df, "sk", ["id"])

        # Assert: All keys are unique
        keys = [row.sk for row in result.collect()]
        assert len(keys) == len(set(keys))  # No duplicates

    def test_keys_start_from_one(self, spark):
        """Test that surrogate keys start from 1."""
        # Arrange
        data = [("X",)]
        df = spark.createDataFrame(data, ["id"])

        # Act
        result = add_surrogate_key(df, "sk", ["id"])

        # Assert
        assert result.collect()[0].sk == 1
