"""
Unit Tests for Data Quality Module
==================================

These tests verify that our DQ checks work correctly.

How to run:
    pytest tests/unit/test_data_quality.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from src.utils.data_quality import (
    DataQualityChecker,
    DQSeverity,
    DQCheckType,
)


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("DQTests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestCompletenessCheck:
    """Tests for completeness (null) checks."""

    def test_passes_when_no_nulls(self, spark):
        """Test that check passes when there are no nulls."""
        # Arrange: All values present
        data = [("TXN1", "CUST1"), ("TXN2", "CUST2")]
        df = spark.createDataFrame(data, ["txn_id", "customer_id"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_completeness(["txn_id", "customer_id"]).run()

        # Assert: All checks should pass
        assert report.passed
        assert all(check.passed for check in report.checks)

    def test_fails_when_nulls_present(self, spark):
        """Test that check fails when nulls are present."""
        # Arrange: One null value
        data = [("TXN1", "CUST1"), ("TXN2", None)]
        df = spark.createDataFrame(data, ["txn_id", "customer_id"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_completeness(
            ["customer_id"],
            threshold=1.0,  # Require 100% non-null
            severity=DQSeverity.CRITICAL,
        ).run()

        # Assert: Check should fail
        assert not report.passed
        assert len(report.critical_failures) == 1

    def test_passes_when_within_threshold(self, spark):
        """Test that check passes when null rate is within threshold."""
        # Arrange: 1 out of 10 is null = 90% complete
        data = [(f"TXN{i}", f"CUST{i}" if i < 9 else None) for i in range(10)]
        df = spark.createDataFrame(data, ["txn_id", "customer_id"])

        # Act: Threshold is 80%, so 90% should pass
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_completeness(
            ["customer_id"],
            threshold=0.80,  # 80% threshold
        ).run()

        # Assert: Should pass (90% > 80%)
        assert report.passed


class TestUniquenessCheck:
    """Tests for uniqueness (duplicate) checks."""

    def test_passes_when_unique(self, spark):
        """Test that check passes when all keys are unique."""
        # Arrange: All unique
        data = [("TXN1",), ("TXN2",), ("TXN3",)]
        df = spark.createDataFrame(data, ["txn_id"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_uniqueness(["txn_id"]).run()

        # Assert
        assert report.passed

    def test_fails_when_duplicates_exist(self, spark):
        """Test that check fails when duplicates exist."""
        # Arrange: TXN1 appears twice
        data = [("TXN1",), ("TXN1",), ("TXN2",)]
        df = spark.createDataFrame(data, ["txn_id"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_uniqueness(["txn_id"]).run()

        # Assert
        assert not report.passed


class TestRangeCheck:
    """Tests for range (min/max) checks."""

    def test_passes_when_in_range(self, spark):
        """Test that check passes when values are in range."""
        # Arrange: All amounts between 0.01 and 100
        data = [(0.01,), (50.0,), (100.0,)]
        df = spark.createDataFrame(data, ["amount"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_range("amount", min_val=0.01, max_val=100.0).run()

        # Assert
        assert report.passed

    def test_fails_when_below_min(self, spark):
        """Test that check fails when value is below minimum."""
        # Arrange: One negative amount
        data = [(100.0,), (-50.0,), (200.0,)]
        df = spark.createDataFrame(data, ["amount"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_range("amount", min_val=0.0).run()

        # Assert
        assert not report.passed

    def test_fails_when_above_max(self, spark):
        """Test that check fails when value is above maximum."""
        # Arrange: One amount over 100,000
        data = [(100.0,), (50.0,), (999999.0,)]
        df = spark.createDataFrame(data, ["amount"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_range("amount", max_val=100000.0).run()

        # Assert
        assert not report.passed


class TestValuesInSetCheck:
    """Tests for enum value checks."""

    def test_passes_when_valid_values(self, spark):
        """Test that check passes when all values are in the allowed set."""
        # Arrange
        data = [("COMPLETED",), ("PENDING",), ("FAILED",)]
        df = spark.createDataFrame(data, ["status"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_values_in_set(
            "status", valid_values=["COMPLETED", "PENDING", "FAILED", "REVERSED"]
        ).run()

        # Assert
        assert report.passed

    def test_fails_when_invalid_value(self, spark):
        """Test that check fails when an invalid value exists."""
        # Arrange: "UNKNOWN" is not a valid status
        data = [("COMPLETED",), ("UNKNOWN",), ("FAILED",)]
        df = spark.createDataFrame(data, ["status"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_values_in_set(
            "status", valid_values=["COMPLETED", "PENDING", "FAILED", "REVERSED"]
        ).run()

        # Assert
        assert not report.passed


class TestValidInvalidSplit:
    """Tests for splitting data into valid and invalid DataFrames."""

    def test_splits_correctly(self, spark):
        """Test that data is correctly split into valid and invalid."""
        # Arrange: 2 valid, 1 invalid (null customer_id)
        data = [
            ("TXN1", "CUST1", 100.0),
            ("TXN2", "CUST2", 200.0),
            ("TXN3", None, 300.0),  # Invalid: null customer_id
        ]
        df = spark.createDataFrame(data, ["txn_id", "customer_id", "amount"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        checker.check_completeness(["customer_id"], threshold=1.0).run()
        valid_df, invalid_df = checker.get_valid_invalid_dfs()

        # Assert
        assert valid_df.count() == 2
        assert invalid_df.count() == 1

    def test_invalid_has_failure_reasons(self, spark):
        """Test that invalid records have failure reasons."""
        # Arrange
        data = [
            ("TXN1", None, 100.0),  # Invalid: null customer_id
        ]
        df = spark.createDataFrame(data, ["txn_id", "customer_id", "amount"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        checker.check_completeness(["customer_id"]).run()
        _, invalid_df = checker.get_valid_invalid_dfs()

        # Assert: Should have _validation_failures column
        assert "_validation_failures" in invalid_df.columns

        # The failure reason should be recorded
        failures = invalid_df.collect()[0]["_validation_failures"]
        assert len(failures) > 0
        assert "NULL_OR_EMPTY_CUSTOMER_ID" in failures


class TestMultipleChecks:
    """Tests for running multiple checks together."""

    def test_all_checks_run(self, spark):
        """Test that all registered checks are executed."""
        # Arrange
        data = [("TXN1", "CUST1", 100.0, "COMPLETED")]
        df = spark.createDataFrame(data, ["txn_id", "customer_id", "amount", "status"])

        # Act: Register multiple checks
        checker = DataQualityChecker(df, "test_table")
        report = (
            checker.check_completeness(["txn_id", "customer_id"])
            .check_uniqueness(["txn_id"])
            .check_range("amount", min_val=0.01, max_val=100000)
            .check_values_in_set("status", ["COMPLETED", "PENDING", "FAILED"])
            .run()
        )

        # Assert: 4 checks should have run
        # completeness_txn_id, completeness_customer_id, uniqueness_txn_id,
        # range_amount, valid_values_status = 5 checks
        assert len(report.checks) == 5

    def test_report_summary(self, spark):
        """Test that report summary is generated correctly."""
        # Arrange
        data = [("TXN1", "CUST1", 100.0)]
        df = spark.createDataFrame(data, ["txn_id", "customer_id", "amount"])

        # Act
        checker = DataQualityChecker(df, "test_table")
        report = checker.check_completeness(["txn_id"]).run()

        # Assert: Summary should be a string
        summary = report.summary()
        assert isinstance(summary, str)
        assert "test_table" in summary
