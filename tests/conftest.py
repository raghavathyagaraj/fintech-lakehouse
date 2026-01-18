"""
Pytest Configuration and Shared Fixtures
=========================================

This file contains fixtures that are shared across all tests.

Why conftest.py?
- Fixtures defined here are available to all test files
- No need to import — pytest discovers automatically
- Centralized test configuration
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for the entire test session.

    Why session scope?
    - Creating SparkSession is expensive (takes seconds)
    - Reuse the same session for all tests
    - Clean up after ALL tests complete

    Usage in tests:
        def test_something(spark):
            df = spark.createDataFrame(...)
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PytestSession")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
        .config("spark.executor.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
        # Delta Lake configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )

    # Set log level to reduce noise during tests
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    # Cleanup after all tests
    spark.stop()


@pytest.fixture
def sample_transactions_df(spark):
    """
    Create a sample transactions DataFrame for testing.

    This fixture creates fresh data for each test (default scope).
    """
    data = [
        ("TXN001", "CUST001", "MERCH001", "100.00", "USD", "COMPLETED"),
        ("TXN002", "CUST002", "MERCH001", "250.50", "USD", "COMPLETED"),
        ("TXN003", "CUST001", "MERCH002", "75.25", "USD", "PENDING"),
        ("TXN004", "CUST003", "MERCH003", "500.00", "USD", "FAILED"),
        ("TXN005", "CUST002", "MERCH002", "1000.00", "USD", "COMPLETED"),
    ]

    columns = ["transaction_id", "customer_id", "merchant_id", "amount", "currency", "status"]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_customers_df(spark):
    """
    Create a sample customers DataFrame for testing.
    """
    data = [
        ("CUST001", "John", "Doe", "john@email.com", "US"),
        ("CUST002", "Jane", "Smith", "jane@email.com", "UK"),
        ("CUST003", "Bob", "Wilson", "bob@email.com", "US"),
    ]

    columns = ["customer_id", "first_name", "last_name", "email", "country"]

    return spark.createDataFrame(data, columns)
