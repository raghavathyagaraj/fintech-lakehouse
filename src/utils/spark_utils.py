"""
Spark Utilities Module
======================

This module provides reusable functions for common Spark operations.

Why this module?
- DRY (Don't Repeat Yourself): Write once, use everywhere
- Consistency: All notebooks handle data the same way
- Maintainability: Fix a bug once, fixed everywhere
- Testing: Can test these functions in isolation

Usage:
    from src.utils.spark_utils import read_csv_to_df, write_delta_table
"""

from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    input_file_name,
    current_timestamp,
    lit,
    col,
    trim,
    upper,
    lower,
    when,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType
from delta.tables import DeltaTable


# ============================================================
# SPARK SESSION HELPERS
# ============================================================


def get_spark() -> SparkSession:
    """
    Get or create a SparkSession.

    Why this function?
    - In Databricks, 'spark' is already available
    - For local testing, we need to create it
    - This function works in both environments

    Returns:
        SparkSession: Active Spark session
    """
    return SparkSession.builder.getOrCreate()


# ============================================================
# READING DATA
# ============================================================


def read_csv_to_df(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    header: bool = True,
    delimiter: str = ",",
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read CSV file(s) into a DataFrame.

    Why this function?
    - Consistent options across all CSV reads
    - Schema enforcement (important for Bronze layer)
    - Easy to add common options

    Args:
        spark: SparkSession
        path: Path to CSV file or directory
        schema: Optional schema (if None, infers schema)
        header: Whether CSV has header row
        delimiter: Field delimiter
        options: Additional read options

    Returns:
        DataFrame: Loaded data

    Example:
        df = read_csv_to_df(spark, "/data/transactions.csv", schema=TXN_SCHEMA)
    """
    reader = spark.read.format("csv")

    # Apply schema if provided (recommended for Bronze)
    if schema:
        reader = reader.schema(schema)
    else:
        # If no schema, infer it (not recommended for production)
        reader = reader.option("inferSchema", "true")

    # Standard options
    reader = reader.option("header", str(header).lower())
    reader = reader.option("delimiter", delimiter)

    # Handle bad records gracefully
    reader = reader.option("mode", "PERMISSIVE")  # Don't fail on bad records
    reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")

    # Apply any additional options
    if options:
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(path)


def read_json_to_df(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    multiline: bool = False,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read JSON file(s) into a DataFrame.

    Why this function?
    - JSON Lines (multiline=False) is more efficient for big data
    - Schema enforcement for consistency
    - Corrupt record handling

    Args:
        spark: SparkSession
        path: Path to JSON file or directory
        schema: Optional schema
        multiline: If True, each file is one JSON object
                   If False, each line is a JSON object (JSON Lines format)
        options: Additional read options

    Returns:
        DataFrame: Loaded data

    Example:
        df = read_json_to_df(spark, "/data/customers/", schema=CUST_SCHEMA)
    """
    reader = spark.read.format("json")

    if schema:
        reader = reader.schema(schema)

    reader = reader.option("multiLine", str(multiline).lower())
    reader = reader.option("mode", "PERMISSIVE")
    reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")

    if options:
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(path)


def read_delta_table(
    spark: SparkSession,
    path: Optional[str] = None,
    table_name: Optional[str] = None,
) -> DataFrame:
    """
    Read a Delta table by path or table name.

    Why this function?
    - Unified interface for reading Delta tables
    - Can read by path (file-based) or table name (metastore)

    Args:
        spark: SparkSession
        path: Path to Delta table (e.g., "/mnt/silver/transactions")
        table_name: Table name (e.g., "fintech_silver.transactions")
        Note: Provide either path OR table_name, not both

    Returns:
        DataFrame: Loaded data

    Example:
        df = read_delta_table(spark, path="/mnt/silver/transactions")
        # OR
        df = read_delta_table(spark, table_name="fintech_silver.transactions")
    """
    if path and table_name:
        raise ValueError("Provide either 'path' or 'table_name', not both")

    if not path and not table_name:
        raise ValueError("Must provide either 'path' or 'table_name'")

    if path:
        return spark.read.format("delta").load(path)
    else:
        return spark.table(table_name)


# ============================================================
# WRITING DATA
# ============================================================


def write_delta_table(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = True,
    optimize_write: bool = True,
) -> None:
    """
    Write DataFrame to Delta table.

    Why this function?
    - Consistent write options across all notebooks
    - Automatic schema evolution (merge_schema)
    - Partitioning support for query performance

    Args:
        df: DataFrame to write
        path: Destination path
        mode: Write mode ("overwrite", "append", "merge")
        partition_by: List of columns to partition by
        merge_schema: If True, allows adding new columns
        optimize_write: If True, optimizes file sizes

    Example:
        write_delta_table(df, "/mnt/silver/transactions",
                         partition_by=["transaction_date"])
    """
    writer = df.write.format("delta").mode(mode)

    # Schema evolution - allows adding new columns without breaking
    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    # Optimize write - creates better file sizes
    if optimize_write:
        writer = writer.option("optimizeWrite", "true")

    # Partitioning - critical for query performance
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)


def register_delta_table(
    spark: SparkSession,
    path: str,
    database: str,
    table: str,
) -> None:
    """
    Register a Delta table in the metastore (Hive).

    Why register tables?
    - Can query using SQL: SELECT * FROM database.table
    - Visible in Databricks Data tab
    - Can set permissions at table level

    Args:
        spark: SparkSession
        path: Path to Delta table
        database: Database name (e.g., "fintech_silver")
        table: Table name (e.g., "transactions")

    Example:
        register_delta_table(spark, "/mnt/silver/transactions",
                            "fintech_silver", "transactions")
    """
    # Create database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Register table pointing to Delta location
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table}
        USING DELTA
        LOCATION '{path}'
    """
    )


def upsert_delta_table(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: List[str],
    update_columns: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Upsert (MERGE) data into a Delta table.

    Why upsert?
    - Updates existing records
    - Inserts new records
    - All in one atomic operation (ACID)

    Args:
        spark: SparkSession
        source_df: New/updated data
        target_path: Path to target Delta table
        merge_keys: Columns to match on (e.g., ["customer_id"])
        update_columns: Columns to update (if None, updates all)

    Returns:
        Dict with counts: {"inserted": X, "updated": Y}

    Example:
        stats = upsert_delta_table(spark, new_customers_df,
                                   "/mnt/silver/customers",
                                   merge_keys=["customer_id"])
    """
    # Check if target table exists
    if not DeltaTable.isDeltaTable(spark, target_path):
        # First time - just write the data
        write_delta_table(source_df, target_path)
        return {"inserted": source_df.count(), "updated": 0}

    # Get Delta table reference
    target_table = DeltaTable.forPath(spark, target_path)

    # Build merge condition: source.key1 = target.key1 AND source.key2 = target.key2
    merge_condition = " AND ".join([f"source.{key} = target.{key}" for key in merge_keys])

    # Perform merge
    merge_builder = target_table.alias("target").merge(source_df.alias("source"), merge_condition)

    # When matched, update
    if update_columns:
        update_dict = {col: f"source.{col}" for col in update_columns}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_dict)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    # When not matched, insert
    merge_builder = merge_builder.whenNotMatchedInsertAll()

    # Execute merge
    merge_builder.execute()

    # Return approximate counts (exact counts require additional query)
    return {"inserted": -1, "updated": -1}  # Delta doesn't return counts easily


# ============================================================
# METADATA HELPERS
# ============================================================


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """
    Add metadata columns for Bronze layer.

    Why add metadata?
    - _source_file: Know which file each record came from (debugging)
    - _ingestion_timestamp: Know when data was loaded (auditing)

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with metadata columns added

    Example:
        df = add_ingestion_metadata(df)
        # Now df has _source_file and _ingestion_timestamp columns
    """
    return df.withColumn("_source_file", input_file_name()).withColumn(
        "_ingestion_timestamp", current_timestamp()
    )


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """
    Add processing timestamp for Silver/Gold layers.

    Why track processing time?
    - Know when data was transformed
    - Debug latency issues
    - Audit trail

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with _processing_timestamp column added
    """
    return df.withColumn("_processing_timestamp", current_timestamp())


# ============================================================
# DATA TRANSFORMATION HELPERS
# ============================================================


def standardize_string_columns(
    df: DataFrame,
    columns: List[str],
    case: str = "upper",
    trim_whitespace: bool = True,
) -> DataFrame:
    """
    Standardize string columns (trim, case conversion).

    Why standardize?
    - " USD " and "usd" should be treated as the same
    - Consistent data for joins and aggregations

    Args:
        df: Input DataFrame
        columns: List of columns to standardize
        case: "upper", "lower", or "none"
        trim_whitespace: Whether to trim leading/trailing spaces

    Returns:
        DataFrame with standardized columns

    Example:
        df = standardize_string_columns(df, ["currency", "country"], case="upper")
    """
    for column in columns:
        if column in df.columns:
            col_expr = col(column)

            # Trim whitespace
            if trim_whitespace:
                col_expr = trim(col_expr)

            # Apply case conversion
            if case == "upper":
                col_expr = upper(col_expr)
            elif case == "lower":
                col_expr = lower(col_expr)

            df = df.withColumn(column, col_expr)

    return df


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_by_column: str,
    order_ascending: bool = False,
) -> DataFrame:
    """
    Remove duplicates, keeping one record per key.

    Why deduplicate?
    - Source systems may send duplicates
    - Data replays may reload same records
    - We want only the latest/first version

    Args:
        df: Input DataFrame
        key_columns: Columns that define uniqueness
        order_by_column: Column to order by (to pick which to keep)
        order_ascending: If True, keeps first; if False, keeps latest

    Returns:
        DataFrame with duplicates removed

    Example:
        # Keep the most recent record for each transaction_id
        df = deduplicate_by_key(df,
                               key_columns=["transaction_id"],
                               order_by_column="_ingestion_timestamp",
                               order_ascending=False)  # False = keep latest
    """
    # Define window: partition by key, order by the specified column
    window_spec = Window.partitionBy(*key_columns).orderBy(
        col(order_by_column).asc() if order_ascending else col(order_by_column).desc()
    )

    # Add row number within each partition
    df_with_row_num = df.withColumn("_row_num", row_number().over(window_spec))

    # Keep only the first row in each partition
    df_deduped = df_with_row_num.filter(col("_row_num") == 1).drop("_row_num")

    return df_deduped


def add_surrogate_key(
    df: DataFrame,
    key_column_name: str,
    natural_key_columns: List[str],
) -> DataFrame:
    """
    Add an integer surrogate key column.

    Why surrogate keys?
    - Faster joins (integer vs string comparison)
    - Handles source key changes
    - Standard dimensional modeling practice

    Args:
        df: Input DataFrame
        key_column_name: Name for the new surrogate key column
        natural_key_columns: Columns that define natural uniqueness

    Returns:
        DataFrame with surrogate key column added

    Example:
        df = add_surrogate_key(df, "customer_key", ["customer_id"])
    """
    window_spec = Window.orderBy(*natural_key_columns)
    return df.withColumn(key_column_name, row_number().over(window_spec))


# ============================================================
# DELTA TABLE MAINTENANCE
# ============================================================


def optimize_delta_table(
    spark: SparkSession,
    path: Optional[str] = None,
    table_name: Optional[str] = None,
    z_order_columns: Optional[List[str]] = None,
) -> None:
    """
    Optimize a Delta table for query performance.

    Why optimize?
    - Compacts small files into larger ones (faster reads)
    - Z-ordering co-locates related data (faster filters)

    Args:
        spark: SparkSession
        path: Path to Delta table
        table_name: Table name (alternative to path)
        z_order_columns: Columns to Z-order by (commonly filtered columns)

    Example:
        # Optimize and Z-order by transaction_date
        optimize_delta_table(spark,
                            table_name="fintech_silver.transactions",
                            z_order_columns=["transaction_date"])
    """
    if path:
        target = f"delta.`{path}`"
    elif table_name:
        target = table_name
    else:
        raise ValueError("Must provide either 'path' or 'table_name'")

    if z_order_columns:
        z_order_clause = ", ".join(z_order_columns)
        spark.sql(f"OPTIMIZE {target} ZORDER BY ({z_order_clause})")
    else:
        spark.sql(f"OPTIMIZE {target}")


def vacuum_delta_table(
    spark: SparkSession,
    path: Optional[str] = None,
    table_name: Optional[str] = None,
    retention_hours: int = 168,  # 7 days default
) -> None:
    """
    Remove old files from a Delta table.

    Why vacuum?
    - Delta keeps old file versions for time travel
    - These accumulate and waste storage
    - Vacuum removes files older than retention period

    Args:
        spark: SparkSession
        path: Path to Delta table
        table_name: Table name
        retention_hours: Keep files newer than this (default 7 days)

    Example:
        vacuum_delta_table(spark, table_name="fintech_silver.transactions")
    """
    if path:
        target = f"delta.`{path}`"
    elif table_name:
        target = table_name
    else:
        raise ValueError("Must provide either 'path' or 'table_name'")

    spark.sql(f"VACUUM {target} RETAIN {retention_hours} HOURS")


# ============================================================
# UTILITY FUNCTIONS
# ============================================================


def get_table_row_count(
    spark: SparkSession,
    path: Optional[str] = None,
    table_name: Optional[str] = None,
) -> int:
    """
    Get row count of a table efficiently.

    Args:
        spark: SparkSession
        path: Path to Delta table
        table_name: Table name

    Returns:
        Number of rows
    """
    df = read_delta_table(spark, path=path, table_name=table_name)
    return df.count()


def table_exists(
    spark: SparkSession,
    database: str,
    table: str,
) -> bool:
    """
    Check if a table exists in the metastore.

    Args:
        spark: SparkSession
        database: Database name
        table: Table name

    Returns:
        True if table exists, False otherwise
    """
    try:
        spark.sql(f"DESCRIBE {database}.{table}")
        return True
    except Exception:
        return False


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    """
    Check if a Delta table exists at the given path.

    Args:
        spark: SparkSession
        path: Path to check

    Returns:
        True if Delta table exists, False otherwise
    """
    return DeltaTable.isDeltaTable(spark, path)
