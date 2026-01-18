"""
Data Quality Module
===================

This module provides functions to validate data quality.

Why data quality checks?
- Catch problems BEFORE they affect analytics
- Ensure compliance (financial regulations require data accuracy)
- Build trust in data (users won't use data they don't trust)
- Prevent garbage in, garbage out

The Quarantine Pattern:
- Good data → Goes to Silver/Gold tables
- Bad data → Goes to Quarantine table with failure reasons
- No data is lost!

Usage:
    from src.utils.data_quality import DataQualityChecker

    checker = DataQualityChecker(df, "transactions")
    report = checker.check_completeness(["customer_id", "amount"]) \
                    .check_uniqueness(["transaction_id"]) \
                    .check_range("amount", min_val=0.01, max_val=100000) \
                    .run()

    valid_df, invalid_df = checker.get_valid_invalid_dfs()
"""

from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    count,
    sum as spark_sum,
    isnan,
    isnull,
    length,
    trim,
    array,
    array_compact,
    current_timestamp,
    expr,
)


class DQSeverity(Enum):
    """
    Severity levels for data quality issues.

    Why severity levels?
    - CRITICAL: Pipeline should stop, data is unusable
    - WARNING: Data has issues but can proceed with caution
    - INFO: Minor issues, informational only
    """

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


class DQCheckType(Enum):
    """
    Types of data quality checks.

    Why categorize checks?
    - Helps understand what kind of problem occurred
    - Can filter/group issues by type
    - Standard DQ framework terminology
    """

    COMPLETENESS = "COMPLETENESS"  # Are required fields present?
    UNIQUENESS = "UNIQUENESS"  # Are keys unique?
    VALIDITY = "VALIDITY"  # Are values in valid range/format?
    CONSISTENCY = "CONSISTENCY"  # Do related fields match?
    TIMELINESS = "TIMELINESS"  # Is data fresh enough?
    REFERENTIAL = "REFERENTIAL"  # Do foreign keys exist?


@dataclass
class DQCheckResult:
    """
    Result of a single data quality check.

    Why a dataclass?
    - Structured way to store check results
    - Easy to convert to dict/JSON for logging
    - Type hints for IDE support
    """

    check_name: str
    check_type: DQCheckType
    severity: DQSeverity
    passed: bool
    total_records: int
    failed_records: int
    pass_rate: float
    column: Optional[str] = None
    threshold: Optional[float] = None
    details: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/storage."""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type.value,
            "severity": self.severity.value,
            "passed": self.passed,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "pass_rate": round(self.pass_rate, 4),
            "column": self.column,
            "threshold": self.threshold,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class DQReport:
    """
    Complete data quality report for a table.

    Contains all check results and overall status.
    """

    table_name: str
    total_records: int
    checks: List[DQCheckResult] = field(default_factory=list)
    run_timestamp: datetime = field(default_factory=datetime.now)

    @property
    def passed(self) -> bool:
        """Overall pass: True if no CRITICAL checks failed."""
        critical_failures = [
            c for c in self.checks if c.severity == DQSeverity.CRITICAL and not c.passed
        ]
        return len(critical_failures) == 0

    @property
    def critical_failures(self) -> List[DQCheckResult]:
        """List of failed CRITICAL checks."""
        return [c for c in self.checks if c.severity == DQSeverity.CRITICAL and not c.passed]

    @property
    def warnings(self) -> List[DQCheckResult]:
        """List of failed WARNING checks."""
        return [c for c in self.checks if c.severity == DQSeverity.WARNING and not c.passed]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/storage."""
        return {
            "table_name": self.table_name,
            "total_records": self.total_records,
            "overall_passed": self.passed,
            "total_checks": len(self.checks),
            "passed_checks": len([c for c in self.checks if c.passed]),
            "failed_checks": len([c for c in self.checks if not c.passed]),
            "critical_failures": len(self.critical_failures),
            "warnings": len(self.warnings),
            "checks": [c.to_dict() for c in self.checks],
            "run_timestamp": self.run_timestamp.isoformat(),
        }

    def summary(self) -> str:
        """Human-readable summary of the report."""
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        lines = [
            f"\n{'='*60}",
            f"DATA QUALITY REPORT: {self.table_name}",
            f"{'='*60}",
            f"Status: {status}",
            f"Total Records: {self.total_records:,}",
            f"Checks Run: {len(self.checks)}",
            f"Passed: {len([c for c in self.checks if c.passed])}",
            f"Failed: {len([c for c in self.checks if not c.passed])}",
            f"{'='*60}",
        ]

        if self.critical_failures:
            lines.append("\n🚨 CRITICAL FAILURES:")
            for check in self.critical_failures:
                lines.append(
                    f"  - {check.check_name}: {check.failed_records:,} records "
                    f"({100 - check.pass_rate:.2f}% failed)"
                )

        if self.warnings:
            lines.append("\n⚠️ WARNINGS:")
            for check in self.warnings:
                lines.append(
                    f"  - {check.check_name}: {check.failed_records:,} records "
                    f"({100 - check.pass_rate:.2f}% failed)"
                )

        return "\n".join(lines)


class DataQualityChecker:
    """
    Main class for running data quality checks.

    This class uses the Builder pattern (fluent API):
    - Chain multiple checks together
    - Run all checks at once
    - Get results in a structured report

    Example:
        checker = DataQualityChecker(df, "transactions")
        report = checker \
            .check_completeness(["customer_id", "amount"]) \
            .check_uniqueness(["transaction_id"]) \
            .check_range("amount", min_val=0.01, max_val=100000) \
            .run()

        if not report.passed:
            valid_df, invalid_df = checker.get_valid_invalid_dfs()
            # Write invalid_df to quarantine
    """

    def __init__(self, df: DataFrame, table_name: str):
        """
        Initialize the checker.

        Args:
            df: DataFrame to check
            table_name: Name of the table (for reporting)
        """
        self.df = df
        self.table_name = table_name
        self.total_records = df.count()
        self._checks: List[Dict] = []  # Stores check definitions
        self._results: List[DQCheckResult] = []  # Stores check results
        self._failure_columns: List[str] = []  # Columns tracking failures

    # ============================================================
    # CHECK: COMPLETENESS (Are required fields present?)
    # ============================================================

    def check_completeness(
        self,
        columns: List[str],
        threshold: float = 0.95,
        severity: DQSeverity = DQSeverity.CRITICAL,
    ) -> "DataQualityChecker":
        """
        Check that columns are not null/empty.

        Why check completeness?
        - NULL customer_id = can't link to customer dimension
        - NULL amount = can't calculate revenue
        - Missing data breaks downstream analytics

        Args:
            columns: Columns to check for nulls
            threshold: Minimum pass rate (0.95 = 95% must be non-null)
            severity: How serious is a failure?

        Returns:
            self (for chaining)

        Example:
            checker.check_completeness(["customer_id", "amount"])
        """
        for column in columns:
            self._checks.append(
                {
                    "name": f"completeness_{column}",
                    "type": DQCheckType.COMPLETENESS,
                    "severity": severity,
                    "column": column,
                    "threshold": threshold,
                    "condition": (
                        col(column).isNull()
                        | isnan(col(column))
                        | (trim(col(column).cast("string")) == "")
                    ),
                    "failure_reason": f"NULL_OR_EMPTY_{column.upper()}",
                }
            )

        return self

    # ============================================================
    # CHECK: UNIQUENESS (Are keys unique?)
    # ============================================================

    def check_uniqueness(
        self,
        columns: List[str],
        severity: DQSeverity = DQSeverity.CRITICAL,
    ) -> "DataQualityChecker":
        """
        Check that columns form a unique key (no duplicates).

        Why check uniqueness?
        - Duplicate transaction_id = double-counting revenue
        - Duplicate customer_id = wrong customer counts
        - Primary keys MUST be unique

        Args:
            columns: Columns that should be unique together
            severity: How serious is a failure?

        Returns:
            self (for chaining)

        Example:
            checker.check_uniqueness(["transaction_id"])
        """
        key_name = "_".join(columns)
        self._checks.append(
            {
                "name": f"uniqueness_{key_name}",
                "type": DQCheckType.UNIQUENESS,
                "severity": severity,
                "column": key_name,
                "threshold": 1.0,  # 100% unique required
                "is_uniqueness_check": True,
                "uniqueness_columns": columns,
                "failure_reason": f"DUPLICATE_{key_name.upper()}",
            }
        )

        return self

    # ============================================================
    # CHECK: VALUE IN SET (Are values valid enum values?)
    # ============================================================

    def check_values_in_set(
        self,
        column: str,
        valid_values: List[Any],
        severity: DQSeverity = DQSeverity.WARNING,
    ) -> "DataQualityChecker":
        """
        Check that column values are in a set of allowed values.

        Why check enum values?
        - status should be: COMPLETED, PENDING, FAILED, REVERSED
        - Unknown status = data quality issue at source
        - Prevents "garbage" categories in reports

        Args:
            column: Column to check
            valid_values: List of allowed values
            severity: How serious is a failure?

        Returns:
            self (for chaining)

        Example:
            checker.check_values_in_set("status",
                                        ["COMPLETED", "PENDING", "FAILED"])
        """
        self._checks.append(
            {
                "name": f"valid_values_{column}",
                "type": DQCheckType.VALIDITY,
                "severity": severity,
                "column": column,
                "threshold": 1.0,
                "condition": ~col(column).isin(valid_values) & col(column).isNotNull(),
                "failure_reason": f"INVALID_{column.upper()}_VALUE",
            }
        )

        return self

    # ============================================================
    # CHECK: RANGE (Are numeric values in valid range?)
    # ============================================================

    def check_range(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        severity: DQSeverity = DQSeverity.CRITICAL,
    ) -> "DataQualityChecker":
        """
        Check that numeric values are within a range.

        Why check ranges?
        - Negative transaction amount = impossible (likely bug)
        - $1 billion transaction = suspicious, needs review
        - Catches data entry errors and system bugs

        Args:
            column: Numeric column to check
            min_val: Minimum allowed value (inclusive)
            max_val: Maximum allowed value (inclusive)
            severity: How serious is a failure?

        Returns:
            self (for chaining)

        Example:
            checker.check_range("amount", min_val=0.01, max_val=100000)
        """
        conditions = []
        if min_val is not None:
            conditions.append(col(column) < min_val)
        if max_val is not None:
            conditions.append(col(column) > max_val)

        if conditions:
            combined_condition = conditions[0]
            for cond in conditions[1:]:
                combined_condition = combined_condition | cond

            # Only check non-null values
            combined_condition = combined_condition & col(column).isNotNull()

            self._checks.append(
                {
                    "name": f"range_{column}",
                    "type": DQCheckType.VALIDITY,
                    "severity": severity,
                    "column": column,
                    "threshold": 1.0,
                    "condition": combined_condition,
                    "failure_reason": f"{column.upper()}_OUT_OF_RANGE",
                }
            )

        return self

    # ============================================================
    # CHECK: CUSTOM CONDITION
    # ============================================================

    def check_custom(
        self,
        name: str,
        condition: Any,  # PySpark Column expression that is TRUE for BAD records
        failure_reason: str,
        check_type: DQCheckType = DQCheckType.VALIDITY,
        severity: DQSeverity = DQSeverity.WARNING,
    ) -> "DataQualityChecker":
        """
        Check a custom condition.

        Why custom checks?
        - Business rules that don't fit standard checks
        - Complex multi-column validations
        - Industry-specific requirements

        Args:
            name: Name for this check
            condition: PySpark Column expression (TRUE = record is BAD)
            failure_reason: Reason string for quarantine
            check_type: Category of check
            severity: How serious is a failure?

        Returns:
            self (for chaining)

        Example:
            # Net amount should equal amount minus fee
            checker.check_custom(
                name="net_amount_calculation",
                condition=col("net_amount") != (col("amount") - col("fee_amount")),
                failure_reason="NET_AMOUNT_MISMATCH"
            )
        """
        self._checks.append(
            {
                "name": name,
                "type": check_type,
                "severity": severity,
                "column": None,
                "threshold": 1.0,
                "condition": condition,
                "failure_reason": failure_reason,
            }
        )

        return self

    # ============================================================
    # RUN ALL CHECKS
    # ============================================================

    def run(self) -> DQReport:
        """
        Execute all registered checks and return a report.

        Returns:
            DQReport with all check results
        """
        self._results = []

        for check in self._checks:
            if check.get("is_uniqueness_check"):
                result = self._run_uniqueness_check(check)
            else:
                result = self._run_standard_check(check)

            self._results.append(result)

        return DQReport(
            table_name=self.table_name,
            total_records=self.total_records,
            checks=self._results,
        )

    def _run_standard_check(self, check: Dict) -> DQCheckResult:
        """Run a standard condition-based check."""
        # Count records that fail the check
        failed_count = self.df.filter(check["condition"]).count()
        pass_rate = (self.total_records - failed_count) / max(self.total_records, 1)
        passed = pass_rate >= check["threshold"]

        # Track this failure column for later splitting
        if not passed:
            failure_col_name = f"_fail_{check['name']}"
            self._failure_columns.append(failure_col_name)

        return DQCheckResult(
            check_name=check["name"],
            check_type=check["type"],
            severity=check["severity"],
            passed=passed,
            total_records=self.total_records,
            failed_records=failed_count,
            pass_rate=pass_rate * 100,
            column=check["column"],
            threshold=check["threshold"],
        )

    def _run_uniqueness_check(self, check: Dict) -> DQCheckResult:
        """Run a uniqueness check (requires aggregation)."""
        columns = check["uniqueness_columns"]

        # Count duplicates: group by key, count > 1 means duplicate
        duplicate_count = self.df.groupBy(*columns).count().filter(col("count") > 1).count()

        # Failed records = total - unique
        unique_count = self.df.select(*columns).distinct().count()
        failed_count = self.total_records - unique_count

        pass_rate = unique_count / max(self.total_records, 1)
        passed = duplicate_count == 0

        return DQCheckResult(
            check_name=check["name"],
            check_type=check["type"],
            severity=check["severity"],
            passed=passed,
            total_records=self.total_records,
            failed_records=failed_count,
            pass_rate=pass_rate * 100,
            column=check["column"],
            threshold=check["threshold"],
            details=f"{duplicate_count} duplicate key combinations found",
        )

    # ============================================================
    # GET VALID AND INVALID DATAFRAMES
    # ============================================================

    def get_valid_invalid_dfs(self) -> Tuple[DataFrame, DataFrame]:
        """
        Split DataFrame into valid and invalid records.

        Why split?
        - Valid records → continue to Silver/Gold
        - Invalid records → Quarantine for investigation
        - No data is lost!

        Returns:
            Tuple of (valid_df, invalid_df)

        Example:
            valid_df, invalid_df = checker.get_valid_invalid_dfs()
            write_delta_table(valid_df, silver_path)
            write_delta_table(invalid_df, quarantine_path)
        """
        # Add failure flag columns for each check
        df_with_flags = self.df

        all_failure_reasons = []

        for check in self._checks:
            if check.get("is_uniqueness_check"):
                continue  # Skip uniqueness checks (handled differently)

            failure_col = f"_fail_{check['name']}"
            df_with_flags = df_with_flags.withColumn(
                failure_col,
                when(check["condition"], lit(check["failure_reason"])).otherwise(lit(None)),
            )
            all_failure_reasons.append(col(failure_col))

        # Combine all failure reasons into an array
        if all_failure_reasons:
            df_with_flags = df_with_flags.withColumn(
                "_validation_failures", array_compact(array(*all_failure_reasons))
            )

            # Valid: empty failure array
            valid_df = df_with_flags.filter(expr("size(_validation_failures) = 0")).drop(
                "_validation_failures",
                *[f"_fail_{c['name']}" for c in self._checks if not c.get("is_uniqueness_check")],
            )

            # Invalid: non-empty failure array
            invalid_df = df_with_flags.filter(expr("size(_validation_failures) > 0")).withColumn(
                "_quarantine_timestamp", current_timestamp()
            )

            # Keep only relevant columns for quarantine
            quarantine_columns = [c for c in self.df.columns] + [
                "_validation_failures",
                "_quarantine_timestamp",
            ]
            invalid_df = invalid_df.select(
                *[c for c in quarantine_columns if c in invalid_df.columns]
            )

        else:
            # No checks defined, all records are valid
            valid_df = self.df
            invalid_df = self.df.limit(0)  # Empty DataFrame with same schema

        return valid_df, invalid_df


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================


def quick_quality_check(
    df: DataFrame,
    table_name: str,
    pk_columns: List[str],
    required_columns: List[str],
) -> DQReport:
    """
    Run a quick quality check with common validations.

    This is a convenience function for simple use cases.

    Args:
        df: DataFrame to check
        table_name: Name of table
        pk_columns: Primary key columns (must be unique)
        required_columns: Columns that must not be null

    Returns:
        DQReport with results

    Example:
        report = quick_quality_check(df, "transactions",
                                     pk_columns=["transaction_id"],
                                     required_columns=["customer_id", "amount"])
    """
    checker = DataQualityChecker(df, table_name)

    checker.check_completeness(required_columns)
    checker.check_uniqueness(pk_columns)

    return checker.run()
