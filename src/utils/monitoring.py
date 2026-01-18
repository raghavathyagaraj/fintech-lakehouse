"""
Monitoring Module
=================

This module provides functions to track pipeline execution.

Why monitoring?
- Know when pipelines run (and for how long)
- Track data volumes over time
- Detect anomalies (sudden volume changes)
- Audit trail for compliance
- Debug issues faster

What we track:
1. Pipeline runs (start, end, status, duration)
2. Data quality metrics (from DQ checks)
3. Data freshness (how old is the data?)
4. Data volume (row counts, changes)

Usage:
    from src.utils.monitoring import PipelineMonitor

    monitor = PipelineMonitor(spark, "silver_transactions")

    stage = monitor.start_stage("read_bronze")
    df = spark.read.format("delta").load(bronze_path)
    monitor.end_stage(stage, "SUCCESS", records_read=df.count())
"""

import uuid
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max, count, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType,
    BooleanType,
)


class PipelineStatus(Enum):
    """Pipeline execution status."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"  # Some stages failed


@dataclass
class StageMetrics:
    """
    Metrics for a single pipeline stage.

    Why track per-stage?
    - Identify which stage is slow
    - Identify which stage failed
    - Fine-grained debugging
    """

    run_id: str
    pipeline_name: str
    stage_name: str
    status: str = "RUNNING"
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_read: int = 0
    records_written: int = 0
    records_quarantined: int = 0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "stage_name": self.stage_name,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_quarantined": self.records_quarantined,
            "error_message": self.error_message,
        }


class PipelineMonitor:
    """
    Monitor pipeline execution.

    This class:
    - Generates unique run IDs
    - Tracks each stage's metrics
    - Writes metrics to Delta table
    - Calculates overall pipeline status

    Example:
        monitor = PipelineMonitor(spark, "silver_transactions")

        # Stage 1
        stage1 = monitor.start_stage("read_bronze")
        try:
            df = spark.read.format("delta").load(path)
            monitor.end_stage(stage1, "SUCCESS", records_read=df.count())
        except Exception as e:
            monitor.end_stage(stage1, "FAILED", error_message=str(e))
            raise

        # Stage 2
        stage2 = monitor.start_stage("transform")
        ...

        # Finish
        monitor.finish()
    """

    # Schema for metrics table
    METRICS_SCHEMA = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("stage_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("records_read", LongType(), True),
            StructField("records_written", LongType(), True),
            StructField("records_quarantined", LongType(), True),
            StructField("error_message", StringType(), True),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        pipeline_name: str,
        metrics_table: str = "fintech_gold.pipeline_metrics",
    ):
        """
        Initialize the monitor.

        Args:
            spark: SparkSession
            pipeline_name: Name of the pipeline (e.g., "silver_transactions")
            metrics_table: Table to write metrics to
        """
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.metrics_table = metrics_table
        self.run_id = str(uuid.uuid4())
        self.stages: List[StageMetrics] = []
        self.pipeline_start_time = datetime.now()

    def start_stage(self, stage_name: str) -> StageMetrics:
        """
        Mark the start of a pipeline stage.

        Args:
            stage_name: Name of the stage (e.g., "read_bronze", "transform")

        Returns:
            StageMetrics object (pass to end_stage when done)
        """
        stage = StageMetrics(
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            stage_name=stage_name,
            status="RUNNING",
            start_time=datetime.now(),
        )
        self.stages.append(stage)
        print(f"[{self.pipeline_name}] Starting stage: {stage_name}")
        return stage

    def end_stage(
        self,
        stage: StageMetrics,
        status: str,
        records_read: int = 0,
        records_written: int = 0,
        records_quarantined: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Mark the end of a pipeline stage.

        Args:
            stage: StageMetrics from start_stage
            status: "SUCCESS" or "FAILED"
            records_read: Number of records read
            records_written: Number of records written
            records_quarantined: Number of records sent to quarantine
            error_message: Error message if failed
        """
        stage.end_time = datetime.now()
        stage.status = status
        stage.duration_seconds = (stage.end_time - stage.start_time).total_seconds()
        stage.records_read = records_read
        stage.records_written = records_written
        stage.records_quarantined = records_quarantined
        stage.error_message = error_message

        status_emoji = "✅" if status == "SUCCESS" else "❌"
        print(
            f"[{self.pipeline_name}] {status_emoji} Stage {stage.stage_name}: "
            f"{status} in {stage.duration_seconds:.2f}s "
            f"(read: {records_read:,}, written: {records_written:,})"
        )

        # Write stage metrics immediately
        self._write_stage_metrics(stage)

    def _write_stage_metrics(self, stage: StageMetrics) -> None:
        """Write a single stage's metrics to the table."""
        try:
            # Create single-row DataFrame
            data = [stage.to_dict()]
            df = self.spark.createDataFrame(data, self.METRICS_SCHEMA)

            # Append to metrics table
            df.write.format("delta").mode("append").saveAsTable(self.metrics_table)
        except Exception as e:
            # Don't fail the pipeline if metrics write fails
            print(f"Warning: Could not write metrics: {e}")

    def finish(self) -> Dict[str, Any]:
        """
        Finish monitoring and return summary.

        Returns:
            Summary dict with overall status and metrics
        """
        pipeline_end_time = datetime.now()
        total_duration = (pipeline_end_time - self.pipeline_start_time).total_seconds()

        # Determine overall status
        failed_stages = [s for s in self.stages if s.status == "FAILED"]
        if failed_stages:
            overall_status = "FAILED"
        elif any(s.status != "SUCCESS" for s in self.stages):
            overall_status = "PARTIAL"
        else:
            overall_status = "SUCCESS"

        summary = {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "overall_status": overall_status,
            "total_duration_seconds": total_duration,
            "total_stages": len(self.stages),
            "successful_stages": len([s for s in self.stages if s.status == "SUCCESS"]),
            "failed_stages": len(failed_stages),
            "total_records_read": sum(s.records_read for s in self.stages),
            "total_records_written": sum(s.records_written for s in self.stages),
            "total_records_quarantined": sum(s.records_quarantined for s in self.stages),
        }

        # Print summary
        print(f"\n{'='*60}")
        print(f"PIPELINE SUMMARY: {self.pipeline_name}")
        print(f"{'='*60}")
        print(f"Status: {overall_status}")
        print(f"Duration: {total_duration:.2f} seconds")
        print(f"Stages: {summary['successful_stages']}/{summary['total_stages']} successful")
        print(f"Records: {summary['total_records_written']:,} written")
        if summary["total_records_quarantined"] > 0:
            print(f"Quarantined: {summary['total_records_quarantined']:,}")
        print(f"{'='*60}\n")

        return summary


def create_metrics_table(spark: SparkSession, table_name: str) -> None:
    """
    Create the pipeline metrics table if it doesn't exist.

    Args:
        spark: SparkSession
        table_name: Full table name (e.g., "fintech_gold.pipeline_metrics")
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id STRING NOT NULL,
            pipeline_name STRING NOT NULL,
            stage_name STRING NOT NULL,
            status STRING NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            duration_seconds DOUBLE,
            records_read BIGINT,
            records_written BIGINT,
            records_quarantined BIGINT,
            error_message STRING
        )
        USING DELTA
    """
    )


# ============================================================
# DATA FRESHNESS MONITORING
# ============================================================


def check_data_freshness(
    spark: SparkSession,
    table_name: str,
    timestamp_column: str,
    max_age_hours: int = 24,
) -> Dict[str, Any]:
    """
    Check if data in a table is fresh enough.

    Why check freshness?
    - Stale data means something is wrong upstream
    - SLA compliance (data must be updated by X time)
    - Prevent making decisions on outdated information

    Args:
        spark: SparkSession
        table_name: Table to check
        timestamp_column: Column containing data timestamp
        max_age_hours: Maximum acceptable age in hours

    Returns:
        Dict with freshness check results

    Example:
        result = check_data_freshness(spark, "fintech_silver.transactions",
                                      "_ingestion_timestamp", max_age_hours=4)
        if not result["is_fresh"]:
            send_alert(f"Data is stale: {result['hours_old']} hours old")
    """
    df = spark.table(table_name)

    # Get the most recent timestamp
    latest_row = df.agg(spark_max(col(timestamp_column)).alias("latest")).collect()[0]

    latest_timestamp = latest_row["latest"]

    if latest_timestamp is None:
        return {
            "table_name": table_name,
            "is_fresh": False,
            "latest_timestamp": None,
            "hours_old": None,
            "max_age_hours": max_age_hours,
            "message": "No data in table",
        }

    # Calculate age
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    # Handle timezone-naive timestamps
    if latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

    age = now - latest_timestamp
    hours_old = age.total_seconds() / 3600

    is_fresh = hours_old <= max_age_hours

    return {
        "table_name": table_name,
        "is_fresh": is_fresh,
        "latest_timestamp": latest_timestamp,
        "hours_old": round(hours_old, 2),
        "max_age_hours": max_age_hours,
        "message": "Fresh" if is_fresh else f"Stale: {hours_old:.1f} hours old",
    }


# ============================================================
# VOLUME MONITORING
# ============================================================


def check_volume_anomaly(
    spark: SparkSession,
    table_name: str,
    date_column: str,
    threshold_percent: float = 50.0,
    lookback_days: int = 7,
) -> Dict[str, Any]:
    """
    Detect unusual changes in data volume.

    Why check volume?
    - Sudden drop = source system problem, data not flowing
    - Sudden spike = duplicate data, replay, or unusual activity
    - Either way, needs investigation

    Args:
        spark: SparkSession
        table_name: Table to check
        date_column: Date column for grouping
        threshold_percent: Alert if change exceeds this percentage
        lookback_days: Days of history to use for baseline

    Returns:
        Dict with volume check results

    Example:
        result = check_volume_anomaly(spark, "fintech_silver.transactions",
                                       "transaction_date", threshold_percent=50)
        if result["is_anomaly"]:
            send_alert(f"Volume anomaly: {result['percent_change']}% change")
    """
    df = spark.table(table_name)

    # Get daily counts for recent days
    daily_counts = (
        df.groupBy(col(date_column))
        .agg(count("*").alias("row_count"))
        .orderBy(col(date_column).desc())
        .limit(lookback_days + 1)  # +1 for today
        .collect()
    )

    if len(daily_counts) < 2:
        return {
            "table_name": table_name,
            "is_anomaly": False,
            "message": "Not enough data for comparison",
        }

    # Today's count
    today_count = daily_counts[0]["row_count"]

    # Historical average (excluding today)
    historical_counts = [row["row_count"] for row in daily_counts[1:]]
    avg_count = sum(historical_counts) / len(historical_counts)

    # Calculate percent change
    if avg_count > 0:
        percent_change = ((today_count - avg_count) / avg_count) * 100
    else:
        percent_change = 100 if today_count > 0 else 0

    is_anomaly = abs(percent_change) > threshold_percent

    return {
        "table_name": table_name,
        "is_anomaly": is_anomaly,
        "today_count": today_count,
        "average_count": round(avg_count, 0),
        "percent_change": round(percent_change, 2),
        "threshold_percent": threshold_percent,
        "direction": "increase" if percent_change > 0 else "decrease",
        "message": (
            f"{'ANOMALY: ' if is_anomaly else ''}"
            f"{abs(percent_change):.1f}% {'increase' if percent_change > 0 else 'decrease'}"
        ),
    }
