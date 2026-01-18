"""
Utility Modules
===============

This package contains reusable utility functions:
- spark_utils: Read, write, transform data
- data_quality: Validate data quality
- monitoring: Track pipeline execution
- alerting: Send notifications

Usage:
    from src.utils.spark_utils import read_csv_to_df, write_delta_table
    from src.utils.data_quality import DataQualityChecker
    from src.utils.monitoring import PipelineMonitor
    from src.utils.alerting import AlertManager, Severity
"""

from .spark_utils import (
    get_spark,
    read_csv_to_df,
    read_json_to_df,
    read_delta_table,
    write_delta_table,
    register_delta_table,
    upsert_delta_table,
    add_ingestion_metadata,
    add_processing_metadata,
    standardize_string_columns,
    deduplicate_by_key,
    add_surrogate_key,
    optimize_delta_table,
    vacuum_delta_table,
    get_table_row_count,
    table_exists,
    delta_table_exists,
)

from .data_quality import (
    DQSeverity,
    DQCheckType,
    DQCheckResult,
    DQReport,
    DataQualityChecker,
    quick_quality_check,
)

from .monitoring import (
    PipelineStatus,
    StageMetrics,
    PipelineMonitor,
    create_metrics_table,
    check_data_freshness,
    check_volume_anomaly,
)

from .alerting import (
    Severity,
    Alert,
    AlertManager,
    send_pipeline_failure_alert,
    send_dq_failure_alert,
    send_freshness_alert,
)

__all__ = [
    # Spark utils
    "get_spark",
    "read_csv_to_df",
    "read_json_to_df",
    "read_delta_table",
    "write_delta_table",
    "register_delta_table",
    "upsert_delta_table",
    "add_ingestion_metadata",
    "add_processing_metadata",
    "standardize_string_columns",
    "deduplicate_by_key",
    "add_surrogate_key",
    "optimize_delta_table",
    "vacuum_delta_table",
    "get_table_row_count",
    "table_exists",
    "delta_table_exists",
    # Data quality
    "DQSeverity",
    "DQCheckType",
    "DQCheckResult",
    "DQReport",
    "DataQualityChecker",
    "quick_quality_check",
    # Monitoring
    "PipelineStatus",
    "StageMetrics",
    "PipelineMonitor",
    "create_metrics_table",
    "check_data_freshness",
    "check_volume_anomaly",
    # Alerting
    "Severity",
    "Alert",
    "AlertManager",
    "send_pipeline_failure_alert",
    "send_dq_failure_alert",
    "send_freshness_alert",
]
