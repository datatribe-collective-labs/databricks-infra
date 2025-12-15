"""
Spark utility functions for data pipeline operations.

Educational Notes:
    - Demonstrates Spark session management (Week 1)
    - Shows Delta Lake optimization patterns (Week 1)
    - Implements performance best practices from the course
"""

from typing import Optional, List
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: str = "end2end_pipeline",
    config: Optional[dict] = None,
) -> SparkSession:
    """
    Create or get Spark session with optimized configuration.

    In Databricks notebooks, this returns the existing spark session.
    For local development, creates a new session with Delta Lake support.

    Args:
        app_name: Application name for Spark UI
        config: Additional Spark configuration (optional)

    Returns:
        SparkSession instance

    Example:
        >>> spark = create_spark_session(
        ...     app_name="my_pipeline",
        ...     config={"spark.sql.shuffle.partitions": "8"}
        ... )

    Educational Note:
        In Databricks, spark session is pre-configured with:
        - Unity Catalog integration
        - Delta Lake support
        - Optimized cluster settings
        (See Week 1: 04_spark_on_databricks.py)
    """
    builder = SparkSession.builder.appName(app_name)

    # Apply custom configuration
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)

    # For local development (not Databricks)
    try:
        spark = builder.getOrCreate()
    except Exception as e:
        logger.warning(f"Failed to create Spark session: {e}")
        # Fallback to Delta Lake configuration for local development
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )

    logger.info(
        f"Spark session created: {spark.version}, "
        f"App: {app_name}, "
        f"Master: {spark.sparkContext.master}"
    )

    return spark


def get_spark() -> SparkSession:
    """
    Get active Spark session.

    In Databricks notebooks, use the pre-configured 'spark' variable.
    For package usage, get or create session.

    Returns:
        Active SparkSession

    Example:
        >>> spark = get_spark()
        >>> df = spark.read.table("catalog.schema.table")
    """
    try:
        return SparkSession.getActiveSession()
    except Exception:
        return create_spark_session()


def optimize_table(
    table_name: str,
    zorder_columns: Optional[List[str]] = None,
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Optimize Delta Lake table with optional Z-ordering.

    Educational Note from Week 1 (05_delta_lake_concepts_explained.py):
        - OPTIMIZE: Compacts small files into larger ones
        - ZORDER: Co-locates related data for faster queries
        - Run after large ingestion batches

    Args:
        table_name: Full table path (catalog.schema.table)
        zorder_columns: Columns for Z-ordering (optional)
        spark: SparkSession (uses active session if not provided)

    Example:
        >>> # Basic optimization
        >>> optimize_table("databricks_course.user.bronze_sales")
        >>>
        >>> # With Z-ordering on date column
        >>> optimize_table(
        ...     "databricks_course.user.silver_sales",
        ...     zorder_columns=["transaction_date"]
        ... )

    Performance Impact:
        - Reduces number of data files
        - Improves query performance
        - Especially beneficial for large tables
    """
    if spark is None:
        spark = get_spark()

    logger.info(f"Optimizing table: {table_name}")

    try:
        if zorder_columns:
            zorder_cols = ", ".join(zorder_columns)
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
            logger.info(f"Optimized {table_name} with Z-ORDER on {zorder_cols}")
        else:
            spark.sql(f"OPTIMIZE {table_name}")
            logger.info(f"Optimized {table_name}")
    except Exception as e:
        logger.error(f"Failed to optimize {table_name}: {e}", exc_info=True)
        raise


def vacuum_table(
    table_name: str,
    retention_hours: int = 168,  # 7 days default
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Vacuum Delta Lake table to remove old file versions.

    Educational Note from Week 1 (05_delta_lake_concepts_explained.py):
        - VACUUM: Removes uncommitted files and old versions
        - Default retention: 7 days (168 hours)
        - Enables time travel up to retention period
        - Reduces storage costs

    Args:
        table_name: Full table path (catalog.schema.table)
        retention_hours: Retention period in hours (default: 168 = 7 days)
        spark: SparkSession (uses active session if not provided)

    Example:
        >>> # Vacuum with default 7-day retention
        >>> vacuum_table("databricks_course.user.bronze_sales")
        >>>
        >>> # Vacuum with custom 30-day retention
        >>> vacuum_table("databricks_course.user.gold_summary", retention_hours=720)

    Warning:
        - Cannot time travel beyond retention period after VACUUM
        - Ensure retention period meets your recovery requirements
        - Production recommendation: 30 days (720 hours)
    """
    if spark is None:
        spark = get_spark()

    logger.info(f"Vacuuming table: {table_name} (retention: {retention_hours}h)")

    try:
        spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        logger.info(f"Vacuumed {table_name} with {retention_hours}h retention")
    except Exception as e:
        logger.error(f"Failed to vacuum {table_name}: {e}", exc_info=True)
        raise


def analyze_table(
    table_name: str,
    compute_statistics: bool = True,
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Analyze table to update statistics for query optimization.

    Educational Note:
        - ANALYZE TABLE: Updates table statistics
        - Helps Spark optimizer make better query plans
        - Run after major data changes

    Args:
        table_name: Full table path (catalog.schema.table)
        compute_statistics: Compute column statistics (default: True)
        spark: SparkSession (uses active session if not provided)

    Example:
        >>> analyze_table("databricks_course.user.silver_sales")
    """
    if spark is None:
        spark = get_spark()

    logger.info(f"Analyzing table: {table_name}")

    try:
        if compute_statistics:
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
            logger.info(f"Analyzed {table_name} with column statistics")
        else:
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
            logger.info(f"Analyzed {table_name}")
    except Exception as e:
        logger.error(f"Failed to analyze {table_name}: {e}", exc_info=True)
        raise


def get_table_info(table_name: str, spark: Optional[SparkSession] = None) -> dict:
    """
    Get comprehensive table information.

    Args:
        table_name: Full table path (catalog.schema.table)
        spark: SparkSession (uses active session if not provided)

    Returns:
        Dictionary with table metadata

    Example:
        >>> info = get_table_info("databricks_course.user.bronze_sales")
        >>> print(f"Rows: {info['num_rows']}, Files: {info['num_files']}")
    """
    if spark is None:
        spark = get_spark()

    try:
        # Get table details
        details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]

        return {
            "name": table_name,
            "format": details["format"],
            "location": details["location"],
            "num_files": details["numFiles"],
            "size_bytes": details["sizeInBytes"],
            "created_at": details["createdAt"],
            "last_modified": details["lastModified"],
        }
    except Exception as e:
        logger.error(f"Failed to get table info for {table_name}: {e}", exc_info=True)
        return {}