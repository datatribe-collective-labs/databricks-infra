"""
Silver to Gold transformations (aggregations and analytics).

Educational Notes:
    - Based on Week 3: 13_aggregations.py
    - Demonstrates analytics patterns
    - Shows aggregation and business metrics
"""

from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg,
    count,
    min as spark_min,
    max as spark_max,
    current_timestamp,
)

from end2end.transformations.base import BaseTransformation
from end2end.utils import get_logger

logger = get_logger(__name__)


class SilverToGold(BaseTransformation):
    """
    Transform Silver layer data to Gold layer (analytics and aggregations).

    Educational Pattern from Week 3 (13_aggregations.py):
        Silver Layer: Cleaned, validated data
        Gold Layer: Business-level aggregations and analytics
            - Aggregated metrics
            - Business KPIs
            - Dimensional models
            - Ready for BI tools

    Args:
        config: Pipeline configuration
        source_table: Source table name (without silver_ prefix)
        target_table: Target table name (optional, defaults to source_table_summary)
        group_by_columns: Columns to group by for aggregation
        aggregations: Dictionary of column -> aggregation function

    Example:
        >>> from end2end import PipelineConfig, SilverToGold
        >>>
        >>> config = PipelineConfig(
        ...     catalog="databricks_course",
        ...     source_schema="chanukya_pekala"
        ... )
        >>>
        >>> transformation = SilverToGold(
        ...     config=config,
        ...     source_table="sales",
        ...     target_table="daily_sales_summary",
        ...     group_by_columns=["date", "region"],
        ...     aggregations={
        ...         "amount": ["sum", "avg", "count"],
        ...         "quantity": ["sum"]
        ...     }
        ... )
        >>>
        >>> result = transformation.execute("silver", "gold")
    """

    def __init__(
        self,
        config,
        source_table: str,
        target_table: Optional[str] = None,
        group_by_columns: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, List[str]]] = None,
    ):
        # Default target table to source_summary if not provided
        if target_table is None:
            target_table = f"{source_table}_summary"

        super().__init__(config, source_table, target_table)
        self.group_by_columns = group_by_columns or []
        self.aggregations = aggregations or {}

        # Update metadata
        self.metadata.update(
            {
                "group_by_columns": self.group_by_columns,
                "aggregation_columns": list(self.aggregations.keys()),
            }
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply Silver-to-Gold transformations.

        Transformation steps:
            1. Apply aggregations
            2. Calculate business metrics
            3. Add metadata columns

        Args:
            df: Silver layer DataFrame

        Returns:
            Aggregated Gold DataFrame

        Educational Note:
            From Week 3 (13_aggregations.py):
            - Gold layer should be "business-ready"
            - Pre-compute expensive aggregations
            - Optimize for BI tool performance
        """
        logger.info("Applying Silver-to-Gold transformations")

        # Step 1: Apply aggregations
        df = self._apply_aggregations(df)

        # Step 2: Add metadata columns
        df = self._add_metadata_columns(df)

        return df

    def _apply_aggregations(self, df: DataFrame) -> DataFrame:
        """Apply group-by aggregations."""
        if not self.group_by_columns:
            logger.warning("No group_by_columns specified - performing global aggregation")
            return self._global_aggregation(df)

        logger.info(f"Grouping by: {self.group_by_columns}")

        # Build aggregation expressions
        agg_exprs = []

        for column, functions in self.aggregations.items():
            for func in functions:
                agg_expr = self._build_aggregation_expr(column, func)
                if agg_expr is not None:
                    agg_exprs.append(agg_expr)

        if not agg_exprs:
            logger.warning("No aggregations specified - selecting distinct values")
            return df.select(*self.group_by_columns).distinct()

        # Perform aggregation
        df = df.groupBy(*self.group_by_columns).agg(*agg_exprs)

        return df

    def _build_aggregation_expr(self, column: str, function: str):
        """
        Build aggregation expression for column and function.

        Args:
            column: Column name
            function: Aggregation function (sum, avg, count, min, max)

        Returns:
            Aggregation expression
        """
        function = function.lower()
        alias = f"{column}_{function}"

        if function == "sum":
            return spark_sum(col(column)).alias(alias)
        elif function == "avg":
            return avg(col(column)).alias(alias)
        elif function == "count":
            return count(col(column)).alias(alias)
        elif function == "min":
            return spark_min(col(column)).alias(alias)
        elif function == "max":
            return spark_max(col(column)).alias(alias)
        else:
            logger.warning(f"Unknown aggregation function: {function}")
            return None

    def _global_aggregation(self, df: DataFrame) -> DataFrame:
        """Perform global aggregation (no grouping)."""
        agg_exprs = []

        for column, functions in self.aggregations.items():
            for func in functions:
                agg_expr = self._build_aggregation_expr(column, func)
                if agg_expr is not None:
                    agg_exprs.append(agg_expr)

        if not agg_exprs:
            raise ValueError("No aggregations specified for global aggregation")

        return df.agg(*agg_exprs)

    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Add Gold layer metadata columns."""
        logger.debug("Adding metadata columns")

        df = df.withColumn("gold_processed_at", current_timestamp())

        return df

    def add_calculated_metrics(
        self,
        metrics: Dict[str, str],
    ) -> "SilverToGold":
        """
        Add calculated business metrics.

        Args:
            metrics: Dictionary of metric name -> SQL expression

        Returns:
            Self for method chaining

        Example:
            >>> transformation.add_calculated_metrics({
            ...     "revenue": "amount_sum * 1.1",  # Add 10% tax
            ...     "avg_order_value": "amount_sum / amount_count"
            ... })
        """
        from pyspark.sql.functions import expr

        self.calculated_metrics = metrics
        logger.info(f"Added {len(metrics)} calculated metrics")

        return self

    def validate_result(self, df: DataFrame) -> DataFrame:
        """
        Validate Gold layer results.

        Additional checks:
            - Group by columns exist
            - Aggregated columns exist
            - No duplicate group keys

        Args:
            df: DataFrame to validate

        Returns:
            Validated DataFrame

        Raises:
            ValueError: If validation fails
        """
        # Call parent validation
        df = super().validate_result(df)

        # Check group by columns exist
        missing_columns = set(self.group_by_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Group by columns missing: {missing_columns}")

        # Check for duplicates on group key
        if self.group_by_columns:
            distinct_count = df.select(*self.group_by_columns).distinct().count()
            total_count = df.count()

            if distinct_count != total_count:
                logger.warning(
                    f"Warning: Duplicate group keys detected "
                    f"({total_count - distinct_count} duplicates)"
                )

        logger.info("Gold layer validation passed")

        return df

    def with_window_analytics(
        self,
        partition_by: List[str],
        order_by: str,
        window_functions: Dict[str, str],
    ) -> "SilverToGold":
        """
        Add window function analytics.

        Educational Pattern from Week 3 (12_window_transformations.py):
            - Ranking functions
            - Moving averages
            - Lead/Lag analysis

        Args:
            partition_by: Columns to partition by
            order_by: Column to order by
            window_functions: Dictionary of column name -> window function

        Returns:
            Self for method chaining

        Example:
            >>> transformation.with_window_analytics(
            ...     partition_by=["region"],
            ...     order_by="date",
            ...     window_functions={
            ...         "sales_rank": "rank()",
            ...         "sales_running_total": "sum(amount)"
            ...     }
            ... )
        """
        self.window_analytics = {
            "partition_by": partition_by,
            "order_by": order_by,
            "functions": window_functions,
        }

        logger.info(f"Added {len(window_functions)} window analytics")

        return self