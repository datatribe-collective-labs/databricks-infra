"""
Base class for data transformations.

Educational Notes:
    - Demonstrates transformation patterns from Week 3
    - Shows medallion architecture (Bronze -> Silver -> Gold)
    - Implements reusable transformation logic
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime

from end2end.config import PipelineConfig
from end2end.utils import get_logger, get_spark

logger = get_logger(__name__)


class BaseTransformation(ABC):
    """
    Abstract base class for all transformation operations.

    Provides common functionality:
        - Spark session management
        - Configuration handling
        - Read/write operations
        - Metadata tracking

    Educational Pattern from Week 3:
        All transformation notebooks (11-13) follow similar patterns:
        1. Read from source layer
        2. Apply transformations
        3. Validate results
        4. Write to target layer

    Args:
        config: Pipeline configuration
        source_table: Source table name (without layer prefix)
        target_table: Target table name (without layer prefix)
        spark: SparkSession (optional, uses active session if not provided)

    Example:
        >>> class MyTransformation(BaseTransformation):
        ...     def transform(self, df: DataFrame) -> DataFrame:
        ...         return df.filter(df["amount"] > 0)
        ...
        >>> config = PipelineConfig(catalog="databricks_course", source_schema="user")
        >>> transformation = MyTransformation(
        ...     config=config,
        ...     source_table="sales",
        ...     target_table="sales_positive"
        ... )
        >>> result = transformation.execute(source_layer="bronze", target_layer="silver")
    """

    def __init__(
        self,
        config: PipelineConfig,
        source_table: str,
        target_table: Optional[str] = None,
        spark: Optional[SparkSession] = None,
    ):
        self.config = config
        self.source_table = source_table
        self.target_table = target_table or source_table
        self.spark = spark or get_spark()
        self.metadata: Dict[str, Any] = {
            "source": source_table,
            "target": self.target_table,
            "start_time": None,
            "end_time": None,
            "rows_read": 0,
            "rows_written": 0,
            "status": "pending",
        }

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to DataFrame.

        Must be implemented by subclasses for specific transformation logic.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement transform()")

    def read_source(self, source_layer: str) -> DataFrame:
        """
        Read data from source table.

        Args:
            source_layer: Source data layer (bronze, silver)

        Returns:
            DataFrame with source data

        Example:
            >>> df = self.read_source("bronze")
        """
        table_path = self.config.get_table_path(source_layer, self.source_table)

        logger.info(f"Reading from: {table_path}")

        try:
            df = self.spark.table(table_path)
            row_count = df.count()

            self.metadata["rows_read"] = row_count
            self.metadata["source_path"] = table_path

            logger.info(f"Read {row_count} rows from {table_path}")

            return df

        except Exception as e:
            logger.error(f"Failed to read from {table_path}: {e}", exc_info=True)
            raise

    def write_target(
        self,
        df: DataFrame,
        target_layer: str,
        mode: str = "overwrite",
    ) -> str:
        """
        Write DataFrame to target table.

        Args:
            df: DataFrame to write
            target_layer: Target data layer (silver, gold)
            mode: Write mode (overwrite, append)

        Returns:
            Full table path where data was written

        Example:
            >>> table_path = self.write_target(df, "silver")
        """
        table_path = self.config.get_table_path(target_layer, self.target_table)

        logger.info(f"Writing to: {table_path} (mode: {mode})")

        try:
            row_count = df.count()

            df.write.format("delta").mode(mode).saveAsTable(table_path)

            self.metadata["rows_written"] = row_count
            self.metadata["target_path"] = table_path

            logger.info(f"Wrote {row_count} rows to {table_path}")

            return table_path

        except Exception as e:
            logger.error(f"Failed to write to {table_path}: {e}", exc_info=True)
            raise

    def validate_result(self, df: DataFrame) -> DataFrame:
        """
        Validate transformation results.

        Default implementation checks for empty DataFrame.
        Override in subclasses for custom validation.

        Args:
            df: DataFrame to validate

        Returns:
            Validated DataFrame

        Raises:
            ValueError: If validation fails
        """
        if df.isEmpty():
            raise ValueError(f"Transformation produced no data: {self.source_table}")

        row_count = df.count()
        logger.info(f"Validation passed: {row_count} rows")

        return df

    def execute(
        self,
        source_layer: str,
        target_layer: str,
        write_mode: str = "overwrite",
    ) -> Dict[str, Any]:
        """
        Execute the complete transformation pipeline.

        Pipeline steps:
            1. Read from source layer
            2. Apply transformations
            3. Validate results
            4. Write to target layer
            5. Track metadata

        Args:
            source_layer: Source data layer (bronze, silver)
            target_layer: Target data layer (silver, gold)
            write_mode: Write mode (overwrite, append)

        Returns:
            Metadata dictionary with execution details

        Example:
            >>> transformation = MyTransformation(config, "sales", "sales_cleaned")
            >>> result = transformation.execute("bronze", "silver")
            >>> print(f"Rows: {result['rows_read']} -> {result['rows_written']}")
        """
        self.metadata["start_time"] = datetime.utcnow().isoformat()

        try:
            logger.info(
                f"Starting transformation: {self.source_table} "
                f"({source_layer} -> {target_layer})"
            )

            # Read from source
            df = self.read_source(source_layer)

            # Apply transformations
            transformed_df = self.transform(df)

            # Validate results
            validated_df = self.validate_result(transformed_df)

            # Write to target
            table_path = self.write_target(validated_df, target_layer, mode=write_mode)

            # Update metadata
            self.metadata["status"] = "success"
            self.metadata["end_time"] = datetime.utcnow().isoformat()

            logger.info(
                f"Transformation completed: {self.metadata['rows_read']} -> "
                f"{self.metadata['rows_written']} rows"
            )

            return self.metadata

        except Exception as e:
            self.metadata["status"] = "failed"
            self.metadata["error"] = str(e)
            self.metadata["end_time"] = datetime.utcnow().isoformat()
            logger.error(f"Transformation failed: {e}", exc_info=True)
            raise

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get transformation metadata.

        Returns:
            Dictionary with execution metadata
        """
        return self.metadata

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"{self.__class__.__name__}("
            f"source='{self.source_table}', "
            f"target='{self.target_table}')"
        )