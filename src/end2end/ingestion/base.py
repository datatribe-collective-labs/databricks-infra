"""
Base class for data ingestion operations.

Educational Notes:
    - Demonstrates abstract base class pattern (production best practice)
    - Shows common ingestion patterns from Week 2
    - Implements error handling and retry logic
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import time

from end2end.config import PipelineConfig
from end2end.utils import get_logger, get_spark

logger = get_logger(__name__)


class BaseIngestion(ABC):
    """
    Abstract base class for all ingestion operations.

    Provides common functionality:
        - Spark session management
        - Configuration handling
        - Error handling and retry logic
        - Metadata tracking

    Educational Pattern from Week 2:
        All ingestion notebooks (06-09) follow similar patterns:
        1. Read from source
        2. Apply schema
        3. Validate data
        4. Write to Bronze layer

    Args:
        config: Pipeline configuration
        source_name: Descriptive name for the data source
        spark: SparkSession (optional, uses active session if not provided)

    Example:
        >>> class MyIngestion(BaseIngestion):
        ...     def read_source(self) -> DataFrame:
        ...         return self.spark.read.csv(self.source_path)
        ...
        >>> config = PipelineConfig(catalog="databricks_course", source_schema="user")
        >>> ingestion = MyIngestion(config, "my_source")
        >>> df = ingestion.execute()
    """

    def __init__(
        self,
        config: PipelineConfig,
        source_name: str,
        spark: Optional[SparkSession] = None,
    ):
        self.config = config
        self.source_name = source_name
        self.spark = spark or get_spark()
        self.metadata: Dict[str, Any] = {
            "source": source_name,
            "start_time": None,
            "end_time": None,
            "rows_read": 0,
            "status": "pending",
        }

    @abstractmethod
    def read_source(self) -> DataFrame:
        """
        Read data from the source.

        Must be implemented by subclasses for specific source types.

        Returns:
            DataFrame with raw data

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement read_source()")

    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validate data after reading.

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
            raise ValueError(f"No data read from source: {self.source_name}")

        row_count = df.count()
        logger.info(f"Validation passed: {row_count} rows from {self.source_name}")
        self.metadata["rows_read"] = row_count

        return df

    def write_to_bronze(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> str:
        """
        Write DataFrame to Bronze layer table.

        Educational Pattern from Week 2 (06_file_ingestion.py):
            - Bronze layer stores raw data
            - Uses Delta Lake format
            - Preserves source schema

        Args:
            df: DataFrame to write
            table_name: Target table name (without layer prefix)
            mode: Write mode (append, overwrite, merge)

        Returns:
            Full table path where data was written

        Example:
            >>> table_path = self.write_to_bronze(df, "sales_transactions")
            >>> # Result: databricks_course.user_schema.bronze_sales_transactions
        """
        table_path = self.config.get_table_path("bronze", table_name)

        logger.info(
            f"Writing {df.count()} rows to {table_path} (mode: {mode})"
        )

        try:
            df.write.format("delta").mode(mode).saveAsTable(table_path)
            logger.info(f"Successfully wrote data to {table_path}")
            return table_path
        except Exception as e:
            logger.error(f"Failed to write to {table_path}: {e}", exc_info=True)
            raise

    def execute(
        self,
        target_table: str,
        write_mode: str = "append",
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Execute the complete ingestion pipeline with retry logic.

        Pipeline steps:
            1. Read from source
            2. Validate data
            3. Write to Bronze layer
            4. Track metadata

        Args:
            target_table: Target table name (without layer prefix)
            write_mode: Write mode (append, overwrite)
            max_retries: Maximum retry attempts on failure

        Returns:
            Metadata dictionary with execution details

        Example:
            >>> ingestion = FileIngestion(config, "sales.csv")
            >>> result = ingestion.execute("sales_transactions")
            >>> print(f"Status: {result['status']}, Rows: {result['rows_read']}")
        """
        self.metadata["start_time"] = datetime.utcnow().isoformat()
        retry_count = 0

        while retry_count <= max_retries:
            try:
                logger.info(
                    f"Starting ingestion: {self.source_name} -> {target_table} "
                    f"(attempt {retry_count + 1}/{max_retries + 1})"
                )

                # Read from source
                df = self.read_source()

                # Validate data
                df = self.validate_data(df)

                # Write to Bronze
                table_path = self.write_to_bronze(df, target_table, mode=write_mode)

                # Update metadata
                self.metadata["status"] = "success"
                self.metadata["end_time"] = datetime.utcnow().isoformat()
                self.metadata["target_table"] = table_path
                self.metadata["retry_count"] = retry_count

                logger.info(
                    f"Ingestion completed successfully: {self.metadata['rows_read']} rows"
                )

                return self.metadata

            except Exception as e:
                retry_count += 1
                logger.warning(
                    f"Ingestion attempt {retry_count} failed: {e}",
                    exc_info=True,
                )

                if retry_count > max_retries:
                    self.metadata["status"] = "failed"
                    self.metadata["error"] = str(e)
                    self.metadata["end_time"] = datetime.utcnow().isoformat()
                    logger.error(f"Ingestion failed after {max_retries} retries")
                    raise

                # Exponential backoff
                wait_time = 2**retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        return self.metadata

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get ingestion metadata.

        Returns:
            Dictionary with execution metadata
        """
        return self.metadata

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return f"{self.__class__.__name__}(source='{self.source_name}')"