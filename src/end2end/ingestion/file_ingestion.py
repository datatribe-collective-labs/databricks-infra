"""
File-based data ingestion (CSV, JSON, Parquet, etc.).

Educational Notes:
    - Based on Week 2: 06_file_ingestion.py
    - Demonstrates explicit schema definition
    - Shows file format best practices
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from end2end.ingestion.base import BaseIngestion
from end2end.config import PipelineConfig
from end2end.utils import get_logger

logger = get_logger(__name__)


class FileIngestion(BaseIngestion):
    """
    Ingest data from file sources (CSV, JSON, Parquet, etc.).

    Educational Pattern from Week 2 (06_file_ingestion.py):
        - Explicit schema definition prevents inference issues
        - Support for common file formats
        - Header and delimiter handling for CSV
        - Multiline JSON support

    Args:
        config: Pipeline configuration
        source_path: Path to file or directory
        file_format: File format (csv, json, parquet, delta)
        schema: Explicit schema (recommended, optional for parquet/delta)
        options: Additional read options (headers, delimiters, etc.)

    Example:
        >>> from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        >>>
        >>> # Define schema (best practice)
        >>> schema = StructType([
        ...     StructField("transaction_id", StringType(), False),
        ...     StructField("amount", DoubleType(), True),
        ... ])
        >>>
        >>> # Create ingestion task
        >>> config = PipelineConfig(catalog="databricks_course", source_schema="user")
        >>> ingestion = FileIngestion(
        ...     config=config,
        ...     source_path="/data/sales.csv",
        ...     file_format="csv",
        ...     schema=schema,
        ...     options={"header": "true", "delimiter": ","}
        ... )
        >>>
        >>> # Execute
        >>> result = ingestion.execute("sales_transactions")
    """

    def __init__(
        self,
        config: PipelineConfig,
        source_path: str,
        file_format: str = "csv",
        schema: Optional[StructType] = None,
        options: Optional[Dict[str, str]] = None,
    ):
        super().__init__(config, source_name=source_path)
        self.source_path = source_path
        self.file_format = file_format.lower()
        self.schema = schema
        self.options = options or {}

        # Set default options based on file format
        self._set_default_options()

        # Update metadata
        self.metadata.update(
            {
                "source_path": source_path,
                "file_format": file_format,
                "has_schema": schema is not None,
                "options": self.options,
            }
        )

    def _set_default_options(self) -> None:
        """Set default read options based on file format."""
        defaults = {
            "csv": {"header": "true", "inferSchema": "false"},
            "json": {"multiLine": "true"},
            "parquet": {},
            "delta": {},
        }

        format_defaults = defaults.get(self.file_format, {})

        # Merge defaults with user options (user options take precedence)
        for key, value in format_defaults.items():
            if key not in self.options:
                self.options[key] = value

    def read_source(self) -> DataFrame:
        """
        Read data from file source.

        Returns:
            DataFrame with file data

        Raises:
            ValueError: If unsupported file format
            FileNotFoundError: If source path doesn't exist

        Educational Note:
            From Week 2 (06_file_ingestion.py):
            - Use explicit schemas to avoid inference overhead
            - Set correct options for file format
            - Handle missing files gracefully
        """
        logger.info(
            f"Reading {self.file_format} file from: {self.source_path}"
        )

        try:
            # Build reader with format and options
            reader = self.spark.read.format(self.file_format)

            # Apply options
            for key, value in self.options.items():
                reader = reader.option(key, value)

            # Apply schema if provided
            if self.schema:
                reader = reader.schema(self.schema)
                logger.info("Using explicit schema (recommended)")
            else:
                logger.warning(
                    "No schema provided - will infer schema "
                    "(not recommended for production)"
                )

            # Read data
            df = reader.load(self.source_path)

            logger.info(
                f"Successfully read {df.count()} rows from {self.source_path}"
            )

            return df

        except Exception as e:
            logger.error(
                f"Failed to read {self.file_format} file: {self.source_path}",
                exc_info=True,
            )
            raise

    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validate file data.

        Checks:
            - DataFrame not empty
            - Expected columns present (if schema provided)
            - No null values in required columns

        Args:
            df: DataFrame to validate

        Returns:
            Validated DataFrame

        Raises:
            ValueError: If validation fails
        """
        # Call parent validation (empty check)
        df = super().validate_data(df)

        # Additional file-specific validation
        if self.schema:
            # Check if all schema columns are present
            expected_columns = {field.name for field in self.schema.fields}
            actual_columns = set(df.columns)

            missing = expected_columns - actual_columns
            if missing:
                raise ValueError(
                    f"Missing expected columns: {missing}. "
                    f"Found columns: {actual_columns}"
                )

            # Check required (non-nullable) columns have no nulls
            for field in self.schema.fields:
                if not field.nullable:
                    null_count = df.filter(df[field.name].isNull()).count()
                    if null_count > 0:
                        raise ValueError(
                            f"Required column '{field.name}' has {null_count} null values"
                        )

            logger.info("Schema validation passed")

        return df

    def read_with_schema_evolution(
        self,
        target_table: str,
    ) -> DataFrame:
        """
        Read file with schema evolution support.

        Useful when source schema may change over time.
        Merges new columns while preserving existing data.

        Args:
            target_table: Target table name for schema comparison

        Returns:
            DataFrame with evolved schema

        Educational Note:
            Advanced pattern for production pipelines.
            Handles schema changes gracefully.
        """
        # Read new data
        new_df = self.read_source()

        try:
            # Get existing table schema
            table_path = self.config.get_table_path("bronze", target_table)
            existing_df = self.spark.table(table_path).limit(0)

            # Compare schemas
            new_cols = set(new_df.columns)
            existing_cols = set(existing_df.columns)

            added_cols = new_cols - existing_cols
            removed_cols = existing_cols - new_cols

            if added_cols:
                logger.info(f"New columns detected: {added_cols}")

            if removed_cols:
                logger.warning(f"Columns removed in source: {removed_cols}")
                # Add null columns for removed columns
                from pyspark.sql.functions import lit

                for col in removed_cols:
                    new_df = new_df.withColumn(col, lit(None))

            return new_df

        except Exception:
            # Table doesn't exist yet, return as-is
            logger.info("Target table doesn't exist - creating new table")
            return new_df

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"FileIngestion(path='{self.source_path}', "
            f"format='{self.file_format}')"
        )