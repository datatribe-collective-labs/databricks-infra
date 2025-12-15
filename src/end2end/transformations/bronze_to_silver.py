"""
Bronze to Silver transformations (data cleaning and validation).

Educational Notes:
    - Based on Week 3: 11_simple_transformations.py
    - Demonstrates data quality patterns
    - Shows type conversions and cleaning logic
"""

from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    regexp_replace,
    when,
    coalesce,
    lit,
    current_timestamp,
)

from end2end.transformations.base import BaseTransformation
from end2end.utils import get_logger

logger = get_logger(__name__)


class BronzeToSilver(BaseTransformation):
    """
    Transform Bronze layer data to Silver layer (cleaning and validation).

    Educational Pattern from Week 3 (11_simple_transformations.py):
        Bronze Layer: Raw data as ingested
        Silver Layer: Cleaned, validated, conforming data
            - Remove duplicates
            - Handle nulls
            - Standardize formats
            - Type conversions
            - Add metadata columns

    Args:
        config: Pipeline configuration
        source_table: Source table name (without bronze_ prefix)
        target_table: Target table name (optional, defaults to source_table)
        remove_duplicates: Remove duplicate rows (default: True)
        nullable_columns: Columns allowed to be null
        required_columns: Columns that must have values
        transformations: Custom column transformations

    Example:
        >>> from end2end import PipelineConfig, BronzeToSilver
        >>>
        >>> config = PipelineConfig(
        ...     catalog="databricks_course",
        ...     source_schema="chanukya_pekala"
        ... )
        >>>
        >>> transformation = BronzeToSilver(
        ...     config=config,
        ...     source_table="sales_transactions",
        ...     required_columns=["transaction_id", "amount"],
        ...     transformations={
        ...         "email": "lower(trim(email))",
        ...         "status": "upper(status)"
        ...     }
        ... )
        >>>
        >>> result = transformation.execute("bronze", "silver")
    """

    def __init__(
        self,
        config,
        source_table: str,
        target_table: Optional[str] = None,
        remove_duplicates: bool = True,
        nullable_columns: Optional[List[str]] = None,
        required_columns: Optional[List[str]] = None,
        transformations: Optional[Dict[str, str]] = None,
    ):
        super().__init__(config, source_table, target_table)
        self.remove_duplicates = remove_duplicates
        self.nullable_columns = nullable_columns or []
        self.required_columns = required_columns or []
        self.transformations = transformations or {}

        # Update metadata
        self.metadata.update(
            {
                "remove_duplicates": remove_duplicates,
                "required_columns": self.required_columns,
                "custom_transformations": len(self.transformations),
            }
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply Bronze-to-Silver transformations.

        Transformation steps:
            1. Add metadata columns (processed timestamp)
            2. Remove duplicates (if enabled)
            3. Filter out records with missing required fields
            4. Apply custom transformations
            5. Standardize string columns

        Args:
            df: Bronze layer DataFrame

        Returns:
            Cleaned and validated Silver DataFrame

        Educational Note:
            From Week 3 (11_simple_transformations.py):
            - Silver layer should be "clean" data
            - Apply business rules and validations
            - Maintain data lineage
        """
        logger.info("Applying Bronze-to-Silver transformations")

        # Step 1: Add metadata columns
        df = self._add_metadata_columns(df)

        # Step 2: Remove duplicates
        if self.remove_duplicates:
            df = self._remove_duplicates(df)

        # Step 3: Filter required columns
        df = self._filter_required_columns(df)

        # Step 4: Apply custom transformations
        df = self._apply_transformations(df)

        # Step 5: Standardize strings
        df = self._standardize_strings(df)

        return df

    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Add Silver layer metadata columns."""
        logger.debug("Adding metadata columns")

        df = df.withColumn("silver_processed_at", current_timestamp())

        return df

    def _remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicate rows."""
        original_count = df.count()

        df = df.dropDuplicates()

        new_count = df.count()
        duplicates_removed = original_count - new_count

        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate rows")
            self.metadata["duplicates_removed"] = duplicates_removed

        return df

    def _filter_required_columns(self, df: DataFrame) -> DataFrame:
        """Filter out records with missing required columns."""
        if not self.required_columns:
            return df

        original_count = df.count()

        # Build filter condition for all required columns
        filter_condition = col(self.required_columns[0]).isNotNull()

        for column in self.required_columns[1:]:
            filter_condition = filter_condition & col(column).isNotNull()

        df = df.filter(filter_condition)

        new_count = df.count()
        filtered_rows = original_count - new_count

        if filtered_rows > 0:
            logger.warning(
                f"Filtered {filtered_rows} rows with missing required columns: "
                f"{self.required_columns}"
            )
            self.metadata["rows_filtered"] = filtered_rows

        return df

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply custom column transformations."""
        if not self.transformations:
            return df

        logger.debug(f"Applying {len(self.transformations)} custom transformations")

        for column, transformation in self.transformations.items():
            if column in df.columns:
                # Use expr to evaluate transformation string
                from pyspark.sql.functions import expr

                df = df.withColumn(column, expr(transformation))
                logger.debug(f"Applied transformation to {column}: {transformation}")

        return df

    def _standardize_strings(self, df: DataFrame) -> DataFrame:
        """Standardize string columns (trim whitespace)."""
        string_columns = [
            field.name for field in df.schema.fields if str(field.dataType) == "StringType()"
        ]

        for column in string_columns:
            if column not in self.transformations:  # Don't override custom transformations
                df = df.withColumn(column, trim(col(column)))

        logger.debug(f"Standardized {len(string_columns)} string columns")

        return df

    def add_business_rules(
        self,
        rules: Dict[str, str],
    ) -> "BronzeToSilver":
        """
        Add business rule validations.

        Args:
            rules: Dictionary of rule name -> SQL expression

        Returns:
            Self for method chaining

        Example:
            >>> transformation.add_business_rules({
            ...     "valid_amount": "amount > 0",
            ...     "valid_date": "transaction_date <= current_date()"
            ... })
        """
        for rule_name, rule_expr in rules.items():
            logger.info(f"Adding business rule: {rule_name} = {rule_expr}")
            self.transformations[f"_rule_{rule_name}"] = rule_expr

        return self

    def validate_result(self, df: DataFrame) -> DataFrame:
        """
        Validate Silver layer results.

        Additional checks:
            - Required columns exist
            - No nulls in required columns
            - Row count > 0

        Args:
            df: DataFrame to validate

        Returns:
            Validated DataFrame

        Raises:
            ValueError: If validation fails
        """
        # Call parent validation
        df = super().validate_result(df)

        # Check required columns exist
        missing_columns = set(self.required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Required columns missing: {missing_columns}")

        # Check for nulls in required columns
        for column in self.required_columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                logger.warning(
                    f"Warning: {null_count} null values in required column '{column}'"
                )

        logger.info("Silver layer validation passed")

        return df