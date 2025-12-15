"""
Data quality validation functions.

Educational Notes:
    - Demonstrates data quality patterns
    - Shows schema validation
    - Implements data profiling
"""

from typing import Dict, List, Optional, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, count, sum as spark_sum, isnan

from end2end.utils import get_logger

logger = get_logger(__name__)


class SchemaValidator:
    """
    Validate DataFrame schema against expected schema.

    Educational Pattern:
        - Ensures data conforms to expected structure
        - Detects schema drift
        - Validates data types

    Example:
        >>> from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        >>>
        >>> expected_schema = StructType([
        ...     StructField("transaction_id", StringType(), False),
        ...     StructField("amount", DoubleType(), False),
        ... ])
        >>>
        >>> validator = SchemaValidator(expected_schema)
        >>> is_valid = validator.validate(df)
    """

    def __init__(self, expected_schema: StructType):
        self.expected_schema = expected_schema

    def validate(self, df: DataFrame) -> bool:
        """
        Validate DataFrame against expected schema.

        Args:
            df: DataFrame to validate

        Returns:
            True if schema matches, False otherwise
        """
        actual_schema = df.schema

        # Check column names
        expected_columns = {field.name for field in self.expected_schema.fields}
        actual_columns = {field.name for field in actual_schema.fields}

        if expected_columns != actual_columns:
            missing = expected_columns - actual_columns
            extra = actual_columns - expected_columns

            if missing:
                logger.error(f"Missing columns: {missing}")
            if extra:
                logger.warning(f"Extra columns: {extra}")

            return False

        # Check data types
        for expected_field in self.expected_schema.fields:
            actual_field = actual_schema[expected_field.name]

            if str(expected_field.dataType) != str(actual_field.dataType):
                logger.error(
                    f"Type mismatch for column '{expected_field.name}': "
                    f"expected {expected_field.dataType}, "
                    f"got {actual_field.dataType}"
                )
                return False

        logger.info("Schema validation passed")
        return True

    def get_schema_diff(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get detailed schema differences.

        Args:
            df: DataFrame to compare

        Returns:
            Dictionary with schema differences
        """
        actual_schema = df.schema

        expected_columns = {field.name: field for field in self.expected_schema.fields}
        actual_columns = {field.name: field for field in actual_schema.fields}

        return {
            "missing_columns": list(set(expected_columns.keys()) - set(actual_columns.keys())),
            "extra_columns": list(set(actual_columns.keys()) - set(expected_columns.keys())),
            "type_mismatches": [
                {
                    "column": name,
                    "expected": str(expected_columns[name].dataType),
                    "actual": str(actual_columns[name].dataType),
                }
                for name in expected_columns.keys() & actual_columns.keys()
                if str(expected_columns[name].dataType) != str(actual_columns[name].dataType)
            ],
        }


class DataQualityCheck:
    """
    Perform data quality checks on DataFrames.

    Educational Pattern:
        - Profile data quality metrics
        - Detect data issues
        - Generate quality reports

    Example:
        >>> checker = DataQualityCheck(df)
        >>> report = checker.run_all_checks()
        >>> print(f"Completeness: {report['completeness_score']:.2%}")
    """

    def __init__(self, df: DataFrame):
        self.df = df
        self.total_rows = df.count()

    def check_completeness(self) -> Dict[str, float]:
        """
        Check data completeness (null percentages).

        Returns:
            Dictionary of column -> null percentage
        """
        logger.info("Checking data completeness")

        completeness = {}

        for column in self.df.columns:
            null_count = self.df.filter(col(column).isNull()).count()
            null_percentage = null_count / self.total_rows if self.total_rows > 0 else 0
            completeness[column] = 1.0 - null_percentage

        return completeness

    def check_duplicates(self, key_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Check for duplicate rows.

        Args:
            key_columns: Columns to check for duplicates (None = all columns)

        Returns:
            Dictionary with duplicate statistics
        """
        logger.info("Checking for duplicates")

        if key_columns:
            distinct_count = self.df.select(*key_columns).distinct().count()
        else:
            distinct_count = self.df.distinct().count()

        duplicate_count = self.total_rows - distinct_count
        duplicate_percentage = duplicate_count / self.total_rows if self.total_rows > 0 else 0

        return {
            "total_rows": self.total_rows,
            "distinct_rows": distinct_count,
            "duplicate_rows": duplicate_count,
            "duplicate_percentage": duplicate_percentage,
        }

    def check_value_ranges(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Check if numeric column values are within expected range.

        Args:
            column: Column name
            min_value: Minimum expected value (optional)
            max_value: Maximum expected value (optional)

        Returns:
            Dictionary with range validation results
        """
        logger.info(f"Checking value ranges for column: {column}")

        stats = self.df.select(column).summary("min", "max").collect()
        actual_min = float(stats[0][column])
        actual_max = float(stats[1][column])

        violations = 0

        if min_value is not None:
            violations += self.df.filter(col(column) < min_value).count()

        if max_value is not None:
            violations += self.df.filter(col(column) > max_value).count()

        return {
            "column": column,
            "actual_min": actual_min,
            "actual_max": actual_max,
            "expected_min": min_value,
            "expected_max": max_value,
            "violations": violations,
            "violation_percentage": violations / self.total_rows if self.total_rows > 0 else 0,
        }

    def run_all_checks(self) -> Dict[str, Any]:
        """
        Run all data quality checks.

        Returns:
            Comprehensive data quality report
        """
        logger.info("Running comprehensive data quality checks")

        report = {
            "total_rows": self.total_rows,
            "total_columns": len(self.df.columns),
            "completeness": self.check_completeness(),
            "duplicates": self.check_duplicates(),
        }

        # Calculate overall completeness score
        completeness_scores = list(report["completeness"].values())
        report["completeness_score"] = (
            sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        )

        # Calculate overall quality score
        report["quality_score"] = (
            report["completeness_score"] * 0.7  # Completeness: 70%
            + (1.0 - report["duplicates"]["duplicate_percentage"]) * 0.3  # Uniqueness: 30%
        )

        logger.info(f"Data quality score: {report['quality_score']:.2%}")

        return report

    def print_report(self) -> None:
        """Print formatted data quality report."""
        report = self.run_all_checks()

        print("\n" + "=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)
        print(f"Total Rows: {report['total_rows']:,}")
        print(f"Total Columns: {report['total_columns']}")
        print(f"\nOverall Quality Score: {report['quality_score']:.2%}")
        print(f"Completeness Score: {report['completeness_score']:.2%}")
        print(f"\nDuplicate Analysis:")
        print(f"  - Duplicate Rows: {report['duplicates']['duplicate_rows']:,}")
        print(f"  - Duplicate %: {report['duplicates']['duplicate_percentage']:.2%}")
        print("\nColumn Completeness:")

        for column, score in sorted(
            report["completeness"].items(), key=lambda x: x[1]
        ):
            status = "✓" if score > 0.95 else "⚠" if score > 0.8 else "✗"
            print(f"  {status} {column}: {score:.2%}")

        print("=" * 60 + "\n")