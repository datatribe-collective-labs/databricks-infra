"""
Data expectations framework (inspired by Great Expectations).

Educational Notes:
    - Demonstrates expectation-based validation
    - Shows declarative data quality testing
    - Implements validation reporting
"""

from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from end2end.utils import get_logger

logger = get_logger(__name__)


class Expectation:
    """
    Base class for data expectations.

    Educational Pattern:
        Expectations are declarative assertions about data:
        - Column exists
        - Values are within range
        - No null values
        - Unique values

    Example:
        >>> expectation = Expectation("amount")
        >>> expectation.expect_column_values_to_be_between(0, 1000000)
        >>> result = expectation.validate(df)
        >>> print(f"Success: {result['success']}")
    """

    def __init__(self, column: str):
        self.column = column
        self.expectations: List[Dict[str, Any]] = []

    def expect_column_to_exist(self) -> "Expectation":
        """Expect column to exist in DataFrame."""
        self.expectations.append(
            {
                "type": "column_exists",
                "column": self.column,
            }
        )
        return self

    def expect_column_values_to_not_be_null(self) -> "Expectation":
        """Expect column to have no null values."""
        self.expectations.append(
            {
                "type": "values_not_null",
                "column": self.column,
            }
        )
        return self

    def expect_column_values_to_be_between(
        self,
        min_value: float,
        max_value: float,
    ) -> "Expectation":
        """Expect column values to be within range."""
        self.expectations.append(
            {
                "type": "values_in_range",
                "column": self.column,
                "min_value": min_value,
                "max_value": max_value,
            }
        )
        return self

    def expect_column_values_to_be_unique(self) -> "Expectation":
        """Expect column values to be unique."""
        self.expectations.append(
            {
                "type": "values_unique",
                "column": self.column,
            }
        )
        return self

    def expect_column_values_to_be_in_set(self, value_set: List[Any]) -> "Expectation":
        """Expect column values to be in specified set."""
        self.expectations.append(
            {
                "type": "values_in_set",
                "column": self.column,
                "value_set": value_set,
            }
        )
        return self

    def validate(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate all expectations against DataFrame.

        Args:
            df: DataFrame to validate

        Returns:
            Validation results dictionary
        """
        results = {
            "column": self.column,
            "total_expectations": len(self.expectations),
            "passed": 0,
            "failed": 0,
            "success": True,
            "details": [],
        }

        for expectation in self.expectations:
            result = self._validate_single(df, expectation)
            results["details"].append(result)

            if result["success"]:
                results["passed"] += 1
            else:
                results["failed"] += 1
                results["success"] = False

        logger.info(
            f"Validation for '{self.column}': "
            f"{results['passed']}/{results['total_expectations']} passed"
        )

        return results

    def _validate_single(self, df: DataFrame, expectation: Dict[str, Any]) -> Dict[str, Any]:
        """Validate single expectation."""
        exp_type = expectation["type"]

        try:
            if exp_type == "column_exists":
                return self._validate_column_exists(df, expectation)
            elif exp_type == "values_not_null":
                return self._validate_not_null(df, expectation)
            elif exp_type == "values_in_range":
                return self._validate_in_range(df, expectation)
            elif exp_type == "values_unique":
                return self._validate_unique(df, expectation)
            elif exp_type == "values_in_set":
                return self._validate_in_set(df, expectation)
            else:
                return {
                    "expectation": exp_type,
                    "success": False,
                    "message": f"Unknown expectation type: {exp_type}",
                }

        except Exception as e:
            logger.error(f"Validation failed for {exp_type}: {e}", exc_info=True)
            return {
                "expectation": exp_type,
                "success": False,
                "message": f"Error during validation: {str(e)}",
            }

    def _validate_column_exists(self, df: DataFrame, expectation: Dict) -> Dict:
        """Validate column exists."""
        column = expectation["column"]
        exists = column in df.columns

        return {
            "expectation": "column_exists",
            "column": column,
            "success": exists,
            "message": f"Column '{column}' {'exists' if exists else 'does not exist'}",
        }

    def _validate_not_null(self, df: DataFrame, expectation: Dict) -> Dict:
        """Validate column has no nulls."""
        column = expectation["column"]
        null_count = df.filter(col(column).isNull()).count()
        total_rows = df.count()

        success = null_count == 0

        return {
            "expectation": "values_not_null",
            "column": column,
            "success": success,
            "null_count": null_count,
            "total_rows": total_rows,
            "message": (
                f"Column '{column}' has no null values"
                if success
                else f"Column '{column}' has {null_count} null values"
            ),
        }

    def _validate_in_range(self, df: DataFrame, expectation: Dict) -> Dict:
        """Validate values are within range."""
        column = expectation["column"]
        min_value = expectation["min_value"]
        max_value = expectation["max_value"]

        out_of_range = df.filter(
            (col(column) < min_value) | (col(column) > max_value)
        ).count()

        success = out_of_range == 0

        return {
            "expectation": "values_in_range",
            "column": column,
            "success": success,
            "out_of_range_count": out_of_range,
            "min_value": min_value,
            "max_value": max_value,
            "message": (
                f"All values in range [{min_value}, {max_value}]"
                if success
                else f"{out_of_range} values out of range"
            ),
        }

    def _validate_unique(self, df: DataFrame, expectation: Dict) -> Dict:
        """Validate values are unique."""
        column = expectation["column"]
        total_rows = df.count()
        distinct_rows = df.select(column).distinct().count()

        success = total_rows == distinct_rows

        return {
            "expectation": "values_unique",
            "column": column,
            "success": success,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": total_rows - distinct_rows,
            "message": (
                f"All values are unique"
                if success
                else f"{total_rows - distinct_rows} duplicate values found"
            ),
        }

    def _validate_in_set(self, df: DataFrame, expectation: Dict) -> Dict:
        """Validate values are in specified set."""
        column = expectation["column"]
        value_set = expectation["value_set"]

        invalid_count = df.filter(~col(column).isin(value_set)).count()

        success = invalid_count == 0

        return {
            "expectation": "values_in_set",
            "column": column,
            "success": success,
            "invalid_count": invalid_count,
            "value_set": value_set,
            "message": (
                f"All values in expected set"
                if success
                else f"{invalid_count} values not in expected set"
            ),
        }

    def __repr__(self) -> str:
        """Readable representation."""
        return f"Expectation(column='{self.column}', expectations={len(self.expectations)})"