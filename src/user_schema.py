"""
User Schema Management for Databricks Course Notebooks

This module provides utilities for managing user-specific schemas in Unity Catalog,
ensuring data isolation and preventing conflicts in multi-user environments.

Usage in Databricks notebooks:
    from src.user_schema import UserSchemaConfig

    config = UserSchemaConfig(spark, dbutils)

    # Use the configured paths
    bronze_table = config.get_table_path("bronze", "sales_transactions")
    # Result: databricks_course.chanukya_pekala.bronze_sales_transactions
"""

import re
from typing import Optional


class UserSchemaConfig:
    """
    Manages user-specific schema configuration for Databricks Unity Catalog.

    Automatically:
    - Extracts logged-in user's email from Databricks context
    - Converts email to valid schema name (e.g., chanukya.pekala@gmail.com -> chanukya_pekala)
    - Creates user's personal schema if it doesn't exist
    - Provides helper methods for building table paths

    Attributes:
        catalog (str): Target catalog name (default: "databricks_course")
        user_schema (str): User's personal schema name (e.g., "chanukya_pekala")
        user_email (str): Original user email address
    """

    def __init__(self, spark, dbutils, catalog: str = "databricks_course"):
        """
        Initialize user schema configuration.

        Args:
            spark: SparkSession instance from Databricks notebook
            dbutils: DBUtils instance from Databricks notebook
            catalog: Target Unity Catalog name (default: "databricks_course")
        """
        self.spark = spark
        self.dbutils = dbutils
        self.catalog = catalog

        # Get logged-in user's email
        self.user_email = self._get_user_email()

        # Convert email to valid schema name
        self.user_schema = self._email_to_schema_name(self.user_email)

        # Create schema if it doesn't exist
        self._create_schema_if_not_exists()

    def _get_user_email(self) -> str:
        """Extract user email from Databricks notebook context."""
        try:
            return self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        except Exception as e:
            raise RuntimeError(f"Failed to get user email from Databricks context: {e}")

    def _email_to_schema_name(self, email: str) -> str:
        """
        Convert email address to valid SQL schema name.

        Rules:
        - Extract username part (before @)
        - Replace all non-alphanumeric characters with underscore
        - Result is lowercase

        Examples:
            chanukya.pekala@gmail.com -> chanukya_pekala
            komal.azram@datatribe.group -> komal_azram
            test-user+123@example.com -> test_user_123

        Args:
            email: User email address

        Returns:
            Valid SQL schema name
        """
        username = email.split('@')[0]
        schema_name = re.sub(r'[^a-zA-Z0-9_]', '_', username)
        return schema_name.lower()

    def _create_schema_if_not_exists(self):
        """Create user's personal schema in Unity Catalog if it doesn't exist."""
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.user_schema}")
        except Exception as e:
            raise RuntimeError(
                f"Failed to create schema {self.catalog}.{self.user_schema}. "
                f"Ensure you have CREATE_SCHEMA privileges. Error: {e}"
            )

    def get_table_path(self, layer: str, table_name: str) -> str:
        """
        Build fully qualified table path in user's schema.

        Uses flat naming convention with layer prefix to avoid nested schema complexity.

        Args:
            layer: Data layer (bronze, silver, gold)
            table_name: Base table name

        Returns:
            Fully qualified table path

        Examples:
            >>> config.get_table_path("bronze", "sales_transactions")
            'databricks_course.chanukya_pekala.bronze_sales_transactions'

            >>> config.get_table_path("silver", "sales_cleaned")
            'databricks_course.chanukya_pekala.silver_sales_cleaned'
        """
        layer = layer.lower()
        if layer not in ["bronze", "silver", "gold"]:
            raise ValueError(f"Invalid layer '{layer}'. Must be one of: bronze, silver, gold")

        return f"{self.catalog}.{self.user_schema}.{layer}_{table_name}"

    def get_schema_path(self) -> str:
        """
        Get fully qualified schema path for the user.

        Returns:
            Schema path (e.g., "databricks_course.chanukya_pekala")
        """
        return f"{self.catalog}.{self.user_schema}"

    def print_config(self):
        """Print current configuration for debugging."""
        print("=" * 60)
        print("User Schema Configuration")
        print("=" * 60)
        print(f"User Email:       {self.user_email}")
        print(f"User Schema:      {self.user_schema}")
        print(f"Catalog:          {self.catalog}")
        print(f"Full Schema Path: {self.get_schema_path()}")
        print("=" * 60)
        print("\nExample table paths:")
        print(f"  Bronze: {self.get_table_path('bronze', 'example_table')}")
        print(f"  Silver: {self.get_table_path('silver', 'example_table')}")
        print(f"  Gold:   {self.get_table_path('gold', 'example_table')}")
        print("=" * 60)


def get_user_schema_config(spark, dbutils, catalog: str = "databricks_course") -> UserSchemaConfig:
    """
    Convenience function to create UserSchemaConfig instance.

    Args:
        spark: SparkSession instance
        dbutils: DBUtils instance
        catalog: Target Unity Catalog name

    Returns:
        Configured UserSchemaConfig instance

    Example:
        from src.user_schema import get_user_schema_config

        config = get_user_schema_config(spark, dbutils)
        bronze_table = config.get_table_path("bronze", "sales")
    """
    return UserSchemaConfig(spark, dbutils, catalog)