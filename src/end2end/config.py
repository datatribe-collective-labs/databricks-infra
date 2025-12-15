"""
Configuration management for end2end data pipelines.

This module provides configuration classes for managing:
- Unity Catalog namespaces (catalog.schema.table)
- Environment settings (dev, prod)
- Pipeline parameters and metadata

Educational Notes:
    - Demonstrates the three-level namespace pattern from Week 1
    - Shows configuration best practices for production pipelines
    - Implements environment-based configuration (Week 5 concepts)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Any
import re


class Environment(Enum):
    """Pipeline environment types."""

    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


@dataclass
class EnvironmentConfig:
    """
    Environment-specific configuration settings.

    Examples from the course:
        - sales_dev.bronze (development)
        - sales_prod.gold (production)

    Attributes:
        environment: Environment type (dev, staging, prod)
        enable_validation: Enable data quality checks
        enable_monitoring: Enable pipeline monitoring
        retry_attempts: Number of retry attempts for failed tasks
        timeout_seconds: Task timeout in seconds
    """

    environment: Environment = Environment.DEV
    enable_validation: bool = True
    enable_monitoring: bool = True
    retry_attempts: int = 3
    timeout_seconds: int = 3600

    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PROD


@dataclass
class PipelineConfig:
    """
    Main configuration class for data pipelines.

    Implements Unity Catalog three-level namespace pattern:
        {catalog}.{schema}.{table}

    Example from course notebooks:
        >>> config = PipelineConfig(
        ...     catalog="databricks_course",
        ...     source_schema="chanukya_pekala",
        ...     environment="dev"
        ... )
        >>> config.get_table_path("bronze", "sales")
        'databricks_course.chanukya_pekala.bronze_sales'

    Attributes:
        catalog: Unity Catalog name (e.g., "databricks_course")
        source_schema: Source schema name (e.g., user personal schema)
        target_schema: Target schema name (optional, defaults to source_schema)
        environment: Environment type (dev, staging, prod)
        metadata: Additional pipeline metadata
        env_config: Environment-specific settings
    """

    catalog: str
    source_schema: str
    target_schema: Optional[str] = None
    environment: str = "dev"
    metadata: Dict[str, Any] = field(default_factory=dict)
    env_config: Optional[EnvironmentConfig] = None

    def __post_init__(self):
        """Initialize environment configuration and validate inputs."""
        # Default target schema to source schema
        if self.target_schema is None:
            self.target_schema = self.source_schema

        # Create environment config if not provided
        if self.env_config is None:
            env = Environment(self.environment.lower())
            self.env_config = EnvironmentConfig(environment=env)

        # Validate catalog and schema names
        self._validate_namespace_component(self.catalog, "catalog")
        self._validate_namespace_component(self.source_schema, "source_schema")
        self._validate_namespace_component(self.target_schema, "target_schema")

    @staticmethod
    def _validate_namespace_component(name: str, component_type: str) -> None:
        """
        Validate Unity Catalog namespace component.

        Unity Catalog naming rules:
            - Must start with a letter or underscore
            - Can contain letters, numbers, and underscores
            - Cannot contain special characters or spaces
        """
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
            raise ValueError(
                f"Invalid {component_type} name: '{name}'. "
                f"Must start with letter or underscore and contain only "
                f"alphanumeric characters and underscores."
            )

    def get_table_path(
        self,
        layer: str,
        table_name: str,
        use_target_schema: bool = True,
    ) -> str:
        """
        Build Unity Catalog three-level namespace path.

        Pattern from user_schema_setup.py (Week 2-4 notebooks):
            {catalog}.{schema}.{layer}_{table_name}

        Args:
            layer: Data layer (bronze, silver, gold)
            table_name: Base table name
            use_target_schema: Use target schema (True) or source schema (False)

        Returns:
            Full Unity Catalog table path

        Examples:
            >>> config = PipelineConfig(
            ...     catalog="databricks_course",
            ...     source_schema="chanukya_pekala"
            ... )
            >>> config.get_table_path("bronze", "sales")
            'databricks_course.chanukya_pekala.bronze_sales'
            >>> config.get_table_path("gold", "daily_summary")
            'databricks_course.chanukya_pekala.gold_daily_summary'
        """
        schema = self.target_schema if use_target_schema else self.source_schema
        return f"{self.catalog}.{schema}.{layer}_{table_name}"

    def get_schema_path(self, use_target: bool = True) -> str:
        """
        Get schema path for USE SCHEMA or CREATE SCHEMA statements.

        Args:
            use_target: Use target schema (True) or source schema (False)

        Returns:
            Full schema path (catalog.schema)

        Example:
            >>> config.get_schema_path()
            'databricks_course.chanukya_pekala'
        """
        schema = self.target_schema if use_target else self.source_schema
        return f"{self.catalog}.{schema}"

    def get_checkpoint_path(self, pipeline_name: str) -> str:
        """
        Get checkpoint path for streaming pipelines.

        Used for structured streaming checkpoints (Week 4 concepts).

        Args:
            pipeline_name: Name of the pipeline

        Returns:
            Checkpoint path in DBFS

        Example:
            >>> config.get_checkpoint_path("sales_pipeline")
            '/dbfs/checkpoints/databricks_course/chanukya_pekala/sales_pipeline'
        """
        return (
            f"/dbfs/checkpoints/{self.catalog}/{self.target_schema}/{pipeline_name}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.

        Useful for logging, serialization, and job parameters.

        Returns:
            Dictionary representation of configuration
        """
        return {
            "catalog": self.catalog,
            "source_schema": self.source_schema,
            "target_schema": self.target_schema,
            "environment": self.environment,
            "metadata": self.metadata,
            "env_config": {
                "environment": self.env_config.environment.value,
                "enable_validation": self.env_config.enable_validation,
                "enable_monitoring": self.env_config.enable_monitoring,
                "retry_attempts": self.env_config.retry_attempts,
                "timeout_seconds": self.env_config.timeout_seconds,
            },
        }

    def __repr__(self) -> str:
        """Readable representation for debugging."""
        return (
            f"PipelineConfig("
            f"catalog='{self.catalog}', "
            f"source_schema='{self.source_schema}', "
            f"target_schema='{self.target_schema}', "
            f"environment='{self.environment}'"
            f")"
        )


def create_user_config(
    user_email: str,
    catalog: str = "databricks_course",
    environment: str = "dev",
) -> PipelineConfig:
    """
    Create configuration from user email (pattern from user_schema_setup.py).

    This helper function mimics the user schema creation logic from
    course/notebooks/utils/user_schema_setup.py.

    Args:
        user_email: User email address
        catalog: Unity Catalog name
        environment: Environment type (dev, staging, prod)

    Returns:
        PipelineConfig configured for the user

    Example:
        >>> config = create_user_config("chanukya.pekala@gmail.com")
        >>> config.source_schema
        'chanukya_pekala'
    """
    # Extract username and convert to valid schema name
    # chanukya.pekala@gmail.com -> chanukya_pekala
    user_name = user_email.split("@")[0]
    user_schema = re.sub(r"[^a-zA-Z0-9_]", "_", user_name).lower()

    return PipelineConfig(
        catalog=catalog,
        source_schema=user_schema,
        environment=environment,
        metadata={"user_email": user_email},
    )