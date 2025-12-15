"""
end2end - Educational Data Engineering Framework for Databricks

A comprehensive framework for building production-grade data pipelines on Databricks,
demonstrating patterns taught in the Databricks Course (Weeks 1-5).

Core Modules:
    - config: Configuration management for catalogs, schemas, and environments
    - ingestion: Multi-source data ingestion (files, APIs, databases, cloud storage)
    - transformations: Medallion architecture (Bronze -> Silver -> Gold)
    - quality: Data quality validation and expectations
    - pipeline: Pipeline orchestration with retry logic and monitoring
    - utils: Logging, Spark utilities, and helper functions

Educational Focus:
    This framework is designed for learning. Each module includes:
    - Clear, well-documented code
    - Examples from the course
    - Best practices for production deployment
    - Error handling and validation patterns

Example Usage:
    >>> from end2end import Pipeline, FileIngestion, BronzeToSilver
    >>> from end2end.config import PipelineConfig
    >>>
    >>> # Configure pipeline
    >>> config = PipelineConfig(
    ...     catalog="databricks_course",
    ...     source_schema="user_schema",
    ...     environment="dev"
    ... )
    >>>
    >>> # Build pipeline
    >>> pipeline = Pipeline(name="sales_pipeline", config=config)
    >>> pipeline.add_task(FileIngestion(source_path="/data/sales.csv"))
    >>> pipeline.add_task(BronzeToSilver(table_name="sales"))
    >>>
    >>> # Execute
    >>> pipeline.run()

For complete documentation, see:
    - docs/END2END_FRAMEWORK_GUIDE.md
    - course/notebooks/advanced/03_end2end_framework_guide.py
"""

__version__ = "1.0.0"
__author__ = "DataTribe Collective"

# Configuration
from end2end.config import PipelineConfig, EnvironmentConfig

# Ingestion
from end2end.ingestion import (
    FileIngestion,
    APIIngestion,
    DatabaseIngestion,
    BaseIngestion,
)

# Transformations
from end2end.transformations import (
    BronzeToSilver,
    SilverToGold,
    BaseTransformation,
)

# Data Quality
from end2end.quality import (
    SchemaValidator,
    DataQualityCheck,
    Expectation,
)

# Pipeline Orchestration
from end2end.pipeline import Pipeline, Task

# Utilities
from end2end.utils import get_logger, create_spark_session

__all__ = [
    # Version
    "__version__",
    # Configuration
    "PipelineConfig",
    "EnvironmentConfig",
    # Ingestion
    "FileIngestion",
    "APIIngestion",
    "DatabaseIngestion",
    "BaseIngestion",
    # Transformations
    "BronzeToSilver",
    "SilverToGold",
    "BaseTransformation",
    # Quality
    "SchemaValidator",
    "DataQualityCheck",
    "Expectation",
    # Pipeline
    "Pipeline",
    "Task",
    # Utilities
    "get_logger",
    "create_spark_session",
]