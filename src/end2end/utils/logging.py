"""
Structured logging for data pipelines.

Educational Notes:
    - Demonstrates production logging best practices
    - Shows how to track pipeline execution and errors
    - Implements structured logging for monitoring (Week 5 concepts)
"""

import logging
import sys
from typing import Optional
from datetime import datetime


def configure_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
) -> None:
    """
    Configure logging for the end2end framework.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom log format (optional)

    Example:
        >>> configure_logging(level="DEBUG")
        >>> logger = get_logger(__name__)
        >>> logger.info("Pipeline started")
    """
    if format_string is None:
        format_string = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(funcName)s:%(lineno)d - %(message)s"
        )

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (typically __name__ from calling module)

    Returns:
        Configured logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started")
        >>> logger.error("Failed to read file", exc_info=True)
    """
    logger = logging.getLogger(name)

    # Add custom methods for structured logging
    def log_pipeline_start(pipeline_name: str, config: dict) -> None:
        """Log pipeline execution start."""
        logger.info(
            f"Pipeline '{pipeline_name}' started",
            extra={
                "event": "pipeline_start",
                "pipeline": pipeline_name,
                "config": config,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

    def log_pipeline_end(
        pipeline_name: str,
        status: str,
        duration_seconds: float,
    ) -> None:
        """Log pipeline execution end."""
        logger.info(
            f"Pipeline '{pipeline_name}' {status}",
            extra={
                "event": "pipeline_end",
                "pipeline": pipeline_name,
                "status": status,
                "duration_seconds": duration_seconds,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

    def log_task_metrics(
        task_name: str,
        rows_read: int,
        rows_written: int,
        duration_seconds: float,
    ) -> None:
        """Log task execution metrics."""
        logger.info(
            f"Task '{task_name}' completed: "
            f"{rows_read} rows read, {rows_written} rows written "
            f"in {duration_seconds:.2f}s",
            extra={
                "event": "task_metrics",
                "task": task_name,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "duration_seconds": duration_seconds,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

    # Add custom methods to logger
    logger.log_pipeline_start = log_pipeline_start
    logger.log_pipeline_end = log_pipeline_end
    logger.log_task_metrics = log_task_metrics

    return logger


# Configure default logging on module import
configure_logging()