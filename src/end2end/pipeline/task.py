"""
Task abstraction for pipeline orchestration.

Educational Notes:
    - Demonstrates task-based workflow patterns
    - Shows retry logic and error handling
    - Implements task dependencies
"""

from typing import Any, Callable, Dict, List, Optional
from datetime import datetime
from enum import Enum

from end2end.utils import get_logger

logger = get_logger(__name__)


class TaskStatus(Enum):
    """Task execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class Task:
    """
    Represents a single task in a data pipeline.

    Educational Pattern from Week 5:
        - Tasks are units of work in a pipeline
        - Can have dependencies on other tasks
        - Support retry logic
        - Track execution metadata

    Args:
        name: Task name
        function: Callable function to execute
        dependencies: List of task names this task depends on
        retry_attempts: Number of retry attempts on failure
        args: Positional arguments for function
        kwargs: Keyword arguments for function

    Example:
        >>> def ingest_sales():
        ...     # Ingestion logic
        ...     return {"rows": 1000}
        ...
        >>> task = Task(
        ...     name="ingest_sales",
        ...     function=ingest_sales,
        ...     retry_attempts=3
        ... )
        >>> result = task.execute()
    """

    def __init__(
        self,
        name: str,
        function: Callable,
        dependencies: Optional[List[str]] = None,
        retry_attempts: int = 0,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ):
        self.name = name
        self.function = function
        self.dependencies = dependencies or []
        self.retry_attempts = retry_attempts
        self.args = args or ()
        self.kwargs = kwargs or {}

        self.status = TaskStatus.PENDING
        self.result = None
        self.error = None
        self.metadata: Dict[str, Any] = {
            "name": name,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "attempt": 0,
        }

    def execute(self) -> Any:
        """
        Execute the task with retry logic.

        Returns:
            Task execution result

        Raises:
            Exception: If task fails after all retries
        """
        self.status = TaskStatus.RUNNING
        self.metadata["start_time"] = datetime.utcnow().isoformat()

        attempt = 0

        while attempt <= self.retry_attempts:
            self.metadata["attempt"] = attempt + 1

            try:
                logger.info(
                    f"Executing task '{self.name}' "
                    f"(attempt {attempt + 1}/{self.retry_attempts + 1})"
                )

                # Execute function
                start_time = datetime.utcnow()
                self.result = self.function(*self.args, **self.kwargs)
                end_time = datetime.utcnow()

                # Update metadata
                self.status = TaskStatus.SUCCESS
                self.metadata["end_time"] = end_time.isoformat()
                self.metadata["duration_seconds"] = (end_time - start_time).total_seconds()

                logger.info(
                    f"Task '{self.name}' completed successfully "
                    f"in {self.metadata['duration_seconds']:.2f}s"
                )

                return self.result

            except Exception as e:
                attempt += 1
                self.error = str(e)

                logger.warning(
                    f"Task '{self.name}' failed (attempt {attempt}): {e}",
                    exc_info=True,
                )

                if attempt > self.retry_attempts:
                    self.status = TaskStatus.FAILED
                    self.metadata["end_time"] = datetime.utcnow().isoformat()
                    logger.error(f"Task '{self.name}' failed after {self.retry_attempts + 1} attempts")
                    raise

        return None

    def skip(self, reason: str = "Dependency failed") -> None:
        """
        Mark task as skipped.

        Args:
            reason: Reason for skipping
        """
        self.status = TaskStatus.SKIPPED
        self.metadata["skip_reason"] = reason
        logger.info(f"Task '{self.name}' skipped: {reason}")

    def is_complete(self) -> bool:
        """Check if task is complete (success or failed)."""
        return self.status in [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.SKIPPED]

    def was_successful(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.SUCCESS

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get task execution metadata.

        Returns:
            Dictionary with task metadata
        """
        return {
            **self.metadata,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
        }

    def __repr__(self) -> str:
        """Readable representation."""
        return (
            f"Task(name='{self.name}', "
            f"status={self.status.value}, "
            f"dependencies={self.dependencies})"
        )