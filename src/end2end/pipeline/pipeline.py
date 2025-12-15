"""
Pipeline orchestration for data workflows.

Educational Notes:
    - Based on Week 4 & 5 concepts
    - Demonstrates DAG-based workflow execution
    - Shows dependency management
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

from end2end.pipeline.task import Task, TaskStatus
from end2end.config import PipelineConfig
from end2end.utils import get_logger

logger = get_logger(__name__)


class Pipeline:
    """
    Orchestrates execution of multiple tasks with dependencies.

    Educational Pattern from Week 4 & 5:
        - DAG-based workflow execution
        - Automatic dependency resolution
        - Parallel task execution where possible
        - Centralized error handling

    Args:
        name: Pipeline name
        config: Pipeline configuration

    Example:
        >>> from end2end import Pipeline, PipelineConfig
        >>>
        >>> config = PipelineConfig(
        ...     catalog="databricks_course",
        ...     source_schema="chanukya_pekala"
        ... )
        >>>
        >>> pipeline = Pipeline("sales_pipeline", config)
        >>>
        >>> # Add tasks
        >>> pipeline.add_task(Task("ingest", ingest_function))
        >>> pipeline.add_task(Task("transform", transform_function, dependencies=["ingest"]))
        >>> pipeline.add_task(Task("aggregate", aggregate_function, dependencies=["transform"]))
        >>>
        >>> # Execute
        >>> results = pipeline.run()
    """

    def __init__(self, name: str, config: PipelineConfig):
        self.name = name
        self.config = config
        self.tasks: Dict[str, Task] = {}
        self.metadata: Dict[str, Any] = {
            "name": name,
            "start_time": None,
            "end_time": None,
            "status": "pending",
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_skipped": 0,
        }

    def add_task(self, task: Task) -> "Pipeline":
        """
        Add task to pipeline.

        Args:
            task: Task to add

        Returns:
            Self for method chaining

        Example:
            >>> pipeline.add_task(Task("ingest", ingest_func))
            ...         .add_task(Task("transform", transform_func, dependencies=["ingest"]))
        """
        if task.name in self.tasks:
            logger.warning(f"Task '{task.name}' already exists - overwriting")

        self.tasks[task.name] = task
        logger.info(f"Added task '{task.name}' to pipeline '{self.name}'")

        return self

    def validate_dependencies(self) -> bool:
        """
        Validate task dependencies.

        Checks:
            - All dependencies exist
            - No circular dependencies

        Returns:
            True if dependencies are valid

        Raises:
            ValueError: If dependencies are invalid
        """
        logger.info("Validating pipeline dependencies")

        # Check all dependencies exist
        for task_name, task in self.tasks.items():
            for dep in task.dependencies:
                if dep not in self.tasks:
                    raise ValueError(
                        f"Task '{task_name}' depends on '{dep}' which doesn't exist"
                    )

        # Check for circular dependencies (simple cycle detection)
        for task_name in self.tasks:
            if self._has_circular_dependency(task_name):
                raise ValueError(f"Circular dependency detected involving task '{task_name}'")

        logger.info("Dependency validation passed")
        return True

    def _has_circular_dependency(
        self,
        task_name: str,
        visited: Optional[set] = None,
    ) -> bool:
        """Check if task has circular dependencies."""
        if visited is None:
            visited = set()

        if task_name in visited:
            return True

        visited.add(task_name)

        task = self.tasks[task_name]
        for dep in task.dependencies:
            if self._has_circular_dependency(dep, visited.copy()):
                return True

        return False

    def get_execution_order(self) -> List[List[str]]:
        """
        Get task execution order respecting dependencies.

        Returns:
            List of task groups where each group can run in parallel

        Example:
            >>> order = pipeline.get_execution_order()
            >>> # Result: [['ingest'], ['transform1', 'transform2'], ['aggregate']]
        """
        # Build dependency graph
        remaining_tasks = set(self.tasks.keys())
        completed_tasks = set()
        execution_order = []

        while remaining_tasks:
            # Find tasks with all dependencies met
            ready_tasks = [
                task_name
                for task_name in remaining_tasks
                if all(dep in completed_tasks for dep in self.tasks[task_name].dependencies)
            ]

            if not ready_tasks:
                # This shouldn't happen if validate_dependencies passed
                raise ValueError("Unable to resolve task dependencies - possible circular dependency")

            execution_order.append(ready_tasks)
            completed_tasks.update(ready_tasks)
            remaining_tasks -= set(ready_tasks)

        return execution_order

    def run(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Execute the pipeline.

        Args:
            dry_run: If True, only validate without executing

        Returns:
            Pipeline execution results

        Example:
            >>> results = pipeline.run()
            >>> print(f"Status: {results['status']}")
            >>> print(f"Tasks completed: {results['tasks_completed']}")
        """
        self.metadata["start_time"] = datetime.utcnow().isoformat()

        logger.info(f"Starting pipeline '{self.name}' with {len(self.tasks)} tasks")

        try:
            # Validate dependencies
            self.validate_dependencies()

            if dry_run:
                logger.info("Dry run - skipping execution")
                self.metadata["status"] = "dry_run"
                return self.metadata

            # Get execution order
            execution_order = self.get_execution_order()

            logger.info(f"Execution plan: {len(execution_order)} stages")
            for i, stage in enumerate(execution_order, 1):
                logger.info(f"  Stage {i}: {stage}")

            # Execute tasks in order
            for stage_num, stage_tasks in enumerate(execution_order, 1):
                logger.info(f"Executing stage {stage_num}: {stage_tasks}")

                for task_name in stage_tasks:
                    task = self.tasks[task_name]

                    # Check if dependencies succeeded
                    if not self._dependencies_successful(task):
                        task.skip("One or more dependencies failed")
                        self.metadata["tasks_skipped"] += 1
                        continue

                    # Execute task
                    try:
                        task.execute()
                        self.metadata["tasks_completed"] += 1

                    except Exception as e:
                        logger.error(f"Task '{task_name}' failed: {e}")
                        self.metadata["tasks_failed"] += 1

                        # Continue with independent tasks, skip dependent tasks

            # Determine overall status
            if self.metadata["tasks_failed"] > 0:
                self.metadata["status"] = "failed"
            elif self.metadata["tasks_skipped"] > 0:
                self.metadata["status"] = "partial_success"
            else:
                self.metadata["status"] = "success"

            self.metadata["end_time"] = datetime.utcnow().isoformat()

            logger.info(
                f"Pipeline '{self.name}' completed: "
                f"{self.metadata['tasks_completed']} succeeded, "
                f"{self.metadata['tasks_failed']} failed, "
                f"{self.metadata['tasks_skipped']} skipped"
            )

            return self.get_results()

        except Exception as e:
            self.metadata["status"] = "error"
            self.metadata["error"] = str(e)
            self.metadata["end_time"] = datetime.utcnow().isoformat()
            logger.error(f"Pipeline '{self.name}' failed: {e}", exc_info=True)
            raise

    def _dependencies_successful(self, task: Task) -> bool:
        """Check if all task dependencies completed successfully."""
        return all(
            self.tasks[dep].was_successful() for dep in task.dependencies
        )

    def get_results(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline results.

        Returns:
            Dictionary with pipeline and task results
        """
        return {
            **self.metadata,
            "config": self.config.to_dict(),
            "tasks": {
                name: task.get_metadata() for name, task in self.tasks.items()
            },
        }

    def print_summary(self) -> None:
        """Print formatted pipeline execution summary."""
        results = self.get_results()

        print("\n" + "=" * 60)
        print(f"PIPELINE: {self.name}")
        print("=" * 60)
        print(f"Status: {results['status'].upper()}")
        print(f"Tasks: {len(self.tasks)} total")
        print(f"  ✓ Completed: {results['tasks_completed']}")
        print(f"  ✗ Failed: {results['tasks_failed']}")
        print(f"  ⊘ Skipped: {results['tasks_skipped']}")

        print("\nTask Details:")
        for task_name, task_meta in results["tasks"].items():
            status = task_meta["status"]
            duration = task_meta.get("duration_seconds", 0)

            status_icon = {
                "success": "✓",
                "failed": "✗",
                "skipped": "⊘",
                "pending": "○",
                "running": "⋯",
            }.get(status, "?")

            print(f"  {status_icon} {task_name}: {status} ({duration:.2f}s)")

        print("=" * 60 + "\n")

    def __repr__(self) -> str:
        """Readable representation."""
        return f"Pipeline(name='{self.name}', tasks={len(self.tasks)})"