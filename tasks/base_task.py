#!/usr/bin/env python3
"""
Base Task Interface - Agent Foundation Framework

Provides the standardized foundation interface that all 5 agents inherit from.
Defines common patterns for task execution, error handling, logging, and result
management across the entire pipeline.

Used by:
- Agent 1 (File Converter Task)
- Agent 2 (Column Mapper Agent Task) 
- Agent 3 (Data Consolidator Agent Task)
- Agent 4 (Smart Deduplicator Agent Task)
- Agent 5 (Web Scraper Agent Task)

Core Abstractions:
- TaskStatus enumeration for execution state tracking
- TaskResult dataclass for standardized result reporting
- TaskConfig dataclass for agent configuration
- PipelineContext for inter-agent data sharing
- BaseTask abstract class with common execution patterns
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskResult:
    """Standardized task result container"""

    task_name: str
    status: TaskStatus
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class TaskConfig:
    """Task configuration container"""

    name: str
    dependencies: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    retry_count: int = 3
    timeout: float | None = None
    critical: bool = True  # If false, failure won't stop pipeline


class PipelineContext:
    """
    Centralized context for sharing data between tasks
    Similar to React Context API
    """

    def __init__(self):
        self._data: dict[str, TaskResult] = {}
        self._metadata: dict[str, Any] = {}
        self._start_time = datetime.now()

    def set_task_result(self, task_name: str, result: TaskResult) -> None:
        """Store task result in context"""
        self._data[task_name] = result

    def get_task_result(self, task_name: str) -> TaskResult | None:
        """Get task result from context"""
        return self._data.get(task_name)

    def get_task_data(self, task_name: str) -> dict[str, Any]:
        """Get just the data from a task result"""
        result = self.get_task_result(task_name)
        return result.data if result else {}

    def has_task_succeeded(self, task_name: str) -> bool:
        """Check if a task completed successfully"""
        result = self.get_task_result(task_name)
        return result is not None and result.status == TaskStatus.SUCCESS

    def has_task_result(self, task_name: str) -> bool:
        """Check if a task result exists (regardless of success/failure)"""
        return task_name in self._data

    def get_all_results(self) -> dict[str, TaskResult]:
        """Get all task results"""
        return self._data.copy()

    def set_metadata(self, key: str, value: Any) -> None:
        """Set pipeline metadata"""
        self._metadata[key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get pipeline metadata"""
        return self._metadata.get(key, default)

    def get_pipeline_stats(self) -> dict[str, Any]:
        """Get pipeline execution statistics"""
        now = datetime.now()
        total_time = (now - self._start_time).total_seconds()

        task_stats = {}
        for task_name, result in self._data.items():
            task_stats[task_name] = {
                "status": result.status.value,
                "execution_time": result.execution_time,
                "timestamp": result.timestamp,
            }

        return {
            "pipeline_start": self._start_time,
            "total_execution_time": total_time,
            "tasks_completed": len(
                [r for r in self._data.values() if r.status in [TaskStatus.SUCCESS, TaskStatus.FAILED]]
            ),
            "tasks_successful": len([r for r in self._data.values() if r.status == TaskStatus.SUCCESS]),
            "task_details": task_stats,
        }


class TaskExecutionError(Exception):
    """Custom exception for task execution errors"""

    def __init__(self, task_name: str, message: str, original_error: Exception | None = None):
        self.task_name = task_name
        self.original_error = original_error
        super().__init__(f"Task '{task_name}' failed: {message}")


class BaseTask(ABC):
    """
    Base class for all pipeline tasks
    Follows React component pattern - each task is self-contained with clear interface
    """

    def __init__(self, config: TaskConfig):
        self.config = config
        self.name = config.name
        self.dependencies = config.dependencies
        self.logger = logging.getLogger(f"task.{self.name}")

    @abstractmethod
    async def execute(self, context: PipelineContext) -> TaskResult:
        """
        Execute the task - must be implemented by subclasses
        This is like the render() method in React components

        Args:
            context: Pipeline context containing shared data

        Returns:
            TaskResult: Standardized result object
        """
        pass

    def validate_dependencies(self, context: PipelineContext) -> bool:
        """
        Validate that all dependencies have completed successfully
        Similar to React's dependency checking in useEffect
        """
        for dep_name in self.dependencies:
            if not context.has_task_succeeded(dep_name):
                self.logger.error(f"Dependency '{dep_name}' has not completed successfully")
                return False
        return True

    def get_dependency_data(self, context: PipelineContext, dependency_name: str) -> dict[str, Any]:
        """Get data from a specific dependency"""
        if dependency_name not in self.dependencies:
            raise ValueError(f"'{dependency_name}' is not a declared dependency of '{self.name}'")
        return context.get_task_data(dependency_name)

    def get_all_dependency_data(self, context: PipelineContext) -> dict[str, dict[str, Any]]:
        """Get data from all dependencies"""
        return {dep: context.get_task_data(dep) for dep in self.dependencies}

    async def run_with_retry(self, context: PipelineContext) -> TaskResult:
        """
        Execute task with retry logic and error handling
        This is the public interface that orchestrator calls
        """
        start_time = time.time()
        last_error = None

        for attempt in range(self.config.retry_count + 1):
            try:
                self.logger.info(f"Executing task '{self.name}' (attempt {attempt + 1})")

                # Validate dependencies before execution
                if not self.validate_dependencies(context):
                    return TaskResult(
                        task_name=self.name,
                        status=TaskStatus.FAILED,
                        error="Dependency validation failed",
                        execution_time=time.time() - start_time,
                    )

                # Execute with timeout if configured
                if self.config.timeout:
                    result = await asyncio.wait_for(self.execute(context), timeout=self.config.timeout)
                else:
                    result = await self.execute(context)

                # Ensure result has correct execution time
                result.execution_time = time.time() - start_time

                self.logger.info(f"Task '{self.name}' completed successfully in {result.execution_time:.2f}s")
                return result

            except TimeoutError as e:
                last_error = e
                self.logger.warning(f"Task '{self.name}' timed out (attempt {attempt + 1})")

            except Exception as e:
                last_error = e
                self.logger.warning(f"Task '{self.name}' failed on attempt {attempt + 1}: {str(e)}")

                if attempt < self.config.retry_count:
                    # Exponential backoff
                    delay = min(2**attempt, 30)  # Max 30 seconds
                    self.logger.info(f"Retrying task '{self.name}' in {delay} seconds...")
                    await asyncio.sleep(delay)

        # All attempts failed
        execution_time = time.time() - start_time
        error_msg = str(last_error) if last_error else "Unknown error"

        self.logger.error(f"Task '{self.name}' failed after {self.config.retry_count + 1} attempts: {error_msg}")

        return TaskResult(task_name=self.name, status=TaskStatus.FAILED, error=error_msg, execution_time=execution_time)


class SyncTask(BaseTask):
    """
    Base class for synchronous tasks
    Automatically handles async/sync conversion
    """

    @abstractmethod
    def execute_sync(self, context: PipelineContext) -> TaskResult:
        """Synchronous execution method - implement this instead of execute()"""
        pass

    async def execute(self, context: PipelineContext) -> TaskResult:
        """Convert sync execution to async"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, self.execute_sync, context)
