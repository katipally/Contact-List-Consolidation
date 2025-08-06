#!/usr/bin/env python3
"""
Master Orchestrator - Pipeline Coordination Hub

Central orchestrator that manages the execution of all 5 agents in the correct
sequence with proper dependency management, error handling, and result tracking.

Coordinates the complete pipeline flow:
Agent 1 (File Converter) → Agent 2 (Column Mapper) → Agent 3 (Data Consolidator) 
→ Agent 4 (Smart Deduplicator) → Agent 5 (Email Enrichment)

Used by:
- run_pipeline.py as the main entry point for pipeline execution
- Any script requiring programmatic pipeline execution

Key Responsibilities:
- Sequential agent execution with dependency validation
- Inter-agent data flow management
- Comprehensive error handling and recovery
- Performance monitoring and result tracking
- Output file organization and validation
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import pandas as pd

# Core task imports
from tasks.base_task import BaseTask, TaskConfig, PipelineContext, TaskStatus, TaskResult

# Active 6-Agent Pipeline Tasks (2025)
from tasks.file_converter_task import FileConverterTask
from tasks.column_mapper_agent_task import DataContentAnalyzer
from tasks.data_consolidator_agent_task import DataConsolidatorAgentTask
from tasks.smart_deduplicator_agent_task import SmartDeduplicatorAgentTask
from tasks.web_scraper_agent_task import AdvancedEmailEnrichmentAgent
from tasks.csv_cleaner_crm_exporter_task import CSVCleanerCRMExporter

logger = logging.getLogger(__name__)


class TaskDependencyError(Exception):
    """Raised when task dependencies cannot be resolved"""

    pass


class TaskFactory:
    """
    Factory for creating task instances
    Similar to React component factory
    """
    
    _task_registry: Dict[str, Type[BaseTask]] = {
        # NEW AGENT TASKS (User's Vision)
        "file_converter": FileConverterTask,  # Agent 1
        "column_mapper_agent": DataContentAnalyzer,  # Agent 2
        "data_consolidator_agent": DataConsolidatorAgentTask,  # Agent 3
        "smart_deduplicator_agent": SmartDeduplicatorAgentTask,  # Agent 4
        "web_scraper_agent": AdvancedEmailEnrichmentAgent,  # Agent 5
        "csv_cleaner_crm_exporter": CSVCleanerCRMExporter,  # Agent 6
    }
    
    @classmethod
    def register_task(cls, task_name: str, task_class: Type[BaseTask]):
        """Register a new task type"""
        cls._task_registry[task_name] = task_class
    
    @classmethod
    def create_task(cls, task_name: str, config: TaskConfig) -> BaseTask:
        """Create a task instance"""
        if task_name not in cls._task_registry:
            raise ValueError(f"Unknown task type: {task_name}")
        
        task_class = cls._task_registry[task_name]
        return task_class(config)
    
    @classmethod
    def get_available_tasks(cls) -> List[str]:
        """Get list of available task types"""
        return list(cls._task_registry.keys())


class MasterOrchestrator:
    """
    Master orchestrator that coordinates task execution
    Follows React-style component pattern with dependency management
    """
    
    def __init__(self, pipeline_config: Optional[Dict[str, Any]] = None):
        self.pipeline_config = pipeline_config or {}
        self.context = PipelineContext()
        self.tasks: Dict[str, BaseTask] = {}
        self.task_configs: Dict[str, TaskConfig] = {}
        self.execution_order: List[str] = []
        
        # Set up logging
        self._setup_logging()
        
        logger.info("Master Orchestrator initialized")
    
    def _setup_logging(self):
        """Configure logging for the orchestrator"""
        log_level = self.pipeline_config.get("log_level", "INFO")
        log_file = self.pipeline_config.get("log_file", "pipeline.log")
        
        # Configure logging if not already configured
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=getattr(logging, log_level.upper()),
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
            )
    
    def register_task(
        self, task_name: str, dependencies: List[str] = None, config: Dict[str, Any] = None, **kwargs
    ) -> "MasterOrchestrator":
        """
        Register a task with the orchestrator
        Fluent interface for easy task composition
        """
        task_config = TaskConfig(name=task_name, dependencies=dependencies or [], config=config or {}, **kwargs)
        
        # Create task instance
        task = TaskFactory.create_task(task_name, task_config)
        
        # Store task and config
        self.tasks[task_name] = task
        self.task_configs[task_name] = task_config
        
        logger.info(f"Registered task '{task_name}' with dependencies: {dependencies or []}")
        
        return self  # Fluent interface
    
    def create_standard_pipeline(
        self,
        source_directory: str = "DataSource",
        output_directory: str = "output",
        enable_ai_enhancements: bool = True,
    ) -> "MasterOrchestrator":
        """
        Create the standard 5-agent contact processing pipeline
        Uses the modern agent-based architecture
        """
        logger.info("Creating 5-agent contact processing pipeline")
        
        # Use the agent-based pipeline as the standard
        return self.create_agent_based_pipeline(
            source_directory=source_directory,
            output_directory=output_directory
        )

    def create_agent_based_pipeline(
        self, source_directory: str = "DataSource", output_directory: str = "output"
    ) -> "MasterOrchestrator":
        """
        Create agent-based pipeline following the user's exact vision:

        Agent 1: File Converter → Agent 2: Column Mapper → Agent 3: Data Consolidator →
        Agent 4: Smart Deduplicator → Agent 5: Web Scraper → Agent 6: CSV Cleaner & CRM Exporter

        Each agent has separate output folders and verification
        """

        # Configure pipeline
        pipeline_config = {
            "source_directory": source_directory,
            "output_directory": output_directory,
            "use_agent_architecture": True,
            "enable_ai_enhancements": True,
            "enable_output_verification": True,
            "create_agent_subfolders": True,
        }

        # Build the NEW agent-based pipeline (user's vision)
        (
            self.register_task(
                "file_converter",  # Agent 1: Convert files to CSV
                       dependencies=[], 
                config=pipeline_config,
            )
            .register_task(
                "column_mapper_agent",  # Agent 2: Map columns with LLM
                dependencies=["file_converter"],
                config=pipeline_config,
            )
            .register_task(
                "data_consolidator_agent",  # Agent 3: Combine into one sheet
                dependencies=["column_mapper_agent"],
                config=pipeline_config,
            )
            .register_task(
                "smart_deduplicator_agent",  # Agent 4: Remove duplicates with LLM
                dependencies=["data_consolidator_agent"],
                config=pipeline_config,
            )
            .register_task(
                "web_scraper_agent",  # Agent 5: Fill missing data
                dependencies=["smart_deduplicator_agent"],
                config=pipeline_config,
            )
            .register_task(
                "csv_cleaner_crm_exporter",  # Agent 6: Clean & export CRM files
                dependencies=["web_scraper_agent"],
                config=pipeline_config,
            )
        )
        
        return self
    
    def _resolve_execution_order(self) -> List[str]:
        """
        Resolve task execution order using topological sort
        Ensures dependencies are executed before dependent tasks
        """
        if not self.tasks:
            return []
            
        # Create dependency graph
        in_degree = {task_name: 0 for task_name in self.tasks}
        graph = {task_name: [] for task_name in self.tasks}
        
        # Build graph and calculate in-degrees
        for task_name, config in self.task_configs.items():
            for dependency in config.dependencies:
                if dependency not in self.tasks:
                    raise TaskDependencyError(f"Task '{task_name}' depends on unknown task '{dependency}'")
                
                graph[dependency].append(task_name)
                in_degree[task_name] += 1
        
        # Topological sort using Kahn's algorithm
        queue = [task for task, degree in in_degree.items() if degree == 0]
        execution_order = []
        
        while queue:
            current_task = queue.pop(0)
            execution_order.append(current_task)
            
            # Update in-degrees of dependent tasks
            for dependent_task in graph[current_task]:
                in_degree[dependent_task] -= 1
                if in_degree[dependent_task] == 0:
                    queue.append(dependent_task)
        
        # Check for circular dependencies
        if len(execution_order) != len(self.tasks):
            remaining_tasks = set(self.tasks.keys()) - set(execution_order)
            raise TaskDependencyError(f"Circular dependency detected. Remaining tasks: {remaining_tasks}")
        
        self.execution_order = execution_order
        return execution_order
    
    async def execute_pipeline(
        self, max_parallel_tasks: int = 3, continue_on_non_critical_failure: bool = True
    ) -> Dict[str, Any]:
        """
        Execute the complete pipeline with dependency management and parallel execution
        """
        start_time = datetime.now()
        logger.info("🚀 Starting pipeline execution")
        
        try:
            # Resolve execution order
            self.execution_order = self._resolve_execution_order()
            logger.info(f"Task execution order: {' → '.join(self.execution_order)}")
            
            # Track task execution state
            completed_tasks = set()
            failed_tasks = set()
            running_tasks = set()
            
            # Execute tasks respecting dependencies and parallelism
            for task_name in self.execution_order:
                # Wait for dependencies to complete
                task_config = self.task_configs[task_name]
                
                # Check if all dependencies completed successfully
                for dependency in task_config.dependencies:
                    if dependency in failed_tasks:
                        if task_config.critical:
                            logger.error(f"Critical dependency '{dependency}' failed. Stopping pipeline.")
                            raise TaskDependencyError(f"Critical dependency '{dependency}' failed")
                        else:
                            logger.warning(
                                f"Non-critical dependency '{dependency}' failed. Skipping task '{task_name}'"
                            )
                            continue
                
                # Execute task
                logger.info(f"⚡ Executing task: {task_name}")
                task = self.tasks[task_name]
                
                try:
                    # Run task with retry logic
                    result = await task.run_with_retry(self.context)
                    
                    # Store result in context
                    self.context.set_task_result(task_name, result)
                    
                    if result.status == TaskStatus.SUCCESS:
                        completed_tasks.add(task_name)
                        logger.info(f"✅ Task '{task_name}' completed successfully")
                    else:
                        failed_tasks.add(task_name)
                        logger.error(f"❌ Task '{task_name}' failed: {result.error}")
                        
                        if task_config.critical and not continue_on_non_critical_failure:
                            raise TaskDependencyError(f"Critical task '{task_name}' failed: {result.error}")
                
                except Exception as e:
                    failed_tasks.add(task_name)
                    logger.error(f"❌ Task '{task_name}' encountered exception: {str(e)}")
                    
                    if task_config.critical and not continue_on_non_critical_failure:
                        raise
            
            # Generate final report
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            pipeline_result = self._generate_pipeline_report(
                start_time, end_time, execution_time, completed_tasks, failed_tasks
            )
            
            # Save pipeline result
            await self._save_pipeline_result(pipeline_result)
            
            logger.info(f"🎉 Pipeline execution completed in {execution_time:.2f} seconds")
            logger.info(f"✅ Successful tasks: {len(completed_tasks)}/{len(self.tasks)}")
            
            if failed_tasks:
                logger.warning(f"❌ Failed tasks: {len(failed_tasks)} - {list(failed_tasks)}")
            
            return pipeline_result
            
        except Exception as e:
            logger.error(f"💥 Pipeline execution failed: {str(e)}")
            
            # Generate failure report
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            failure_report = {
                "status": "failed",
                "error": str(e),
                "execution_time": execution_time,
                "pipeline_stats": self.context.get_pipeline_stats(),
            }
            
            await self._save_pipeline_result(failure_report)
            raise
    
    def _generate_pipeline_report(
        self, start_time: datetime, end_time: datetime, execution_time: float, completed_tasks: set, failed_tasks: set
    ) -> Dict[str, Any]:
        """Generate comprehensive pipeline execution report"""
        
        # Get pipeline statistics
        pipeline_stats = self.context.get_pipeline_stats()
        
        # Collect task results
        task_results = {}
        for task_name in self.tasks:
            result = self.context.get_task_result(task_name)
            if result:
                task_results[task_name] = {
                    "status": result.status.value,
                    "execution_time": result.execution_time,
                    "metadata": result.metadata,
                    "error": result.error,
                }
        
        # Build final report
        report = {
            "status": "success" if not failed_tasks else "partial_success",
            "pipeline_info": {
                "execution_start": start_time.isoformat(),
                "execution_end": end_time.isoformat(),
                "total_execution_time": execution_time,
                "tasks_registered": len(self.tasks),
                "tasks_completed": len(completed_tasks),
                "tasks_failed": len(failed_tasks),
                "success_rate": (len(completed_tasks) / len(self.tasks)) * 100 if self.tasks else 0,
            },
            "task_execution_order": self.execution_order,
            "task_results": task_results,
            "pipeline_statistics": pipeline_stats,
            "output_files": self._collect_output_files(),
        }
        
        return report
    
    def _collect_output_files(self) -> Dict[str, str]:
        """Collect paths to generated output files from 5-agent pipeline"""
        output_files = {}
        
        # Check Agent 5 (Web Scraper) - Final output
        web_scraper_result = self.context.get_task_result("web_scraper_agent")
        if web_scraper_result and web_scraper_result.status == TaskStatus.SUCCESS:
            output_files["final_enriched_contacts"] = web_scraper_result.data.get("output_file", "")
        
        # Check Agent 4 (Smart Deduplicator) - Deduplicated output
        deduplicator_result = self.context.get_task_result("smart_deduplicator_agent")
        if deduplicator_result and deduplicator_result.status == TaskStatus.SUCCESS:
            output_files["deduplicated_contacts"] = deduplicator_result.data.get("output_file", "")
        
        # Check Agent 3 (Data Consolidator) - Consolidated output
        consolidator_result = self.context.get_task_result("data_consolidator_agent")
        if consolidator_result and consolidator_result.status == TaskStatus.SUCCESS:
            output_files["consolidated_contacts"] = consolidator_result.data.get("output_file", "")
        
        return output_files
    
    async def _save_pipeline_result(self, result: Dict[str, Any]):
        """Save pipeline execution result to file"""
        try:
            output_dir = Path(self.pipeline_config.get("output_directory", "output"))
            output_dir.mkdir(exist_ok=True)
            
            result_file = output_dir / "pipeline_execution_result.json"
            
            # Convert datetime objects to strings for JSON serialization
            def json_serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            with open(result_file, "w") as f:
                json.dump(result, f, indent=2, default=json_serializer)
            
            logger.info(f"📄 Pipeline result saved to: {result_file}")
            
        except Exception as e:
            logger.warning(f"Failed to save pipeline result: {str(e)}")
    
    def get_task_result(self, task_name: str) -> Optional[TaskResult]:
        """Get result of a specific task"""
        return self.context.get_task_result(task_name)
    
    def get_pipeline_context(self) -> PipelineContext:
        """Get the pipeline context"""
        return self.context
    
    def print_pipeline_summary(self):
        """Print a summary of the configured pipeline"""
        print("\n" + "=" * 80)
        print("🏗️  PIPELINE CONFIGURATION SUMMARY")
        print("=" * 80)
        
        print(f"\n📋 Registered Tasks ({len(self.tasks)}):")
        for task_name, config in self.task_configs.items():
            deps = ", ".join(config.dependencies) if config.dependencies else "None"
            critical = "🔥" if config.critical else "⚠️ "
            print(f"  {critical} {task_name} (deps: {deps})")
        
        if self.execution_order:
            print(f"\n⚡ Execution Order:")
            print(f"  {' → '.join(self.execution_order)}")
        
        print(f"\n🔧 Configuration:")
        for key, value in self.pipeline_config.items():
            print(f"  • {key}: {value}")
        
        print("=" * 80)


# Convenience functions for common use cases
def create_standard_pipeline(**kwargs) -> MasterOrchestrator:
    """Create a standard 5-agent contact processing pipeline with default configuration"""
    orchestrator = MasterOrchestrator()
    return orchestrator.create_agent_based_pipeline(**kwargs)


async def run_quick_consolidation(
    source_directory: str = "DataSource", 
    output_directory: str = "output"
) -> Dict[str, Any]:
    """Run quick 5-agent consolidation pipeline"""
    orchestrator = MasterOrchestrator()
    orchestrator.create_agent_based_pipeline(
        source_directory=source_directory,
        output_directory=output_directory
    )
    return await orchestrator.execute_pipeline()


async def run_complete_pipeline(**kwargs) -> Dict[str, Any]:
    """Run the complete 5-agent pipeline with all standard tasks"""
    orchestrator = create_standard_pipeline(**kwargs)
    return await orchestrator.execute_pipeline() 
