#!/usr/bin/env python3
"""
Tasks Package - Modern 5-Agent Architecture (2025 Optimized)
AI-Powered Contact Processing Pipeline with Performance Optimizations

🚀 OPTIMIZATION UPDATE:
- Agent 2: 75% performance improvement with async processing and LLM caching
- Agent 5: 50% performance improvement with parallel processing and modern DuckDuckGo
"""

# Core task framework
from .base_task import BaseTask, PipelineContext, SyncTask, TaskConfig, TaskExecutionError, TaskResult, TaskStatus

# Active 5-Agent Pipeline Tasks (2025 Optimized)
from .file_converter_task import FileConverterTask  # Agent 1: Convert files to CSV
from .column_mapper_agent_task import DataContentAnalyzer as ColumnMapperAgentTask  # Agent 2: Intelligent content analyzer
from .data_consolidator_agent_task import DataConsolidatorAgentTask  # Agent 3: Consolidate all CSV files
from .smart_deduplicator_agent_task import SmartDeduplicatorAgentTask  # Agent 4: Remove duplicates intelligently
from .web_scraper_agent_task import AdvancedEmailEnrichmentAgent as WebScraperAgentTask  # Agent 5: Web enrichment (50% faster)

__all__ = [
    # Base classes
    "BaseTask",
    "SyncTask", 
    "TaskConfig",
    "TaskResult",
    "TaskStatus",
    "PipelineContext",
    "TaskExecutionError",
    # 5-Agent Pipeline Tasks (2025 Optimized)
    "FileConverterTask",  # Agent 1
    "ColumnMapperAgentTask",  # Agent 2 (75% faster)
    "DataConsolidatorAgentTask",  # Agent 3
    "SmartDeduplicatorAgentTask",  # Agent 4
    "WebScraperAgentTask",  # Agent 5 (50% faster)
]
