#!/usr/bin/env python3
"""
Agent 2: Column Mapper Agent Task
Analyzes data content and maps columns to standard fields using AI

Purpose: Intelligently map diverse column names to standardized fields
Features: Parallel processing, advanced sampling, and robust field detection
Input: CSV files from Agent 1
Output: Analyzed and mapped CSV files in output/agent_2_column_mapper/
"""

import asyncio
import json
import logging
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np

from utils.ai_agent_core import AIAgentCore
from .base_task import PipelineContext, SyncTask, TaskResult, TaskStatus
from constants import (
    STANDARD_FIELDS, FIELD_PRIORITIES, EMAIL_PATTERNS, BUSINESS_INDICATORS,
    SAMPLING_CONFIG, LLM_CONFIG, PERFORMANCE_CONFIG, HEADER_DETECTION,
    QUALITY_THRESHOLDS
)

# IMPROVEMENT: Compiled regex patterns for performance
import re
HTTP_PATTERN = re.compile(r'https?://', re.IGNORECASE)
PHONE_PATTERN = re.compile(r'\d{3,}')
EMAIL_PATTERN = re.compile(r'@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')


class DataContentAnalyzer(SyncTask):
    """
    Agent 2: Enhanced Data Content Analyzer (2025 Optimized)
    
    Completely redesigned for speed, accuracy, and robustness:
    - Parallel file processing with async/await patterns
    - Advanced stratified sampling for better LLM input
    - Vectorized operations for performance
    - Comprehensive error handling and quality scoring
    - Testable modular functions
    """

    def __init__(self, config):
        super().__init__(config)
        self.name = "column_mapper_agent"
        self.logger = logging.getLogger(self.name)
        self.output_dir = Path("output/agent_2_column_mapper")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Use shared constants to prevent configuration drift
        self.STANDARD_FIELDS = STANDARD_FIELDS
        self.field_priorities = FIELD_PRIORITIES
        
        # Email validation patterns (moved from instance to shared)
        self.email_patterns = {k: re.compile(v) for k, v in EMAIL_PATTERNS.items()}
        
        # Processing statistics with enhanced tracking
        self.stats = {
            "files_processed": 0,
            "total_rows_analyzed": 0,
            "fields_extracted": 0,
            "anomalies_detected": 0,
            "data_quality_score": 0.0,
            "total_quality_score": 0.0,  # For aggregation
            "llm_calls": 0,
            "fallback_uses": 0,
            "parallel_files_processed": 0
        }

    def execute_sync(self, context: PipelineContext) -> TaskResult:
        """Execute enhanced data content analysis with parallel processing"""
        start_time = time.time()

        self.logger.info("🧠 AGENT 2: Enhanced Data Content Analyzer (2025 Optimized)")
        self.logger.info("🎯 MISSION: Parallel processing with improved sampling and robust analysis")
        self.logger.info("📊 APPROACH: Stratified sampling + vectorized operations + LLM intelligence")
        self.logger.info("⚡ ENHANCED: Speed, accuracy, robustness improvements implemented")

        try:
            # Get converted files from Agent 1
            if not context.has_task_result("file_converter"):
                return self._create_error_result("No data from file converter")

            converted_files = context.get_task_data("file_converter").get("converted_files", [])
            csv_files = [Path(f) for f in converted_files]

            if not csv_files:
                return self._create_error_result("No CSV files to process")

            self.logger.info(f"📁 Processing {len(csv_files)} files with parallel analysis")

            # IMPROVEMENT: Parallel file processing with proper async handling
            analyzed_files = self._process_files_parallel(csv_files)
            
            # IMPROVEMENT: Calculate aggregated data quality score
            self._calculate_final_quality_score()

            processing_time = time.time() - start_time

            # Create enhanced verification report
            verification = self._create_verification_report(analyzed_files, processing_time)

            self.logger.info("🎉 ENHANCED DATA CONTENT ANALYSIS COMPLETED:")
            self.logger.info(f"   📊 Files processed: {self.stats['files_processed']}")
            self.logger.info(f"   📈 Rows analyzed: {self.stats['total_rows_analyzed']}")
            self.logger.info(f"   🎯 Fields extracted: {self.stats['fields_extracted']}")
            self.logger.info(f"   🔍 Anomalies handled: {self.stats['anomalies_detected']}")
            self.logger.info(f"   🧠 LLM calls: {self.stats['llm_calls']}")
            self.logger.info(f"   🔄 Fallback uses: {self.stats['fallback_uses']}")
            self.logger.info(f"   ⚡ Parallel files: {self.stats['parallel_files_processed']}")
            self.logger.info(f"   📈 Data quality: {self.stats['data_quality_score']:.1f}%")
            self.logger.info(f"   ⏱️ Processing time: {processing_time:.1f}s")

            return TaskResult(
                task_name=self.name,
                status=TaskStatus.SUCCESS,
                data={
                    "mapped_files": analyzed_files,
                    "standard_columns": self.STANDARD_FIELDS,
                    "processing_stats": self.stats
                },
                metadata={
                    "agent_number": 2,
                    "agent_name": "Enhanced Data Content Analyzer (2025 Optimized)",
                    "files_processed": len(analyzed_files),
                    "approach": "parallel_content_analysis_with_enhanced_sampling",
                    "optimizations": ["parallel_processing", "stratified_sampling", "vectorized_operations", "robust_header_detection"],
                    "capabilities": ["table_understanding", "anomaly_detection", "header_inference", "field_recognition", "quality_scoring"]
                }
            )

        except Exception as e:
            self.logger.error(f"❌ Critical error in data content analysis: {str(e)}")
            return self._create_error_result(f"Data analysis failed: {str(e)}")
    
    def _process_files_parallel(self, csv_files: List[Path]) -> List[str]:
        """IMPROVEMENT: Robust parallel processing with edge case handling"""
        try:
            # CRITICAL FIX: Use safer async pattern to avoid event loop conflicts
            async def _parallel_driver():
                # Use safe config access with fallback
                max_concurrent = min(len(csv_files), PERFORMANCE_CONFIG.get('max_concurrent_files', 3))
                self.logger.info(f"⚡ Processing {len(csv_files)} files with {max_concurrent} concurrent workers")
                
                semaphore = asyncio.Semaphore(max_concurrent)
                tasks = [self._analyze_csv_with_semaphore(semaphore, csv_file) for csv_file in csv_files]
                return await asyncio.gather(*tasks, return_exceptions=True)
            
            # Use asyncio.run for cleaner event loop management
            results = asyncio.run(_parallel_driver())
            
            analyzed_files = []
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"❌ Error in parallel processing: {str(result)}")
                    continue
                
                if result:
                    analyzed_files.append(result)
                    self.stats["files_processed"] += 1
                    self.stats["parallel_files_processed"] += 1
                    
                    # IMPROVEMENT: Track fallback usage from parallel results
                    if "fallback" in str(result).lower():
                        self.stats["fallback_uses"] += 1
            
            return analyzed_files
                
        except Exception as e:
            self.logger.error(f"❌ Parallel processing failed: {str(e)}")
            # Fallback to sequential processing
            self.logger.info("🔄 Falling back to sequential processing")
            return self._process_files_sequential(csv_files)
    
    def _process_files_sequential(self, csv_files: List[Path]) -> List[str]:
        """Fallback sequential processing if parallel fails"""
        analyzed_files = []
        for csv_file in csv_files:
            try:
                # Use asyncio.run for each file individually (safer fallback)
                analyzed_file = asyncio.run(self._analyze_csv_content(csv_file))
                if analyzed_file:
                    analyzed_files.append(analyzed_file)
                    self.stats["files_processed"] += 1
                    self.stats["fallback_uses"] += 1
            except Exception as e:
                self.logger.error(f"❌ Sequential processing failed for {csv_file.name}: {str(e)}")
                continue
        return analyzed_files
    
    async def _analyze_csv_with_semaphore(self, semaphore: asyncio.Semaphore, csv_file: Path) -> Optional[str]:
        """Analyze CSV with concurrency limiting"""
        async with semaphore:
            return await self._analyze_csv_content(csv_file)
    
    def _calculate_final_quality_score(self):
        """IMPROVEMENT: Calculate aggregated data quality score"""
        if self.stats["files_processed"] > 0:
            self.stats["data_quality_score"] = (
                self.stats["total_quality_score"] / max(1, self.stats["files_processed"])
            )
        else:
            self.stats["data_quality_score"] = 0.0

    async def _analyze_csv_content(self, csv_file: Path) -> Optional[str]:
        """Analyze CSV content using LLM for intelligent field recognition"""
        
        self.logger.info(f"🔍 ANALYZING: {csv_file.name}")
        
        try:
            # Read CSV with flexible header detection
            df = self._smart_csv_read(csv_file)
            
            if df is None or df.empty:
                self.logger.warning(f"❌ Could not read or file is empty: {csv_file.name}")
                return None

            self.logger.info(f"📊 Loaded: {len(df)} rows, {len(df.columns)} columns")
            self.stats["total_rows_analyzed"] += len(df)

            # Convert DataFrame to JSON for LLM analysis
            data_sample = self._prepare_data_for_llm(df)
            
            # Analyze content with LLM
            analysis_result = await self._llm_analyze_content(data_sample, csv_file.name)
            
            if not analysis_result:
                self.logger.error(f"❌ LLM analysis failed for {csv_file.name}")
                return None

            # Extract and standardize data based on LLM analysis
            standardized_df = self._extract_standardized_data(df, analysis_result)
            
            # Save standardized file
            output_file = self.output_dir / f"analyzed_{csv_file.name}"
            standardized_df.to_csv(output_file, index=False, encoding="utf-8")
            
            # Save analysis report
            analysis_report = {
                "source_file": str(csv_file),
                "output_file": str(output_file),
                "original_shape": list(df.shape),
                "standardized_shape": list(standardized_df.shape),
                "fields_identified": analysis_result.get("field_mappings", {}),
                "anomalies_detected": analysis_result.get("anomalies", []),
                "data_quality": analysis_result.get("quality_score", 0),
                "processing_method": "intelligent_content_analysis"
            }
            
            report_file = self.output_dir / f"analysis_{csv_file.stem}.json"
            with open(report_file, "w") as f:
                json.dump(analysis_report, f, indent=2)

            self.stats["fields_extracted"] += len(analysis_result.get("field_mappings", {}))
            self.stats["anomalies_detected"] += len(analysis_result.get("anomalies", []))

            self.logger.info(f"✅ Successfully analyzed {csv_file.name}")
            self.logger.info(f"   🎯 Fields identified: {len(analysis_result.get('field_mappings', {}))}")
            self.logger.info(f"   🔍 Anomalies handled: {len(analysis_result.get('anomalies', []))}")
            
            return str(output_file)

        except Exception as e:
            self.logger.error(f"❌ Error analyzing {csv_file.name}: {str(e)}")
            return None

    def _smart_csv_read(self, csv_file: Path) -> Optional[pd.DataFrame]:
        """IMPROVEMENT: Enhanced smart CSV reading with robust header detection"""
        
        try:
            # First, try to find the real header row using intelligent scoring
            header_row = self._find_real_header_row(csv_file)
            
            if header_row is not None:
                # Read CSV with detected header row
                df = pd.read_csv(csv_file, header=header_row, low_memory=False)
                
                # Validate and clean the headers
                if self._validate_and_clean_headers(df):
                    self.logger.info(f"🎯 Successfully detected header at row {header_row}: {list(df.columns)[:5]}...")
                    return df
                else:
                    self.logger.warning(f"⚠️ Header validation failed for row {header_row}")
            
            # Fallback: Use basic detection method
            return self._basic_csv_read(csv_file)
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced CSV reading failed for {csv_file}: {str(e)}")
            return self._basic_csv_read(csv_file)
    
    def _find_real_header_row(self, csv_file: Path) -> Optional[int]:
        """IMPROVEMENT: Intelligent header row detection using content analysis"""
        
        try:
            # Read first several rows to analyze
            preview_df = pd.read_csv(csv_file, header=None, nrows=10, low_memory=False)
            
            best_score = -1
            best_row = None
            
            # Analyze each potential header row
            for row_idx in range(min(6, len(preview_df))):
                row_data = preview_df.iloc[row_idx]
                score = self._score_header_row(row_data)
                
                self.logger.debug(f"Row {row_idx} score: {score} - {row_data.tolist()[:3]}...")
                
                if score > best_score and score > 0:
                    best_score = score
                    best_row = row_idx
            
            if best_row is not None:
                self.logger.info(f"🎯 Best header row detected: {best_row} (score: {best_score})")
            else:
                self.logger.warning("⚠️ No valid header row found using scoring")
            
            return best_row
            
        except Exception as e:
            self.logger.warning(f"Header detection failed: {str(e)}")
            return None
    
    def _score_header_row(self, row_data: pd.Series) -> int:
        """IMPROVEMENT: Optimized header scoring with performance enhancements"""
        
        score = 0
        # CRITICAL FIX: Clip long cells to prevent performance issues with HTML/embedded content
        row_str = ' '.join(str(val).lower()[:100] for val in row_data if pd.notna(val))
        
        # Header keywords (positive scoring)
        header_keywords = [
            'name', 'first', 'last', 'email', 'linkedin', 'company', 'designation',
            'role', 'title', 'phone', 'location', 'profile', 'organization',
            'contact', 'address', 'position', 'job', 'work', 'business'
        ]
        
        # Metadata anti-patterns (negative scoring)
        metadata_patterns = [
            'event link', 'https://', 'http://', 'www.', '.com', '.org',
            'unnamed:', 'highlight', 'contact directly', 'account', 'requests'
        ]
        
        # Positive scoring for header-like content
        for keyword in header_keywords:
            if keyword in row_str:
                score += 3
        
        # Additional bonuses
        if any(pattern in row_str for pattern in ['id', 'number', 'no', '#']):
            score += 2
        if any(pattern in row_str for pattern in ['profile', 'url', 'link']):
            score += 2
        if len([val for val in row_data if pd.notna(val) and len(str(val).strip()) < 20]) >= 3:
            score += 1  # Short headers are good
        
        # Negative scoring for metadata patterns
        for pattern in metadata_patterns:
            if pattern in row_str:
                score -= 5
        
        # Require minimum number of non-empty columns
        non_empty = sum(1 for val in row_data if pd.notna(val) and str(val).strip())
        if non_empty < 3:
            score -= 10
        
        return score
    
    def _validate_and_clean_headers(self, df: pd.DataFrame) -> bool:
        """IMPROVEMENT: Validate and clean detected headers"""
        
        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]
        
        # Remove unnamed columns at the end
        while len(df.columns) > 0 and (df.columns[-1].startswith('Unnamed:') or pd.isna(df.columns[-1])):
            df.drop(df.columns[-1], axis=1, inplace=True)
        
        # Validate we have reasonable headers
        valid_headers = sum(1 for col in df.columns 
                          if not col.startswith('Unnamed:') and len(str(col).strip()) > 0)
        
        return valid_headers >= 2 and len(df) > 0
    
    def _basic_csv_read(self, csv_file: Path) -> Optional[pd.DataFrame]:
        """IMPROVEMENT: Fallback basic CSV reading method"""
        
        try:
            # Try standard header positions
            for header_row in [0, 1, 2]:
                try:
                    df = pd.read_csv(csv_file, header=header_row, low_memory=False)
                    if len(df) > 0 and not df.iloc[0].isna().all():
                        self.logger.info(f"📍 Basic detection: headers at row {header_row}")
                        return df
                except:
                    continue
            
            # Final fallback: no header assumption
            self.logger.info("📍 Using generic column names")
            df = pd.read_csv(csv_file, header=None, low_memory=False)
            df.columns = [f"column_{i}" for i in range(len(df.columns))]
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Basic CSV reading failed: {str(e)}")
            return None

    def _prepare_data_for_llm(self, df: pd.DataFrame) -> Dict[str, Any]:
        """IMPROVEMENT: Enhanced data sampling with stratified sampling for better accuracy"""
        
        # Use enhanced sampling configuration
        max_sample_rows = SAMPLING_CONFIG['max_sample_rows']
        min_sample_rows = SAMPLING_CONFIG['min_sample_rows']
        sample_per_column = SAMPLING_CONFIG['sample_per_column']
        use_stratified = SAMPLING_CONFIG['stratified_sampling']
        
        total_rows = len(df)
        sample_size = min(max_sample_rows, max(min_sample_rows, total_rows))
        
        if use_stratified and total_rows > sample_size:
            # IMPROVEMENT: Stratified sampling for better representation
            sample_df = self._stratified_sample(df, sample_size, sample_per_column)
        else:
            # Simple head sampling for small datasets
            sample_df = df.head(sample_size)
        
        # IMPROVEMENT: Include data type hints and column statistics
        data_sample = {
            "column_names": list(df.columns),
            "total_rows": total_rows,
            "sample_rows": sample_df.to_dict('records'),
            # IMPROVEMENT: Add back essential metadata for better LLM decisions
            "column_stats": self._get_column_statistics(df),
            "data_types": {col: str(df[col].dtype) for col in df.columns},
            "sample_size": len(sample_df),
            "sampling_method": "stratified" if use_stratified else "head"
        }
        
        return data_sample
    
    def _stratified_sample(self, df: pd.DataFrame, sample_size: int, sample_per_column: int) -> pd.DataFrame:
        """IMPROVEMENT: Robust stratified sampling with edge case handling"""
        try:
            # Start with head sampling (avoid duplicates later)
            sampled_indices = set(range(min(sample_size // 2, len(df))))
            
            # Add stratified samples per column to capture rare values
            for col in df.columns:
                if len(sampled_indices) >= sample_size:
                    break
                    
                # Sample from different parts of the column
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    # CRITICAL FIX: Prevent step=0 ValueError and ensure minimum step
                    step = max(1, len(col_data) // max(1, sample_per_column))
                    
                    # Use while loop to control exact sample size
                    i = 0
                    while i < len(col_data) and len(sampled_indices) < sample_size:
                        sampled_indices.add(col_data.index[i])
                        i += step
            
            # IMPROVEMENT: Handle edge case where we have fewer unique indices than needed
            if len(sampled_indices) < min(sample_size, len(df)):
                # Fill remaining with random indices
                remaining_indices = set(range(len(df))) - sampled_indices
                needed = min(sample_size - len(sampled_indices), len(remaining_indices))
                import random
                sampled_indices.update(random.sample(list(remaining_indices), needed))
            
            # Convert to sorted list and create sample
            final_indices = sorted(list(sampled_indices))[:sample_size]
            return df.iloc[final_indices]
            
        except Exception as e:
            self.logger.warning(f"Stratified sampling failed: {str(e)}, using head sampling")
            return df.head(sample_size)
    
    def _get_column_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """IMPROVEMENT: Vectorized column statistics with optimized pattern detection"""
        stats = {}
        for col in df.columns:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                # IMPROVEMENT: Vectorized pattern detection for better performance
                sample_data = col_data.head(10)  # Use sample for pattern detection
                
                stats[col] = {
                    "non_null_count": len(col_data),
                    "null_percentage": ((len(df) - len(col_data)) / len(df)) * 100,
                    "unique_count": col_data.nunique(),
                    "sample_values": col_data.head(3).tolist(),
                    # IMPROVEMENT: Vectorized email detection
                    "has_email_pattern": self._vectorized_email_check(sample_data),
                    # IMPROVEMENT: Vectorized URL detection
                    "has_url_pattern": self._vectorized_url_check(sample_data),
                    # IMPROVEMENT: Vectorized phone detection
                    "has_phone_pattern": self._vectorized_phone_check(sample_data)
                }
            else:
                stats[col] = {
                    "non_null_count": 0,
                    "null_percentage": 100,
                    "unique_count": 0,
                    "sample_values": [],
                    "has_email_pattern": False,
                    "has_url_pattern": False,
                    "has_phone_pattern": False
                }
        return stats
    
    def _vectorized_email_check(self, data: pd.Series) -> bool:
        """IMPROVEMENT: Optimized email detection with compiled regex"""
        if len(data) == 0:
            return False
        # Use compiled regex for better performance
        str_data = data.astype(str)
        return str_data.str.contains(EMAIL_PATTERN, na=False, regex=True).any()
    
    def _vectorized_url_check(self, data: pd.Series) -> bool:
        """IMPROVEMENT: Optimized URL detection with compiled regex"""
        if len(data) == 0:
            return False
        # Use compiled regex for better performance
        str_data = data.astype(str)
        return str_data.str.contains(HTTP_PATTERN, na=False, regex=True).any()
    
    def _vectorized_phone_check(self, data: pd.Series) -> bool:
        """IMPROVEMENT: Optimized phone detection with compiled regex"""
        if len(data) == 0:
            return False
        # Use compiled regex for better performance
        str_data = data.astype(str)
        return str_data.str.contains(PHONE_PATTERN, na=False, regex=True).any()

    async def _llm_analyze_content(self, data_sample: Dict[str, Any], filename: str) -> Optional[Dict[str, Any]]:
        """Use LLM to analyze data content and identify fields"""
        
        self.logger.info("🤖 LLM analyzing data content...")
        
        ai_agent = AIAgentCore()
        
        # 🚀 PERFORMANCE OPTIMIZATION: Much more concise prompt
        llm_prompt = f"""Analyze CSV data and map columns to target fields.

FILE: {filename}
COLUMNS: {data_sample['column_names']}
SAMPLE DATA (first 3 rows): {json.dumps(data_sample['sample_rows'][:3], default=str)}

TARGET FIELDS:
- First Name, Last Name, Current Company, Designation / Role, LinkedIn Profile URL, Email, Phone Number, Geo (Location by City)

SPECIAL: If a column contains full names (first+last together), map to "FULL_NAME_TO_SPLIT"

JSON RESPONSE ONLY:
{{
  "field_mappings": {{"column_name": "target_field"}},
  "quality_score": 85
}}"""

        try:
            # IMPROVEMENT: Use config timeout and track LLM calls
            self.stats["llm_calls"] += 1
            start_time = time.time()
            
            # Use configured timeout from LLM_CONFIG
            timeout = LLM_CONFIG.get('timeout', 45)
            response = await asyncio.wait_for(
                ai_agent._make_ollama_request(llm_prompt),
                timeout=timeout
            )
            processing_time = time.time() - start_time
            
            self.logger.info(f"🤖 LLM Response time: {processing_time:.1f}s")
            
            # Clean and parse JSON response
            cleaned_response = self._clean_llm_response(response)
            
            if not cleaned_response or cleaned_response.strip() == "":
                self.logger.warning("❌ LLM returned empty response, using fallback")
                fallback_result = self._create_fallback_analysis(data_sample)
                self.stats["fallback_uses"] += 1
                self.stats["total_quality_score"] += fallback_result.get("quality_score", 0)
                return fallback_result
            
            try:
                analysis_result = json.loads(cleaned_response)
                
                # Validate required fields
                if "field_mappings" not in analysis_result:
                    self.logger.warning("❌ LLM response missing field_mappings, using fallback")
                    fallback_result = self._create_fallback_analysis(data_sample)
                    self.stats["fallback_uses"] += 1
                    self.stats["total_quality_score"] += fallback_result.get("quality_score", 0)
                    return fallback_result
                
                # CRITICAL FIX: Track quality score from successful LLM analysis
                quality_score = analysis_result.get("quality_score", 75)
                self.stats["total_quality_score"] += quality_score
                
                self.logger.info("✅ LLM analysis completed")
                return analysis_result
            
            except json.JSONDecodeError as e:
                self.logger.warning(f"❌ JSON parsing failed, using fallback: {e}")
                fallback_result = self._create_fallback_analysis(data_sample)
                self.stats["fallback_uses"] += 1
                self.stats["total_quality_score"] += fallback_result.get("quality_score", 0)
                fallback_result["quality_score"] = 50
                return fallback_result
            
            # Ensure required fields exist
            if "field_mappings" not in analysis_result:
                analysis_result["field_mappings"] = {}
            if "anomalies" not in analysis_result:
                analysis_result["anomalies"] = []
            if "quality_score" not in analysis_result:
                analysis_result["quality_score"] = 50
            
            self.logger.info("✅ LLM analysis completed")
            self.logger.info(f"   🎯 Fields identified: {len(analysis_result.get('field_mappings', {}))}")
            self.logger.info(f"   📊 Quality score: {analysis_result.get('quality_score', 0)}")
            
            return analysis_result
            
        except asyncio.TimeoutError:
            self.logger.warning("⏰ LLM analysis timed out (30s), using fast fallback")
            return self._create_fallback_analysis(data_sample)
        except Exception as e:
            self.logger.warning(f"❌ LLM analysis failed: {str(e)}, using fallback")
            return self._create_fallback_analysis(data_sample)
        finally:
            # Ensure proper cleanup
            try:
                if hasattr(ai_agent, 'close'):
                    await ai_agent.close()
            except:
                pass

    def _clean_llm_response(self, response: str) -> str:
        """Clean LLM response to extract valid JSON"""
        
        # Remove thinking blocks
        if '<think>' in response:
            parts = response.split('</think>')
            if len(parts) > 1:
                response = parts[-1]
        
        # Remove code block markers
        response = response.strip()
        if response.startswith('```json'):
            response = response[7:]
        if response.startswith('```'):
            response = response[3:]
        if response.endswith('```'):
            response = response[:-3]
        
        # Find JSON object
        start_idx = response.find('{')
        end_idx = response.rfind('}')
        
        if start_idx >= 0 and end_idx >= 0:
            return response[start_idx:end_idx + 1]
        
        return response.strip()

    def _extract_standardized_data(self, df: pd.DataFrame, analysis_result: Dict[str, Any]) -> pd.DataFrame:
        """IMPROVEMENT: Extract standardized data with duplicate mapping detection"""
        
        field_mappings = analysis_result.get("field_mappings", {})
        
        # IMPROVEMENT: Validate and clean mappings before processing
        validated_mappings = self._validate_and_resolve_duplicate_mappings(df, field_mappings)
        
        # Create standardized DataFrame
        standardized_data = {}
        
        # Initialize all standard fields as empty
        for field in self.STANDARD_FIELDS:
            standardized_data[field] = [""] * len(df)
        
        # Map validated fields
        for original_col, target_field in validated_mappings.items():
            if original_col in df.columns:
                
                # 🔧 SPECIAL HANDLING: Full name splitting
                if target_field == "FULL_NAME_TO_SPLIT":
                    self.logger.info(f"   🔄 Splitting full names in '{original_col}'")
                    first_names, last_names = self._split_full_names(df[original_col])
                    standardized_data["First Name"] = first_names
                    standardized_data["Last Name"] = last_names
                    self.logger.info(f"   ✅ Split {len([n for n in first_names if n])} names into First/Last")
                
                # Regular field mapping with basic cleaning
                elif target_field in self.STANDARD_FIELDS:
                    # Clean and standardize the data
                    values = df[original_col].fillna("").astype(str)
                    standardized_data[target_field] = values.tolist()
                    self.logger.info(f"   📋 Mapped '{original_col}' → '{target_field}'")
        
        standardized_df = pd.DataFrame(standardized_data)
        
        return standardized_df
    
    def _validate_and_resolve_duplicate_mappings(self, df: pd.DataFrame, field_mappings: Dict[str, str]) -> Dict[str, str]:
        """IMPROVEMENT: Detect and resolve duplicate mappings to the same standard field"""
        
        # Group mappings by target field to detect duplicates
        field_to_columns = {}
        for col, field in field_mappings.items():
            if field not in field_to_columns:
                field_to_columns[field] = []
            field_to_columns[field].append(col)
        
        validated_mappings = {}
        
        # Process each target field with standard field validation
        for target_field, mapped_columns in field_to_columns.items():
            # IMPROVEMENT: Only accept mappings to standard fields
            if target_field in self.STANDARD_FIELDS or target_field == "FULL_NAME_TO_SPLIT":
                if len(mapped_columns) == 1:
                    # No duplicates, use as-is
                    validated_mappings[mapped_columns[0]] = target_field
                else:
                    # IMPROVEMENT: Handle duplicate mappings
                    self.logger.warning(f"🔍 Duplicate mappings detected for '{target_field}': {mapped_columns}")
                    best_column = self._choose_best_column_for_field(df, mapped_columns, target_field)
                    validated_mappings[best_column] = target_field
            else:
                # Reject non-standard field mappings
                self.logger.warning(f"❌ Rejected mapping to non-standard field: {mapped_columns} → '{target_field}'")
                self.logger.info(f"   🎯 Standard fields: {', '.join(self.STANDARD_FIELDS)}")
        
        return validated_mappings
    
    def _choose_best_column_for_field(self, df: pd.DataFrame, columns: List[str], target_field: str) -> str:
        """IMPROVEMENT: Choose the best column when multiple columns map to the same field"""
        
        best_col = columns[0]
        best_score = -1
        
        for col in columns:
            if col not in df.columns:
                continue
                
            score = self._score_column_quality(df, col, target_field)
            self.logger.debug(f"   📊 '{col}' quality score for '{target_field}': {score}")
            
            if score > best_score:
                best_score = score
                best_col = col
        
        return best_col
    
    def _score_column_quality(self, df: pd.DataFrame, column: str, target_field: str) -> int:
        """IMPROVEMENT: Score column quality for a specific target field"""
        
        if column not in df.columns:
            return 0
            
        col_data = df[column].dropna()
        if len(col_data) == 0:
            return 0
        
        score = 0
        
        # Base score: non-null count (more data = better)
        score += len(col_data) * 2
        
        # Field-specific scoring
        if target_field == "Email":
            # Score based on email pattern matches
            email_count = sum(1 for val in col_data if '@' in str(val))
            score += email_count * 10
            
        elif target_field in ["First Name", "Last Name"]:
            # Score based on name-like content
            name_count = sum(1 for val in col_data if self._looks_like_person_name(str(val)))
            score += name_count * 5
            
        elif target_field == "LinkedIn Profile URL":
            # Score based on LinkedIn URL patterns
            linkedin_count = sum(1 for val in col_data if 'linkedin.com' in str(val).lower())
            score += linkedin_count * 10
            
        elif target_field == "Phone Number":
            # Score based on phone number patterns
            phone_count = sum(1 for val in col_data if bool(re.search(r'\d{3}', str(val))))
            score += phone_count * 8
            
        elif target_field == "Current Company":
            # Score based on business indicators
            business_count = sum(1 for val in col_data 
                               if any(indicator in str(val).lower() 
                                    for indicator in BUSINESS_INDICATORS))
            score += business_count * 3
            
        # Uniqueness bonus (more unique = better for most fields)
        uniqueness = col_data.nunique() / len(col_data) if len(col_data) > 0 else 0
        score += int(uniqueness * 20)
        
        return score

    def _choose_best_email_column(self, df: pd.DataFrame, email_columns: List[str]) -> str:
        """Choose the best email column when multiple exist"""
        
        best_col = email_columns[0]
        max_email_count = 0
        
        for col in email_columns:
            if col in df.columns:
                # Count how many actual emails this column has
                email_count = df[col].fillna("").astype(str).str.contains('@', na=False).sum()
                self.logger.info(f"   📊 '{col}': {email_count} emails out of {len(df)} rows")
                
                if email_count > max_email_count:
                    max_email_count = email_count
                    best_col = col
        
        return best_col

    def _split_full_names(self, name_series: pd.Series) -> Tuple[List[str], List[str]]:
        """Split full names into first and last names"""
        
        first_names = []
        last_names = []
        
        for name in name_series:
            if pd.isna(name) or str(name).strip() == "":
                first_names.append("")
                last_names.append("")
                continue
            
            name_str = str(name).strip()
            
            try:
                # Split by spaces
                parts = name_str.split()
                
                if len(parts) >= 2:
                    # First part = first name, rest = last name
                    first_names.append(parts[0])
                    last_names.append(" ".join(parts[1:]))
                elif len(parts) == 1:
                    # Only one name part
                    first_names.append(parts[0])
                    last_names.append("")
                else:
                    first_names.append("")
                    last_names.append("")
                    
            except Exception as e:
                self.logger.warning(f"Error splitting name '{name_str}': {str(e)}")
                first_names.append("")
                last_names.append("")
        
        return first_names, last_names

    def _create_fallback_analysis(self, data_sample: Dict[str, Any]) -> Dict[str, Any]:
        """Create fallback analysis when LLM fails"""
        
        self.logger.info("🔄 Creating ENHANCED fallback analysis based on data patterns...")
        
        field_mappings = {}
        column_names = data_sample.get('column_names', [])
        sample_rows = data_sample.get('sample_rows', [])
        
        # 🚀 ENHANCED: Better pattern matching with priority for full names
        for col in column_names:
            col_lower = str(col).lower()
            
            # Check actual data in the column
            if sample_rows:
                sample_values = []
                for row in sample_rows[:5]:
                    if col in row and row[col] and str(row[col]).strip():
                        sample_values.append(str(row[col]).strip())
                
                # Analyze sample values
                if sample_values:
                    # 🎯 PRIORITY 1: Check for FULL NAME patterns FIRST (this was the main issue!)
                    if self._is_full_name_column(col_lower, sample_values):
                        field_mappings[col] = "FULL_NAME_TO_SPLIT"  # Special marker for splitting
                        self.logger.info(f"   👤 Fallback: '{col}' → Full Name (needs splitting)")
                    
                    # 🎯 PRIORITY 2: Email patterns (strong indicators)
                    elif any('@' in val for val in sample_values) or 'email' in col_lower:
                        field_mappings[col] = "Email"
                        self.logger.info(f"   📧 Fallback: '{col}' → Email")
                    
                    # 🎯 PRIORITY 3: LinkedIn URLs
                    elif any('linkedin.com' in val.lower() for val in sample_values) or 'linkedin' in col_lower:
                        field_mappings[col] = "LinkedIn Profile URL"
                        self.logger.info(f"   🔗 Fallback: '{col}' → LinkedIn")
                    
                    # 🎯 PRIORITY 4: Clear first/last name patterns (only if not full name)
                    elif 'first' in col_lower and 'name' in col_lower:
                        field_mappings[col] = "First Name"
                        self.logger.info(f"   👤 Fallback: '{col}' → First Name")
                    
                    elif 'last' in col_lower and 'name' in col_lower:
                        field_mappings[col] = "Last Name"
                        self.logger.info(f"   👤 Fallback: '{col}' → Last Name")
                    
                    # 🎯 PRIORITY 5: Company patterns
                    elif ('company' in col_lower or 'organization' in col_lower or 
                          'account name' in col_lower or col_lower == 'company'):
                        field_mappings[col] = "Current Company"
                        self.logger.info(f"   🏢 Fallback: '{col}' → Current Company")
                    
                    # 🎯 PRIORITY 6: Title/role patterns
                    elif any(keyword in col_lower for keyword in ['title', 'role', 'position', 'designation']):
                        field_mappings[col] = "Designation / Role"
                        self.logger.info(f"   💼 Fallback: '{col}' → Designation / Role")
                    
                    # 🎯 PRIORITY 7: Phone patterns
                    elif any(any(char.isdigit() for char in val) and len(val) > 6 for val in sample_values):
                        if 'phone' in col_lower or 'mobile' in col_lower:
                            field_mappings[col] = "Phone Number"
                            self.logger.info(f"   📞 Fallback: '{col}' → Phone Number")
                    
                    # 🎯 PRIORITY 8: Location patterns
                    elif any(keyword in col_lower for keyword in ['city', 'location', 'address', 'geo']):
                        field_mappings[col] = "Geo (Location by City)"
                        self.logger.info(f"   📍 Fallback: '{col}' → Location")
        
        return {
            "field_mappings": field_mappings,
            "anomalies": ["LLM analysis failed - used enhanced pattern-based fallback"],
            "quality_score": 70,  # Higher score for improved fallback
            "notes": "Enhanced fallback analysis with improved full name detection"
        }

    def _is_full_name_column(self, col_lower: str, sample_values: List[str]) -> bool:
        """🚀 ENHANCED: Better detection of full name columns"""
        
        # 🎯 ENHANCED: More comprehensive full name indicators
        full_name_indicators = [
            'speaker', 'name', 'full name', 'contact name', 'primary contact', 
            'person', 'attendee', 'participant', 'lead', 'contact', 'user',
            'member', 'guest', 'presenter', 'client', 'customer'
        ]
        
        # Check if column name suggests it contains names
        column_indicates_name = any(indicator in col_lower for indicator in full_name_indicators)
        
        # 🚀 ENHANCED: Better name pattern detection
        name_like_count = 0
        for val in sample_values[:5]:  # Check first 5 values
            if self._looks_like_person_name(val):
                name_like_count += 1
        
        # 🎯 MORE FLEXIBLE: If column name suggests names AND at least 40% look like names
        name_percentage = (name_like_count / len(sample_values)) if sample_values else 0
        
        result = column_indicates_name and name_percentage >= 0.4
        
        if result:
            self.logger.info(f"   🔍 Full name detected: '{col_lower}' - {name_like_count}/{len(sample_values)} samples look like names")
        
        return result

    def _looks_like_person_name(self, value: str) -> bool:
        """🚀 ENHANCED: Better person name detection"""
        if not value or len(value.strip()) < 3:
            return False
            
        value = value.strip()
        
        # Skip obvious non-names
        if any(char in value for char in ['@', '.com', 'http', 'www']):
            return False
            
        parts = value.split()
        
        # 🎯 ENHANCED: Better name pattern checking
        if len(parts) >= 2:  # At least first and last name
            # Check if parts are mostly alphabetic
            alpha_parts = [p for p in parts if p.replace('.', '').replace(',', '').isalpha() and len(p) > 1]
            
            # Check if most parts start with capital letters (proper names)
            capitalized_parts = [p for p in alpha_parts if p[0].isupper()]
            
            # 🚀 MORE FLEXIBLE: Most parts should be alphabetic and capitalized
            if len(alpha_parts) >= 2 and len(capitalized_parts) >= 2:
                # Additional check: avoid obvious company names
                business_indicators = ['inc', 'llc', 'corp', 'ltd', 'company', 'technologies', 'solutions']
                if not any(indicator in value.lower() for indicator in business_indicators):
                    return True
        
        return False

    def _create_verification_report(self, processed_files: List[str], processing_time: float) -> Dict[str, Any]:
        """Create comprehensive verification report"""
        
        verification = {
            "agent_name": "Data Content Analyzer",
            "approach": "intelligent_content_analysis",
            "files_processed": len(processed_files),
            "total_rows_analyzed": self.stats["total_rows_analyzed"],
            "fields_extracted": self.stats["fields_extracted"],
            "anomalies_handled": self.stats["anomalies_detected"],
            "data_quality_score": self.stats.get("data_quality_score", 0),
            "processing_time": processing_time,
            "capabilities_used": [
                "table_understanding",
                "anomaly_detection", 
                "header_inference",
                "field_recognition",
                "batch_processing"
            ],
            "output_files": processed_files,
            "standard_fields": self.STANDARD_FIELDS,
            "status": "SUCCESS"
        }
        
        # Save verification report
        verification_file = self.output_dir / "agent_2_verification_report.json"
        with open(verification_file, "w") as f:
            json.dump(verification, f, indent=2)
        
        return verification

    def _create_error_result(self, message: str) -> TaskResult:
        """Create error result"""
        self.logger.error(f"❌ Agent 2 Error: {message}")
        return TaskResult(
            task_name=self.name,
            status=TaskStatus.FAILED,
            error=message,
            metadata={"agent_number": 2, "agent_name": "Data Content Analyzer", "error": message}
        )
 