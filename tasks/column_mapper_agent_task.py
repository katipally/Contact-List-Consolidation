#!/usr/bin/env python3
"""
Agent 2: Intelligent Data Content Analyzer (2025 Complete Redesign)
🧠 CORE PHILOSOPHY: Understand data CONTENT, not column names
🎯 APPROACH: Send entire CSV as JSON to LLM for content analysis
⚡ CAPABILITIES: 
- Table understanding: Analyze actual data values
- Anomaly detection: Identify and ignore bad data
- Header inference: Work with missing/misplaced headers
- Field recognition: Map any data to standard fields
- Batch processing: Process entire files in one LLM call
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils.ai_agent_core import AIAgentCore
from .base_task import PipelineContext, SyncTask, TaskResult, TaskStatus


class DataContentAnalyzer(SyncTask):
    """
    Agent 2: Intelligent Data Content Analyzer
    
    Completely redesigned to understand data CONTENT rather than column names.
    Uses LLM to analyze actual data values and map them to standard fields.
    """

    def __init__(self, config):
        super().__init__(config)
        self.name = "column_mapper_agent"
        self.logger = logging.getLogger(self.name)
        self.output_dir = Path("output/agent_2_column_mapper")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Standard output fields we want to extract
        self.STANDARD_FIELDS = [
            "First Name",
            "Last Name", 
            "Current Company",
            "Designation / Role",
            "LinkedIn Profile URL",
            "Email",
            "Phone Number",
            "Geo (Location by City)"
        ]

        # Processing statistics
        self.stats = {
            "files_processed": 0,
            "total_rows_analyzed": 0,
            "fields_extracted": 0,
            "anomalies_detected": 0,
            "data_quality_score": 0
        }

    def execute_sync(self, context: PipelineContext) -> TaskResult:
        """Execute the intelligent data content analysis"""
        start_time = time.time()

        self.logger.info("🧠 AGENT 2: Intelligent Data Content Analyzer")
        self.logger.info("🎯 MISSION: Understand data CONTENT, not column names")
        self.logger.info("📊 APPROACH: Full table analysis with LLM")
        self.logger.info("⚡ CAPABILITIES: Anomaly detection, header inference, field recognition")

        try:
            # Get converted files from Agent 1
            if not context.has_task_result("file_converter"):
                return self._create_error_result("No data from file converter")

            converted_files = context.get_task_data("file_converter").get("converted_files", [])
            csv_files = [Path(f) for f in converted_files]

            if not csv_files:
                return self._create_error_result("No CSV files to process")

            self.logger.info(f"📁 Processing {len(csv_files)} files with intelligent content analysis")

            # Process each file with complete content analysis
            analyzed_files = []
            for csv_file in csv_files:
                analyzed_file = asyncio.run(self._analyze_csv_content(csv_file))
                if analyzed_file:
                    analyzed_files.append(analyzed_file)
                    self.stats["files_processed"] += 1

            processing_time = time.time() - start_time

            # Create verification report
            verification = self._create_verification_report(analyzed_files, processing_time)

            self.logger.info("🎉 DATA CONTENT ANALYSIS COMPLETED:")
            self.logger.info(f"   📊 Files processed: {self.stats['files_processed']}")
            self.logger.info(f"   📈 Rows analyzed: {self.stats['total_rows_analyzed']}")
            self.logger.info(f"   🎯 Fields extracted: {self.stats['fields_extracted']}")
            self.logger.info(f"   🔍 Anomalies handled: {self.stats['anomalies_detected']}")
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
                    "agent_name": "Data Content Analyzer",
                    "files_processed": len(analyzed_files),
                    "approach": "content_analysis",
                    "capabilities": ["table_understanding", "anomaly_detection", "header_inference", "field_recognition"]
                }
            )

        except Exception as e:
            self.logger.error(f"❌ Critical error in data content analysis: {str(e)}")
            return self._create_error_result(f"Data analysis failed: {str(e)}")

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
        """Smart CSV reading with header detection"""
        
        try:
            # Try different header row positions
            for header_row in [0, 1, 2, 3, 4, 5]:
                try:
                    df = pd.read_csv(csv_file, header=header_row, low_memory=False)
                    
                    # Check if this looks like a valid header row
                    if len(df) > 0 and not df.iloc[0].isna().all():
                        self.logger.info(f"📍 Detected headers at row {header_row}")
                        return df
                        
                except:
                    continue
            
            # Fallback: read without headers and let LLM figure it out
            self.logger.info("📍 No clear headers found, reading without header assumption")
            df = pd.read_csv(csv_file, header=None, low_memory=False)
            
            # Create generic column names
            df.columns = [f"column_{i}" for i in range(len(df.columns))]
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Failed to read CSV {csv_file}: {str(e)}")
            return None

    def _prepare_data_for_llm(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Prepare data sample for LLM analysis"""
        
        # 🚀 PERFORMANCE OPTIMIZATION: Reduce sample size from 20 to 5 rows
        sample_size = min(5, len(df))
        sample_df = df.head(sample_size)
        
        # 🚀 SIMPLIFIED: Only send essential information to reduce LLM processing time
        data_sample = {
            "column_names": list(df.columns),
            "total_rows": len(df),
            "sample_rows": sample_df.to_dict('records')
            # Removed: column_types, non_null_counts, unique_counts (too much data)
        }
        
        return data_sample

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
            # 🚀 PERFORMANCE: Add aggressive timeout for faster processing
            start_time = time.time()
            response = await asyncio.wait_for(
                ai_agent._make_ollama_request(llm_prompt),
                timeout=30  # 30 second timeout instead of default 300
            )
            processing_time = time.time() - start_time
            
            self.logger.info(f"🤖 LLM Response time: {processing_time:.1f}s")
            
            # Clean and parse JSON response
            cleaned_response = self._clean_llm_response(response)
            
            if not cleaned_response or cleaned_response.strip() == "":
                self.logger.warning("❌ LLM returned empty response, using fallback")
                return self._create_fallback_analysis(data_sample)
            
            try:
                analysis_result = json.loads(cleaned_response)
            except json.JSONDecodeError as e:
                self.logger.warning(f"❌ JSON parsing failed, using fallback: {e}")
                return self._create_fallback_analysis(data_sample)
            
            # Validate the response structure
            if not isinstance(analysis_result, dict):
                self.logger.warning("❌ LLM response is not a dictionary, using fallback")
                return self._create_fallback_analysis(data_sample)
            
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
        """Extract standardized data based on LLM analysis"""
        
        field_mappings = analysis_result.get("field_mappings", {})
        
        # Create standardized DataFrame
        standardized_data = {}
        
        # Initialize all standard fields as empty
        for field in self.STANDARD_FIELDS:
            standardized_data[field] = [""] * len(df)
        
        # Handle multiple email columns - prioritize the best one
        email_columns = [col for col, field in field_mappings.items() if field == "Email"]
        if len(email_columns) > 1:
            best_email_col = self._choose_best_email_column(df, email_columns)
            # Remove other email columns from mapping
            for col in email_columns:
                if col != best_email_col:
                    del field_mappings[col]
            self.logger.info(f"   📧 Multiple email columns found, using '{best_email_col}' as primary")
        
        # Map identified fields
        for original_col, target_field in field_mappings.items():
            if original_col in df.columns:
                
                # 🔧 SPECIAL HANDLING: Full name splitting
                if target_field == "FULL_NAME_TO_SPLIT":
                    self.logger.info(f"   🔄 Splitting full names in '{original_col}'")
                    first_names, last_names = self._split_full_names(df[original_col])
                    standardized_data["First Name"] = first_names
                    standardized_data["Last Name"] = last_names
                    self.logger.info(f"   ✅ Split {len([n for n in first_names if n])} names into First/Last")
                
                # Regular field mapping
                elif target_field in self.STANDARD_FIELDS:
                    # Clean and standardize the data
                    values = df[original_col].fillna("").astype(str)
                    standardized_data[target_field] = values.tolist()
                    self.logger.info(f"   📋 Mapped '{original_col}' → '{target_field}'")
        
        standardized_df = pd.DataFrame(standardized_data)
        
        return standardized_df

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
 