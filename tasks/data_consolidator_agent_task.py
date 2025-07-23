#!/usr/bin/env python3
"""
Agent 3: Data Consolidator Task
Combines all normalized CSV files into one big consolidated sheet

Purpose: Take normalized CSV files with standard columns and combine into one sheet
Input: Normalized CSV files from Agent 2
Output: Single consolidated CSV in output/agent_3_data_consolidator/
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from .base_task import PipelineContext, SyncTask, TaskResult, TaskStatus

logger = logging.getLogger(__name__)


class DataConsolidatorAgentTask(SyncTask):
    """
    Agent 3: Combine all normalized CSV files into one consolidated sheet
    Single responsibility: Data consolidation only
    """

    def execute_sync(self, context: PipelineContext) -> TaskResult:
        """Combine all normalized CSV files into one big sheet"""
        try:
            # Create Agent 3 output directory
            output_dir = Path("output/agent_3_data_consolidator")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Get normalized CSV files from Agent 2
            agent_2_data = self.get_dependency_data(context, "column_mapper_agent")
            csv_files = [Path(f) for f in agent_2_data["mapped_files"]]
            
            # Use our standard columns (consistent across all agents)
            standard_columns = [
                "First Name", "Last Name", "Current Company", "Designation / Role", 
                "LinkedIn Profile URL", "Email", "Phone Number", "Geo (Location by City)"
            ]

            self.logger.info(f"🤖 AGENT 3: Starting data consolidation for {len(csv_files)} CSV files")
            self.logger.info("📋 Agent 3 Task: Combine all normalized CSV files into one sheet")
            self.logger.info(f"🎯 Standard columns: {', '.join(standard_columns)}")

            if not csv_files:
                return TaskResult(
                    task_name=self.name, status=TaskStatus.FAILED, error="No normalized CSV files received from Agent 2"
                )

            # Consolidate all CSV files
            consolidated_df, consolidation_stats = self._consolidate_csv_files(csv_files, standard_columns)

            if consolidated_df is None or len(consolidated_df) == 0:
                return TaskResult(
                    task_name=self.name,
                    status=TaskStatus.FAILED,
                    error="Failed to consolidate CSV files or no data found",
                )

            # Save consolidated data
            consolidated_file = output_dir / "consolidated_contacts.csv"
            consolidated_df.to_csv(consolidated_file, index=False, encoding="utf-8")

            # Add source tracking
            enhanced_df = self._add_source_tracking(consolidated_df, csv_files)
            enhanced_file = output_dir / "consolidated_contacts_with_sources.csv"
            enhanced_df.to_csv(enhanced_file, index=False, encoding="utf-8")

            # Save Agent 3 results
            result_data = {
                "consolidated_file": str(consolidated_file),
                "enhanced_file": str(enhanced_file),
                "consolidation_statistics": consolidation_stats,
                "output_directory": str(output_dir),
                "total_contacts": len(consolidated_df),
                "standard_columns": standard_columns,
            }

            # Verify and save Agent 3 output
            verification_result = self._verify_agent_output(consolidated_file, enhanced_file, consolidation_stats)

            self.logger.info("🔍 AGENT 3 OUTPUT VERIFICATION:")
            self.logger.info(f"   • Files consolidated: {consolidation_stats['files_processed']}")
            self.logger.info(f"   • Total contacts: {len(consolidated_df)}")
            self.logger.info(f"   • Output directory: {output_dir}")
            self.logger.info(f"   • Verification: {verification_result['status']}")

            return TaskResult(
                task_name=self.name,
                status=TaskStatus.SUCCESS,
                data=result_data,
                metadata={
                    "agent_number": 3,
                    "agent_name": "Data Consolidator",
                    "total_contacts": len(consolidated_df),
                    "output_verification": verification_result,
                },
            )

        except Exception as e:
            self.logger.error(f"Agent 3 failed: {str(e)}")
            return TaskResult(
                task_name=self.name, status=TaskStatus.FAILED, error=f"Data consolidator agent error: {str(e)}"
            )

    def _consolidate_csv_files(self, csv_files: list[Path], standard_columns: list[str]) -> tuple:
        """Consolidate all CSV files into one DataFrame"""

        consolidated_data = []
        consolidation_stats = {
            "files_processed": 0,
            "files_successful": 0,
            "files_failed": 0,
            "total_rows_processed": 0,
            "file_details": [],
        }

        self.logger.info("📊 Starting consolidation process...")

        for csv_file in csv_files:
            try:
                # Read CSV file
                df = pd.read_csv(csv_file)
                consolidation_stats["files_processed"] += 1

                # Ensure all standard columns exist
                for col in standard_columns:
                    if col not in df.columns:
                        df[col] = ""

                # Select only standard columns in correct order
                df_standardized = df[standard_columns].copy()

                # Add source information
                df_standardized["_source_file"] = csv_file.name
                df_standardized["_source_agent"] = "Agent_2_Column_Mapper"

                # Add to consolidated data
                consolidated_data.append(df_standardized)

                consolidation_stats["files_successful"] += 1
                consolidation_stats["total_rows_processed"] += len(df)
                consolidation_stats["file_details"].append(
                    {"file": str(csv_file), "rows": len(df), "status": "success"}
                )

                self.logger.info(f"✅ Consolidated: {csv_file.name} ({len(df)} rows)")

            except Exception as e:
                consolidation_stats["files_failed"] += 1
                consolidation_stats["file_details"].append(
                    {"file": str(csv_file), "rows": 0, "status": "failed", "error": str(e)}
                )
                self.logger.error(f"❌ Failed to consolidate {csv_file.name}: {str(e)}")

        if not consolidated_data:
            self.logger.error("No data could be consolidated")
            return None, consolidation_stats

        # Combine all DataFrames
        consolidated_df = pd.concat(consolidated_data, ignore_index=True)

        # Basic data cleaning
        consolidated_df = self._clean_consolidated_data(consolidated_df)

        self.logger.info(f"✅ Consolidation completed: {len(consolidated_df)} total contacts")

        return consolidated_df, consolidation_stats

    def _clean_consolidated_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhanced cleaning of consolidated data - PRESERVE valuable rows"""

        initial_count = len(df)
        self.logger.info(f"🧹 Cleaning consolidated data: {initial_count} initial rows")

        # Remove completely empty rows (where ALL fields are empty)
        df = df.dropna(how="all")
        after_empty_removal = len(df)
        self.logger.info(f"   Removed {initial_count - after_empty_removal} completely empty rows")

        # Clean up text fields
        text_columns = [
            "First Name",
            "Last Name",
            "Current Company",
            "Designation / Role",
            "Email",
            "LinkedIn Profile URL",
            "Phone Number",
            "Geo (Location by City)",
        ]

        for col in text_columns:
            if col in df.columns:
                # Convert to string and clean
                df[col] = df[col].astype(str)
                df[col] = df[col].str.strip()
                df[col] = df[col].replace(["nan", "None", "null", ""], "")

        # 🔧 ENHANCED: More intelligent row filtering - preserve rows with ANY valuable information
        valuable_fields = [
            "First Name", "Last Name", "Email", "Current Company", 
            "Designation / Role", "LinkedIn Profile URL", "Phone Number"
        ]
        
        # Count how many valuable fields each row has
        def count_valuable_fields(row):
            count = 0
            for field in valuable_fields:
                if field in row and pd.notna(row[field]) and str(row[field]).strip():
                    count += 1
            return count
        
        df['_valuable_field_count'] = df.apply(count_valuable_fields, axis=1)
        
        # Keep rows that have at least 1 valuable field (instead of requiring name+email+company)
        before_filtering = len(df)
        df = df[df['_valuable_field_count'] > 0]
        df = df.drop('_valuable_field_count', axis=1)
        after_filtering = len(df)
        
        removed_count = before_filtering - after_filtering
        self.logger.info(f"   Removed {removed_count} rows with no valuable information")
        self.logger.info(f"   ✅ Preserved {after_filtering} rows with valuable data")
        
        # 🔧 ENHANCED: Log what types of data we're preserving
        preservation_stats = {
            "with_names": len(df[(df["First Name"] != "") | (df["Last Name"] != "")]),
            "with_emails": len(df[df["Email"] != ""]),
            "with_companies": len(df[df["Current Company"] != ""]),
            "with_titles": len(df[df["Designation / Role"] != ""]),
            "with_linkedin": len(df[df["LinkedIn Profile URL"] != ""]),
            "with_phones": len(df[df["Phone Number"] != ""])
        }
        
        self.logger.info(f"   📊 Data preservation breakdown:")
        for field_type, count in preservation_stats.items():
            if count > 0:
                self.logger.info(f"      • {count} rows {field_type}")

        return df

    def _add_source_tracking(self, df: pd.DataFrame, csv_files: list[Path]) -> pd.DataFrame:
        """Add enhanced source tracking information"""

        enhanced_df = df.copy()

        # Add consolidation metadata
        enhanced_df["_consolidation_date"] = datetime.now().isoformat()
        enhanced_df["_total_source_files"] = len(csv_files)
        enhanced_df["_consolidation_agent"] = "Agent_3_Data_Consolidator"

        # Add data completeness score
        standard_columns = [
            "First Name",
            "Last Name",
            "Current Company",
            "Designation / Role",
            "Email",
            "LinkedIn Profile URL",
            "Phone Number",
            "Geo (Location by City)",
        ]

        def calculate_completeness(row):
            filled_fields = sum(1 for col in standard_columns if col in row and str(row[col]).strip() != "")
            return (filled_fields / len(standard_columns)) * 100

        enhanced_df["_data_completeness_percentage"] = enhanced_df.apply(calculate_completeness, axis=1)

        return enhanced_df

    def _verify_agent_output(self, consolidated_file: Path, enhanced_file: Path, stats: dict) -> dict:
        """Verify Agent 3 output and save verification report"""
        verification = {
            "status": "SUCCESS",
            "consolidated_file_readable": False,
            "enhanced_file_readable": False,
            "total_contacts": 0,
            "data_quality_stats": {},
            "issues": [],
        }

        try:
            # Check consolidated file
            if consolidated_file.exists():
                try:
                    df_consolidated = pd.read_csv(consolidated_file)
                    verification["consolidated_file_readable"] = True
                    verification["total_contacts"] = len(df_consolidated)

                    # Basic quality checks
                    verification["data_quality_stats"] = {
                        "total_rows": len(df_consolidated),
                        "rows_with_names": len(
                            df_consolidated[
                                (df_consolidated["First Name"] != "") | (df_consolidated["Last Name"] != "")
                            ]
                        ),
                        "rows_with_email": len(df_consolidated[df_consolidated["Email"] != ""]),
                        "rows_with_company": len(df_consolidated[df_consolidated["Current Company"] != ""]),
                        "rows_with_linkedin": len(df_consolidated[df_consolidated["LinkedIn Profile URL"] != ""]),
                    }

                except Exception as e:
                    verification["issues"].append(f"Consolidated file not readable: {str(e)}")
            else:
                verification["issues"].append("Consolidated file does not exist")

            # Check enhanced file
            if enhanced_file.exists():
                try:
                    df_enhanced = pd.read_csv(enhanced_file)
                    verification["enhanced_file_readable"] = True
                except Exception as e:
                    verification["issues"].append(f"Enhanced file not readable: {str(e)}")
            else:
                verification["issues"].append("Enhanced file does not exist")

            # Save verification report
            verification_report = {
                "agent": "Agent 3 - Data Consolidator",
                "timestamp": datetime.now().isoformat(),
                "verification_results": verification,
                "consolidation_statistics": stats,
                "output_files": {"consolidated": str(consolidated_file), "enhanced": str(enhanced_file)},
            }

            report_file = consolidated_file.parent / "agent_3_verification_report.json"
            with open(report_file, "w") as f:
                json.dump(verification_report, f, indent=2)

            self.logger.info(f"📄 Agent 3 verification report saved: {report_file}")

        except Exception as e:
            verification["status"] = "ERROR"
            verification["issues"].append(f"Verification error: {str(e)}")

        return verification
 