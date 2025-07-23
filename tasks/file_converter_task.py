#!/usr/bin/env python3
"""
Agent 1: File Converter Task
Converts uploaded spreadsheets to CSV format

Purpose: Take any format spreadsheet and convert to standardized CSV
Input: Excel/CSV files from DataSource
Output: CSV files in output/agent_1_file_converter/
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from .base_task import PipelineContext, SyncTask, TaskResult, TaskStatus

logger = logging.getLogger(__name__)


class FileConverterTask(SyncTask):
    """
    Agent 1: Convert uploaded files to CSV format
    Single responsibility: File format conversion only
    """

    def execute_sync(self, context: PipelineContext) -> TaskResult:
        """Convert all files in DataSource to CSV format"""
        try:
            # Create Agent 1 output directory
            output_dir = Path("output/agent_1_file_converter")
            output_dir.mkdir(parents=True, exist_ok=True)

            source_dir = Path(self.config.config.get("source_directory", "DataSource"))

            self.logger.info(f"🤖 AGENT 1: Starting file conversion from {source_dir}")
            self.logger.info("📋 Agent 1 Task: Convert ALL files to CSV format")

            # Discover input files
            input_files = self._discover_files(source_dir)
            self.logger.info(f"📁 Found {len(input_files)} files to convert")

            if not input_files:
                return TaskResult(
                    task_name=self.name, status=TaskStatus.FAILED, error="No input files found to convert"
                )

            # Convert each file to CSV
            converted_files = []
            conversion_stats = {
                "total_files": len(input_files),
                "successful_conversions": 0,
                "failed_conversions": 0,
                "conversion_details": [],
            }

            for file_path in input_files:
                try:
                    converted_file = self._convert_file_to_csv(file_path, output_dir)
                    if converted_file:
                        converted_files.append(converted_file)
                        conversion_stats["successful_conversions"] += 1
                        conversion_stats["conversion_details"].append(
                            {"source": str(file_path), "output": str(converted_file), "status": "success"}
                        )
                        self.logger.info(f"✅ Converted: {file_path.name}")
                    else:
                        conversion_stats["failed_conversions"] += 1
                        conversion_stats["conversion_details"].append(
                            {"source": str(file_path), "output": None, "status": "failed"}
                        )
                        self.logger.warning(f"❌ Failed: {file_path.name}")
                except Exception as e:
                    conversion_stats["failed_conversions"] += 1
                    conversion_stats["conversion_details"].append(
                        {"source": str(file_path), "output": None, "status": "error", "error": str(e)}
                    )
                    self.logger.error(f"❌ Error converting {file_path.name}: {str(e)}")

            # Save Agent 1 results
            result_data = {
                "converted_files": [str(f) for f in converted_files],
                "conversion_statistics": conversion_stats,
                "output_directory": str(output_dir),
            }

            # Verify and save Agent 1 output
            verification_result = self._verify_agent_output(converted_files, output_dir, conversion_stats)

            self.logger.info("🔍 AGENT 1 OUTPUT VERIFICATION:")
            self.logger.info(
                f"   • Files converted: {conversion_stats['successful_conversions']}/{conversion_stats['total_files']}"
            )
            self.logger.info(f"   • Output directory: {output_dir}")
            self.logger.info(f"   • Verification: {verification_result['status']}")

            if conversion_stats["successful_conversions"] == 0:
                return TaskResult(
                    task_name=self.name, status=TaskStatus.FAILED, error="No files were successfully converted to CSV"
                )

            return TaskResult(
                task_name=self.name,
                status=TaskStatus.SUCCESS,
                data=result_data,
                metadata={
                    "agent_number": 1,
                    "agent_name": "File Converter",
                    "files_converted": conversion_stats["successful_conversions"],
                    "output_verification": verification_result,
                },
            )

        except Exception as e:
            self.logger.error(f"Agent 1 failed: {str(e)}")
            return TaskResult(
                task_name=self.name, status=TaskStatus.FAILED, error=f"File converter agent error: {str(e)}"
            )

    def _discover_files(self, source_dir: Path) -> list[Path]:
        """Discover all convertible files in source directory"""
        supported_extensions = {".xlsx", ".xls", ".csv"}
        files = []

        for file_path in source_dir.rglob("*"):
            if file_path.is_file() and file_path.suffix.lower() in supported_extensions:
                files.append(file_path)

        return sorted(files)

    def _convert_file_to_csv(self, file_path: Path, output_dir: Path) -> Path:
        """Convert a single file to CSV format"""
        # Generate output filename
        output_filename = file_path.stem + ".csv"
        output_path = output_dir / output_filename

        try:
            # Handle different file formats
            if file_path.suffix.lower() == ".csv":
                # Already CSV, just copy
                df = pd.read_csv(file_path, encoding="utf-8")
            elif file_path.suffix.lower() in [".xlsx", ".xls"]:
                # Excel file
                try:
                    df = pd.read_excel(file_path, engine="openpyxl")
                except Exception:
                    # Try alternative engine for older files
                    try:
                        df = pd.read_excel(file_path, engine="xlrd")
                    except Exception:
                        self.logger.warning(f"Could not read Excel file {file_path.name}, trying CSV parsing")
                        return None
            else:
                self.logger.warning(f"Unsupported file format: {file_path.suffix}")
                return None

            # Save as CSV
            df.to_csv(output_path, index=False, encoding="utf-8")
            return output_path

        except Exception as e:
            self.logger.error(f"Error converting {file_path.name}: {str(e)}")
            return None

    def _verify_agent_output(self, converted_files: list[Path], output_dir: Path, stats: dict) -> dict:
        """Verify Agent 1 output and save verification report"""
        verification = {"status": "SUCCESS", "files_verified": 0, "files_readable": 0, "total_rows": 0, "issues": []}

        try:
            # Check each converted file
            for file_path in converted_files:
                verification["files_verified"] += 1

                # Test if file is readable
                try:
                    df = pd.read_csv(file_path, low_memory=False)
                    verification["files_readable"] += 1
                    verification["total_rows"] += len(df)
                except Exception as e:
                    verification["issues"].append(f"File {file_path.name} not readable: {str(e)}")

            # Save verification report
            verification_report = {
                "agent": "Agent 1 - File Converter",
                "timestamp": datetime.now().isoformat(),
                "verification_results": verification,
                "conversion_statistics": stats,
                "output_directory": str(output_dir),
            }

            report_file = output_dir / "agent_1_verification_report.json"
            import json

            with open(report_file, "w") as f:
                json.dump(verification_report, f, indent=2)

            self.logger.info(f"📄 Agent 1 verification report saved: {report_file}")

        except Exception as e:
            verification["status"] = "ERROR"
            verification["issues"].append(f"Verification error: {str(e)}")

        return verification
