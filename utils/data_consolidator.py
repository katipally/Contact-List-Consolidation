#!/usr/bin/env python3
"""
Data Consolidator for Contact Information
Merges 25+ Excel files with different structures into unified format
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from column_mapper import SmartColumnMapper

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ConsolidationStats:
    """Statistics from the consolidation process"""

    total_files: int
    successful_files: int
    failed_files: int
    total_records: int
    duplicate_records: int
    invalid_records: int
    final_record_count: int
    processing_time: float


class DataConsolidator:
    """
    Intelligent data consolidator that merges multiple Excel files
    with different column structures into a unified contact database
    """

    def __init__(self, source_directory: str = "DataSource"):
        self.source_directory = Path(source_directory)
        self.column_mapper = SmartColumnMapper()
        self.consolidated_data = []
        self.stats = None

        # Standard output columns for Apollo/HubSpot compatibility
        self.output_columns = [
            "first_name",
            "last_name",
            "company",
            "title",
            "email",
            "phone",
            "linkedin",
            "location",
            "industry",
            "source_file",
            "confidence_score",
            "data_quality",
        ]

    def clean_text_value(self, value: Any) -> str | None:
        """Clean and normalize text values"""
        if pd.isna(value) or value is None:
            return None

        # Convert to string and clean
        text = str(value).strip()

        # Remove common artifacts
        text = re.sub(r"^nan$|^none$|^null$|^#N/A$|^#NULL!$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^\s*$", "", text)
        text = re.sub(r"\s+", " ", text)

        # Return None if empty after cleaning
        return text if text else None

    def validate_email(self, email: str) -> bool:
        """Basic email validation"""
        if not email:
            return False

        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return re.match(email_pattern, email) is not None

    def validate_phone(self, phone: str) -> bool:
        """Basic phone number validation"""
        if not phone:
            return False

        # Clean phone number
        cleaned = re.sub(r"[^\d+]", "", phone)

        # Basic validation - should have 7-15 digits
        return 7 <= len(re.sub(r"[^\d]", "", cleaned)) <= 15

    def calculate_record_quality(self, record: dict[str, Any]) -> float:
        """Calculate data quality score for a record"""
        quality_score = 0.0
        total_possible = 0.0

        # Essential fields with weights
        field_weights = {
            "first_name": 1.0,
            "last_name": 1.0,
            "email": 1.5,  # Email is most important
            "company": 1.0,
            "title": 0.8,
            "phone": 0.8,
            "linkedin": 0.6,
            "location": 0.4,
            "industry": 0.3,
        }

        for field, weight in field_weights.items():
            total_possible += weight
            value = record.get(field)

            if value and str(value).strip():
                # Base score for having the field
                field_score = weight * 0.7

                # Bonus for field-specific validation
                if field == "email" and self.validate_email(value):
                    field_score = weight * 1.0
                elif field == "phone" and self.validate_phone(value):
                    field_score = weight * 1.0
                elif field in ["first_name", "last_name"] and len(str(value).strip()) >= 2:
                    field_score = weight * 1.0
                elif field == "linkedin" and "linkedin.com" in str(value).lower():
                    field_score = weight * 1.0

                quality_score += field_score

        return min(100.0, (quality_score / total_possible) * 100)

    def deduplicate_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Remove duplicate records based on email or name+company combination"""
        seen_emails = set()
        seen_name_company = set()
        unique_records = []

        for record in records:
            email = record.get("email", "").lower().strip()
            first_name = record.get("first_name", "").lower().strip()
            last_name = record.get("last_name", "").lower().strip()
            company = record.get("company", "").lower().strip()

            # Primary deduplication by email
            if email and self.validate_email(email):
                if email in seen_emails:
                    continue
                seen_emails.add(email)

            # Secondary deduplication by name + company
            name_company_key = f"{first_name}|{last_name}|{company}"
            if name_company_key in seen_name_company:
                continue

            if first_name and last_name:  # Only add to seen if we have names
                seen_name_company.add(name_company_key)

            unique_records.append(record)

        logger.info(f"Deduplicated {len(records) - len(unique_records)} records")
        return unique_records

    def process_single_file(self, file_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Process a single Excel file and return standardized records"""
        logger.info(f"Processing file: {file_path.name}")

        try:
            # Try to read the Excel file
            if file_path.suffix.lower() == ".xls":
                # Handle legacy .xls files
                try:
                    df = pd.read_excel(file_path, engine="xlrd")
                except Exception:
                    # Try with openpyxl for .xls files that are actually .xlsx
                    df = pd.read_excel(file_path, engine="openpyxl")
            else:
                df = pd.read_excel(file_path, engine="openpyxl")

            if df.empty:
                logger.warning(f"File {file_path.name} is empty")
                return [], {"status": "empty", "records": 0}

            # Map columns using the smart mapper
            columns = [str(col).strip() for col in df.columns if str(col).strip()]
            column_mappings = self.column_mapper.map_columns(columns)

            if not column_mappings:
                logger.warning(f"No mappable columns found in {file_path.name}")
                return [], {"status": "no_mappings", "records": len(df)}

            logger.info(f"Mapped {len(column_mappings)} columns in {file_path.name}")

            # Process records
            records = []
            for idx, row in df.iterrows():
                record = {}

                # Map known columns
                for original_col, mapping in column_mappings.items():
                    if original_col in df.columns:
                        value = self.clean_text_value(row[original_col])
                        if value:
                            record[mapping.standard_name] = value

                # Add metadata
                record["source_file"] = file_path.name
                record["confidence_score"] = sum(m.confidence for m in column_mappings.values()) / len(column_mappings)
                record["data_quality"] = self.calculate_record_quality(record)

                # Only include records with minimum quality
                if record["data_quality"] >= 20.0:  # Minimum 20% quality score
                    records.append(record)

            logger.info(f"Extracted {len(records)} valid records from {file_path.name}")

            return records, {
                "status": "success",
                "records": len(records),
                "columns_mapped": len(column_mappings),
                "mappings": {m.original_name: m.standard_name for m in column_mappings.values()},
            }

        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {str(e)}")
            return [], {"status": "error", "error": str(e), "records": 0}

    def consolidate_data(self, min_quality_score: float = 30.0) -> ConsolidationStats:
        """Consolidate all Excel files in the source directory"""
        start_time = datetime.now()

        logger.info(f"Starting consolidation from {self.source_directory}")

        if not self.source_directory.exists():
            raise FileNotFoundError(f"Source directory not found: {self.source_directory}")

        # Find all Excel files
        excel_files = []
        for pattern in ["*.xlsx", "*.xls"]:
            excel_files.extend(self.source_directory.glob(pattern))

        logger.info(f"Found {len(excel_files)} Excel files to process")

        all_records = []
        file_stats = {}
        successful_files = 0
        failed_files = 0

        # Process each file
        for file_path in excel_files:
            records, stats = self.process_single_file(file_path)
            file_stats[file_path.name] = stats

            if stats["status"] == "success":
                all_records.extend(records)
                successful_files += 1
            else:
                failed_files += 1

        logger.info(f"Processed {successful_files} files successfully, {failed_files} failed")
        logger.info(f"Total records before deduplication: {len(all_records)}")

        # Deduplicate records
        initial_count = len(all_records)
        unique_records = self.deduplicate_records(all_records)
        duplicates_removed = initial_count - len(unique_records)

        # Filter by quality score
        high_quality_records = [r for r in unique_records if r["data_quality"] >= min_quality_score]
        low_quality_filtered = len(unique_records) - len(high_quality_records)

        logger.info(f"Filtered {low_quality_filtered} low-quality records")
        logger.info(f"Final record count: {len(high_quality_records)}")

        self.consolidated_data = high_quality_records

        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()

        # Create statistics
        self.stats = ConsolidationStats(
            total_files=len(excel_files),
            successful_files=successful_files,
            failed_files=failed_files,
            total_records=initial_count,
            duplicate_records=duplicates_removed,
            invalid_records=low_quality_filtered,
            final_record_count=len(high_quality_records),
            processing_time=processing_time,
        )

        # Save processing log
        self.save_processing_log(file_stats)

        return self.stats

    def save_processing_log(self, file_stats: dict[str, dict[str, Any]]):
        """Save detailed processing log"""
        log_data = {
            "consolidation_timestamp": datetime.now().isoformat(),
            "statistics": {
                "total_files": self.stats.total_files,
                "successful_files": self.stats.successful_files,
                "failed_files": self.stats.failed_files,
                "total_records": self.stats.total_records,
                "duplicate_records": self.stats.duplicate_records,
                "invalid_records": self.stats.invalid_records,
                "final_record_count": self.stats.final_record_count,
                "processing_time_seconds": self.stats.processing_time,
            },
            "file_details": file_stats,
        }

        with open("consolidation_log.json", "w") as f:
            json.dump(log_data, f, indent=2, default=str)

        logger.info("Processing log saved to consolidation_log.json")

    def export_to_csv(self, output_file: str = "consolidated_contacts.csv") -> str:
        """Export consolidated data to CSV for Apollo/HubSpot import"""
        if not self.consolidated_data:
            raise ValueError("No consolidated data available. Run consolidate_data() first.")

        # Create DataFrame with standard columns
        df = pd.DataFrame(self.consolidated_data)

        # Ensure all output columns exist
        for col in self.output_columns:
            if col not in df.columns:
                df[col] = None

        # Reorder columns for Apollo/HubSpot compatibility
        df = df[self.output_columns + [col for col in df.columns if col not in self.output_columns]]

        # Clean up the data for export
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).replace("nan", "").replace("None", "")

        # Save to CSV
        df.to_csv(output_file, index=False)
        logger.info(f"Exported {len(df)} records to {output_file}")

        return output_file

    def get_summary_report(self) -> str:
        """Generate a human-readable summary report"""
        if not self.stats:
            return "No consolidation has been performed yet."

        report = f"""
📊 CONTACT CONSOLIDATION SUMMARY REPORT
{"=" * 50}

📁 File Processing:
   • Total files found: {self.stats.total_files}
   • Successfully processed: {self.stats.successful_files}
   • Failed to process: {self.stats.failed_files}
   • Success rate: {(self.stats.successful_files / self.stats.total_files * 100):.1f}%

📋 Record Processing:
   • Total records extracted: {self.stats.total_records:,}
   • Duplicates removed: {self.stats.duplicate_records:,}
   • Low-quality filtered: {self.stats.invalid_records:,}
   • Final record count: {self.stats.final_record_count:,}
   • Data retention rate: {(self.stats.final_record_count / self.stats.total_records * 100):.1f}%

⏱️ Performance:
   • Processing time: {self.stats.processing_time:.2f} seconds
   • Records per second: {(self.stats.total_records / self.stats.processing_time):.1f}

✅ Ready for Apollo/HubSpot import!
"""
        return report


def test_consolidator():
    """Test the data consolidator"""
    consolidator = DataConsolidator()

    print("🧪 Testing Data Consolidator")
    print("=" * 50)

    # Run consolidation
    stats = consolidator.consolidate_data()

    # Print summary
    print(consolidator.get_summary_report())

    # Export to CSV
    output_file = consolidator.export_to_csv()
    print(f"📁 Output saved to: {output_file}")


if __name__ == "__main__":
    test_consolidator()
