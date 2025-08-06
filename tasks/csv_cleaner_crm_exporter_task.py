#!/usr/bin/env python3
"""
Agent 6: CSV Cleaner & CRM Exporter
Takes enriched contacts from Agent 5 and creates 3 outputs:
1. Cleaned CSV with only standard fields
2. Apollo.io compatible CSV  
3. HubSpot compatible CSV

Based on July 2025 requirements for both platforms.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .base_task import PipelineContext, SyncTask, TaskResult, TaskStatus
from constants import STANDARD_FIELDS


class CSVCleanerCRMExporter(SyncTask):
    """Agent 6: Clean and export CRM-compatible files"""
    
    def __init__(self, context: PipelineContext):
        super().__init__(context)
        self.logger = logging.getLogger(__name__)
        
        # Apollo.io field mappings (based on July 2025 requirements)
        self.apollo_field_mapping = {
            "First Name": "Contact first name",
            "Last Name": "Contact last name", 
            "Current Company": "Account name",
            "Designation / Role": "Contact title",
            "LinkedIn Profile URL": "Contact LinkedIn URL",
            "Email": "Contact email",
            "Phone Number": "First phone",
            "Geo (Location by City)": "Contact place city"
        }
        
        # HubSpot field mappings (based on July 2025 requirements)
        self.hubspot_field_mapping = {
            "First Name": "First Name",
            "Last Name": "Last Name",
            "Current Company": "Company name", 
            "Designation / Role": "Job Title",
            "LinkedIn Profile URL": "LinkedIn URL",
            "Email": "Email",
            "Phone Number": "Phone Number",
            "Geo (Location by City)": "City"
        }
        
        self.stats = {
            "files_processed": 0,
            "contacts_cleaned": 0,
            "apollo_exports": 0,
            "hubspot_exports": 0,
            "fields_removed": 0
        }

    def execute_sync(self, context: PipelineContext) -> TaskResult:
        """Main execution method for Agent 6"""
        start_time = time.time()
        
        self.logger.info("🧹 AGENT 6: Starting CSV Cleaner & CRM Exporter")
        self.logger.info("📋 Agent 6 Task: Clean enriched CSV and export Apollo/HubSpot files")
        self.logger.info(f"🎯 Standard fields: {', '.join(STANDARD_FIELDS)}")
        
        try:
            # Find enriched CSV from Agent 5
            agent5_output_dir = Path("output/agent_5_email_enrichment")
            enriched_csv = agent5_output_dir / "enriched_contacts.csv"
            
            if not enriched_csv.exists():
                raise FileNotFoundError(f"No enriched CSV found at {enriched_csv}")
                
            self.logger.info(f"📁 Processing enriched CSV: {enriched_csv}")
            
            # Read enriched data
            enriched_df = pd.read_csv(enriched_csv)
            original_columns = list(enriched_df.columns)
            original_contacts = len(enriched_df)
            
            self.logger.info(f"📊 Loaded: {original_contacts} contacts, {len(original_columns)} columns")
            self.logger.info(f"🔍 Original columns: {original_columns}")
            
            # Clean data to only standard fields
            cleaned_df = self._clean_to_standard_fields(enriched_df)
            
            # Create output directory
            output_dir = Path("output/agent_6_crm_exporter")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Export 3 files
            outputs = self._export_all_formats(cleaned_df, output_dir)
            
            # Generate verification report
            verification_report = self._generate_verification_report(
                original_contacts, len(original_columns), len(cleaned_df.columns), outputs
            )
            
            report_path = output_dir / "agent_6_verification_report.json"
            with open(report_path, 'w') as f:
                json.dump(verification_report, f, indent=2)
            
            self.logger.info(f"📄 Agent 6 verification report saved: {report_path}")
            
            # Log summary
            self._log_completion_summary()
            
            processing_time = time.time() - start_time
            self.logger.info(f"Task 'csv_cleaner_crm_exporter' completed successfully in {processing_time:.2f}s")
            
            return TaskResult(
                task_name="csv_cleaner_crm_exporter",
                status=TaskStatus.SUCCESS,
                data=outputs,
                metadata={
                    "contacts_processed": len(cleaned_df),
                    "files_exported": 3,
                    "processing_time": processing_time,
                    "verification_report": str(report_path)
                },
                execution_time=processing_time
            )
            
        except Exception as e:
            self.logger.error(f"❌ Agent 6 failed: {str(e)}")
            return TaskResult(
                task_name="csv_cleaner_crm_exporter",
                status=TaskStatus.FAILED,
                error=str(e),
                metadata={"processing_time": time.time() - start_time},
                execution_time=time.time() - start_time
            )

    def _clean_to_standard_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame to contain only standard fields"""
        self.logger.info("🧹 Cleaning to standard fields only...")
        
        # Identify columns to keep vs remove
        columns_to_keep = []
        columns_to_remove = []
        
        for col in df.columns:
            if col in STANDARD_FIELDS:
                columns_to_keep.append(col)
            else:
                columns_to_remove.append(col)
        
        # Log what's being removed
        if columns_to_remove:
            self.logger.info(f"❌ Removing {len(columns_to_remove)} non-standard columns:")
            for col in columns_to_remove:
                self.logger.info(f"   - {col}")
            self.stats["fields_removed"] = len(columns_to_remove)
        
        # Create cleaned DataFrame with only standard fields
        cleaned_df = df[columns_to_keep].copy()
        
        # Ensure all standard fields are present (add empty columns if missing)
        for field in STANDARD_FIELDS:
            if field not in cleaned_df.columns:
                cleaned_df[field] = ""
                self.logger.info(f"➕ Added missing standard field: {field}")
        
        # Reorder columns to match STANDARD_FIELDS order
        cleaned_df = cleaned_df[STANDARD_FIELDS]
        
        self.logger.info(f"✅ Cleaned data: {len(cleaned_df)} contacts, {len(cleaned_df.columns)} standard columns")
        self.stats["contacts_cleaned"] = len(cleaned_df)
        
        return cleaned_df

    def _export_all_formats(self, df: pd.DataFrame, output_dir: Path) -> Dict[str, str]:
        """Export cleaned data in all 3 formats"""
        outputs = {}
        
        # 1. Clean CSV with standard fields
        clean_csv_path = output_dir / "cleaned_contacts_standard_fields.csv"
        df.to_csv(clean_csv_path, index=False)
        outputs["cleaned_csv"] = str(clean_csv_path)
        self.logger.info(f"✅ Exported clean CSV: {clean_csv_path}")
        
        # 2. Apollo.io compatible CSV
        apollo_df = self._create_apollo_format(df)
        apollo_csv_path = output_dir / "apollo_import_ready.csv"
        apollo_df.to_csv(apollo_csv_path, index=False)
        outputs["apollo_csv"] = str(apollo_csv_path)
        self.stats["apollo_exports"] = len(apollo_df)
        self.logger.info(f"🚀 Exported Apollo CSV: {apollo_csv_path}")
        
        # 3. HubSpot compatible CSV  
        hubspot_df = self._create_hubspot_format(df)
        hubspot_csv_path = output_dir / "hubspot_import_ready.csv"
        hubspot_df.to_csv(hubspot_csv_path, index=False)
        outputs["hubspot_csv"] = str(hubspot_csv_path)
        self.stats["hubspot_exports"] = len(hubspot_df)
        self.logger.info(f"🧡 Exported HubSpot CSV: {hubspot_csv_path}")
        
        self.stats["files_processed"] = 3
        return outputs

    def _create_apollo_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create Apollo.io compatible format based on July 2025 requirements"""
        self.logger.info("🚀 Creating Apollo.io compatible format...")
        
        apollo_df = pd.DataFrame()
        
        # Map fields according to Apollo requirements
        for standard_field, apollo_field in self.apollo_field_mapping.items():
            if standard_field in df.columns:
                apollo_df[apollo_field] = df[standard_field]
            else:
                apollo_df[apollo_field] = ""
        
        # Apollo-specific data cleaning
        apollo_df = self._clean_apollo_data(apollo_df)
        
        self.logger.info(f"🚀 Apollo format ready: {len(apollo_df)} contacts, {len(apollo_df.columns)} Apollo fields")
        return apollo_df

    def _create_hubspot_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create HubSpot compatible format based on July 2025 requirements"""
        self.logger.info("🧡 Creating HubSpot compatible format...")
        
        hubspot_df = pd.DataFrame()
        
        # Map fields according to HubSpot requirements
        for standard_field, hubspot_field in self.hubspot_field_mapping.items():
            if standard_field in df.columns:
                hubspot_df[hubspot_field] = df[standard_field]
            else:
                hubspot_df[hubspot_field] = ""
        
        # HubSpot-specific data cleaning
        hubspot_df = self._clean_hubspot_data(hubspot_df)
        
        self.logger.info(f"🧡 HubSpot format ready: {len(hubspot_df)} contacts, {len(hubspot_df.columns)} HubSpot fields")
        return hubspot_df

    def _clean_apollo_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Apollo-specific data cleaning rules"""
        cleaned_df = df.copy()
        
        # Ensure required fields for Apollo (contact email OR company name OR LinkedIn URL)
        has_identifier = (
            (~cleaned_df["Contact email"].fillna("").str.strip().eq("")) |
            (~cleaned_df["Account name"].fillna("").str.strip().eq("")) |
            (~cleaned_df["Contact LinkedIn URL"].fillna("").str.strip().eq(""))
        )
        
        # Keep only contacts with at least one identifier
        contacts_before = len(cleaned_df)
        cleaned_df = cleaned_df[has_identifier]
        contacts_after = len(cleaned_df)
        
        if contacts_before > contacts_after:
            self.logger.info(f"🚀 Apollo validation: Removed {contacts_before - contacts_after} contacts without required identifiers")
        
        # Clean phone numbers (Apollo accepts various formats)
        if "First phone" in cleaned_df.columns:
            cleaned_df["First phone"] = cleaned_df["First phone"].fillna("").astype(str).str.strip()
        
        return cleaned_df

    def _clean_hubspot_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply HubSpot-specific data cleaning rules"""
        cleaned_df = df.copy()
        
        # Ensure required fields for HubSpot (at least First Name, Last Name, or Email)
        has_identifier = (
            (~cleaned_df["First Name"].fillna("").str.strip().eq("")) |
            (~cleaned_df["Last Name"].fillna("").str.strip().eq("")) |
            (~cleaned_df["Email"].fillna("").str.strip().eq(""))
        )
        
        # Keep only contacts with at least one identifier
        contacts_before = len(cleaned_df)
        cleaned_df = cleaned_df[has_identifier]
        contacts_after = len(cleaned_df)
        
        if contacts_before > contacts_after:
            self.logger.info(f"🧡 HubSpot validation: Removed {contacts_before - contacts_after} contacts without required identifiers")
        
        # Clean LinkedIn URLs (HubSpot has specific format requirements)
        if "LinkedIn URL" in cleaned_df.columns:
            cleaned_df["LinkedIn URL"] = cleaned_df["LinkedIn URL"].fillna("").astype(str).str.strip()
            # Ensure LinkedIn URLs are properly formatted
            linkedin_mask = cleaned_df["LinkedIn URL"].str.contains("linkedin.com", na=False, case=False)
            invalid_linkedin = (~linkedin_mask) & (cleaned_df["LinkedIn URL"] != "")
            if invalid_linkedin.any():
                cleaned_df.loc[invalid_linkedin, "LinkedIn URL"] = ""
                self.logger.info(f"🧡 HubSpot cleaning: Cleared {invalid_linkedin.sum()} invalid LinkedIn URLs")
        
        return cleaned_df

    def _generate_verification_report(self, original_contacts: int, original_columns: int, 
                                    final_columns: int, outputs: Dict[str, str]) -> Dict[str, Any]:
        """Generate comprehensive verification report"""
        return {
            "agent": "Agent 6: CSV Cleaner & CRM Exporter",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "input_summary": {
                "original_contacts": original_contacts,
                "original_columns": original_columns,
                "source_file": "output/agent_5_email_enrichment/enriched_contacts.csv"
            },
            "processing_summary": {
                "standard_fields_count": len(STANDARD_FIELDS),
                "final_columns": final_columns,
                "fields_removed": self.stats["fields_removed"],
                "contacts_cleaned": self.stats["contacts_cleaned"]
            },
            "outputs_created": {
                "files_generated": len(outputs),
                "cleaned_csv": outputs.get("cleaned_csv"),
                "apollo_csv": outputs.get("apollo_csv"), 
                "hubspot_csv": outputs.get("hubspot_csv")
            },
            "export_statistics": {
                "apollo_contacts": self.stats["apollo_exports"],
                "hubspot_contacts": self.stats["hubspot_exports"],
                "files_processed": self.stats["files_processed"]
            },
            "standard_fields_used": STANDARD_FIELDS,
            "validation_status": "SUCCESS"
        }

    def _log_completion_summary(self):
        """Log completion summary with key metrics"""
        self.logger.info("🎉 CSV CLEANER & CRM EXPORTER COMPLETED:")
        self.logger.info(f"   📊 Contacts processed: {self.stats['contacts_cleaned']}")
        self.logger.info(f"   🧹 Fields removed: {self.stats['fields_removed']}")
        self.logger.info(f"   📁 Files exported: {self.stats['files_processed']}")
        self.logger.info(f"   🚀 Apollo contacts: {self.stats['apollo_exports']}")
        self.logger.info(f"   🧡 HubSpot contacts: {self.stats['hubspot_exports']}")
        self.logger.info(f"   ✅ Standard fields: {len(STANDARD_FIELDS)}")
