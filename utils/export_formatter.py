#!/usr/bin/env python3
"""
Export Formatter for Apollo and HubSpot
Creates specific format exports with exact column mappings and data formatting
"""

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class ContactExportFormatter:
    """
    Formats contact data for specific CRM platforms
    Handles Apollo and HubSpot export requirements
    """

    def __init__(self):
        # Apollo.io specific column mapping
        self.apollo_column_mapping = {
            "first_name": "First Name",
            "last_name": "Last Name",
            "email": "Email",
            "company": "Organization Name",
            "title": "Title",
            "phone": "Phone Number",
            "linkedin": "LinkedIn URL",
            "location": "City",
            "industry": "Industry",
            "website": "Website",
            "company_size": "Employee Count",
            "revenue": "Annual Revenue",
        }

        # HubSpot specific column mapping
        self.hubspot_column_mapping = {
            "first_name": "First Name",
            "last_name": "Last Name",
            "email": "Email",
            "company": "Company Name",
            "title": "Job Title",
            "phone": "Phone Number",
            "linkedin": "LinkedIn Bio URL",
            "location": "City",
            "industry": "Industry",
            "website": "Website URL",
            "lead_source": "Lead Source",
            "lifecycle_stage": "Lifecycle Stage",
        }

        # Data quality thresholds
        self.quality_thresholds = {
            "apollo_minimum": 60.0,  # Minimum quality for Apollo export
            "hubspot_minimum": 50.0,  # Minimum quality for HubSpot export
            "premium_minimum": 80.0,  # Premium contacts threshold
        }

    def clean_phone_number(self, phone: str) -> str:
        """Clean and format phone numbers for CRM import"""
        if not phone:
            return ""

        # Remove all non-digit characters except +
        cleaned = re.sub(r"[^\d+]", "", str(phone))

        # Format US numbers
        if len(cleaned) == 10 and cleaned.isdigit():
            return f"+1{cleaned}"
        elif len(cleaned) == 11 and cleaned.startswith("1"):
            return f"+{cleaned}"
        elif cleaned.startswith("+"):
            return cleaned
        else:
            return phone  # Return original if can't format

    def clean_email(self, email: str) -> str:
        """Clean and validate email addresses"""
        if not email:
            return ""

        email = str(email).strip().lower()

        # Basic email validation
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if re.match(email_pattern, email):
            return email

        return ""

    def clean_linkedin_url(self, linkedin: str) -> str:
        """Clean and format LinkedIn URLs"""
        if not linkedin:
            return ""

        linkedin = str(linkedin).strip()

        # Extract LinkedIn username and create clean URL
        patterns = [r"linkedin\.com/in/([a-zA-Z0-9\-]+)", r"linkedin\.com/pub/([a-zA-Z0-9\-]+)"]

        for pattern in patterns:
            match = re.search(pattern, linkedin)
            if match:
                username = match.group(1)
                return f"https://www.linkedin.com/in/{username}"

        # If it's already a valid LinkedIn URL, return as is
        if "linkedin.com" in linkedin and linkedin.startswith("http"):
            return linkedin

        return ""

    def format_company_name(self, company: str) -> str:
        """Clean and format company names"""
        if not company:
            return ""

        company = str(company).strip()

        # Remove common suffixes for cleaner display
        suffixes = [", Inc.", ", LLC", ", Ltd.", ", Corp.", " Inc", " LLC", " Ltd", " Corp"]
        for suffix in suffixes:
            if company.endswith(suffix):
                company = company[: -len(suffix)]

        return company.strip()

    def categorize_industry(self, company: str, title: str = "") -> str:
        """Categorize contacts by industry based on company and title"""
        if not company:
            return ""

        company_lower = company.lower()
        title_lower = title.lower() if title else ""

        # Technology keywords
        tech_keywords = ["tech", "software", "ai", "data", "cloud", "digital", "app", "platform", "saas"]
        if any(keyword in company_lower or keyword in title_lower for keyword in tech_keywords):
            return "Technology"

        # Healthcare keywords
        health_keywords = ["health", "medical", "pharma", "bio", "clinical", "hospital"]
        if any(keyword in company_lower or keyword in title_lower for keyword in health_keywords):
            return "Healthcare"

        # Finance keywords
        finance_keywords = ["bank", "financial", "investment", "capital", "fund", "fintech"]
        if any(keyword in company_lower or keyword in title_lower for keyword in finance_keywords):
            return "Financial Services"

        # Manufacturing keywords
        manufacturing_keywords = ["manufacturing", "automotive", "industrial", "factory"]
        if any(keyword in company_lower or keyword in title_lower for keyword in manufacturing_keywords):
            return "Manufacturing"

        # Education keywords
        education_keywords = ["university", "school", "education", "academic", "college"]
        if any(keyword in company_lower or keyword in title_lower for keyword in education_keywords):
            return "Education"

        return "Other"

    def determine_lifecycle_stage(self, data_quality: float, has_email: bool, has_phone: bool) -> str:
        """Determine HubSpot lifecycle stage based on contact completeness"""
        if data_quality >= 80 and has_email and has_phone:
            return "Marketing Qualified Lead"
        elif data_quality >= 60 and has_email:
            return "Lead"
        elif has_email:
            return "Subscriber"
        else:
            return "Other"

    def format_for_apollo(self, contacts: list[dict[str, Any]]) -> pd.DataFrame:
        """Format contacts for Apollo.io import"""
        logger.info(f"Formatting {len(contacts)} contacts for Apollo.io")

        formatted_contacts = []

        for contact in contacts:
            # Skip low-quality contacts
            if contact.get("data_quality", 0) < self.quality_thresholds["apollo_minimum"]:
                continue

            formatted_contact = {}

            # Map and clean each field
            formatted_contact[self.apollo_column_mapping["first_name"]] = contact.get("first_name", "").strip()
            formatted_contact[self.apollo_column_mapping["last_name"]] = contact.get("last_name", "").strip()
            formatted_contact[self.apollo_column_mapping["email"]] = self.clean_email(contact.get("email", ""))
            formatted_contact[self.apollo_column_mapping["company"]] = self.format_company_name(
                contact.get("company", "")
            )
            formatted_contact[self.apollo_column_mapping["title"]] = contact.get("title", "").strip()
            formatted_contact[self.apollo_column_mapping["phone"]] = self.clean_phone_number(contact.get("phone", ""))
            formatted_contact[self.apollo_column_mapping["linkedin"]] = self.clean_linkedin_url(
                contact.get("linkedin", "")
            )
            formatted_contact[self.apollo_column_mapping["location"]] = contact.get("location", "").strip()

            # Industry categorization
            formatted_contact[self.apollo_column_mapping["industry"]] = self.categorize_industry(
                contact.get("company", ""), contact.get("title", "")
            )

            # Additional Apollo-specific fields
            formatted_contact[self.apollo_column_mapping["website"]] = contact.get("company_domain", "")
            formatted_contact[self.apollo_column_mapping["company_size"]] = contact.get("company_size", "")
            formatted_contact[self.apollo_column_mapping["revenue"]] = contact.get("company_revenue", "")

            # Only include contacts with at least email or phone
            if (
                formatted_contact[self.apollo_column_mapping["email"]]
                or formatted_contact[self.apollo_column_mapping["phone"]]
            ):
                formatted_contacts.append(formatted_contact)

        logger.info(f"Formatted {len(formatted_contacts)} contacts for Apollo.io (filtered for quality)")
        return pd.DataFrame(formatted_contacts)

    def format_for_hubspot(self, contacts: list[dict[str, Any]]) -> pd.DataFrame:
        """Format contacts for HubSpot import"""
        logger.info(f"Formatting {len(contacts)} contacts for HubSpot")

        formatted_contacts = []

        for contact in contacts:
            # Skip low-quality contacts
            if contact.get("data_quality", 0) < self.quality_thresholds["hubspot_minimum"]:
                continue

            formatted_contact = {}

            # Map and clean each field
            formatted_contact[self.hubspot_column_mapping["first_name"]] = contact.get("first_name", "").strip()
            formatted_contact[self.hubspot_column_mapping["last_name"]] = contact.get("last_name", "").strip()
            formatted_contact[self.hubspot_column_mapping["email"]] = self.clean_email(contact.get("email", ""))
            formatted_contact[self.hubspot_column_mapping["company"]] = self.format_company_name(
                contact.get("company", "")
            )
            formatted_contact[self.hubspot_column_mapping["title"]] = contact.get("title", "").strip()
            formatted_contact[self.hubspot_column_mapping["phone"]] = self.clean_phone_number(contact.get("phone", ""))
            formatted_contact[self.hubspot_column_mapping["linkedin"]] = self.clean_linkedin_url(
                contact.get("linkedin", "")
            )
            formatted_contact[self.hubspot_column_mapping["location"]] = contact.get("location", "").strip()

            # Industry categorization
            formatted_contact[self.hubspot_column_mapping["industry"]] = self.categorize_industry(
                contact.get("company", ""), contact.get("title", "")
            )

            # HubSpot-specific fields
            formatted_contact[self.hubspot_column_mapping["website"]] = contact.get("company_domain", "")
            formatted_contact[self.hubspot_column_mapping["lead_source"]] = contact.get("source_file", "Data Import")

            # Determine lifecycle stage
            has_email = bool(formatted_contact[self.hubspot_column_mapping["email"]])
            has_phone = bool(formatted_contact[self.hubspot_column_mapping["phone"]])
            data_quality = contact.get("data_quality", 0)

            formatted_contact[self.hubspot_column_mapping["lifecycle_stage"]] = self.determine_lifecycle_stage(
                data_quality, has_email, has_phone
            )

            # Only include contacts with at least email or phone
            if (
                formatted_contact[self.hubspot_column_mapping["email"]]
                or formatted_contact[self.hubspot_column_mapping["phone"]]
            ):
                formatted_contacts.append(formatted_contact)

        logger.info(f"Formatted {len(formatted_contacts)} contacts for HubSpot (filtered for quality)")
        return pd.DataFrame(formatted_contacts)

    def create_premium_contact_list(self, contacts: list[dict[str, Any]]) -> pd.DataFrame:
        """Create a premium contact list with highest quality contacts"""
        premium_contacts = [
            contact
            for contact in contacts
            if contact.get("data_quality", 0) >= self.quality_thresholds["premium_minimum"]
        ]

        logger.info(f"Identified {len(premium_contacts)} premium contacts")

        # Format for Apollo (premium list uses Apollo format)
        return self.format_for_apollo(premium_contacts)

    def export_all_formats(self, contacts: list[dict[str, Any]], output_dir: Path) -> dict[str, str]:
        """Export contacts in all formats to the specified directory"""
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)

        exported_files = {}

        try:
            # Apollo format
            apollo_df = self.format_for_apollo(contacts)
            apollo_file = output_dir / "apollo_contacts_formatted.csv"
            apollo_df.to_csv(apollo_file, index=False)
            exported_files["apollo"] = str(apollo_file)
            logger.info(f"Exported {len(apollo_df)} contacts to Apollo format: {apollo_file}")

            # HubSpot format
            hubspot_df = self.format_for_hubspot(contacts)
            hubspot_file = output_dir / "hubspot_contacts_formatted.csv"
            hubspot_df.to_csv(hubspot_file, index=False)
            exported_files["hubspot"] = str(hubspot_file)
            logger.info(f"Exported {len(hubspot_df)} contacts to HubSpot format: {hubspot_file}")

            # Premium contacts
            premium_df = self.create_premium_contact_list(contacts)
            premium_file = output_dir / "premium_contacts_apollo.csv"
            premium_df.to_csv(premium_file, index=False)
            exported_files["premium"] = str(premium_file)
            logger.info(f"Exported {len(premium_df)} premium contacts: {premium_file}")

            # Combined Excel file with multiple sheets
            excel_file = output_dir / "all_contacts_formatted.xlsx"
            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                apollo_df.to_excel(writer, sheet_name="Apollo_Format", index=False)
                hubspot_df.to_excel(writer, sheet_name="HubSpot_Format", index=False)
                premium_df.to_excel(writer, sheet_name="Premium_Contacts", index=False)

                # Summary sheet
                summary_data = {
                    "Format": ["Apollo", "HubSpot", "Premium"],
                    "Contact Count": [len(apollo_df), len(hubspot_df), len(premium_df)],
                    "Quality Threshold": [
                        self.quality_thresholds["apollo_minimum"],
                        self.quality_thresholds["hubspot_minimum"],
                        self.quality_thresholds["premium_minimum"],
                    ],
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name="Summary", index=False)

            exported_files["excel"] = str(excel_file)
            logger.info(f"Exported combined Excel file: {excel_file}")

        except Exception as e:
            logger.error(f"Error during export: {e}")
            raise

        return exported_files


def test_export_formatter():
    """Test the export formatter with sample data"""
    formatter = ContactExportFormatter()

    # Sample contacts with different quality levels
    test_contacts = [
        {
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@techcorp.com",
            "company": "TechCorp Inc.",
            "title": "Senior Software Engineer",
            "phone": "4155551234",
            "linkedin": "https://linkedin.com/in/johndoe",
            "location": "San Francisco, CA",
            "data_quality": 85.0,
            "source_file": "tech_contacts.xlsx",
        },
        {
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane.smith@healthcorp.com",
            "company": "HealthCorp",
            "title": "Product Manager",
            "phone": "(206) 555-1234",
            "data_quality": 75.0,
            "source_file": "health_contacts.xlsx",
        },
        {
            "first_name": "Bob",
            "last_name": "Johnson",
            "company": "StartupXYZ",
            "data_quality": 45.0,  # Low quality - should be filtered
            "source_file": "startup_contacts.xlsx",
        },
    ]

    print("🧪 Testing Export Formatter")
    print("=" * 50)

    # Test individual formatters
    apollo_df = formatter.format_for_apollo(test_contacts)
    print(f"Apollo format: {len(apollo_df)} contacts")

    hubspot_df = formatter.format_for_hubspot(test_contacts)
    print(f"HubSpot format: {len(hubspot_df)} contacts")

    premium_df = formatter.create_premium_contact_list(test_contacts)
    print(f"Premium contacts: {len(premium_df)} contacts")

    # Test full export
    output_dir = Path("test_output")
    exported_files = formatter.export_all_formats(test_contacts, output_dir)

    print("\nExported files:")
    for format_name, file_path in exported_files.items():
        print(f"  {format_name}: {file_path}")


if __name__ == "__main__":
    test_export_formatter()
