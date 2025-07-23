#!/usr/bin/env python3
"""
Intelligent Column Mapper for Contact Consolidation
Handles inconsistent column naming across 25+ Excel files
Enhanced with LLM-powered content analysis and transformation
"""

import re
from typing import Dict, List, Optional, Tuple
from fuzzywuzzy import fuzz
from dataclasses import dataclass


@dataclass
class ColumnMapping:
    """Represents a column mapping with confidence score"""

    original_name: str
    standard_name: str
    confidence: int
    pattern_matched: str
    requires_content_transformation: bool = False
    transformation_type: Optional[str] = None


class SmartColumnMapper:
    """
    Intelligent column mapper that handles inconsistent naming
    across multiple Excel files with different formats.
    Enhanced with content transformation detection.
    """
    
    def __init__(self):
        # Define standard contact fields with comprehensive pattern matching
        self.standard_fields = {
            "first_name": {
                "primary_patterns": ["first.?name", "fname", "given.?name", "firstname", "first", "^f.?name$"],
                "secondary_patterns": ["given", "name.*first", "fn"],
                "exclusions": ["first.*date", "first.*order", "first.*engagement", "first.*contact"],
                "weight": 1.0,
            },
            "last_name": {
                "primary_patterns": ["last.?name", "lname", "surname", "lastname", "family.?name", "last", "^l.?name$"],
                "secondary_patterns": ["surname", "family", "name.*last", "ln"],
                "exclusions": ["last.*date", "last.*contact", "last.*email", "last.*booking"],
                "weight": 1.0,
            },
            # Enhanced name field detection for full names
            "full_name": {
                "primary_patterns": [
                    "^name$",
                    "full.?name",
                    "contact.?name",
                    "person.?name",
                    "customer.?name",
                    "attendee.?name",
                    "lead.?name",
                    "prospect.?name",
                ],
                "secondary_patterns": ["name", "fullname", "contactname", "complete.?name"],
                "exclusions": [
                    "first.*name", "last.*name", "company.*name", "file.*name", 
                    "user.*name", "account.*name", "business.*name", "org.*name",
                    "organization.*name", "client.*name.*company", "firm.*name"
                ],
                "weight": 1.0,
                "requires_transformation": True,
                "transformation_type": "split_full_name",
            },
            "company": {
                "primary_patterns": [
                    "company.*name", "company", "organization", "employer", "org", 
                    "current.?company", "account.*name", "business.*name", "client.*company",
                    "organization.*name", "account"
                ],
                "secondary_patterns": [
                    "firm", "workplace", "business", "corp", "corporation", "enterprise",
                    "account", "client", "customer.*company", "vendor"
                ],
                "exclusions": ["partner.*company", "referrer.*company", "previous.*company", "contact.*name"],
                "weight": 1.0,
            },
            "title": {
                "primary_patterns": ["job.*title", "title", "position", "designation", "role", "job.*role"],
                "secondary_patterns": ["job", "function", "occupation", "professional.?title", "current.?title"],
                "exclusions": ["page.*title", "original.*title", "personal.*title"],
                "weight": 0.9,
            },
            "email": {
                "primary_patterns": ["email.*address", "email", "e.?mail", "mail.*address", "e.?mail.*address"],
                "secondary_patterns": ["contact.*email", "business.*email", "work.*email", "primary.*email"],
                "exclusions": ["old.*email", "opted.*out", "email.*send", "email.*date", "secondary.*email"],
                "weight": 1.0,
            },
            "phone": {
                "primary_patterns": ["phone.*number", "phone", "mobile", "cell", "telephone", "mobile.*number"],
                "secondary_patterns": ["contact.*phone", "business.*phone", "work.*phone", "primary.*phone", "tel"],
                "exclusions": ["home.*phone", "personal.*phone", "fax"],
                "weight": 0.9,
            },
            "linkedin": {
                "primary_patterns": ["linkedin.*url", "linkedin.*profile", "linkedin", "li.*url", "linkedin.*link"],
                "secondary_patterns": ["social.*profile", "profile.*url", "professional.*profile", "li.*profile"],
                "exclusions": ["events.*linkedin", "attendees.*linkedin"],
                "weight": 0.8,
            },
            "location": {
                "primary_patterns": [
                    "location",
                    "city",
                    "state",
                    "country",
                    "address",
                    "geo.*location",
                    "headquarters",
                ],
                "secondary_patterns": ["region", "area", "place", "locale", "geography", "where"],
                "exclusions": ["contact.*city", "contact.*state", "shipping.*address"],
                "weight": 0.7,
            },
            "industry": {
                "primary_patterns": ["industry", "sector", "business.*type", "vertical", "market"],
                "secondary_patterns": ["field", "domain", "category", "business.*area"],
                "exclusions": [],
                "weight": 0.6,
            },
        }
        
        # Common false positives to avoid
        self.global_exclusions = [
            "unnamed.*",
            r"column.*\d+",
            "index",
            "id",
            r"record.*id",
            r"created.*date",
            r"modified.*date",
            "timestamp",
        ]
    
    def normalize_column_name(self, column_name: str) -> str:
        """Normalize column name for pattern matching"""
        if not column_name or column_name.strip() == "":
            return ""
        
        # Convert to lowercase and handle common separators
        normalized = str(column_name).lower().strip()
        normalized = re.sub(r"[_\s\-\.]+", ".", normalized)
        normalized = re.sub(r"[^\w\.]", "", normalized)
        normalized = re.sub(r"\.+", ".", normalized)
        normalized = normalized.strip(".")
        
        return normalized
    
    def check_exclusions(self, column_name: str, exclusions: List[str]) -> bool:
        """Check if column name matches any exclusion patterns"""
        normalized = self.normalize_column_name(column_name)
        
        # Check global exclusions first
        for exclusion in self.global_exclusions:
            if re.search(exclusion, normalized):
                return True
        
        # Check field-specific exclusions
        for exclusion in exclusions:
            if re.search(exclusion, normalized):
                return True
        
        return False
    
    def calculate_pattern_score(
        self, column_name: str, patterns: List[str], exclusions: List[str], weight: float
    ) -> Tuple[int, str]:
        """Calculate confidence score for pattern matching"""
        normalized = self.normalize_column_name(column_name)
        
        # Check exclusions first
        if self.check_exclusions(column_name, exclusions):
            return 0, ""
        
        best_score = 0
        best_pattern = ""
        
        for pattern in patterns:
            # Exact match gets highest score
            if normalized == pattern:
                return int(95 * weight), pattern
            
            # Regex pattern matching
            if re.search(pattern, normalized):
                # Calculate fuzzy match score
                fuzzy_score = fuzz.ratio(normalized, pattern)
                pattern_score = min(90, fuzzy_score + 10)
                
                if pattern_score > best_score:
                    best_score = pattern_score
                    best_pattern = pattern
        
        return int(best_score * weight), best_pattern
    
    def map_single_column(self, column_name: str) -> Optional[ColumnMapping]:
        """Map a single column to standard field with improved accuracy"""
        if not column_name or column_name.strip() == "":
            return None
        
        best_mapping = None
        best_confidence = 0
        
        for standard_field, config in self.standard_fields.items():
            # Check primary patterns first
            primary_score, primary_pattern = self.calculate_pattern_score(
                column_name, config["primary_patterns"], config["exclusions"], config["weight"]
            )
            
            if primary_score > best_confidence:
                best_confidence = primary_score
                best_mapping = ColumnMapping(
                    original_name=column_name,
                    standard_name=standard_field,
                    confidence=primary_score,
                    pattern_matched=primary_pattern,
                    requires_content_transformation=config.get("requires_transformation", False),
                    transformation_type=config.get("transformation_type", None),
                )
            
            # Check secondary patterns if primary didn't yield high confidence
            if primary_score < 70:  # Reduced from 80 for better coverage
                secondary_score, secondary_pattern = self.calculate_pattern_score(
                    column_name,
                    config["secondary_patterns"],
                    config["exclusions"],
                    config["weight"] * 0.75,  # Slightly increased weight for secondary patterns
                )
                
                if secondary_score > best_confidence:
                    best_confidence = secondary_score
                    best_mapping = ColumnMapping(
                        original_name=column_name,
                        standard_name=standard_field,
                        confidence=secondary_score,
                        pattern_matched=secondary_pattern,
                        requires_content_transformation=config.get("requires_transformation", False),
                        transformation_type=config.get("transformation_type", None),
                    )
        
        # Return mappings with reasonable confidence (lowered threshold)
        if best_mapping and best_mapping.confidence >= 35:  # Reduced from 50 for better coverage
            return best_mapping
        
        return None
    
    def map_columns(self, columns: List[str], min_confidence: int = 40) -> Dict[str, ColumnMapping]:
        """Map all columns to standard fields with improved accuracy"""
        mappings = {}
        used_standards = set()
        
        # Sort columns by length to prioritize more specific column names
        sorted_columns = sorted(columns, key=len, reverse=True)

        # First pass: high confidence mappings (80%+)
        for column in sorted_columns:
            mapping = self.map_single_column(column)
            if mapping and mapping.confidence >= 75:
                # Allow multiple mappings to name fields since full_name needs special handling
                if mapping.standard_name == "full_name" or mapping.standard_name not in used_standards:
                    mappings[column] = mapping
                    if mapping.standard_name != "full_name":  # Don't mark full_name as used
                    used_standards.add(mapping.standard_name)
        
        # Second pass: medium confidence mappings (60%+) for unmapped standards
        for column in sorted_columns:
            if column in mappings:
                continue
                
            mapping = self.map_single_column(column)
            if mapping and mapping.confidence >= 60:
                if mapping.standard_name == "full_name" or mapping.standard_name not in used_standards:
                    mappings[column] = mapping
                    if mapping.standard_name != "full_name":  # Don't mark full_name as used
                    used_standards.add(mapping.standard_name)
        
        # Third pass: lower confidence mappings (40%+) for still unmapped critical standards
        critical_standards = {"first_name", "last_name", "full_name", "email", "company", "title"}
        unmapped_critical = critical_standards - used_standards

        if unmapped_critical:
            for column in sorted_columns:
                if column in mappings:
                    continue

                mapping = self.map_single_column(column)
                if mapping and mapping.confidence >= min_confidence:
                    if mapping.standard_name in unmapped_critical or mapping.standard_name == "full_name":
                        mappings[column] = mapping
                        if mapping.standard_name != "full_name":
                            used_standards.add(mapping.standard_name)
                            unmapped_critical.discard(mapping.standard_name)

        return mappings
    
    def get_mapping_summary(self, mappings: Dict[str, ColumnMapping]) -> Dict[str, str]:
        """Get a simple mapping dictionary for data processing"""
        return {mapping.original_name: mapping.standard_name for mapping in mappings.values()}

    def get_transformation_requirements(self, mappings: Dict[str, ColumnMapping]) -> Dict[str, Dict]:
        """Get fields that require content transformation"""
        transformations = {}
        for original_col, mapping in mappings.items():
            if mapping.requires_content_transformation:
                transformations[original_col] = {
                    "transformation_type": mapping.transformation_type,
                    "standard_name": mapping.standard_name,
                    "confidence": mapping.confidence,
                }
        return transformations
    
    def validate_mappings(self, mappings: Dict[str, ColumnMapping]) -> Dict[str, List[str]]:
        """Validate mappings and return issues"""
        issues = {
            "high_confidence": [],
            "medium_confidence": [],
            "missing_fields": [],
            "duplicates": [],
            "transformations_needed": [],
        }
        
        standard_to_original = {}
        
        for original, mapping in mappings.items():
            if mapping.confidence >= 80:
                issues["high_confidence"].append(f"{original} -> {mapping.standard_name} ({mapping.confidence}%)")
            elif mapping.confidence >= 60:
                issues["medium_confidence"].append(f"{original} -> {mapping.standard_name} ({mapping.confidence}%)")

            # Track transformations needed
            if mapping.requires_content_transformation:
                issues["transformations_needed"].append(f"{original} -> {mapping.transformation_type}")
            
            # Check for duplicates (but allow full_name to coexist with first/last names)
            if mapping.standard_name != "full_name" and mapping.standard_name in standard_to_original:
                issues["duplicates"].append(
                    f"Both '{original}' and '{standard_to_original[mapping.standard_name]}' map to '{mapping.standard_name}'"
                )
            else:
                standard_to_original[mapping.standard_name] = original
        
        # Check for missing critical fields (but allow full_name as alternative to first/last)
        mapped_standards = set(mapping.standard_name for mapping in mappings.values())
        has_name_fields = bool(mapped_standards & {"first_name", "last_name", "full_name"})

        critical_fields = ["email"]  # Reduced since full_name can substitute for first/last
        if not has_name_fields:
            critical_fields.extend(["first_name", "last_name"])
        
        for field in critical_fields:
            if field not in mapped_standards:
                issues["missing_fields"].append(field)
        
        return issues


def test_column_mapper():
    """Test the column mapper with sample data including NAME columns"""
    mapper = SmartColumnMapper()
    
    # Test with sample columns including the problematic NAME column
    test_columns = [
        "NAME",  # This should now map to full_name and require transformation
        "First Name",
        "Last Name",
        "Email",
        "Company Name",
        "Title",
        "Phone",
        "LinkedIn",
        "City",
        "Industry",
        "Contact: Email",
        "Job Title",
        "Company",
        "Original Title",
        "First Order Closed Date",  # Should be excluded
        "Unnamed: 0",  # Should be excluded
        "LinkedIn profile URL",
    ]
    
    print("🧪 Testing Enhanced Column Mapper with NAME Detection")
    print("=" * 60)
    
    mappings = mapper.map_columns(test_columns)
    
    for original, mapping in mappings.items():
        transform_info = (
            f" [TRANSFORM: {mapping.transformation_type}]" if mapping.requires_content_transformation else ""
        )
        print(f"✓ {original} -> {mapping.standard_name} ({mapping.confidence}%){transform_info}")
    
    print("\n📊 Validation Summary:")
    issues = mapper.validate_mappings(mappings)
    
    for category, items in issues.items():
        if items:
            print(f"\n{category.replace('_', ' ').title()}:")
            for item in items:
                print(f"  • {item}")

    print("\n🔄 Content Transformations Needed:")
    transformations = mapper.get_transformation_requirements(mappings)
    for col, transform_info in transformations.items():
        print(f"  • {col}: {transform_info['transformation_type']} (confidence: {transform_info['confidence']}%)")


if __name__ == "__main__":
    test_column_mapper() 
