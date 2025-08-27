#!/usr/bin/env python3
"""
Shared Constants for Contact List Consolidation Pipeline

Centralized constants used across all agents to prevent configuration drift
and ensure consistency throughout the pipeline.
"""

from typing import List, Dict, Any

# Standard output fields used across all agents
STANDARD_FIELDS = [
    "First Name",
    "Last Name", 
    "Current Company",
    "Designation / Role",
    "LinkedIn Profile URL",
    "Email",
    "Phone Number",
    "Geo (Location by City)"
]

# Field mapping priorities (higher = more important)
FIELD_PRIORITIES = {
    "First Name": 10,
    "Last Name": 10,
    "Email": 9,
    "Current Company": 8,
    "LinkedIn Profile URL": 7,
    "Designation / Role": 6,
    "Phone Number": 5,
    "Geo (Location by City)": 4
}

# Email validation patterns
EMAIL_PATTERNS = {
    'standard': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    'obfuscated_at': r'\b[A-Za-z0-9._%+-]+(?:\s*\[?\s*at\s*\]?\s*)[A-Za-z0-9.-]+(?:\s*\[?\s*dot\s*\]?\s*)[A-Z|a-z]{2,}\b',
    'obfuscated_brackets': r'\b[A-Za-z0-9._%+-]+\s*\[\s*@\s*\]\s*[A-Za-z0-9.-]+\s*\[\s*\.\s*\]\s*[A-Z|a-z]{2,}\b',
    'cloudflare_encoded': r'data-cfemail="([a-f0-9]+)"'
}

# Phone number patterns
PHONE_PATTERNS = [
    r'^\+?[1-9]\d{1,14}$',  # International format
    r'^\(?[0-9]{3}\)?[-. ]?[0-9]{3}[-. ]?[0-9]{4}$',  # US format
    r'^[0-9]{10,15}$'  # Simple digits
]

# LinkedIn URL patterns
LINKEDIN_PATTERNS = [
    r'(https?://)?(www\.)?(linkedin\.com/in/[a-zA-Z0-9\-]+/?)',
    r'linkedin\.com/pub/[a-zA-Z0-9\-]+',
    r'linkedin\.com/profile/view\?id='
]

# Business indicators for company name detection
BUSINESS_INDICATORS = [
    'inc', 'llc', 'corp', 'ltd', 'company', 'corporation', 'incorporated',
    'technologies', 'solutions', 'services', 'systems', 'group', 'holdings',
    'university', 'hospital', 'government', 'agency', 'department'
]

# Data sampling configuration
SAMPLING_CONFIG = {
    'max_sample_rows': 20,  # Increased from 5 to 20 for better accuracy
    'min_sample_rows': 10,  # Minimum rows to sample
    'stratified_sampling': True,  # Use stratified sampling
    'sample_per_column': 5,  # Sample k rows per column for rare values
}

# LLM configuration
LLM_CONFIG = {
    'timeout': 45,  # Increased timeout for better results
    'temperature': 0.1,  # Low temperature for consistency
    'max_retries': 3,
    'model': 'gemma3:4b'
}

# Performance configuration
PERFORMANCE_CONFIG = {
    'max_concurrent_files': 3,  # Parallel file processing
    'log_level_for_loops': 'DEBUG',  # Reduce logging overhead
    'vectorized_operations': True,  # Use pandas vectorization
}

# Header detection configuration
HEADER_DETECTION = {
    'max_header_row_check': 5,  # Check first 5 rows for headers
    'min_header_score': 3,  # Minimum score to consider as header
    'header_indicators': [
        'name', 'first', 'last', 'email', 'linkedin', 'company', 
        'designation', 'role', 'title', 'phone', 'location', 'city',
        'contact', 'profile', 'organization', 'position'
    ],
    'metadata_anti_patterns': [
        'event link', 'https://', 'http://', 'www.', '.com', '.org', 
        'unnamed:', 'note:', 'comment:', 'instruction:', 'highlight'
    ]
}

# Quality thresholds
QUALITY_THRESHOLDS = {
    'min_name_coverage': 0.3,  # 30% minimum name coverage
    'min_email_validity': 0.8,  # 80% email validity
    'min_linkedin_validity': 0.7,  # 70% LinkedIn validity
    'min_phone_validity': 0.6,  # 60% phone validity
    'min_data_completeness': 0.2  # 20% minimum data completeness
}
