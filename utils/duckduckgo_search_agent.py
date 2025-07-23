#!/usr/bin/env python3
"""
Modern Contact Enrichment Engine - 2025 Edition
Hybrid approach combining multiple search sources for reliable contact data enrichment

Features:
- SerpAPI integration for reliable search results (primary)
- Improved DuckDuckGo with anti-detection (fallback)
- Local pattern matching for common contact formats
- Rate limiting and ethical search practices
- LinkedIn profile discovery and business email patterns
"""

import asyncio
import aiohttp  # type: ignore
import json
import logging
import re
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import quote_plus, urljoin
import random
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)


@dataclass
class ContactSearchResults:
    """Enriched contact search results with confidence scoring"""

    contact_name: str
    company: str
    contact_emails: List[str]
    linkedin_profiles: List[str]
    phone_numbers: List[str]
    locations: List[str]
    job_titles: List[str]
    confidence_score: float
    data_sources: List[str]
    search_timestamp: datetime


class ModernContactEnrichmentEngine:
    """
    Modern Contact Enrichment Engine with 2025 Best Practices

    Combines multiple search sources for reliable contact data enrichment:
    - SerpAPI for reliable search results (if API key available)
    - Improved DuckDuckGo with rotation and anti-detection
    - Local pattern matching for common contact formats
    - LinkedIn and business email discovery
    """

    def __init__(self, rate_limit: float = 1.0, max_results_per_query: int = 10):
        self.rate_limit = rate_limit
        self.max_results_per_query = max_results_per_query
        self.session = None
        self.last_request_time = 0.0

        # API Keys (optional - will use fallback methods if not available)
        self.serpapi_key = os.getenv("SERPAPI_KEY")  # For SerpAPI access

        # Enhanced search statistics
        self.stats = {
            "searches_performed": 0,
            "results_found": 0,
            "serpapi_searches": 0,
            "duckduckgo_searches": 0,
            "pattern_matches": 0,
            "rate_limit_delays": 0,
            "failed_searches": 0,
            "enrichment_success_rate": 0.0,
        }

        # Modern user agents for 2025
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
        ]

        # Contact data patterns for intelligent extraction
        self.contact_patterns = {
            "email": [
                r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
                r"\b[a-zA-Z0-9._%+-]+\s*@\s*[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b",
            ],
            "phone": [
                r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
                r"\(\d{3}\)\s?\d{3}[-.]?\d{4}",
                r"\+\d{1,3}[-.]?\d{3,4}[-.]?\d{3,4}[-.]?\d{3,4}",
                r"\b\d{10,15}\b",
            ],
            "linkedin": [
                r"linkedin\.com/in/[a-zA-Z0-9-]+/?",
                r"linkedin\.com/pub/[a-zA-Z0-9-]+/?",
                r"www\.linkedin\.com/in/[a-zA-Z0-9-]+/?",
            ],
            "location": [
                r"\b[A-Z][a-z]+,\s*[A-Z]{2}\b",
                r"\b[A-Z][a-z]+ [A-Z][a-z]+,\s*[A-Z]{2}\b",
                r"\b[A-Z][a-z]+,\s*[A-Z][a-z]+\b",
            ],
            "job_title": [
                r"\b(CEO|CTO|VP|Vice President|Director|Manager|Engineer|Developer|Analyst|Specialist|Consultant|Lead|Senior|Principal)\b",
                r"\b(Chief Executive Officer|Chief Technology Officer|Chief Operating Officer|Chief Financial Officer)\b",
            ],
        }

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": random.choice(self.user_agents)},
            connector=aiohttp.TCPConnector(limit=10, limit_per_host=5),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def search_contact_info(self, query: str) -> List[str]:
        """
        Modern contact search using hybrid approach
        Returns list of search result texts for enrichment processing
        """
        try:
            # Create a fresh session for each call to avoid session closure issues
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": random.choice(self.user_agents)},
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            ) as session:
                # Temporarily use this session
                old_session = self.session
                self.session = session
                
                # Update stats
                self.stats["searches_performed"] += 1

                # Try SerpAPI first if available (premium, reliable)
                if self.serpapi_key:
                    self.logger.debug(f"Attempting SerpAPI search for: {query}")
                    results = await self._serpapi_search(query)
                    if results:
                        self.stats["serpapi_searches"] += 1
                        self.stats["results_found"] += len(results)
                        self.logger.debug(f"SerpAPI returned {len(results)} results")
                        return results[:self.max_results_per_query]

                # Fallback to DuckDuckGo with enhanced anti-detection
                self.logger.debug(f"Using DuckDuckGo search for: {query}")
                results = await self._enhanced_duckduckgo_search(query)
                
                if results:
                    self.stats["duckduckgo_searches"] += 1
                    self.stats["results_found"] += len(results)
                    self.logger.debug(f"DuckDuckGo returned {len(results)} results")
                else:
                    self.stats["failed_searches"] += 1
                    self.logger.debug(f"No results found for query: {query}")

                # Restore old session
                self.session = old_session
                
                return results[:self.max_results_per_query]

        except Exception as e:
            self.stats["failed_searches"] += 1
            self.logger.warning(f"Search failed for query '{query}': {str(e)}")
            return []

    async def _serpapi_search(self, query: str) -> List[str]:
        """Search using SerpAPI for reliable results"""
        if not self.serpapi_key:
            return []

        try:
            await self._rate_limit()

            params = {
                "q": query,
                "engine": "duckduckgo",
                "api_key": self.serpapi_key,
                "num": self.max_results_per_query,
            }

            async with self.session.get("https://serpapi.com/search", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_serpapi_results(data)
                else:
                    logger.warning(f"SerpAPI returned status {response.status}")
                    return []

        except Exception as e:
            logger.warning(f"SerpAPI search failed: {str(e)}")
            return []

    def _parse_serpapi_results(self, data: Dict[str, Any]) -> List[str]:
        """Parse SerpAPI response into text results"""
        results = []

        # Parse organic results
        for result in data.get("organic_results", []):
            title = result.get("title", "")
            snippet = result.get("snippet", "")
            link = result.get("link", "")

            if title and snippet:
                result_text = f"{title} {snippet} {link}"
                results.append(result_text)

        return results[: self.max_results_per_query]

    async def _enhanced_duckduckgo_search(self, query: str) -> List[str]:
        """Enhanced DuckDuckGo search with 2025 anti-detection techniques"""
        try:
            await self._rate_limit()

            # Rotate user agent for each request
            headers = {
                "User-Agent": random.choice(self.user_agents),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
            }

            # Add random delay to appear more human-like
            await asyncio.sleep(random.uniform(0.5, 1.5))

            params = {
                "q": query,
                "kl": "us-en",
                "s": "0",
                "df": "",  # Any time
                "vqd": "",  # DuckDuckGo verification token (will be empty for simplicity)
            }

            async with self.session.get(
                "https://html.duckduckgo.com/html/", params=params, headers=headers, allow_redirects=True
            ) as response:
                if response.status == 200:
                    html_content = await response.text()
                    return self._parse_duckduckgo_html(html_content)
                elif response.status == 403:
                    logger.warning("DuckDuckGo returned 403 - rate limited or blocked")
                    return []
                else:
                    logger.warning(f"DuckDuckGo returned status {response.status}")
                    return []

        except Exception as e:
            logger.warning(f"Enhanced DuckDuckGo search failed: {str(e)}")
            return []

    def _parse_duckduckgo_html(self, html_content: str) -> List[str]:
        """Parse DuckDuckGo HTML results with improved extraction"""
        results = []

        try:
            # Extract result blocks using more robust patterns
            result_patterns = [
                r'class="result__title">.*?<a[^>]*href="([^"]*)"[^>]*>([^<]*)</a>',
                r'class="result__snippet">([^<]*)</span>',
                r'class="result__url"[^>]*>([^<]*)</span>',
            ]

            # Combine patterns to extract comprehensive results
            title_pattern = r'class="result__title">.*?<a[^>]*href="([^"]*)"[^>]*>([^<]*)</a>'
            snippet_pattern = r'class="result__snippet">([^<]*)</span>'

            titles_urls = re.findall(title_pattern, html_content, re.DOTALL)
            snippets = re.findall(snippet_pattern, html_content, re.DOTALL)

            # Combine results
            for i, (url, title) in enumerate(titles_urls[: self.max_results_per_query]):
                snippet = snippets[i] if i < len(snippets) else ""

                # Clean up HTML entities and formatting
                title = self._clean_html_text(title)
                snippet = self._clean_html_text(snippet)
                url = self._clean_html_text(url)

                if title and (snippet or url):
                    result_text = f"{title} {snippet} {url}".strip()
                    results.append(result_text)

        except Exception as e:
            logger.warning(f"Error parsing DuckDuckGo HTML: {str(e)}")

        return results

    def _pattern_based_enrichment(self, query: str) -> List[str]:
        """Generate potential contact information using pattern-based logic"""
        results = []

        # Extract name and company from query
        name_match = re.search(r'"([^"]+)"', query)
        company_match = re.search(r"\b([A-Z][a-zA-Z\s]+(?:Inc|Corp|LLC|Ltd|Company|Co))\b", query)

        if name_match:
            name = name_match.group(1)
            company = company_match.group(1) if company_match else ""

            # Generate common email patterns
            email_patterns = self._generate_email_patterns(name, company)
            if email_patterns:
                email_result = f"Common email patterns for {name}: {', '.join(email_patterns)}"
                results.append(email_result)

            # Generate LinkedIn profile suggestions
            linkedin_patterns = self._generate_linkedin_patterns(name)
            if linkedin_patterns:
                linkedin_result = f"LinkedIn profile suggestions: {', '.join(linkedin_patterns)}"
                results.append(linkedin_result)

        return results

    def _generate_email_patterns(self, name: str, company: str) -> List[str]:
        """Generate common email patterns based on name and company"""
        if not name:
            return []

        # Clean and format name
        name_parts = name.lower().replace(".", "").replace(",", "").split()
        if len(name_parts) < 2:
            return []

        first_name = name_parts[0]
        last_name = name_parts[-1]

        # Common company domains (simplified)
        domains = []
        if company:
            company_clean = company.lower().replace(" ", "").replace(",", "")
            company_clean = re.sub(r"\b(inc|corp|llc|ltd|company|co)\b", "", company_clean).strip()
            if company_clean:
                domains.extend([f"{company_clean}.com", f"{company_clean}.org"])

        # Generic domains for pattern recognition
        domains.extend(["company.com", "organization.org"])

        patterns = []
        for domain in domains[:2]:  # Limit to 2 domains
            patterns.extend(
                [
                    f"{first_name}.{last_name}@{domain}",
                    f"{first_name}{last_name}@{domain}",
                    f"{first_name[0]}{last_name}@{domain}",
                    f"{first_name}.{last_name[0]}@{domain}",
                ]
            )

        return patterns[:4]  # Return top 4 patterns

    def _generate_linkedin_patterns(self, name: str) -> List[str]:
        """Generate LinkedIn profile URL patterns"""
        if not name:
            return []

        name_clean = name.lower().replace(".", "").replace(",", "")
        name_parts = name_clean.split()

        if len(name_parts) >= 2:
            first_name = name_parts[0]
            last_name = name_parts[-1]

            patterns = [
                f"linkedin.com/in/{first_name}-{last_name}",
                f"linkedin.com/in/{first_name}{last_name}",
                f"linkedin.com/in/{first_name}-{last_name}-{random.randint(100, 999)}",
            ]
            return patterns[:2]

        return []

    async def _rate_limit(self):
        """Enhanced rate limiting with jitter"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.rate_limit:
            # Add random jitter to appear more human-like
            delay = self.rate_limit - time_since_last + random.uniform(0.2, 0.8)
            await asyncio.sleep(delay)
            self.stats["rate_limit_delays"] += 1

        self.last_request_time = time.time()

    def _clean_html_text(self, text: str) -> str:
        """Enhanced HTML cleaning for better text extraction"""
        if not text:
            return ""

        # Replace common HTML entities
        replacements = {
            "&amp;": "&",
            "&lt;": "<",
            "&gt;": ">",
            "&quot;": '"',
            "&#x27;": "'",
            "&#39;": "'",
            "&nbsp;": " ",
            "&hellip;": "...",
            "&mdash;": "—",
            "&ndash;": "–",
        }

        for entity, replacement in replacements.items():
            text = text.replace(entity, replacement)

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)

        # Clean up whitespace
        text = re.sub(r"\s+", " ", text).strip()

        return text

    def get_search_stats(self) -> Dict[str, Any]:
        """Get enhanced search performance statistics"""
        total_searches = max(1, self.stats["searches_performed"])

        return {
            "searches_performed": self.stats["searches_performed"],
            "results_found": self.stats["results_found"],
            "serpapi_searches": self.stats["serpapi_searches"],
            "duckduckgo_searches": self.stats["duckduckgo_searches"],
            "pattern_matches": self.stats["pattern_matches"],
            "success_rate": ((total_searches - self.stats["failed_searches"]) / total_searches),
            "average_results_per_search": (self.stats["results_found"] / total_searches),
            "rate_limit_delays": self.stats["rate_limit_delays"],
            "failed_searches": self.stats["failed_searches"],
            "has_serpapi": bool(self.serpapi_key),
            "search_methods_available": self._get_available_methods(),
        }

    def _get_available_methods(self) -> List[str]:
        """Get list of available search methods"""
        methods = ["enhanced_duckduckgo", "pattern_matching"]
        if self.serpapi_key:
            methods.insert(0, "serpapi")
        return methods


# Compatibility alias - maintain backward compatibility
DuckDuckGoSearchAgent = ModernContactEnrichmentEngine


# Utility functions for easy integration
async def quick_contact_search(first_name: str, last_name: str, company: str = "") -> List[str]:
    """Quick contact search using modern enrichment engine"""
    query = f'"{first_name} {last_name}" {company} contact email'

    async with ModernContactEnrichmentEngine() as search_agent:
        return await search_agent.search_contact_info(query)


async def quick_linkedin_search(first_name: str, last_name: str, company: str = "") -> List[str]:
    """Quick LinkedIn profile search"""
    query = f'site:linkedin.com/in "{first_name} {last_name}" {company}'

    async with ModernContactEnrichmentEngine() as search_agent:
        return await search_agent.search_contact_info(query)
 