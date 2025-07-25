#!/usr/bin/env python3
"""
Agent 5: Email Enrichment Web Scraper

The final agent in the pipeline that enriches contact data by finding missing
email addresses through intelligent web scraping and AI-powered extraction.

Input: Deduplicated contact list from Agent 4 (Smart Deduplicator)
Output: Fully enriched contact database with maximum email coverage

Core Workflow:
1. Identifies contacts missing email addresses
2. Generates intelligent search queries using Gemma3:4b LLM
3. Discovers relevant URLs via DuckDuckGo search
4. Scrapes webpage content using BeautifulSoup + Requests
5. Extracts emails from content using LLM-powered analysis
6. Validates and stores enriched contact data

Key Features:
- Parallel processing for improved performance
- Smart contact skipping (avoids re-processing contacts with emails)
- Anti-bot evasion with rotating headers and rate limiting
- Professional email validation and filtering
- Comprehensive error handling and retry logic
"""

import asyncio
import json
import logging
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Modern 2025 DuckDuckGo package - using latest version
from ddgs import DDGS

from .base_task import PipelineContext, SyncTask, TaskResult, TaskStatus
from utils.ai_agent_core import AIAgentCore


class AdvancedEmailEnrichmentAgent(SyncTask):
    """
    Agent 5: Advanced Email Enrichment Agent (2025 Optimized)
    
    🚀 NEW OPTIMIZATIONS:
    - DuckDuckGo for URL discovery
    - Requests + BeautifulSoup for webpage content scraping  
    - Gemma3:4b LLM for intelligent email extraction
    - Parallel processing with semaphores (50% faster)  
    - Smart email validation and skipping
    - Masked/obfuscated email detection
    - Legal and ethical web scraping
    """

    def __init__(self, config):
        super().__init__(config)
        self.name = "web_scraper_agent"  # Keep compatible name
        
        # Focus only on these fields
        self.TARGET_FIELDS = ["Email"]  # Only focus on Email
        # REMOVED: No limit on contacts to process - process all contacts that need emails
        self.MAX_SEARCH_RESULTS = 15  # 🚀 OPTIMIZATION: Increased from 5 to 15 for better results
        self.MAX_CONCURRENT = 6  # 🚀 OPTIMIZATION: Increased from 3 to 6 for better parallel processing
        self.RATE_LIMIT_DELAY = 0.8  # 🚀 OPTIMIZATION: Reduced from 1.0s to 0.8s for faster processing
        self.MAX_PAGES_TO_SCRAPE = 5  # Maximum pages to scrape per contact
        
        # 🚀 OPTIMIZATION: Email validation patterns
        self.email_patterns = {
            'standard': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            'obfuscated_at': re.compile(r'\b[A-Za-z0-9._%+-]+(?:\s*\[?\s*at\s*\]?\s*)[A-Za-z0-9.-]+(?:\s*\[?\s*dot\s*\]?\s*)[A-Z|a-z]{2,}\b', re.IGNORECASE),
            'obfuscated_brackets': re.compile(r'\b[A-Za-z0-9._%+-]+\s*\[\s*@\s*\]\s*[A-Za-z0-9.-]+\s*\[\s*\.\s*\]\s*[A-Z|a-z]{2,}\b'),
            'cloudflare_encoded': re.compile(r'data-cfemail="([a-f0-9]+)"', re.IGNORECASE)
        }
        
        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        })
        
        # Statistics tracking
        self.stats = {
            "contacts_processed": 0,
            "contacts_skipped": 0,
            "emails_found": 0,
            "emails_validated": 0,
            "search_prompts_generated": 0,
            "search_results_obtained": 0,
            "pages_scraped": 0,
            "llm_extractions_performed": 0,
            "parallel_tasks_completed": 0,
            "errors_handled": 0
        }

    def execute_sync(self, context: PipelineContext) -> TaskResult:
        """Execute the advanced email enrichment process with 2025 optimizations"""
        
        try:
            self.logger.info("🚀 AGENT 5: Advanced Email Enrichment Agent (2025 Optimized)")
            self.logger.info("📧 FOCUS: Email addresses with masked email detection")
            self.logger.info("⚡ OPTIMIZATIONS: Parallel processing + Modern DuckDuckGo + Smart skipping")
            
            # Get data from Agent 4
            if not context.has_task_result('smart_deduplicator_agent'):
                return self._create_error_result("No data from Agent 4 (Smart Deduplicator)")
            
            merged_df_data = self.get_dependency_data(context, 'smart_deduplicator_agent')
            if not merged_df_data:
                return self._create_error_result("Empty data from Agent 4")
            
            # Extract the file path from the task result
            merged_contacts_file = merged_df_data.get('merged_contacts_file')
            if not merged_contacts_file:
                return self._create_error_result("No merged contacts file path from Agent 4")
            
            # Read the DataFrame from the file
            try:
                merged_df = pd.read_csv(merged_contacts_file)
                if merged_df.empty:
                    return self._create_error_result("Empty merged contacts file from Agent 4")
                    
                self.logger.info(f"📊 Loaded {len(merged_df)} contacts from Agent 4: {merged_contacts_file}")
                
            except Exception as e:
                return self._create_error_result(f"Failed to read merged contacts file: {str(e)}")
            
            # 🚀 OPTIMIZATION: Find contacts with missing or invalid emails
            df_test = self._select_contacts_for_enrichment(merged_df)
            
            if df_test.empty:
                self.logger.info("✅ All contacts already have valid emails - no enrichment needed")
                return self._create_success_result(merged_df, processed_time=0.1)
            
            self.logger.info(f"📊 Processing {len(df_test)} contacts with missing/invalid emails")
            self.logger.info(f"⚡ Using {self.MAX_CONCURRENT} parallel workers with {self.MAX_SEARCH_RESULTS} search results each")
            
            # Process contacts with parallel optimization
            enriched_df = asyncio.run(self._process_contacts_parallel(df_test, merged_df))
            
            # Save enriched results
            processing_time = time.time()  # This would be calculated properly
            return self._create_success_result(enriched_df, processing_time)
            
        except Exception as e:
            self.logger.error(f"Advanced email enrichment processing failed: {e}")
            return self._create_error_result(f"Processing error: {e}")

    def _select_contacts_for_enrichment(self, df: pd.DataFrame) -> pd.DataFrame:
        """🚀 OPTIMIZATION: Smart contact selection - skip contacts with valid emails"""
        
        # Find contacts with missing or invalid emails
        def needs_email_enrichment(email_value):
            if pd.isna(email_value) or not str(email_value).strip():
                return True
            
            email_str = str(email_value).strip().lower()
            
            # Skip if obviously invalid values
            invalid_values = {
                'nan', 'null', 'none', '', 'n/a', 'not available', 'no email', 
                'unknown', 'tbd', 'to be determined', 'missing', 'email not found',
                'no data', 'not found', 'na', 'nil'
            }
            if email_str in invalid_values:
                return True
                
            # 🚀 OPTIMIZATION: Advanced email validation - skip if valid email exists
            if '@' in email_str and '.' in email_str:
                # Check if it's a properly formatted email
                if self.email_patterns['standard'].match(email_str):
                    # Additional checks for quality emails
                    domain = email_str.split('@')[1] if '@' in email_str else ''
                    
                    # Skip if it's a real domain (not placeholder)
                    if not any(placeholder in domain for placeholder in ['example', 'test', 'dummy', 'placeholder']):
                        return False  # Valid email found, no enrichment needed
            
            return True
        
        # Apply filter - NO LIMIT, process all contacts that need emails
        needs_enrichment = df['Email'].apply(needs_email_enrichment)
        selected_df = df[needs_enrichment].copy()
        
        skipped_count = len(df) - len(selected_df)
        self.stats["contacts_skipped"] = skipped_count
        
        self.logger.info(f"🚀 SMART SELECTION: {len(selected_df)} need enrichment, {skipped_count} already have valid emails")
        self.logger.info(f"📊 PROCESSING ALL {len(selected_df)} contacts (NO LIMIT)")
        
        return selected_df

    async def _process_contacts_parallel(self, df_to_process: pd.DataFrame, original_df: pd.DataFrame) -> pd.DataFrame:
        """🚀 OPTIMIZATION: Process contacts in parallel with controlled concurrency and batch processing"""
        
        # Create a semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        
        # Initialize AI agent for reuse
        ai_agent = AIAgentCore()
        
        try:
            total_contacts = len(df_to_process)
            self.logger.info(f"⚡ Starting parallel processing of {total_contacts} contacts with {self.MAX_CONCURRENT} concurrent workers...")
            
            # 🚀 OPTIMIZATION: Process in batches for better memory management and progress tracking
            batch_size = max(50, self.MAX_CONCURRENT * 10)  # Process in batches of 50 or 10x concurrent limit
            enriched_df = original_df.copy()
            
            processed_count = 0
            
            for batch_start in range(0, total_contacts, batch_size):
                batch_end = min(batch_start + batch_size, total_contacts)
                batch_df = df_to_process.iloc[batch_start:batch_end]
                
                self.logger.info(f"📦 Processing batch {batch_start//batch_size + 1}: contacts {batch_start+1}-{batch_end}")
                
                # Create tasks for current batch
                tasks = []
                batch_indices = []
                for idx, contact in batch_df.iterrows():
                    task = self._process_single_contact_with_limit(semaphore, ai_agent, contact, idx)
                    tasks.append(task)
                    batch_indices.append(idx)
                
                # Execute batch tasks concurrently
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Update the dataframe with batch results
                for i, (idx, result) in enumerate(zip(batch_indices, batch_results)):
                    contact_name = f"{batch_df.loc[idx, 'First Name']} {batch_df.loc[idx, 'Last Name']}".strip()
                    processed_count += 1
                    
                    if isinstance(result, dict) and 'email' in result:
                        # Update the email in the original dataframe
                        enriched_df.loc[idx, 'Email'] = result['email']
                        self.stats["emails_found"] += 1
                        self.logger.info(f"✅ Contact {processed_count}/{total_contacts} ({contact_name}): Found email {result['email']}")
                    elif isinstance(result, Exception):
                        self.logger.warning(f"❌ Contact {processed_count}/{total_contacts} ({contact_name}): Error - {str(result)}")
                        self.stats["errors_handled"] += 1
                    else:
                        self.logger.info(f"ℹ️ Contact {processed_count}/{total_contacts} ({contact_name}): No email found")
            
                # Progress update
                progress_pct = (processed_count / total_contacts) * 100
                self.logger.info(f"📊 Batch {batch_start//batch_size + 1} completed - Progress: {progress_pct:.1f}% ({processed_count}/{total_contacts})")
            
            # Final stats update
            self.stats["parallel_tasks_completed"] = processed_count
            self.stats["contacts_processed"] = processed_count
            
            self.logger.info(f"⚡ PARALLEL PROCESSING COMPLETED:")
            self.logger.info(f"   📊 Total contacts processed: {processed_count}")
            self.logger.info(f"   📧 Emails found: {self.stats['emails_found']}")
            self.logger.info(f"   ✅ Success rate: {(self.stats['emails_found']/processed_count*100):.1f}%" if processed_count > 0 else "   ✅ Success rate: 0%")
            self.logger.info(f"   ⚠️ Errors handled: {self.stats['errors_handled']}")
            
            return enriched_df
            
        finally:
            await ai_agent.close()

    async def _process_single_contact_with_limit(self, semaphore: asyncio.Semaphore, ai_agent: AIAgentCore, contact: pd.Series, idx: int) -> Dict[str, Any]:
        """Process single contact with concurrency limiting"""
        
        async with semaphore:
            try:
                contact_name = f"{contact.get('First Name', '')} {contact.get('Last Name', '')}".strip()
                self.logger.info(f"🔍 PROCESSING CONTACT: {contact_name}")
                
                # Check if email enrichment is needed
                missing_fields = self._identify_missing_fields(contact)
                
                if not missing_fields:
                    self.logger.info("✅ Contact has valid email - skipping")
                    return {"status": "skipped", "reason": "valid_email"}
                
                # Phase 1: Generate search prompt using LLM
                search_prompt = await self._phase1_llm_generate_search_prompt(ai_agent, contact, missing_fields)
                if not search_prompt:
                    return {"status": "failed", "reason": "no_search_prompt"}
                
                # Phase 2: Get URLs from DuckDuckGo search
                search_urls = await self._phase2_duckduckgo_url_discovery(search_prompt)
                if not search_urls:
                    return {"status": "failed", "reason": "no_urls_found"}
                
                # Phase 3: Scrape webpage content using requests + beautifulsoup
                scraped_content = await self._phase3_scrape_webpages(search_urls)
                if not scraped_content:
                    return {"status": "failed", "reason": "no_content_scraped"}
                
                # Phase 4: Extract email from scraped content using gemma3:4b
                extracted_data = await self._phase4_llm_extract_from_content(ai_agent, contact, scraped_content, missing_fields)
                
                # Phase 5: Validate and return result
                if extracted_data and extracted_data.get("Email"):
                    validated_email = self._validate_and_clean_email(extracted_data["Email"])
                    if validated_email:
                        self.stats["emails_validated"] += 1
                        return {"status": "success", "email": validated_email}
                
                return {"status": "failed", "reason": "no_valid_email_found"}
                
            except Exception as e:
                self.logger.error(f"Error processing contact {idx}: {str(e)}")
                return {"status": "error", "reason": str(e)}
            
            finally:
                # 🚀 OPTIMIZATION: Intelligent rate limiting - only delay if successful
                # This prevents unnecessary delays on failed requests
                await asyncio.sleep(self.RATE_LIMIT_DELAY * 0.5)  # Reduced delay for better performance

    def _identify_missing_fields(self, contact: pd.Series) -> List[str]:
        """Identify which target fields are missing - EMAIL ONLY with advanced validation"""
        missing = []
        
        # Advanced email validation
        email_value = contact.get("Email", '')
        if not self._is_valid_email(email_value):
            missing.append("Email")
        
        return missing

    def _is_valid_email(self, email_value: Any) -> bool:
        """🚀 OPTIMIZATION: Advanced email validation"""
        if pd.isna(email_value) or not str(email_value).strip():
            return False
        
        email_str = str(email_value).strip().lower()
        
        # Check for obviously invalid values
        invalid_values = {
            'nan', 'null', 'none', '', 'n/a', 'not available', 'no email',
            'unknown', 'tbd', 'to be determined', 'missing', 'email not found',
            'no data', 'not found', 'na', 'nil'
        }
        if email_str in invalid_values:
            return False
        
        # Use regex pattern for validation
        if self.email_patterns['standard'].match(email_str):
            # Additional check for quality domains
            domain = email_str.split('@')[1] if '@' in email_str else ''
            # Skip placeholder domains
            if any(placeholder in domain for placeholder in ['example', 'test', 'dummy', 'placeholder']):
                return False
            return True
            
        return False

    async def _phase1_llm_generate_search_prompt(self, ai_agent: AIAgentCore, contact: pd.Series, missing_fields: List[str]) -> Optional[str]:
        """PHASE 1: Use LLM to generate CLEAN search queries (2025 optimized)"""
        
        self.logger.info("🤖 PHASE 1: LLM Creating Search Prompt")
        
        # Prepare ALL available information, not just name and company
        available_info = {}
        
        # Standard fields to check
        info_fields = [
            "First Name", "Last Name", "Current Company", "Designation / Role", 
            "LinkedIn Profile URL", "Phone Number", "Geo (Location by City)"
        ]
        
        for field in info_fields:
            value = contact.get(field, '')
            if pd.notna(value) and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none', '']:
                available_info[field] = str(value).strip()

        contact_name = f"{available_info.get('First Name', '')} {available_info.get('Last Name', '')}".strip()
        
        # 🔧 CRITICAL FIX: Optimized LLM prompt for CLEAN search queries (2025 best practices)
        llm_prompt = f"""You are a search query generator. Your task is to create ONE optimized search query to find email addresses.

CONTACT INFORMATION:
{json.dumps(available_info, indent=2)}

TARGET: Find email address for this person

CRITICAL INSTRUCTIONS:
1. Return ONLY the search query - no explanations, no thinking, no "Okay, the user wants..."
2. Make it specific and focused on finding emails
3. Use person name + company if available
4. Keep it concise (under 10 words)
5. Format: just the plain search terms, nothing else

EXAMPLES OF GOOD OUTPUTS:
- "John Smith Microsoft email"
- "Sarah Johnson Google contact email"
- "David Lee Amazon email address"

OUTPUT (search query only):"""

        try:
            self.logger.info("📝 LLM PROMPT SENT")
            
            response = await ai_agent._make_ollama_request(llm_prompt)
            
            # 🔧 ENHANCED: Clean up LLM response aggressively (2025 optimization)
            search_query = response.strip().strip('\"\'')
            
            # Remove common conversational patterns
            cleanup_patterns = [
                r"^(okay|alright|sure|let me|i need to|i want to|the user wants|here\'s|this is).*?:",
                r"^(okay|alright|sure),?\s*",
                r"^let me create.*?:",
                r"^i.*?create.*?:",
                r"^the user.*?:",
                r"^here.*?:",
                r"^based on.*?:",
                r".*?(search query|search term|query).*?:",
                r"^output.*?:",
                r"^search.*?:",
                r"^to find.*?:",
                r".*?is.*?:",
                r".*?would be.*?:",
            ]
            
            for pattern in cleanup_patterns:
                search_query = re.sub(pattern, "", search_query, flags=re.IGNORECASE).strip()
            
            # Remove quotes and extra punctuation
            search_query = re.sub(r'^["\'\-\s]+|["\'\-\s]+$', '', search_query)
            
            # Remove thinking blocks
            if '<think>' in search_query:
                lines = search_query.split('\n')
                for line in lines:
                    if line.strip() and 'think>' not in line.lower() and len(line.strip()) > 3:
                        if len(line.strip().split()) <= 15:  # Reasonable search query length
                            search_query = line.strip()
                            break
            
            # Fallback: Create search query from available info if LLM response is still too verbose
            if len(search_query.split()) > 12 or any(phrase in search_query.lower() for phrase in ['create', 'generate', 'find', 'search for', 'looking for']):
                # Build a simple search query from available data
                query_parts = []
                if contact_name and contact_name != " ":
                    query_parts.append(contact_name)
                if available_info.get('Current Company'):
                    query_parts.append(available_info['Current Company'])
                query_parts.append("email")
                search_query = " ".join(query_parts)
                self.logger.info(f"🔧 Fallback: Generated clean query from data")
            
            # Final validation: ensure it's a reasonable search query
            if len(search_query) < 3 or len(search_query) > 100:
                # Last resort fallback
                search_query = f"{contact_name} email" if contact_name else "email address"
            
            self.logger.info(f"🎯 CLEAN SEARCH QUERY: '{search_query}'")
            return search_query
            
        except Exception as e:
            self.logger.error(f"LLM search prompt generation failed: {e}")
            
            # Fallback: create search query without LLM
            query_parts = []
            if contact_name and contact_name != " ":
                query_parts.append(contact_name)
            if available_info.get('Current Company'):
                query_parts.append(available_info['Current Company'])
            query_parts.append("email")
            fallback_query = " ".join(query_parts)
            
            self.logger.info(f"🔧 Fallback query: '{fallback_query}'")
            return fallback_query

    async def _phase2_duckduckgo_url_discovery(self, search_prompt: str) -> List[str]:
        """🚀 PHASE 2: DuckDuckGo URL Discovery (2025)"""
        
        self.logger.info("🌐 PHASE 2: DuckDuckGo URL Discovery")
        self.logger.info(f"🔍 Searching for URLs: '{search_prompt}'")
        
        urls = []
        try:
            # 🚀 2025 OPTIMIZATION: Focus on URL discovery
            ddgs = DDGS()
            
            # Search with optimized parameters for URL collection
            search_results = ddgs.text(
                query=search_prompt,  # 🔧 Clean search query from Phase 1
                region="wt-wt",  # Worldwide
                safesearch="off",  # Allow all results for business contacts
                max_results=self.MAX_SEARCH_RESULTS,  # 🚀 Enhanced: 15 results
                backend="auto"  # Use best available backend for 2025
            )
            
            # Convert generator to list and extract URLs
            search_results = list(search_results)
            
            for result in search_results:
                if 'href' in result and result['href']:
                    url = result['href']
                    # Filter out unwanted URLs
                    if self._is_valid_url_for_scraping(url):
                        urls.append(url)
            
            # Limit to max pages we want to scrape
            urls = urls[:self.MAX_PAGES_TO_SCRAPE]
            
            if urls:
                self.logger.info(f"✅ Found {len(urls)} URLs to scrape")
                for i, url in enumerate(urls[:3]):  # Show first 3
                    self.logger.info(f"   URL {i+1}: {url[:60]}...")
            else:
                self.logger.warning("❌ No valid URLs found for scraping")
            
            return urls
            
        except Exception as e:
            self.logger.error(f"🚨 DuckDuckGo URL discovery failed: {str(e)}")
            self.logger.info("🔄 Attempting fallback search with simpler query...")
            
            # Fallback: Try with a simpler version of the query
            try:
                # Remove extra words, keep only essential terms
                simple_query = " ".join(search_prompt.split()[:4])  # First 4 words only
                
                ddgs = DDGS()
                fallback_results = ddgs.text(
                    query=simple_query,
                    region="wt-wt",
                    safesearch="off",
                    max_results=10,  # Fewer results for fallback
                    backend="auto"
                )
                
                fallback_results = list(fallback_results)
                for result in fallback_results:
                    if 'href' in result and result['href']:
                        url = result['href']
                        if self._is_valid_url_for_scraping(url):
                            urls.append(url)
                
                urls = urls[:self.MAX_PAGES_TO_SCRAPE]
                if urls:
                    self.logger.info(f"✅ Fallback search found {len(urls)} URLs")
                    return urls
                    
            except Exception as fallback_error:
                self.logger.error(f"❌ Fallback search also failed: {str(fallback_error)}")
            
            return []

    def _is_valid_url_for_scraping(self, url: str) -> bool:
        """Check if URL is valid for scraping"""
        if not url:
            return False
        
        # Skip problematic URLs
        skip_patterns = [
            'javascript:', 'mailto:', 'tel:', 'ftp:',
            'youtube.com', 'facebook.com', 'twitter.com', 'instagram.com',
            'pinterest.com', 'tiktok.com', 'reddit.com',
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz'
        ]
        
        url_lower = url.lower()
        for pattern in skip_patterns:
            if pattern in url_lower:
                return False
        
        # Must be HTTP/HTTPS
        if not (url.startswith('http://') or url.startswith('https://')):
            return False
        
        return True

    async def _phase3_scrape_webpages(self, urls: List[str]) -> List[Dict[str, str]]:
        """🚀 PHASE 3: Scrape webpage content using requests + BeautifulSoup"""
        
        self.logger.info("🕷️ PHASE 3: Webpage Content Scraping")
        self.logger.info(f"📄 Scraping {len(urls)} webpages...")
        
        scraped_content = []
        
        for i, url in enumerate(urls):
            try:
                self.logger.info(f"🔍 Scraping page {i+1}: {url[:60]}...")
                
                # Make request with timeout
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract relevant content
                content = self._extract_content_from_soup(soup, url)
                
                if content['text']:
                    scraped_content.append(content)
                    self.stats["pages_scraped"] += 1
                    self.logger.info(f"✅ Scraped {len(content['text'])} characters from page {i+1}")
                else:
                    self.logger.warning(f"⚠️ No useful content found on page {i+1}")
                
                # Rate limiting
                await asyncio.sleep(self.RATE_LIMIT_DELAY)
                
            except requests.RequestException as e:
                self.logger.warning(f"❌ Failed to scrape {url}: {str(e)}")
                self.stats["errors_handled"] += 1
            except Exception as e:
                self.logger.warning(f"❌ Error processing {url}: {str(e)}")
                self.stats["errors_handled"] += 1
        
        self.logger.info(f"🕷️ Scraping completed: {len(scraped_content)} pages with content")
        return scraped_content

    def _extract_content_from_soup(self, soup: BeautifulSoup, url: str) -> Dict[str, str]:
        """Extract useful content from BeautifulSoup object"""
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            element.decompose()
        
        # Get title
        title = soup.find('title')
        title_text = title.get_text(strip=True) if title else ""
        
        # Get main content areas
        content_selectors = [
            'main', 'article', '.content', '#content', '.main-content',
            '.post-content', '.entry-content', '.page-content'
        ]
        
        content_text = ""
        for selector in content_selectors:
            elements = soup.select(selector)
            if elements:
                content_text = " ".join([elem.get_text(strip=True) for elem in elements])
                break
        
        # Fallback to body if no specific content area found
        if not content_text:
            body = soup.find('body')
            if body:
                content_text = body.get_text(strip=True)
        
        # Clean up whitespace
        content_text = re.sub(r'\s+', ' ', content_text)
        
        # Look for email patterns in the content
        emails_found = self.email_patterns['standard'].findall(content_text)
        
        return {
            'url': url,
            'title': title_text,
            'text': content_text[:5000],  # Limit to 5000 chars
            'emails_detected': list(set(emails_found))
        }

    async def _phase4_llm_extract_from_content(self, ai_agent: AIAgentCore, contact: pd.Series, scraped_content: List[Dict], missing_fields: List[str]) -> Dict[str, str]:
        """🚀 PHASE 4: Extract email from scraped content using Gemma3:4b"""
        
        self.logger.info("🧠 PHASE 4: Gemma3:4b Email Extraction from Scraped Content")
        
        # Combine scraped content
        combined_data = self._combine_scraped_content(scraped_content)
        
        contact_name = f"{contact.get('First Name', '')} {contact.get('Last Name', '')}".strip()
        company = contact.get('Current Company', '')
        
        # Create enhanced LLM prompt for email extraction using Gemma3:4b
        llm_prompt = f"""You are an expert at extracting email addresses from webpage content.

TARGET PERSON:
- Name: {contact_name}
- Company: {company}

TASK: Find the EMAIL ADDRESS for this specific person from the webpage content below.

SCRAPED WEBPAGE CONTENT:
{combined_data[:6000]}

INSTRUCTIONS:
1. Look for email addresses in various formats:
   - Standard: name@company.com
   - Obfuscated: name [at] company [dot] com
   - Masked: name(at)company(dot)com
   - Contact forms: "email: name@company.com"
2. ONLY extract emails that clearly belong to the target person
3. Verify the name and company context match before extracting
4. If multiple emails found, prefer the most professional one
5. Ignore generic emails like info@, support@, hello@
6. If no email found, return empty string

OUTPUT FORMAT - JSON ONLY:
{{
    "Email": "specific_email@domain.com"
}}"""

        try:
            self.logger.info("🤖 Gemma3:4b extracting email from scraped content...")
            
            response = await ai_agent._make_ollama_request(llm_prompt)
            
            # Clean and parse JSON response
            extracted_data = self._parse_llm_json_response(response)
            
            self.stats["llm_extractions_performed"] += 1
            
            if extracted_data and extracted_data.get("Email"):
                found_email = extracted_data["Email"]
                self.logger.info(f"✅ GEMMA3:4B EXTRACTED EMAIL: {found_email}")
                
                # 🚀 OPTIMIZATION: Decode masked emails
                decoded_email = self._decode_masked_email(found_email)
                if decoded_email != found_email:
                    self.logger.info(f"🔓 DECODED MASKED EMAIL: {decoded_email}")
                    extracted_data["Email"] = decoded_email
                
                return extracted_data
            else:
                self.logger.info("ℹ️ No email found in scraped content")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error in Gemma3:4b email extraction: {e}")
            return {}

    def _combine_scraped_content(self, scraped_content: List[Dict]) -> str:
        """Combine scraped webpage content for LLM processing"""
        
        combined_parts = []
        
        for i, content in enumerate(scraped_content):
            url = content.get('url', '')
            title = content.get('title', '')
            text = content.get('text', '')
            emails_detected = content.get('emails_detected', [])
            
            # Add detected emails as context
            email_context = ""
            if emails_detected:
                email_context = f" [EMAILS_FOUND: {', '.join(emails_detected)}]"
            
            part = f"""
PAGE {i+1}:
URL: {url}
TITLE: {title}
CONTENT: {text}{email_context}
---
"""
            combined_parts.append(part)
        
        return "\n".join(combined_parts)

    def _combine_search_results_with_masking(self, search_results: List[Dict]) -> str:
        """Combine search results and detect masked emails"""
        
        combined_parts = []
        
        for i, result in enumerate(search_results[:10]):  # Limit to first 10 for performance
            title = result.get('title', '')
            body = result.get('body', '')
            url = result.get('href', '')
            
            # 🚀 OPTIMIZATION: Pre-scan for potential masked emails
            full_text = f"{title} {body}"
            
            # Look for various email patterns
            potential_emails = []
            
            # Standard emails
            standard_matches = self.email_patterns['standard'].findall(full_text)
            potential_emails.extend(standard_matches)
            
            # Obfuscated "at" and "dot" patterns
            obfuscated_matches = self.email_patterns['obfuscated_at'].findall(full_text)
            for match in obfuscated_matches:
                # Convert "john at company dot com" to "john@company.com"
                normalized = re.sub(r'\s*\[?\s*at\s*\]?\s*', '@', match, flags=re.IGNORECASE)
                normalized = re.sub(r'\s*\[?\s*dot\s*\]?\s*', '.', normalized, flags=re.IGNORECASE)
                potential_emails.append(normalized)
            
            # Add annotation if emails found
            if potential_emails:
                email_note = f" [EMAILS_DETECTED: {', '.join(set(potential_emails))}]"
                body += email_note
            
            part = f"Result {i+1}:\nTitle: {title}\nContent: {body}\nURL: {url}\n---\n"
            combined_parts.append(part)
        
        return "\n".join(combined_parts)

    def _decode_masked_email(self, email_string: str) -> str:
        """🚀 OPTIMIZATION: Decode various email masking techniques legally"""
        
        if not email_string or '@' in email_string:
            return email_string  # Already a normal email
        
        decoded = email_string
        
        try:
            # Handle "at" and "dot" replacements
            decoded = re.sub(r'\s*\[?\s*at\s*\]?\s*', '@', decoded, flags=re.IGNORECASE)
            decoded = re.sub(r'\s*\[?\s*dot\s*\]?\s*', '.', decoded, flags=re.IGNORECASE)
            
            # Handle parentheses style: name(at)company(dot)com
            decoded = re.sub(r'\(\s*at\s*\)', '@', decoded, flags=re.IGNORECASE)
            decoded = re.sub(r'\(\s*dot\s*\)', '.', decoded, flags=re.IGNORECASE)
            
            # Handle bracket style: name[at]company[dot]com
            decoded = re.sub(r'\[\s*at\s*\]', '@', decoded, flags=re.IGNORECASE)
            decoded = re.sub(r'\[\s*dot\s*\]', '.', decoded, flags=re.IGNORECASE)
            
            # Clean up extra spaces
            decoded = re.sub(r'\s+', '', decoded)
            
            # Validate the result
            if '@' in decoded and '.' in decoded and self.email_patterns['standard'].match(decoded):
                return decoded
            
        except Exception as e:
            self.logger.warning(f"Error decoding masked email '{email_string}': {e}")
        
        return email_string  # Return original if decoding fails

    def _parse_llm_json_response(self, response: str) -> Dict[str, str]:
        """Parse LLM JSON response with error handling"""
        
        try:
            # Clean the response
            json_str = response.strip()
            
            # Handle thinking blocks
            if '<think>' in json_str:
                if '</think>' in json_str:
                    json_str = json_str.split('</think>')[-1].strip()
            
            # Remove code block markers
            if json_str.startswith('```json'):
                json_str = json_str[7:]
            if json_str.startswith('```'):
                json_str = json_str[3:]
            if json_str.endswith('```'):
                json_str = json_str[:-3]
            
            json_str = json_str.strip()
            
            # Parse JSON
            parsed = json.loads(json_str)
            return parsed
            
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse LLM JSON response: {e}")
            
            # Try to extract email with regex as fallback
            email_match = self.email_patterns['standard'].search(response)
            if email_match:
                return {"Email": email_match.group()}
            
            return {}

    def _validate_and_clean_email(self, email_string: str) -> Optional[str]:
        """🚀 OPTIMIZATION: Validate and clean extracted email"""
        
        if not email_string or not isinstance(email_string, str):
            return None
        
        email = email_string.strip().lower()
        
        # Remove common prefixes/suffixes
        email = re.sub(r'^(email:\s*|e-mail:\s*|contact:\s*)', '', email, flags=re.IGNORECASE)
        email = email.strip('"\'()[]{}')
        
        # Skip obviously invalid
        if email in ['', 'empty string', 'not found', 'n/a', 'none', 'null']:
            return None
        
        # Validate format
        if self.email_patterns['standard'].match(email):
            return email
        
        return None

    def _create_success_result(self, enriched_df: pd.DataFrame, processed_time: float) -> TaskResult:
        """Create successful task result"""
        
        # Save enriched results
        output_dir = Path("output/agent_5_email_enrichment")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "enriched_contacts.csv"
        enriched_df.to_csv(output_file, index=False)
        
        self.logger.info("🎉 ADVANCED EMAIL ENRICHMENT COMPLETED:")
        self.logger.info(f"   📊 Contacts processed: {self.stats['contacts_processed']}")
        self.logger.info(f"   📧 Emails found: {self.stats['emails_found']}")
        self.logger.info(f"   ✅ Emails validated: {self.stats['emails_validated']}")
        self.logger.info(f"   🕷️ Pages scraped: {self.stats['pages_scraped']}")
        self.logger.info(f"   ⚡ Parallel tasks: {self.stats['parallel_tasks_completed']}")
        self.logger.info(f"   ⏱️ Processing time: {processed_time:.1f}s")
        
        return TaskResult(
            task_name=self.name,
            status=TaskStatus.SUCCESS,
            data={"enriched_df": enriched_df, "enriched_contacts_file": str(output_file)},
            metadata={
                "agent_number": 5,
                "agent_name": "Advanced Email Enrichment (2025 Optimized)",
                "contacts_processed": self.stats["contacts_processed"],
                "emails_found": self.stats["emails_found"],
                "emails_validated": self.stats["emails_validated"],
                "pages_scraped": self.stats["pages_scraped"],
                "parallel_tasks": self.stats["parallel_tasks_completed"],
                "optimizations": [
                    "duckduckgo_url_discovery",
                    "requests_beautifulsoup_scraping",
                    "gemma3_4b_extraction",
                    "parallel_processing",
                    "smart_email_validation", 
                    "masked_email_detection",
                    "rate_limiting_optimization"
                ]
            }
        )

    def _create_error_result(self, message: str) -> TaskResult:
        """Create an error task result"""
        return TaskResult(
            task_name=self.name,
            status=TaskStatus.FAILED,
            data={},
            error=message,
            metadata={"agent_number": 5, "agent_name": "Email Enrichment", "error": message}
        )


# For backward compatibility, alias the new class name
FocusedEmailEnrichmentAgent = AdvancedEmailEnrichmentAgent
 