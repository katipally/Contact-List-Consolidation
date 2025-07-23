#!/usr/bin/env python3
"""
AI Agent Core - DeepSeek-r1:8b Integration via Ollama
Provides intelligent decision making capabilities throughout the pipeline

Features:
- Contact duplicate detection using AI reasoning
- Data verification and confidence scoring
- Intelligent data enrichment decisions
- Progress estimation and reporting
"""

import asyncio
import aiohttp  # type: ignore
import json
import logging
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
import time
import re

logger = logging.getLogger(__name__)


@dataclass
class AIDecision:
    """Result of an AI agent decision"""

    decision: str
    confidence: float
    reasoning: str
    supporting_data: Dict[str, Any]
    timestamp: datetime
    model_used: str = "deepseek-r1:8b"


@dataclass
class ContactComparison:
    """Contact comparison for duplicate detection"""

    contact1: Dict[str, Any]
    contact2: Dict[str, Any]
    is_duplicate: bool
    confidence: float
    reasoning: str
    merged_data: Optional[Dict[str, Any]] = None


class AIAgentCore:
    """
    AI Agent Core using DeepSeek-r1:8b via Ollama
    Provides intelligent decision making for the entire pipeline
    """

    def __init__(self, ollama_url: str = "http://localhost:11434", model: str = "deepseek-r1:8b", max_retries: int = 3):
        self.ollama_url = ollama_url
        self.model = model
        self.max_retries = max_retries
        self.session = None
        self._session_initialized = False

        # Performance tracking
        self.stats = {
            "requests_made": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_processing_time": 0.0,
            "average_response_time": 0.0,
        }

    async def _ensure_session(self):
        """Ensure aiohttp session is initialized"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=300)  # 5 minute timeout
            )
            self._session_initialized = True

    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def close(self):
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            self._session_initialized = False

    async def _make_ollama_request(self, prompt: str, system_prompt: str = "") -> str:
        """Make a request to Ollama with DeepSeek model"""
        await self._ensure_session()  # Ensure session is initialized

        start_time = time.time()
        self.stats["requests_made"] += 1

        payload = {
            "model": self.model,
            "prompt": prompt,
            "system": system_prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,  # Low temperature for consistent decisions
                "top_p": 0.9,
                "num_predict": 2048,
            },
        }

        for attempt in range(self.max_retries):
            try:
                async with self.session.post(
                    f"{self.ollama_url}/api/generate", json=payload, headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        response_text = result.get("response", "").strip()

                        # Update stats
                        processing_time = time.time() - start_time
                        self.stats["successful_requests"] += 1
                        self.stats["total_processing_time"] += processing_time
                        self.stats["average_response_time"] = (
                            self.stats["total_processing_time"] / self.stats["successful_requests"]
                        )

                        logger.debug(f"Ollama request successful in {processing_time:.2f}s")
                        return response_text
                    else:
                        logger.warning(f"Ollama request failed with status {response.status}")

            except Exception as e:
                logger.warning(f"Ollama request error (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)  # Wait before retry

        # All attempts failed
        self.stats["failed_requests"] += 1
        error_msg = f"Failed to get response from Ollama after {self.max_retries} attempts"
        logger.error(error_msg)
        raise Exception(error_msg)

    def _clean_ai_response(self, response: str) -> str:
        """Clean AI response by removing thinking blocks and extracting JSON"""
        # Remove <think>...</think> blocks that DeepSeek-r1 includes
        cleaned = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL)

        # Clean up extra whitespace
        cleaned = cleaned.strip()

        # If response starts/ends with code blocks, extract the content
        if cleaned.startswith("```json"):
            cleaned = cleaned.split("```json")[1].split("```")[0].strip()
        elif cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1].split("```")[0].strip()

        # Remove any remaining markdown or formatting
        cleaned = cleaned.strip(" \n\r\t`")

        return cleaned

    async def check_duplicate_contacts(self, contact1: Dict[str, Any], contact2: Dict[str, Any]) -> ContactComparison:
        """
        Use AI to intelligently determine if two contacts are duplicates
        This replaces simple string matching with intelligent reasoning
        """

        system_prompt = """You are an AI agent specialized in contact data analysis. Your task is to determine if two contact records represent the same person by analyzing all available information intelligently.

Consider these factors:
1. Name variations (nicknames, maiden names, middle names)
2. Company changes (people move between companies)
3. Email patterns (personal vs work emails)
4. Phone number patterns (mobile vs work numbers)
5. Location changes (people relocate)
6. Role/title changes (career progression)

Respond ONLY with a JSON object in this exact format:
{
    "is_duplicate": true/false,
    "confidence": 0.0-1.0,
    "reasoning": "detailed explanation of your decision",
    "merged_data": {} // if duplicate, provide best merged data
}"""

        prompt = f"""Analyze these two contact records to determine if they represent the same person:

CONTACT 1:
{json.dumps(contact1, indent=2)}

CONTACT 2:
{json.dumps(contact2, indent=2)}

Provide your analysis as JSON only."""

        try:
            response = await self._make_ollama_request(prompt, system_prompt)

            # Clean the response to remove thinking blocks and extract JSON
            cleaned_response = self._clean_ai_response(response)

            # Parse JSON response
            try:
                result = json.loads(cleaned_response)

                return ContactComparison(
                    contact1=contact1,
                    contact2=contact2,
                    is_duplicate=result.get("is_duplicate", False),
                    confidence=float(result.get("confidence", 0.0)),
                    reasoning=result.get("reasoning", "No reasoning provided"),
                    merged_data=result.get("merged_data"),
                )

            except json.JSONDecodeError:
                logger.warning(f"Failed to parse AI response as JSON: {cleaned_response}")
                logger.debug(f"Original response: {response}")
                return ContactComparison(
                    contact1=contact1,
                    contact2=contact2,
                    is_duplicate=False,
                    confidence=0.0,
                    reasoning="Failed to parse AI response",
                )

        except Exception as e:
            logger.error(f"AI duplicate check failed: {str(e)}")
            return ContactComparison(
                contact1=contact1,
                contact2=contact2,
                is_duplicate=False,
                confidence=0.0,
                reasoning=f"AI request failed: {str(e)}",
            )

    async def verify_contact_data(self, contact: Dict[str, Any], scraped_data: List[Dict[str, Any]]) -> AIDecision:
        """
        Use AI to verify contact data against multiple scraped sources
        Determines which data is most accurate and current
        """

        system_prompt = """You are an AI agent specialized in contact data verification. Your task is to analyze contact information from multiple sources and determine the most accurate, current data.

Consider these factors:
1. Data recency (newer data is generally more accurate)
2. Source reliability (company websites > directories > news articles)
3. Data consistency across sources
4. Professional context (LinkedIn vs personal social media)
5. Contact information patterns (corporate emails vs personal)

Respond ONLY with a JSON object in this exact format:
{
    "verified_data": {},  // most accurate contact data
    "confidence": 0.0-1.0,
    "reasoning": "detailed explanation of your decision",
    "source_rankings": []  // sources ranked by reliability
}"""

        prompt = f"""Verify this contact data against multiple scraped sources:

CURRENT CONTACT DATA:
{json.dumps(contact, indent=2)}

SCRAPED DATA FROM SOURCES:
{json.dumps(scraped_data, indent=2)}

Provide the most accurate, verified contact data as JSON only."""

        try:
            response = await self._make_ollama_request(prompt, system_prompt)

            try:
                result = json.loads(response)

                return AIDecision(
                    decision="verified",
                    confidence=float(result.get("confidence", 0.0)),
                    reasoning=result.get("reasoning", "No reasoning provided"),
                    supporting_data={
                        "verified_data": result.get("verified_data", contact),
                        "source_rankings": result.get("source_rankings", []),
                    },
                    timestamp=datetime.now(),
                )

            except json.JSONDecodeError:
                logger.warning(f"Failed to parse AI verification response: {response}")
                return AIDecision(
                    decision="unchanged",
                    confidence=0.0,
                    reasoning="Failed to parse AI response",
                    supporting_data={"verified_data": contact},
                    timestamp=datetime.now(),
                )

        except Exception as e:
            logger.error(f"AI verification failed: {str(e)}")
            return AIDecision(
                decision="unchanged",
                confidence=0.0,
                reasoning=f"AI request failed: {str(e)}",
                supporting_data={"verified_data": contact},
                timestamp=datetime.now(),
            )

    async def estimate_processing_time(self, contacts_count: int, pipeline_tasks: List[str]) -> AIDecision:
        """
        Use AI to estimate processing time based on contact count and pipeline complexity
        """

        system_prompt = """You are an AI agent specialized in pipeline performance estimation. Based on contact count and pipeline tasks, provide realistic time estimates.

Consider these factors:
1. Web scraping rate limits (2-3 seconds per request)
2. AI processing overhead
3. Network latency and retries
4. Data processing complexity
5. Export formatting time

Respond ONLY with a JSON object in this exact format:
{
    "estimated_minutes": 0,
    "confidence": 0.0-1.0,
    "reasoning": "detailed explanation of your estimate",
    "breakdown": {}  // time breakdown by task
}"""

        prompt = f"""Estimate processing time for:

CONTACT COUNT: {contacts_count}
PIPELINE TASKS: {pipeline_tasks}

Consider web scraping rate limits, AI processing, and export formatting. Provide realistic estimates in minutes."""

        try:
            response = await self._make_ollama_request(prompt, system_prompt)

            try:
                result = json.loads(response)

                return AIDecision(
                    decision=f"estimated_{result.get('estimated_minutes', 60)}_minutes",
                    confidence=float(result.get("confidence", 0.7)),
                    reasoning=result.get("reasoning", "No reasoning provided"),
                    supporting_data={
                        "estimated_minutes": result.get("estimated_minutes", 60),
                        "breakdown": result.get("breakdown", {}),
                    },
                    timestamp=datetime.now(),
                )

            except json.JSONDecodeError:
                # Fallback calculation
                estimated_minutes = max(contacts_count * 0.5, 10)  # 30 seconds per contact minimum
                return AIDecision(
                    decision=f"estimated_{int(estimated_minutes)}_minutes",
                    confidence=0.5,
                    reasoning="Fallback estimate: ~30 seconds per contact",
                    supporting_data={
                        "estimated_minutes": int(estimated_minutes),
                        "breakdown": {"fallback": "AI parsing failed"},
                    },
                    timestamp=datetime.now(),
                )

        except Exception as e:
            logger.error(f"AI time estimation failed: {str(e)}")
            estimated_minutes = max(contacts_count * 0.5, 10)
            return AIDecision(
                decision=f"estimated_{int(estimated_minutes)}_minutes",
                confidence=0.3,
                reasoning=f"Fallback estimate due to AI error: {str(e)}",
                supporting_data={"estimated_minutes": int(estimated_minutes), "breakdown": {"error": str(e)}},
                timestamp=datetime.now(),
            )

    async def determine_enrichment_strategy(self, contact: Dict[str, Any], available_sources: List[str]) -> AIDecision:
        """
        Use AI to determine the best enrichment strategy for a contact
        """

        system_prompt = """You are an AI agent specialized in contact enrichment strategy. Based on available contact data and sources, determine the optimal enrichment approach.

Consider these factors:
1. Data completeness (what's missing?)
2. Data quality (what needs verification?)
3. Source suitability (company websites for work info, directories for contact details)
4. Priority (business-critical fields first)

Respond ONLY with a JSON object in this exact format:
{
    "strategy": "comprehensive|selective|minimal",
    "sources_to_use": [],
    "confidence": 0.0-1.0,
    "reasoning": "detailed explanation",
    "priority_fields": []  // fields to enrich first
}"""

        prompt = f"""Determine enrichment strategy for this contact:

CONTACT DATA:
{json.dumps(contact, indent=2)}

AVAILABLE SOURCES: {available_sources}

What's the optimal enrichment strategy to maximize data quality while respecting rate limits?"""

        try:
            response = await self._make_ollama_request(prompt, system_prompt)

            try:
                result = json.loads(response)

                return AIDecision(
                    decision=result.get("strategy", "selective"),
                    confidence=float(result.get("confidence", 0.7)),
                    reasoning=result.get("reasoning", "No reasoning provided"),
                    supporting_data={
                        "sources_to_use": result.get("sources_to_use", available_sources),
                        "priority_fields": result.get("priority_fields", []),
                    },
                    timestamp=datetime.now(),
                )

            except json.JSONDecodeError:
                return AIDecision(
                    decision="selective",
                    confidence=0.5,
                    reasoning="Fallback to selective enrichment due to parsing error",
                    supporting_data={
                        "sources_to_use": available_sources[:3],  # Use first 3 sources
                        "priority_fields": ["email", "current_company", "linkedin"],
                    },
                    timestamp=datetime.now(),
                )

        except Exception as e:
            logger.error(f"AI enrichment strategy failed: {str(e)}")
            return AIDecision(
                decision="selective",
                confidence=0.3,
                reasoning=f"Fallback strategy due to AI error: {str(e)}",
                supporting_data={
                    "sources_to_use": available_sources[:2] if available_sources else [],
                    "priority_fields": ["email", "current_company"],
                },
                timestamp=datetime.now(),
            )

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get AI agent performance statistics"""
        return {
            "requests_made": self.stats["requests_made"],
            "success_rate": (
                self.stats["successful_requests"] / self.stats["requests_made"]
                if self.stats["requests_made"] > 0
                else 0
            ),
            "average_response_time": self.stats["average_response_time"],
            "total_processing_time": self.stats["total_processing_time"],
            "model_used": self.model,
            "ollama_url": self.ollama_url,
        }


# Utility functions for common AI operations
async def quick_duplicate_check(contact1: Dict[str, Any], contact2: Dict[str, Any]) -> bool:
    """Quick duplicate check using AI agent"""
    async with AIAgentCore() as ai_agent:
        result = await ai_agent.check_duplicate_contacts(contact1, contact2)
        return result.is_duplicate and result.confidence > 0.7


async def quick_data_verification(contact: Dict[str, Any], scraped_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Quick data verification using AI agent"""
    async with AIAgentCore() as ai_agent:
        result = await ai_agent.verify_contact_data(contact, scraped_data)
        return result.supporting_data.get("verified_data", contact)
