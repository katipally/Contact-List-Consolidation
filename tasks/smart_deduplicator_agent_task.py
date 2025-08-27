"""
Smart Duplicate Finder (Agent 4)

This is the brain that finds and merges duplicate contacts. Here's how it works:

1. First, it looks for obvious duplicates (same email, phone, or very similar names)
2. Then it asks the AI to double-check each potential duplicate
3. Finally, it merges the contacts the AI confirms are the same person

The cool part: It's really careful not to merge contacts that aren't actually duplicates.
This way you don't lose any real contacts by accident!

Written to be fast, smart, and safe.
"""

import json
import time
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass

import pandas as pd

# Import base classes and utilities
from .base_task import BaseTask, TaskResult, TaskStatus, PipelineContext
from utils.ai_agent_core import AIAgentCore, ContactComparison

# Try to import RapidFuzz for similarity matching
try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


@dataclass
class DuplicateCandidate:
    """Represents a potential duplicate pair"""
    contact1_idx: int
    contact2_idx: int
    similarity_score: float
    match_reason: str
    contact1_data: Dict[str, Any]
    contact2_data: Dict[str, Any]


class SimpleContactDeduplicator(BaseTask):
    """
    Simple Agent 4: ML+LLM Contact Deduplicator
    
    Uses ML for fast candidate detection, LLM for confirmation, and simple merging.
    """
    
    def __init__(self, config):
        super().__init__(config)
        
        # Settings for how strict we are about finding duplicates
        self.EMAIL_PHONE_THRESHOLD = 95.0    # How sure we need to be about email/phone matches (95%)
        self.NAME_SIMILARITY_THRESHOLD = 82.0 # How similar names need to be to consider them (82%)
        self.COMPANY_SIMILARITY_THRESHOLD = 80.0 # How similar company names need to be (80%)
        self.FINAL_CONFIDENCE_THRESHOLD = 85.0   # How confident the AI needs to be before merging (85%)
        self.MAX_CANDIDATES = 8000           # Maximum number of potential duplicates to check
        self.BATCH_SIZE = 10                 # How many contacts to send to AI at once
        
        # Keep track of what we're doing so we can report progress
        self.stats = {
            'initial_contacts': 0,      # How many contacts we started with
            'candidates_found': 0,      # How many potential duplicates we found
            'llm_confirmed_merges': 0,  # How many the AI said to merge
            'final_contacts': 0,        # How many contacts we ended up with
            'processing_time': 0.0      # How long this took
        }
    
    async def execute(self, context: PipelineContext) -> TaskResult:
        """Execute the simple ML+LLM deduplication pipeline"""
        try:
            start_time = time.time()
            self.logger.info("🚀 Starting Simple Agent 4: ML+LLM Contact Deduplicator")
            
            # Load input data
            input_file = self._get_input_file(context)
            if not input_file.exists():
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            df = pd.read_csv(input_file)
            initial_count = len(df)
            self.stats['initial_contacts'] = initial_count
            self.logger.info(f"📊 Processing {initial_count:,} contacts")
            
            # Step 1: ML-based candidate detection
            self.logger.info("🎯 Step 1: Finding duplicate candidates with ML...")
            candidates = self._find_duplicate_candidates(df)
            self.stats['candidates_found'] = len(candidates)
            self.logger.info(f"   ✅ Found {len(candidates)} potential duplicate pairs")
            
            # Step 2: LLM batch confirmation
            self.logger.info("🤖 Step 2: LLM batch confirmation...")
            confirmed_merges = await self._batch_llm_confirmation(candidates)
            self.stats['llm_confirmed_merges'] = len(confirmed_merges)
            self.logger.info(f"   ✅ LLM confirmed {len(confirmed_merges)} merges")
            
            # Step 3: Merge LLM-confirmed duplicates with intelligent data merging
            self.logger.info("🔗 Step 3: Merging LLM-confirmed duplicates with smart data combination...")
            merged_df = self._merge_llm_confirmed_duplicates(df, confirmed_merges)
            final_count = len(merged_df)
            self.stats['final_contacts'] = final_count
            removed_count = initial_count - final_count
            self.logger.info(f"   ✅ Removed {removed_count} duplicate contacts")
            self.logger.info(f"   ✅ Final contacts after deduplication: {final_count:,}")
            
            # Save output
            output_dir = self._get_output_directory()
            output_file = output_dir / "merged_contacts.csv"
            merged_df.to_csv(output_file, index=False)
            self.logger.info(f"💾 Saved deduplicated contacts to: {output_file}")
            
            # Save statistics
            self._save_statistics(output_dir)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            self.stats['processing_time'] = processing_time
            self.logger.info(f"⏱️ Processing completed in {processing_time:.2f}s")
            
            return TaskResult(
                task_name="smart_deduplicator_agent",
                status=TaskStatus.SUCCESS,
                data={
                    "merged_contacts_file": str(output_file),  # Agent 5 expects this field name
                    "output_file": str(output_file),  # Keep for backward compatibility
                    "initial_contacts": self.stats['initial_contacts'],
                    "final_contacts": self.stats['final_contacts'],
                    "candidates_found": self.stats['candidates_found'],
                    "llm_confirmed_merges": self.stats['llm_confirmed_merges'],
                    "processing_time": self.stats['processing_time']
                }
            )
            
        except Exception as e:
            self.logger.error(f"❌ Agent 4 execution failed: {str(e)}")
            return TaskResult(
                task_name="smart_deduplicator_agent",
                status=TaskStatus.FAILED,
                error=str(e)
            )
    
    def _find_duplicate_candidates(self, df: pd.DataFrame) -> List[DuplicateCandidate]:
        """Step 1: Fast ML-based duplicate candidate detection"""
        candidates = []
        
        # Convert to list for faster processing
        contacts = df.to_dict('records')
        
        self.logger.info(f"   🔍 Analyzing {len(contacts)} contacts for duplicates...")
        
        for i in range(len(contacts)):
            for j in range(i + 1, len(contacts)):
                contact1, contact2 = contacts[i], contacts[j]
                
                # Fast similarity checks
                score, reason = self._calculate_similarity(contact1, contact2)
                
                if score >= self.NAME_SIMILARITY_THRESHOLD:
                    candidates.append(DuplicateCandidate(
                        contact1_idx=i,
                        contact2_idx=j,
                        similarity_score=score,
                        match_reason=reason,
                        contact1_data=contact1,
                        contact2_data=contact2
                    ))
                    
                    # Limit candidates to prevent overload
                    if len(candidates) >= self.MAX_CANDIDATES:
                        self.logger.warning(f"   ⚠️ Reached max candidates limit ({self.MAX_CANDIDATES})")
                        return candidates
        
        return candidates
    
    def _calculate_similarity(self, contact1: Dict, contact2: Dict) -> Tuple[float, str]:
        """Calculate conservative similarity score using multiple metrics"""
        # Email exact match - highest confidence
        email1 = str(contact1.get('Email', '')).lower().strip()
        email2 = str(contact2.get('Email', '')).lower().strip()
        if email1 and email2 and len(email1) > 5 and email1 == email2:
            return 98.0, 'email_exact_match'
        
        # Phone exact match - high confidence 
        phone1 = self._clean_phone(str(contact1.get('Phone Number', '')))
        phone2 = self._clean_phone(str(contact2.get('Phone Number', '')))
        if phone1 and phone2 and len(phone1) >= 10 and phone1 == phone2:
            return 96.0, 'phone_exact_match'
        
        # Conservative name + company matching
        if RAPIDFUZZ_AVAILABLE:
            similarity_score = self._calculate_composite_similarity(contact1, contact2)
            if similarity_score >= self.NAME_SIMILARITY_THRESHOLD:
                return similarity_score, 'composite_match'
        
        return 0.0, 'no_match'
    
    def _calculate_composite_similarity(self, contact1: Dict, contact2: Dict) -> float:
        """Calculate composite similarity using multiple fields and metrics"""
        scores = []
        weights = []
        
        # First Name similarity (Jaro-Winkler is better for names)
        fname1 = str(contact1.get('First Name', '')).lower().strip()
        fname2 = str(contact2.get('First Name', '')).lower().strip()
        if fname1 and fname2:
            fname_score = fuzz.ratio(fname1, fname2)  # Using ratio for now
            scores.append(fname_score)
            weights.append(0.3)
        
        # Last Name similarity 
        lname1 = str(contact1.get('Last Name', '')).lower().strip()
        lname2 = str(contact2.get('Last Name', '')).lower().strip()
        if lname1 and lname2:
            lname_score = fuzz.ratio(lname1, lname2)
            scores.append(lname_score)
            weights.append(0.4)
        
        # Company similarity
        company1 = str(contact1.get('Current Company', '')).lower().strip()
        company2 = str(contact2.get('Current Company', '')).lower().strip()
        if company1 and company2:
            company_score = fuzz.ratio(company1, company2)
            scores.append(company_score)
            weights.append(0.2)
        
        # Location similarity
        loc1 = str(contact1.get('Geo (Location by City)', '')).lower().strip()
        loc2 = str(contact2.get('Geo (Location by City)', '')).lower().strip()
        if loc1 and loc2:
            loc_score = fuzz.ratio(loc1, loc2)
            scores.append(loc_score)
            weights.append(0.1)
        
        if not scores:
            return 0.0
        
        # Calculate weighted average
        weighted_score = sum(s * w for s, w in zip(scores, weights)) / sum(weights)
        
        # Apply penalties for mismatches - more lenient
        if len(scores) >= 2:
            # If first/last name don't match well, penalize moderately
            if len(scores) >= 2 and scores[0] < 65 and scores[1] < 65:
                weighted_score *= 0.7
            
            # Boost score if company matches well
            if len(scores) >= 3 and scores[2] > 85:
                weighted_score *= 1.1
        
        return weighted_score
    
    def _clean_phone(self, phone: str) -> str:
        """Clean phone number for comparison"""
        import re
        return re.sub(r'[^0-9]', '', phone)
    
    async def _batch_llm_confirmation(self, candidates: List[DuplicateCandidate]) -> List[Tuple[int, int, Dict]]:
        """Step 2: Intelligent LLM batch confirmation using Gemma3:4b"""
        confirmed_merges = []
        
        # Process in smaller batches to avoid overwhelming the LLM
        batch_size = min(self.BATCH_SIZE, 5)  # Smaller batches for better LLM performance
        
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i + batch_size]
            batch_num = i//batch_size + 1
            self.logger.info(f"   🤖 Processing Gemma3:4b batch {batch_num} ({len(batch)} candidates)...")
            
            # Send batch to LLM for intelligent analysis
            confirmed_batch = await self._llm_confirm_batch(batch)
            confirmed_merges.extend(confirmed_batch)
            
            # Add small delay between batches to be nice to the LLM
            if i + batch_size < len(candidates):
                await asyncio.sleep(0.5)
        
        return confirmed_merges
    
    async def _llm_confirm_batch(self, batch: List[DuplicateCandidate]) -> List[Tuple[int, int, Dict]]:
        """Use actual Gemma3:4b LLM to verify duplicates with intelligent reasoning"""
        confirmed = []
        
        # Initialize AI agent core
        async with AIAgentCore() as ai_core:
            self.logger.info(f"   🤖 Sending {len(batch)} candidate pairs to Gemma3:4b for intelligent analysis...")
            
            for i, candidate in enumerate(batch):
                try:
                    # Use AI to analyze if contacts are duplicates
                    comparison = await ai_core.check_duplicate_contacts(
                        candidate.contact1_data, 
                        candidate.contact2_data
                    )
                    
                    # Only merge if LLM has high confidence (>0.7)
                    if comparison.is_duplicate and comparison.confidence > 0.7:
                        confirmed.append((
                            candidate.contact1_idx, 
                            candidate.contact2_idx,
                            comparison.merged_data or {}
                        ))
                        self.logger.info(f"   ✅ LLM CONFIRMED merge (confidence: {comparison.confidence:.2f}): {comparison.reasoning}")
                    else:
                        self.logger.debug(f"   ❌ LLM rejected (confidence: {comparison.confidence:.2f}): {comparison.reasoning}")
                        
                except Exception as e:
                    self.logger.warning(f"   ⚠️ LLM analysis failed for candidate {i+1}: {e}")
                    continue
        
        return confirmed
    
    def _conservative_merge_decision(self, candidate: DuplicateCandidate) -> bool:
        """Conservative decision logic for merging contacts"""
        c1, c2 = candidate.contact1_data, candidate.contact2_data
        score = candidate.similarity_score
        reason = candidate.match_reason
        
        # Email exact match - high confidence
        if reason == 'email_exact_match' and score >= 98.0:
            return True
        
        # Phone exact match - high confidence  
        if reason == 'phone_exact_match' and score >= 96.0:
            return True
        
        # Composite matching - balanced approach
        if reason == 'composite_match':
            # Require reasonable similarity
            if score < self.FINAL_CONFIDENCE_THRESHOLD:
                return False
                
            # Lower threshold if company matches well
            company1 = str(c1.get('Current Company', '')).lower().strip()
            company2 = str(c2.get('Current Company', '')).lower().strip()
            if company1 and company2 and fuzz.ratio(company1, company2) > 90:
                if score >= (self.FINAL_CONFIDENCE_THRESHOLD - 5):  # Allow 5% lower threshold
                    pass  # Continue with validation
                else:
                    return False
                
            # Additional validation checks
            fname1 = str(c1.get('First Name', '')).lower().strip()
            fname2 = str(c2.get('First Name', '')).lower().strip()
            lname1 = str(c1.get('Last Name', '')).lower().strip() 
            lname2 = str(c2.get('Last Name', '')).lower().strip()
            
            # Both first and last names must have reasonable similarity
            if fname1 and fname2 and lname1 and lname2:
                fname_sim = fuzz.ratio(fname1, fname2)
                lname_sim = fuzz.ratio(lname1, lname2)
                
                # Balanced: require reasonable name similarity
                if fname_sim < 75 or lname_sim < 75:
                    return False
                
                # If names are very different, reject
                if fname_sim < 65 and lname_sim < 65:
                    return False
            
            return True
        
        return False
    
    def _create_llm_prompt(self, batch: List[DuplicateCandidate]) -> str:
        """Create LLM prompt for batch confirmation"""
        prompt = """You are a contact deduplication expert. Review these potential duplicate pairs and decide which should be merged.

For each pair, respond with YES (merge) or NO (keep separate).

Pairs to review:
"""
        
        for i, candidate in enumerate(batch):
            c1, c2 = candidate.contact1_data, candidate.contact2_data
            prompt += f"""
{i+1}. Contact A: {c1.get('First Name', '')} {c1.get('Last Name', '')} | {c1.get('Email', '')} | {c1.get('Current Company', '')}
   Contact B: {c2.get('First Name', '')} {c2.get('Last Name', '')} | {c2.get('Email', '')} | {c2.get('Current Company', '')}
   Similarity: {candidate.similarity_score:.1f}% ({candidate.match_reason})
"""
        
        return prompt
    
    def _merge_llm_confirmed_duplicates(self, df: pd.DataFrame, confirmed_merges: List[Tuple[int, int, Dict]]) -> pd.DataFrame:
        """Step 3: Merge LLM-confirmed duplicates with intelligent data combination"""
        if not confirmed_merges:
            self.logger.info("   ℹ️ No LLM-confirmed merges, returning original data")
            return df.copy()
        
        # Create a copy to work with
        result_df = df.copy()
        indices_to_remove = set()
        
        for idx1, idx2, llm_merged_data in confirmed_merges:
            if idx1 in indices_to_remove or idx2 in indices_to_remove:
                continue  # Already processed
            
            # Use LLM-provided merged data if available, otherwise merge intelligently
            if llm_merged_data:
                merged_contact = llm_merged_data
                self.logger.debug(f"   🤖 Using LLM-provided merged data for contacts {idx1} and {idx2}")
            else:
                merged_contact = self._merge_two_contacts_intelligently(result_df.iloc[idx1], result_df.iloc[idx2])
                self.logger.debug(f"   🔗 Intelligently merged contacts {idx1} and {idx2}")
            
            # Update the first contact with merged data
            for column in result_df.columns:
                if column in merged_contact:
                    result_df.at[idx1, column] = merged_contact[column]
                elif pd.isna(result_df.at[idx1, column]) or result_df.at[idx1, column] == '':
                    # Fill missing data from second contact
                    result_df.at[idx1, column] = result_df.at[idx2, column]
            
            # Mark second contact for removal
            indices_to_remove.add(idx2)
        
        # Remove duplicate contacts
        result_df = result_df.drop(indices_to_remove).reset_index(drop=True)
        
        return result_df
    
    def _merge_two_contacts_intelligently(self, contact1: pd.Series, contact2: pd.Series) -> Dict[str, Any]:
        """Intelligently merge two contacts with priority rules"""
        merged = {}
        
        for column in contact1.index:
            val1 = contact1[column]
            val2 = contact2[column]
            
            # Handle missing values
            if pd.isna(val1) or val1 == '':
                merged[column] = val2
            elif pd.isna(val2) or val2 == '':
                merged[column] = val1
            else:
                # Apply intelligent merging rules by field type
                if column == 'Email':
                    # Prefer corporate email over personal/generic
                    if '@' in str(val1) and '@' in str(val2):
                        if any(domain in str(val1).lower() for domain in ['.com', '.org', '.net']) and \
                           not any(domain in str(val1).lower() for domain in ['gmail', 'yahoo', 'hotmail']):
                            merged[column] = val1  # Corporate email
                        elif any(domain in str(val2).lower() for domain in ['.com', '.org', '.net']) and \
                             not any(domain in str(val2).lower() for domain in ['gmail', 'yahoo', 'hotmail']):
                            merged[column] = val2  # Corporate email  
                        else:
                            merged[column] = val1  # Default to first
                    else:
                        merged[column] = val1
                elif column in ['First Name', 'Last Name']:
                    # Prefer more complete names
                    if len(str(val1)) > len(str(val2)):
                        merged[column] = val1
                    else:
                        merged[column] = val2
                elif column == 'Current Company':
                    # Prefer more specific company names
                    if len(str(val1)) > len(str(val2)):
                        merged[column] = val1
                    else:
                        merged[column] = val2
                elif column == 'Designation / Role':
                    # Prefer more senior/specific titles
                    senior_keywords = ['director', 'vp', 'president', 'chief', 'head', 'lead']
                    val1_senior = any(keyword in str(val1).lower() for keyword in senior_keywords)
                    val2_senior = any(keyword in str(val2).lower() for keyword in senior_keywords)
                    
                    if val1_senior and not val2_senior:
                        merged[column] = val1
                    elif val2_senior and not val1_senior:
                        merged[column] = val2
                    else:
                        # Choose longer/more descriptive title
                        merged[column] = val1 if len(str(val1)) >= len(str(val2)) else val2
                else:
                    # Default: choose longer/more complete value
                    merged[column] = val1 if len(str(val1)) >= len(str(val2)) else val2
        
        return merged
    
    def _get_input_file(self, context: PipelineContext) -> Path:
        """Get input file from Agent 3"""
        return Path("output") / "agent_3_data_consolidator" / "consolidated_contacts.csv"
    
    def _get_output_directory(self) -> Path:
        """Get output directory for Agent 4"""
        output_dir = Path("output") / "agent_4_smart_deduplicator"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
    
    def _save_statistics(self, output_dir: Path):
        """Save processing statistics"""
        stats_file = output_dir / "deduplication_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2)
        self.logger.info(f"📊 Statistics saved to: {stats_file}")


# Create task alias for compatibility
SmartDeduplicatorAgentTask = SimpleContactDeduplicator
IntelligentContactMerger = SimpleContactDeduplicator
