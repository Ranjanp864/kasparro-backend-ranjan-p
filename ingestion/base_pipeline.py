"""
Base ETL Pipeline with common functionality
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime
from difflib import SequenceMatcher

from services.database import DatabaseService
from core.config import settings
from core.logger import setup_logger

logger = setup_logger(__name__)

class BasePipeline(ABC):
    """Abstract base class for ETL pipelines"""
    
    SOURCE_NAME = "base"
    
    def __init__(self, db: DatabaseService):
        self.db = db
    
    @abstractmethod
    async def extract(self) -> List[Dict]:
        """Extract data from source"""
        pass
    
    @abstractmethod
    async def transform(self, raw_data: List[Dict]) -> List[Dict]:
        """Transform data to unified schema"""
        pass
    
    @abstractmethod
    async def load(self, normalized_data: List[Dict]):
        """Load data into database"""
        pass
    
    @abstractmethod
    def get_expected_schema(self) -> Dict:
        """Get expected schema for this source"""
        pass
    
    async def run(self) -> Dict[str, Any]:
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        
        try:
            # Extract
            raw_data = await self.extract()
            
            if not raw_data:
                logger.warning(f"No data extracted from {self.SOURCE_NAME}")
                return {
                    "status": "success",
                    "records_processed": 0,
                    "duration": 0
                }
            
            # Transform
            normalized_data = await self.transform(raw_data)
            
            # Load
            await self.load(normalized_data)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Log successful run
            await self.db.log_run(
                source=self.SOURCE_NAME,
                status="success",
                records=len(normalized_data),
                start_time=start_time,
                end_time=end_time,
                metadata={"raw_count": len(raw_data)}
            )
            
            return {
                "status": "success",
                "records_processed": len(normalized_data),
                "duration": duration
            }
        
        except Exception as e:
            end_time = datetime.now()
            logger.error(f"Pipeline failed for {self.SOURCE_NAME}: {e}")
            
            # Log failed run
            await self.db.log_run(
                source=self.SOURCE_NAME,
                status="failed",
                records=0,
                start_time=start_time,
                end_time=end_time,
                error=str(e)
            )
            
            raise
    
    async def calculate_backoff(self, retry_count: int) -> float:
        """Calculate exponential backoff delay"""
        delay = min(
            settings.BACKOFF_BASE_DELAY * (settings.BACKOFF_FACTOR ** retry_count),
            settings.BACKOFF_MAX_DELAY
        )
        logger.info(f"Applying backoff: {delay:.2f}s (retry {retry_count})")
        return delay
    
    async def detect_schema_drift(self, sample_data: Dict):
        """Detect schema drift using fuzzy matching"""
        expected_schema = self.get_expected_schema()
        actual_keys = set(sample_data.keys())
        expected_keys = set(expected_schema.keys())
        
        # Calculate confidence score
        missing_keys = expected_keys - actual_keys
        extra_keys = actual_keys - expected_keys
        
        # Fuzzy match for renamed fields
        warnings = []
        fuzzy_matches = {}
        
        for missing_key in missing_keys:
            best_match = None
            best_score = 0
            
            for actual_key in extra_keys:
                score = SequenceMatcher(None, missing_key, actual_key).ratio()
                if score > best_score:
                    best_score = score
                    best_match = actual_key
            
            if best_score >= settings.SCHEMA_DRIFT_THRESHOLD:
                fuzzy_matches[missing_key] = (best_match, best_score)
                warnings.append(
                    f"Possible field rename: '{missing_key}' -> '{best_match}' (confidence: {best_score:.2f})"
                )
        
        # Check for type mismatches
        for key in expected_keys & actual_keys:
            expected_type = expected_schema[key]
            actual_value = sample_data[key]
            
            if actual_value is not None:
                if isinstance(expected_type, tuple):
                    type_match = any(isinstance(actual_value, t) for t in expected_type)
                else:
                    type_match = isinstance(actual_value, expected_type)
                
                if not type_match:
                    warnings.append(
                        f"Type mismatch for '{key}': expected {expected_type}, got {type(actual_value).__name__}"
                    )
        
        # Calculate overall confidence
        total_expected = len(expected_keys)
        matched = len(expected_keys & actual_keys) + len(fuzzy_matches)
        confidence = matched / total_expected if total_expected > 0 else 1.0
        
        # Log drift if detected
        if warnings:
            logger.warning(f"Schema drift detected for {self.SOURCE_NAME}")
            for warning in warnings:
                logger.warning(f"  - {warning}")
            
            await self.db.log_schema_drift(
                source=self.SOURCE_NAME,
                expected=expected_schema,
                actual={k: type(v).__name__ for k, v in sample_data.items()},
                confidence=confidence,
                warnings=warnings
            )