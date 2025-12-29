"""
CoinPaprika API ETL Pipeline with rate limiting and backoff
"""
import aiohttp
import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel, Field, ValidationError
from decimal import Decimal

from ingestion.base_pipeline import BasePipeline
from core.config import settings
from core.logger import setup_logger

logger = setup_logger(__name__)

class CoinPaprikaData(BaseModel):
    """Validation model for CoinPaprika data"""
    id: str
    name: str
    symbol: str
    rank: int
    quotes: Dict
    
    class Config:
        extra = "allow"

class CoinPaprikaPipeline(BasePipeline):
    """ETL pipeline for CoinPaprika API"""
    
    SOURCE_NAME = "coinpaprika"
    BASE_URL = "https://api.coinpaprika.com/v1"
    
    def __init__(self, db):
        super().__init__(db)
        self.api_key = settings.COINPAPRIKA_API_KEY
        self.rate_limit = settings.COINPAPRIKA_RATE_LIMIT
    
    async def extract(self) -> List[Dict]:
        """Extract data from CoinPaprika API"""
        logger.info(f"Extracting data from {self.SOURCE_NAME}")
        
        # Check for checkpoint
        checkpoint = await self.db.get_last_checkpoint(self.SOURCE_NAME)
        if checkpoint:
            logger.info(f"Resuming from checkpoint: {checkpoint['records_processed']} records")
        
        headers = {}
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
        
        all_data = []
        retry_count = 0
        
        while retry_count < settings.ETL_MAX_RETRIES:
            try:
                async with aiohttp.ClientSession() as session:
                    # Get ticker data
                    url = f"{self.BASE_URL}/tickers"
                    
                    # Apply rate limiting
                    if settings.RATE_LIMIT_ENABLED:
                        await asyncio.sleep(60 / self.rate_limit)
                    
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # Rate limited
                            logger.warning("Rate limited, applying exponential backoff")
                            delay = await self.calculate_backoff(retry_count)
                            await asyncio.sleep(delay)
                            retry_count += 1
                            continue
                        
                        response.raise_for_status()
                        data = await response.json()
                        
                        # Limit to top 100 for demo
                        all_data = data[:100]
                        logger.info(f"Extracted {len(all_data)} records from {self.SOURCE_NAME}")
                        
                        return all_data
            
            except aiohttp.ClientError as e:
                logger.error(f"HTTP error: {e}")
                retry_count += 1
                if retry_count < settings.ETL_MAX_RETRIES:
                    delay = await self.calculate_backoff(retry_count)
                    await asyncio.sleep(delay)
                else:
                    raise
        
        raise Exception(f"Failed to extract data after {settings.ETL_MAX_RETRIES} retries")
    
    async def transform(self, raw_data: List[Dict]) -> List[Dict]:
        """Transform CoinPaprika data to unified schema"""
        logger.info(f"Transforming {len(raw_data)} records from {self.SOURCE_NAME}")
        
        # Detect schema drift
        if settings.SCHEMA_DRIFT_ENABLED and raw_data:
            await self.detect_schema_drift(raw_data[0])
        
        normalized_records = []
        
        for item in raw_data:
            try:
                # Validate with Pydantic
                validated = CoinPaprikaData(**item)
                
                # Extract USD quote
                usd_quote = validated.quotes.get('USD', {})
                
                normalized = {
                    "source": self.SOURCE_NAME,
                    "symbol": validated.symbol,
                    "name": validated.name,
                    "price_usd": Decimal(str(usd_quote.get('price', 0))),
                    "market_cap_usd": Decimal(str(usd_quote.get('market_cap', 0))),
                    "volume_24h_usd": Decimal(str(usd_quote.get('volume_24h', 0))),
                    "percent_change_24h": Decimal(str(usd_quote.get('percent_change_24h', 0))),
                    "rank": validated.rank,
                    "last_updated": datetime.now(),
                    "raw_data": item
                }
                
                normalized_records.append(normalized)
                
            except ValidationError as e:
                logger.warning(f"Validation error for record: {e}")
                continue
            except Exception as e:
                logger.error(f"Transform error: {e}")
                continue
        
        logger.info(f"Transformed {len(normalized_records)} valid records")
        return normalized_records
    
    async def load(self, normalized_data: List[Dict]):
        """Load data into database with checkpointing"""
        logger.info(f"Loading {len(normalized_data)} records to database")
        
        # Save raw data first
        await self.db.save_raw_data(
            source=self.SOURCE_NAME,
            data=[r['raw_data'] for r in normalized_data],
            source_ids=[r['symbol'] for r in normalized_data]
        )
        
        # Batch load with checkpointing
        batch_size = settings.CHECKPOINT_INTERVAL
        total_loaded = 0
        
        for i in range(0, len(normalized_data), batch_size):
            batch = normalized_data[i:i + batch_size]
            
            try:
                await self.db.save_normalized_data(batch)
                total_loaded += len(batch)
                
                # Save checkpoint
                if settings.CHECKPOINT_ENABLED:
                    await self.db.save_checkpoint(
                        source=self.SOURCE_NAME,
                        checkpoint_data={"last_index": i + batch_size},
                        records_processed=total_loaded
                    )
                
                logger.info(f"Loaded batch: {total_loaded}/{len(normalized_data)} records")
                
            except Exception as e:
                logger.error(f"Error loading batch: {e}")
                raise
        
        # Mark checkpoint as completed
        if settings.CHECKPOINT_ENABLED:
            await self.db.mark_checkpoint_completed(self.SOURCE_NAME)
        
        logger.info(f"Successfully loaded {total_loaded} records")
    
    def get_expected_schema(self) -> Dict:
        """Get expected schema for drift detection"""
        return {
            "id": str,
            "name": str,
            "symbol": str,
            "rank": int,
            "quotes": dict
        }