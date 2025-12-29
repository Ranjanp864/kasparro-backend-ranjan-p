"""
CoinGecko API ETL Pipeline - FIXED VERSION
"""
import aiohttp
import asyncio
from datetime import datetime
from typing import List, Dict
from pydantic import BaseModel, ValidationError
from decimal import Decimal

from ingestion.base_pipeline import BasePipeline
from core.config import settings
from core.logger import setup_logger

logger = setup_logger(__name__)

class CoinGeckoData(BaseModel):
    """Validation model for CoinGecko data"""
    id: str
    symbol: str
    name: str
    current_price: float
    market_cap: float
    total_volume: float
    price_change_percentage_24h: float
    market_cap_rank: int
    
    class Config:
        extra = "allow"

class CoinGeckoPipeline(BasePipeline):
    """ETL pipeline for CoinGecko API"""
    
    SOURCE_NAME = "coingecko"
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self, db):
        super().__init__(db)
        self.api_key = settings.COINGECKO_API_KEY
        self.rate_limit = settings.COINGECKO_RATE_LIMIT
    
    async def extract(self) -> List[Dict]:
        """Extract data from CoinGecko API"""
        logger.info(f"Extracting data from {self.SOURCE_NAME}")
        
        headers = {}
        if self.api_key:
            headers['x-cg-demo-api-key'] = self.api_key
        
        all_data = []
        retry_count = 0
        
        while retry_count < settings.ETL_MAX_RETRIES:
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"{self.BASE_URL}/coins/markets"
                    params = {
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": 100,
                        "page": 1,
                        "sparkline": False,
                        "locale": "en"  # Added to ensure consistent response
                    }
                    
                    # Apply rate limiting
                    if settings.RATE_LIMIT_ENABLED:
                        await asyncio.sleep(60 / self.rate_limit)
                    
                    async with session.get(url, headers=headers, params=params) as response:
                        if response.status == 429:
                            logger.warning("Rate limited, applying exponential backoff")
                            delay = await self.calculate_backoff(retry_count)
                            await asyncio.sleep(delay)
                            retry_count += 1
                            continue
                        
                        response.raise_for_status()
                        data = await response.json()
                        all_data = data
                        
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
        """Transform CoinGecko data to unified schema"""
        logger.info(f"Transforming {len(raw_data)} records from {self.SOURCE_NAME}")
        
        # Detect schema drift
        if settings.SCHEMA_DRIFT_ENABLED and raw_data:
            await self.detect_schema_drift(raw_data[0])
        
        normalized_records = []
        
        for item in raw_data:
            try:
                # Handle None values and type conversions
                # FIXED: Convert None to 0 for numeric fields
                current_price = float(item.get('current_price') or 0)
                market_cap = float(item.get('market_cap') or 0)
                total_volume = float(item.get('total_volume') or 0)
                price_change_24h = float(item.get('price_change_percentage_24h') or 0)
                market_cap_rank = int(item.get('market_cap_rank') or 0)
                
                # Validate with Pydantic
                validated = CoinGeckoData(
                    id=item.get('id', ''),
                    symbol=item.get('symbol', ''),
                    name=item.get('name', ''),
                    current_price=current_price,
                    market_cap=market_cap,
                    total_volume=total_volume,
                    price_change_percentage_24h=price_change_24h,
                    market_cap_rank=market_cap_rank
                )
                
                normalized = {
                    "source": self.SOURCE_NAME,
                    "symbol": validated.symbol.upper(),
                    "name": validated.name,
                    "price_usd": Decimal(str(validated.current_price)),
                    "market_cap_usd": Decimal(str(validated.market_cap)),
                    "volume_24h_usd": Decimal(str(validated.total_volume)),
                    "percent_change_24h": Decimal(str(validated.price_change_percentage_24h)),
                    "rank": validated.market_cap_rank,
                    "last_updated": datetime.now(),
                    "raw_data": item
                }
                
                normalized_records.append(normalized)
                
            except ValidationError as e:
                logger.warning(f"Validation error for record {item.get('id', 'unknown')}: {e}")
                continue
            except Exception as e:
                logger.error(f"Transform error for record {item.get('id', 'unknown')}: {e}")
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
            "symbol": str,
            "name": str,
            "current_price": (int, float),
            "market_cap": (int, float),
            "total_volume": (int, float),
            "price_change_percentage_24h": (int, float),
            "market_cap_rank": int
        }
