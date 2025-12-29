"""
CSV File ETL Pipeline
"""
import csv
import asyncio
from datetime import datetime
from typing import List, Dict
from pydantic import BaseModel, ValidationError
from decimal import Decimal
import os

from ingestion.base_pipeline import BasePipeline
from core.config import settings
from core.logger import setup_logger

logger = setup_logger(__name__)

class CSVCryptoData(BaseModel):
    """Validation model for CSV data"""
    symbol: str
    name: str
    price: float
    market_cap: float
    volume_24h: float
    percent_change_24h: float
    rank: int
    
    class Config:
        extra = "allow"

class CSVPipeline(BasePipeline):
    """ETL pipeline for CSV file"""
    
    SOURCE_NAME = "csv"
    
    def __init__(self, db):
        super().__init__(db)
        self.csv_path = settings.CSV_FILE_PATH
    
    async def extract(self) -> List[Dict]:
        """Extract data from CSV file"""
        logger.info(f"Extracting data from CSV file: {self.csv_path}")
        
        # Check if file exists, if not create sample data
        if not os.path.exists(self.csv_path):
            logger.warning(f"CSV file not found, creating sample data")
            await self.create_sample_csv()
        
        all_data = []
        
        try:
            # Read CSV file
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, self._read_csv)
            all_data = data
            
            logger.info(f"Extracted {len(all_data)} records from CSV")
            return all_data
        
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            raise
    
    def _read_csv(self) -> List[Dict]:
        """Synchronous CSV reading"""
        data = []
        with open(self.csv_path, 'r') as file:
            # Strip whitespace from headers
            reader = csv.DictReader(file)
            reader.fieldnames = [field.strip() for field in reader.fieldnames]
            
            for row in reader:
                # Strip whitespace from values
                cleaned_row = {k.strip(): v.strip() for k, v in row.items()}
                data.append(cleaned_row)
        
        return data
    
    async def create_sample_csv(self):
        """Create sample CSV file for testing"""
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        
        sample_data = [
            {"symbol": "BTC", "name": "Bitcoin", "price": "45000.50", "market_cap": "850000000000", "volume_24h": "25000000000", "percent_change_24h": "2.5", "rank": "1"},
            {"symbol": "ETH", "name": "Ethereum", "price": "3000.25", "market_cap": "360000000000", "volume_24h": "15000000000", "percent_change_24h": "3.2", "rank": "2"},
            {"symbol": "BNB", "name": "Binance Coin", "price": "350.75", "market_cap": "55000000000", "volume_24h": "1200000000", "percent_change_24h": "1.8", "rank": "3"},
            {"symbol": "SOL", "name": "Solana", "price": "110.30", "market_cap": "45000000000", "volume_24h": "2500000000", "percent_change_24h": "-1.2", "rank": "4"},
            {"symbol": "ADA", "name": "Cardano", "price": "0.55", "market_cap": "19000000000", "volume_24h": "450000000", "percent_change_24h": "0.8", "rank": "5"}
        ]
        
        with open(self.csv_path, 'w', newline='') as file:
            fieldnames = ["symbol", "name", "price", "market_cap", "volume_24h", "percent_change_24h", "rank"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(sample_data)
        
        logger.info(f"Created sample CSV file at {self.csv_path}")
    
    async def transform(self, raw_data: List[Dict]) -> List[Dict]:
        """Transform CSV data to unified schema"""
        logger.info(f"Transforming {len(raw_data)} records from CSV")
        
        # Detect schema drift
        if settings.SCHEMA_DRIFT_ENABLED and raw_data:
            await self.detect_schema_drift(raw_data[0])
        
        normalized_records = []
        
        for item in raw_data:
            try:
                # Validate with Pydantic
                validated = CSVCryptoData(**item)
                
                normalized = {
                    "source": self.SOURCE_NAME,
                    "symbol": validated.symbol.upper(),
                    "name": validated.name,
                    "price_usd": Decimal(str(validated.price)),
                    "market_cap_usd": Decimal(str(validated.market_cap)),
                    "volume_24h_usd": Decimal(str(validated.volume_24h)),
                    "percent_change_24h": Decimal(str(validated.percent_change_24h)),
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
        """Load data into database"""
        logger.info(f"Loading {len(normalized_data)} records to database")
        
        # Save raw data first
        await self.db.save_raw_data(
            source=self.SOURCE_NAME,
            data=[r['raw_data'] for r in normalized_data],
            source_ids=[r['symbol'] for r in normalized_data]
        )
        
        # Load normalized data
        await self.db.save_normalized_data(normalized_data)
        
        logger.info(f"Successfully loaded {len(normalized_data)} records")
    
    def get_expected_schema(self) -> Dict:
        """Get expected schema for drift detection"""
        return {
            "symbol": str,
            "name": str,
            "price": (int, float, str),
            "market_cap": (int, float, str),
            "volume_24h": (int, float, str),
            "percent_change_24h": (int, float, str),
            "rank": (int, str)
        }