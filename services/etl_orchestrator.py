"""
ETL Orchestrator to manage all data ingestion pipelines
"""
from datetime import datetime
from typing import Dict, Any
import asyncio

from ingestion.coinpaprika_pipeline import CoinPaprikaPipeline
from ingestion.coingecko_pipeline import CoinGeckoPipeline
from ingestion.csv_pipeline import CSVPipeline
from services.database import DatabaseService
from core.logger import setup_logger

logger = setup_logger(__name__)

class ETLOrchestrator:
    """Orchestrates all ETL pipelines"""
    
    def __init__(self, db: DatabaseService):
        self.db = db
        self.pipelines = {
            "coinpaprika": CoinPaprikaPipeline(db),
            "coingecko": CoinGeckoPipeline(db),
            "csv": CSVPipeline(db)
        }
    
    async def run_full_etl(self) -> Dict[str, Any]:
        """Run all ETL pipelines"""
        logger.info("🚀 Starting full ETL run")
        start_time = datetime.now()
        
        results = {}
        total_records = 0
        failed_sources = []
        
        for source_name, pipeline in self.pipelines.items():
            try:
                logger.info(f"Running pipeline for {source_name}")
                result = await pipeline.run()
                results[source_name] = result
                total_records += result.get('records_processed', 0)
                logger.info(f"✅ {source_name} completed: {result.get('records_processed', 0)} records")
            except Exception as e:
                logger.error(f"❌ {source_name} failed: {e}")
                results[source_name] = {
                    "status": "failed",
                    "error": str(e),
                    "records_processed": 0
                }
                failed_sources.append(source_name)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        overall_status = "success" if not failed_sources else "partial_failure"
        if len(failed_sources) == len(self.pipelines):
            overall_status = "failed"
        
        summary = {
            "status": overall_status,
            "total_records": total_records,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "sources": results,
            "failed_sources": failed_sources
        }
        
        logger.info(f"ETL run completed: {overall_status}, {total_records} total records in {duration:.2f}s")
        return summary
    
    async def run_single_source(self, source: str) -> Dict[str, Any]:
        """Run ETL for a single source"""
        if source not in self.pipelines:
            raise ValueError(f"Unknown source: {source}")
        
        logger.info(f"Running ETL for {source}")
        start_time = datetime.now()
        
        try:
            result = await self.pipelines[source].run()
            end_time = datetime.now()
            
            await self.db.log_run(
                source=source,
                status="success",
                records=result.get('records_processed', 0),
                start_time=start_time,
                end_time=end_time,
                metadata=result
            )
            
            return result
        except Exception as e:
            end_time = datetime.now()
            logger.error(f"ETL failed for {source}: {e}")
            
            await self.db.log_run(
                source=source,
                status="failed",
                records=0,
                start_time=start_time,
                end_time=end_time,
                error=str(e)
            )
            
            raise