"""
Kasparro Backend & ETL System - Main Application
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import Optional, List
import time
import uuid

from api.models import (
    DataResponse, 
    HealthResponse, 
    StatsResponse, 
    RunsResponse,
    MetricsResponse
)
from services.database import DatabaseService, get_db
from services.etl_orchestrator import ETLOrchestrator
from core.config import settings
from core.logger import setup_logger

logger = setup_logger(__name__)

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("🚀 Starting Kasparro Backend & ETL System")
    
    # Initialize database
    db = DatabaseService()
    await db.initialize()
    
    # Run initial ETL if configured
    if settings.RUN_ETL_ON_STARTUP:
        logger.info("Running initial ETL on startup...")
        orchestrator = ETLOrchestrator(db)
        await orchestrator.run_full_etl()
    
    yield
    
    logger.info("Shutting down application")

app = FastAPI(
    title="Kasparro Backend & ETL System",
    description="Production-grade ETL pipeline with cryptocurrency data ingestion",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Kasparro Backend & ETL System",
        "version": "1.0.0",
        "endpoints": [
            "/data",
            "/health",
            "/stats",
            "/runs",
            "/metrics"
        ]
    }

@app.get("/health", response_model=HealthResponse)
async def health_check(db: DatabaseService = Depends(get_db)):
    """
    Health check endpoint
    Reports DB connectivity and ETL last-run status
    """
    start_time = time.time()
    request_id = str(uuid.uuid4())
    
    try:
        # Check database connectivity
        db_healthy = await db.check_health()
        
        # Get ETL status
        etl_status = await db.get_etl_status()
        
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        return HealthResponse(
            status="healthy" if db_healthy else "unhealthy",
            database_connected=db_healthy,
            etl_last_run=etl_status.get("last_run"),
            etl_last_success=etl_status.get("last_success"),
            etl_status=etl_status.get("status", "unknown"),
            request_id=request_id,
            api_latency_ms=latency_ms
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        latency_ms = round((time.time() - start_time) * 1000, 2)
        return HealthResponse(
            status="unhealthy",
            database_connected=False,
            etl_status="error",
            request_id=request_id,
            api_latency_ms=latency_ms
        )

@app.get("/data", response_model=DataResponse)
async def get_data(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Items per page"),
    source: Optional[str] = Query(None, description="Filter by source (coinpaprika, coingecko, csv)"),
    symbol: Optional[str] = Query(None, description="Filter by cryptocurrency symbol"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    db: DatabaseService = Depends(get_db)
):
    """
    Get cryptocurrency data with pagination and filtering
    """
    start_time = time.time()
    request_id = str(uuid.uuid4())
    
    try:
        # Build filters
        filters = {}
        if source:
            filters['source'] = source
        if symbol:
            filters['symbol'] = symbol.upper()
        if min_price is not None:
            filters['min_price'] = min_price
        if max_price is not None:
            filters['max_price'] = max_price
        
        # Get data from database
        result = await db.get_data(
            page=page,
            page_size=page_size,
            filters=filters
        )
        
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        return DataResponse(
            data=result['data'],
            total_records=result['total'],
            page=page,
            page_size=page_size,
            total_pages=result['total_pages'],
            filters=filters,
            request_id=request_id,
            api_latency_ms=latency_ms
        )
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats", response_model=StatsResponse)
async def get_stats(db: DatabaseService = Depends(get_db)):
    """
    Get ETL statistics and summaries
    """
    start_time = time.time()
    request_id = str(uuid.uuid4())
    
    try:
        stats = await db.get_etl_stats()
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        return StatsResponse(
            total_records_processed=stats.get('total_records', 0),
            records_by_source=stats.get('by_source', {}),
            last_run_duration_seconds=stats.get('last_duration', 0),
            last_success_timestamp=stats.get('last_success'),
            last_failure_timestamp=stats.get('last_failure'),
            total_runs=stats.get('total_runs', 0),
            successful_runs=stats.get('successful_runs', 0),
            failed_runs=stats.get('failed_runs', 0),
            request_id=request_id,
            api_latency_ms=latency_ms
        )
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/runs", response_model=RunsResponse)
async def get_runs(
    limit: int = Query(10, ge=1, le=100, description="Number of runs to return"),
    db: DatabaseService = Depends(get_db)
):
    """
    Get recent ETL run history
    """
    start_time = time.time()
    request_id = str(uuid.uuid4())
    
    try:
        runs = await db.get_recent_runs(limit=limit)
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        return RunsResponse(
            runs=runs,
            count=len(runs),
            request_id=request_id,
            api_latency_ms=latency_ms
        )
    except Exception as e:
        logger.error(f"Error fetching runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics(db: DatabaseService = Depends(get_db)):
    """
    Get system metrics in Prometheus format
    """
    start_time = time.time()
    
    try:
        stats = await db.get_etl_stats()
        
        # Generate Prometheus-style metrics
        metrics = []
        metrics.append(f"# HELP etl_total_records Total records processed")
        metrics.append(f"# TYPE etl_total_records gauge")
        metrics.append(f"etl_total_records {stats.get('total_records', 0)}")
        
        metrics.append(f"# HELP etl_total_runs Total ETL runs")
        metrics.append(f"# TYPE etl_total_runs counter")
        metrics.append(f"etl_total_runs {stats.get('total_runs', 0)}")
        
        metrics.append(f"# HELP etl_successful_runs Successful ETL runs")
        metrics.append(f"# TYPE etl_successful_runs counter")
        metrics.append(f"etl_successful_runs {stats.get('successful_runs', 0)}")
        
        metrics.append(f"# HELP etl_failed_runs Failed ETL runs")
        metrics.append(f"# TYPE etl_failed_runs counter")
        metrics.append(f"etl_failed_runs {stats.get('failed_runs', 0)}")
        
        for source, count in stats.get('by_source', {}).items():
            metrics.append(f'etl_records_by_source{{source="{source}"}} {count}')
        
        latency_ms = round((time.time() - start_time) * 1000, 2)
        
        return MetricsResponse(
            metrics="\n".join(metrics),
            timestamp=time.time(),
            api_latency_ms=latency_ms
        )
    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/trigger-etl")
async def trigger_etl(db: DatabaseService = Depends(get_db)):
    """
    Manually trigger ETL process (useful for testing)
    """
    try:
        orchestrator = ETLOrchestrator(db)
        result = await orchestrator.run_full_etl()
        return {"status": "completed", "result": result}
    except Exception as e:
        logger.error(f"ETL trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )