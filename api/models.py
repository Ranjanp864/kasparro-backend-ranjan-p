"""
Pydantic models for API responses
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime
from decimal import Decimal

class CryptoData(BaseModel):
    """Cryptocurrency data model"""
    id: int
    source: str
    symbol: str
    name: Optional[str] = None
    price_usd: Optional[Decimal] = None
    market_cap_usd: Optional[Decimal] = None
    volume_24h_usd: Optional[Decimal] = None
    percent_change_24h: Optional[Decimal] = None
    rank: Optional[int] = None
    last_updated: Optional[datetime] = None
    ingested_at: datetime
    
    class Config:
        from_attributes = True

class DataResponse(BaseModel):
    """Response model for /data endpoint"""
    data: List[Dict[str, Any]]
    total_records: int
    page: int
    page_size: int
    total_pages: int
    filters: Dict[str, Any]
    request_id: str
    api_latency_ms: float

class HealthResponse(BaseModel):
    """Response model for /health endpoint"""
    status: str
    database_connected: bool
    etl_last_run: Optional[str] = None
    etl_last_success: Optional[str] = None
    etl_status: str
    request_id: str
    api_latency_ms: float

class StatsResponse(BaseModel):
    """Response model for /stats endpoint"""
    total_records_processed: int
    records_by_source: Dict[str, int]
    last_run_duration_seconds: float
    last_success_timestamp: Optional[str] = None
    last_failure_timestamp: Optional[str] = None
    total_runs: int
    successful_runs: int
    failed_runs: int
    request_id: str
    api_latency_ms: float

class ETLRun(BaseModel):
    """ETL run model"""
    id: int
    source: str
    status: str
    records_processed: int
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[Decimal] = None
    error_message: Optional[str] = None

class RunsResponse(BaseModel):
    """Response model for /runs endpoint"""
    runs: List[Dict[str, Any]]
    count: int
    request_id: str
    api_latency_ms: float

class MetricsResponse(BaseModel):
    """Response model for /metrics endpoint"""
    metrics: str
    timestamp: float
    api_latency_ms: float