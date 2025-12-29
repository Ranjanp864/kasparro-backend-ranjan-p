"""
Database service for managing PostgreSQL operations
"""
import asyncpg
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
from core.config import settings
from core.logger import setup_logger

logger = setup_logger(__name__)

class DatabaseService:
    """Database service for PostgreSQL operations"""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self):
        """Initialize database connection pool and create tables"""
        try:
            self.pool = await asyncpg.create_pool(
                settings.DATABASE_URL,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            logger.info("Database pool created successfully")
            await self.create_tables()
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def create_tables(self):
        """Create all required database tables"""
        async with self.pool.acquire() as conn:
            # Raw data tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_coinpaprika (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    source_id VARCHAR(255)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_coingecko (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    source_id VARCHAR(255)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_csv (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    source_id VARCHAR(255)
                )
            """)
            
            # Unified normalized table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS crypto_data (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    name VARCHAR(255),
                    price_usd DECIMAL(20, 8),
                    market_cap_usd DECIMAL(20, 2),
                    volume_24h_usd DECIMAL(20, 2),
                    percent_change_24h DECIMAL(10, 4),
                    rank INTEGER,
                    last_updated TIMESTAMP,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    raw_data JSONB,
                    UNIQUE(source, symbol, last_updated)
                )
            """)
            
            # Create indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_crypto_source ON crypto_data(source)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_crypto_symbol ON crypto_data(symbol)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_crypto_updated ON crypto_data(last_updated)
            """)
            
            # ETL checkpoint table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS etl_checkpoints (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(50) NOT NULL,
                    checkpoint_data JSONB NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    completed BOOLEAN DEFAULT FALSE
                )
            """)
            
            # ETL runs metadata table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS etl_runs (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(50) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    duration_seconds DECIMAL(10, 2),
                    error_message TEXT,
                    metadata JSONB
                )
            """)
            
            # Schema drift logs
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_drift_logs (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(50) NOT NULL,
                    detected_at TIMESTAMP DEFAULT NOW(),
                    expected_schema JSONB,
                    actual_schema JSONB,
                    confidence_score DECIMAL(5, 4),
                    warnings TEXT[]
                )
            """)
            
            logger.info("All database tables created successfully")
    
    async def check_health(self) -> bool:
        """Check database connectivity"""
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    async def get_etl_status(self) -> Dict[str, Any]:
        """Get ETL last run status"""
        try:
            async with self.pool.acquire() as conn:
                last_run = await conn.fetchrow("""
                    SELECT status, end_time, source, records_processed
                    FROM etl_runs
                    ORDER BY start_time DESC
                    LIMIT 1
                """)
                
                last_success = await conn.fetchrow("""
                    SELECT end_time, source, records_processed
                    FROM etl_runs
                    WHERE status = 'success'
                    ORDER BY start_time DESC
                    LIMIT 1
                """)
                
                return {
                    "status": last_run['status'] if last_run else "never_run",
                    "last_run": last_run['end_time'].isoformat() if last_run and last_run['end_time'] else None,
                    "last_success": last_success['end_time'].isoformat() if last_success and last_success['end_time'] else None
                }
        except Exception as e:
            logger.error(f"Error getting ETL status: {e}")
            return {"status": "error", "last_run": None, "last_success": None}
    
    async def get_data(self, page: int, page_size: int, filters: Dict) -> Dict:
        """Get paginated and filtered data"""
        offset = (page - 1) * page_size
        
        # Build WHERE clause
        where_clauses = []
        params = []
        param_num = 1
        
        if 'source' in filters:
            where_clauses.append(f"source = ${param_num}")
            params.append(filters['source'])
            param_num += 1
        
        if 'symbol' in filters:
            where_clauses.append(f"symbol = ${param_num}")
            params.append(filters['symbol'])
            param_num += 1
        
        if 'min_price' in filters:
            where_clauses.append(f"price_usd >= ${param_num}")
            params.append(filters['min_price'])
            param_num += 1
        
        if 'max_price' in filters:
            where_clauses.append(f"price_usd <= ${param_num}")
            params.append(filters['max_price'])
            param_num += 1
        
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        async with self.pool.acquire() as conn:
            # Get total count
            count_query = f"SELECT COUNT(*) FROM crypto_data {where_sql}"
            total = await conn.fetchval(count_query, *params)
            
            # Get paginated data
            data_query = f"""
                SELECT 
                    id, source, symbol, name, price_usd, market_cap_usd,
                    volume_24h_usd, percent_change_24h, rank, last_updated, ingested_at
                FROM crypto_data
                {where_sql}
                ORDER BY last_updated DESC
                LIMIT ${param_num} OFFSET ${param_num + 1}
            """
            params.extend([page_size, offset])
            
            rows = await conn.fetch(data_query, *params)
            
            data = [dict(row) for row in rows]
            
            return {
                "data": data,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size
            }
    
    async def get_etl_stats(self) -> Dict:
        """Get comprehensive ETL statistics"""
        async with self.pool.acquire() as conn:
            # Total records
            total_records = await conn.fetchval("SELECT COUNT(*) FROM crypto_data")
            
            # Records by source
            by_source = await conn.fetch("""
                SELECT source, COUNT(*) as count
                FROM crypto_data
                GROUP BY source
            """)
            by_source_dict = {row['source']: row['count'] for row in by_source}
            
            # Run statistics
            runs = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs
                FROM etl_runs
            """)
            
            # Last run duration
            last_run = await conn.fetchrow("""
                SELECT duration_seconds, end_time
                FROM etl_runs
                WHERE status = 'success'
                ORDER BY start_time DESC
                LIMIT 1
            """)
            
            # Last success/failure timestamps
            last_success = await conn.fetchval("""
                SELECT end_time FROM etl_runs
                WHERE status = 'success'
                ORDER BY start_time DESC
                LIMIT 1
            """)
            
            last_failure = await conn.fetchval("""
                SELECT end_time FROM etl_runs
                WHERE status = 'failed'
                ORDER BY start_time DESC
                LIMIT 1
            """)
            
            return {
                "total_records": total_records,
                "by_source": by_source_dict,
                "last_duration": float(last_run['duration_seconds']) if last_run and last_run['duration_seconds'] else 0,
                "last_success": last_success.isoformat() if last_success else None,
                "last_failure": last_failure.isoformat() if last_failure else None,
                "total_runs": runs['total_runs'] or 0,
                "successful_runs": runs['successful_runs'] or 0,
                "failed_runs": runs['failed_runs'] or 0
            }
    
    async def get_recent_runs(self, limit: int = 10) -> List[Dict]:
        """Get recent ETL runs"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    id, source, status, records_processed, 
                    start_time, end_time, duration_seconds, error_message
                FROM etl_runs
                ORDER BY start_time DESC
                LIMIT $1
            """, limit)
            
            return [dict(row) for row in rows]
    
    async def save_raw_data(self, source: str, data: List[Dict], source_ids: List[str] = None):
        """Save raw data to appropriate table"""
        table_map = {
            "coinpaprika": "raw_coinpaprika",
            "coingecko": "raw_coingecko",
            "csv": "raw_csv"
        }
        
        table = table_map.get(source)
        if not table:
            raise ValueError(f"Unknown source: {source}")
        
        async with self.pool.acquire() as conn:
            for idx, item in enumerate(data):
                source_id = source_ids[idx] if source_ids and idx < len(source_ids) else None
                await conn.execute(
                    f"INSERT INTO {table} (data, source_id) VALUES ($1, $2)",
                    json.dumps(item), source_id
                )
    
    async def save_normalized_data(self, records: List[Dict]):
        """Save normalized data with idempotent writes"""
        async with self.pool.acquire() as conn:
            for record in records:
                await conn.execute("""
                    INSERT INTO crypto_data 
                    (source, symbol, name, price_usd, market_cap_usd, volume_24h_usd,
                     percent_change_24h, rank, last_updated, raw_data)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (source, symbol, last_updated) DO NOTHING
                """, 
                    record['source'], record['symbol'], record.get('name'),
                    record.get('price_usd'), record.get('market_cap_usd'),
                    record.get('volume_24h_usd'), record.get('percent_change_24h'),
                    record.get('rank'), record.get('last_updated'),
                    json.dumps(record.get('raw_data', {}))
                )
    
    async def save_checkpoint(self, source: str, checkpoint_data: Dict, records_processed: int):
        """Save ETL checkpoint"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO etl_checkpoints (source, checkpoint_data, records_processed)
                VALUES ($1, $2, $3)
            """, source, json.dumps(checkpoint_data), records_processed)
    
    async def get_last_checkpoint(self, source: str) -> Optional[Dict]:
        """Get last checkpoint for a source"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT checkpoint_data, records_processed
                FROM etl_checkpoints
                WHERE source = $1 AND NOT completed
                ORDER BY created_at DESC
                LIMIT 1
            """, source)
            
            if row:
                return {
                    "data": json.loads(row['checkpoint_data']),
                    "records_processed": row['records_processed']
                }
            return None
    
    async def mark_checkpoint_completed(self, source: str):
        """Mark checkpoint as completed"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE etl_checkpoints
                SET completed = TRUE
                WHERE source = $1 AND NOT completed
            """, source)
    
    async def log_run(self, source: str, status: str, records: int, 
                     start_time: datetime, end_time: datetime = None, 
                     error: str = None, metadata: Dict = None):
        """Log ETL run"""
        duration = (end_time - start_time).total_seconds() if end_time else None
        
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO etl_runs 
                (source, status, records_processed, start_time, end_time, duration_seconds, error_message, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, source, status, records, start_time, end_time, duration, error, 
                json.dumps(metadata) if metadata else None)
    
    async def log_schema_drift(self, source: str, expected: Dict, actual: Dict, 
                               confidence: float, warnings: List[str]):
        """Log schema drift detection"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schema_drift_logs 
                (source, expected_schema, actual_schema, confidence_score, warnings)
                VALUES ($1, $2, $3, $4, $5)
            """, source, json.dumps(expected), json.dumps(actual), confidence, warnings)

async def get_db():
    """Dependency to get database service"""
    db = DatabaseService()
    await db.initialize()
    try:
        yield db
    finally:
        if db.pool:
            await db.pool.close()