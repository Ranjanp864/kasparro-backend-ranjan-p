#  Kasparro Backend & ETL System

A production-grade ETL pipeline and backend API system for cryptocurrency data ingestion, featuring robust error handling, incremental ingestion, schema drift detection, and comprehensive observability.

##  Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Quick Start](#-quick-start)
- [API Endpoints](#-api-endpoints)
- [Project Structure](#-project-structure)
- [Testing](#-testing)
- [Cloud Deployment](#-cloud-deployment)
- [Design Decisions](#-design-decisions)

##  Features

### P0 - Foundation Layer 
- **Multi-Source Data Ingestion**: CoinPaprika API, CoinGecko API, and CSV files
- **PostgreSQL Storage**: Separate raw and normalized data tables
- **Type Validation**: Pydantic models for data validation
- **Incremental Ingestion**: Avoid reprocessing old data
- **Secure Authentication**: Environment-based API key management
- **REST API**: FastAPI with pagination, filtering, and metadata
- **Docker-ized**: Complete containerization with docker-compose
- **Health Checks**: Database connectivity and ETL status monitoring
- **Test Suite**: Comprehensive pytest coverage

### P1 - Growth Layer 
- **Third Data Source**: CSV file ingestion with quirks handling
- **Schema Unification**: Consistent schema across all sources
- **Advanced Checkpointing**: Resume-on-failure with idempotent writes
- **ETL Statistics**: `/stats` endpoint with comprehensive metrics
- **Full Test Coverage**: Tests for incremental ingestion, failures, schema mismatches
- **Clean Architecture**: Well-organized codebase with separation of concerns

### P2 - Differentiator Layer 
- **Schema Drift Detection**: Fuzzy matching with confidence scoring
- **Failure Recovery**: Automatic resume from checkpoints
- **Rate Limiting**: Per-source rate limits with exponential backoff
- **Observability**: Prometheus metrics, structured JSON logging
- **Run Comparison**: `/runs` endpoint for historical analysis
- **Health Checks**: Docker health checks for container orchestration

##  Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     FastAPI Application                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  /data   │  │ /health  │  │  /stats  │  │  /runs   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────┴────────────────┐
        │                                  │
┌───────▼────────┐              ┌─────────▼─────────┐
│  ETL Orchestrator│              │  Database Service │
└───────┬────────┘              └─────────┬─────────┘
        │                                  │
   ┌────┴────┬────────┬────────┐          │
   │         │        │        │          │
┌──▼──┐  ┌──▼──┐  ┌──▼──┐  ┌──▼──┐      │
│Coin │  │Coin │  │ CSV │  │Check│      │
│Papri│  │Gecko│  │ File│  │point│      │
│ka   │  │     │  │     │  │     │      │
└──┬──┘  └──┬──┘  └──┬──┘  └──┬──┘      │
   │        │        │        │          │
   └────────┴────────┴────────┴──────────▼
              PostgreSQL Database
       ┌─────────────────────────────┐
       │ • raw_coinpaprika          │
       │ • raw_coingecko            │
       │ • raw_csv                  │
       │ • crypto_data (unified)    │
       │ • etl_checkpoints          │
       │ • etl_runs                 │
       │ • schema_drift_logs        │
       └─────────────────────────────┘
```

### Data Flow

1. **Extract**: Fetch data from sources (APIs/CSV) with rate limiting
2. **Transform**: Validate with Pydantic, detect schema drift, normalize
3. **Load**: Save to PostgreSQL with checkpointing and idempotent writes
4. **Observe**: Log metrics, track runs, expose health endpoints

##  Quick Start

### Prerequisites

- Docker & Docker Compose
- Make (optional, for convenience commands)
- API Keys (optional, works without them)
  - [CoinPaprika API](https://coinpaprika.com/api)
  - [CoinGecko API](https://www.coingecko.com/en/api)

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd kasparro-backend
```

2. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env and add your API keys (optional)
```

3. **Start the system**
```bash
make up
```

That's it! The system will:
- Build Docker images
- Start PostgreSQL database
- Initialize database schema
- Run initial ETL
- Start API server on http://localhost:8000

### Alternative: Without Make

```bash
docker-compose up -d
```

## 📡 API Endpoints

### Core Endpoints

#### `GET /health`
Health check with database and ETL status

**Response:**
```json
{
  "status": "healthy",
  "database_connected": true,
  "etl_last_run": "2024-01-15T10:30:00",
  "etl_last_success": "2024-01-15T10:30:00",
  "etl_status": "success",
  "request_id": "uuid",
  "api_latency_ms": 15.23
}
```

#### `GET /data`
Get cryptocurrency data with pagination and filtering

**Query Parameters:**
- `page` (int): Page number (default: 1)
- `page_size` (int): Items per page (default: 50, max: 1000)
- `source` (string): Filter by source (coinpaprika, coingecko, csv)
- `symbol` (string): Filter by crypto symbol (e.g., BTC)
- `min_price` (float): Minimum price filter
- `max_price` (float): Maximum price filter

**Example:**
```bash
curl "http://localhost:8000/data?page=1&page_size=10&source=coinpaprika&symbol=BTC"
```

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "source": "coinpaprika",
      "symbol": "BTC",
      "name": "Bitcoin",
      "price_usd": 45000.50,
      "market_cap_usd": 850000000000,
      "volume_24h_usd": 25000000000,
      "percent_change_24h": 2.5,
      "rank": 1,
      "last_updated": "2024-01-15T10:30:00",
      "ingested_at": "2024-01-15T10:30:05"
    }
  ],
  "total_records": 100,
  "page": 1,
  "page_size": 10,
  "total_pages": 10,
  "filters": {},
  "request_id": "uuid",
  "api_latency_ms": 25.67
}
```

#### `GET /stats`
Get comprehensive ETL statistics

**Response:**
```json
{
  "total_records_processed": 500,
  "records_by_source": {
    "coinpaprika": 200,
    "coingecko": 200,
    "csv": 100
  },
  "last_run_duration_seconds": 45.5,
  "last_success_timestamp": "2024-01-15T10:30:00",
  "last_failure_timestamp": null,
  "total_runs": 10,
  "successful_runs": 10,
  "failed_runs": 0,
  "request_id": "uuid",
  "api_latency_ms": 12.34
}
```

#### `GET /runs?limit=10`
Get recent ETL run history

#### `GET /metrics`
Get Prometheus-format metrics

#### `POST /trigger-etl`
Manually trigger ETL process

### Interactive API Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

##  Project Structure

```
kasparro-backend/
├── main.py                    # FastAPI application entry point
├── core/
│   ├── config.py             # Configuration management
│   └── logger.py             # Structured logging setup
├── api/
│   └── models.py             # Pydantic response models
├── services/
│   ├── database.py           # Database operations
│   └── etl_orchestrator.py  # ETL coordination
├── ingestion/
│   ├── base_pipeline.py      # Abstract ETL pipeline
│   ├── coinpaprika_pipeline.py
│   ├── coingecko_pipeline.py
│   └── csv_pipeline.py
├── tests/
│   └── test_etl_and_api.py  # Comprehensive test suite
├── Dockerfile                 # Container definition
├── docker-compose.yml         # Multi-container orchestration
├── Makefile                   # Convenient commands
├── requirements.txt           # Python dependencies
├── .env.example              # Environment template
└── README.md                  # This file
```

##  Testing

### Run All Tests

```bash
make test
```

This runs:
- ETL transformation tests
- API endpoint tests
- Incremental ingestion tests
- Failure recovery tests
- Schema drift detection tests
- Rate limiting tests
- Integration tests

### Test Coverage

The test suite covers:
-  ETL transformation logic (all sources)
-  API endpoints with various filters
-  Checkpoint save/retrieve/complete
-  Failure scenarios and recovery
-  Schema drift detection
-  Idempotent writes
-  Rate limiting and backoff
-  Full orchestration

### Manual Testing

```bash
# Check health
make health

# Get data
make data

# View stats
make stats

# Trigger ETL
make trigger-etl
```

##  Cloud Deployment

### AWS Deployment (Recommended)

#### Prerequisites
- AWS Account
- AWS CLI configured
- ECR for Docker images
- ECS/Fargate for container hosting
- RDS for PostgreSQL
- EventBridge for cron jobs

#### Steps

1. **Build and Push Docker Image**

```bash
# Build image
docker build -t kasparro-backend .

# Tag for ECR
docker tag kasparro-backend:latest <account-id>.dkr.ecr.<region>.amazonaws.com/kasparro-backend:latest

# Login to ECR
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account-id>.dkr.ecr.<region>.amazonaws.com

# Push image
docker push <account-id>.dkr.ecr.<region>.amazonaws.com/kasparro-backend:latest
```

2. **Setup RDS PostgreSQL**

```bash
aws rds create-db-instance \
  --db-instance-identifier kasparro-postgres \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --master-username postgres \
  --master-user-password <password> \
  --allocated-storage 20
```

3. **Deploy to ECS**

Create ECS task definition with:
- Container: Your ECR image
- Environment variables from .env
- Port mapping: 8000
- Health check: /health endpoint

4. **Setup EventBridge Cron**

```bash
aws events put-rule \
  --name kasparro-etl-hourly \
  --schedule-expression "rate(1 hour)" \
  --state ENABLED

aws events put-targets \
  --rule kasparro-etl-hourly \
  --targets "Id"="1","Arn"="<ecs-task-arn>"
```

5. **Configure Load Balancer (Optional)**

Setup ALB for public access to API endpoints.

### GCP Deployment

Similar steps using:
- Cloud Run for containers
- Cloud SQL for PostgreSQL
- Cloud Scheduler for cron jobs

### Azure Deployment

Similar steps using:
- Container Instances
- Azure Database for PostgreSQL
- Logic Apps for scheduling

##  Design Decisions

### Why FastAPI?
- **Performance**: Async support, faster than Flask
- **Type Safety**: Automatic validation with Pydantic
- **Documentation**: Auto-generated OpenAPI docs
- **Modern**: Built for async Python 3.7+

### Why PostgreSQL?
- **ACID Compliance**: Reliable transactions
- **JSONB Support**: Flexible raw data storage
- **Performance**: Great for analytical queries
- **Mature**: Battle-tested with excellent tooling

### Why Separate Raw/Normalized Tables?
- **Data Lineage**: Keep original data for auditing
- **Reprocessing**: Can re-normalize if schema changes
- **Debugging**: Easier to troubleshoot issues
- **Compliance**: Some regulations require raw data

### ETL Architecture Patterns

#### Extract-Transform-Load (ETL)
Classic pattern with benefits:
- Data validation before loading
- Schema normalization
- Error isolation per stage

#### Checkpointing Strategy
- Save progress every N records
- Resume from last checkpoint on failure
- Idempotent writes prevent duplicates

#### Rate Limiting
- Exponential backoff on 429 errors
- Per-source rate limits
- Prevents API blocking

#### Schema Drift Detection
- Fuzzy matching for renamed fields
- Type validation
- Warning logs for manual review

### Error Handling Philosophy

1. **Fail Fast**: Validate early in pipeline
2. **Graceful Degradation**: Continue processing valid records
3. **Detailed Logging**: Structured JSON logs
4. **Observability**: Metrics and health checks
5. **Recovery**: Automatic resume from checkpoints

## 🛠 Development

### Local Development

```bash
# Start services
make up

# View logs
make logs

# Open shell
make shell

# Access database
make db-shell

# Restart services
make restart

# Clean up
make clean
```

### Adding New Data Sources

1. Create new pipeline in `ingestion/`
2. Extend from `BasePipeline`
3. Implement `extract()`, `transform()`, `load()`
4. Add to `ETLOrchestrator`
5. Write tests

Example:
```python
from ingestion.base_pipeline import BasePipeline

class NewSourcePipeline(BasePipeline):
    SOURCE_NAME = "newsource"
    
    async def extract(self) -> List[Dict]:
        # Fetch data
        pass
    
    async def transform(self, raw_data: List[Dict]) -> List[Dict]:
        # Normalize data
        pass
    
    async def load(self, normalized_data: List[Dict]):
        # Save to database
        pass
```

##  Troubleshooting

### Database Connection Issues

```bash
# Check if database is healthy
docker-compose ps

# Restart database
docker-compose restart db

# Check logs
docker-compose logs db
```

### API Not Starting

```bash
# Check logs
make logs

# Verify environment variables
docker-compose exec api env | grep POSTGRES

# Test database connection
make db-shell
```

### ETL Failures

```bash
# Check ETL status
curl http://localhost:8000/stats

# View recent runs
curl http://localhost:8000/runs

# Trigger manual run
make trigger-etl
```

##  Monitoring & Observability

### Structured Logging

All logs are in JSON format for easy parsing:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "logger": "services.etl_orchestrator",
  "message": "ETL run completed",
  "module": "etl_orchestrator",
  "function": "run_full_etl"
}
```

### Metrics

Prometheus-compatible metrics at `/metrics`:

```
# HELP etl_total_records Total records processed
# TYPE etl_total_records gauge
etl_total_records 500

# HELP etl_total_runs Total ETL runs
# TYPE etl_total_runs counter
etl_total_runs 10
```

### Health Checks

Docker health checks ensure container stability:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s \
  CMD python -c "import requests; requests.get('http://localhost:8000/health')"
```

##  License

This project is developed as part of the Kasparro Backend Engineering Assignment.

##  Contributing

This is an assignment submission. For questions or issues, contact the Kasparro team.

## 📧 Contact

- Discord: https://discord.gg/d2zj2sJrc7
- Assignment Form: https://forms.gle/ouW6W1jH5wyRrnEX6

---

**Built with** ❤️ **for Kasparro**
