"""
Configuration management using Pydantic Settings
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Application
    APP_NAME: str = "Kasparro Backend ETL"
    DEBUG: bool = False
    RUN_ETL_ON_STARTUP: bool = True
    
    # Database
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "kasparro_etl"
    POSTGRES_HOST: str = "db"
    POSTGRES_PORT: int = 5432
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    # API Keys
    COINPAPRIKA_API_KEY: Optional[str] = None
    COINGECKO_API_KEY: Optional[str] = None
    
    # ETL Configuration
    ETL_BATCH_SIZE: int = 100
    ETL_MAX_RETRIES: int = 3
    ETL_RETRY_DELAY: int = 5  # seconds
    
    # Rate Limiting
    RATE_LIMIT_ENABLED: bool = True
    COINPAPRIKA_RATE_LIMIT: int = 25  # requests per minute
    COINGECKO_RATE_LIMIT: int = 50  # requests per minute
    
    # Backoff Configuration
    BACKOFF_BASE_DELAY: float = 1.0  # seconds
    BACKOFF_MAX_DELAY: float = 60.0  # seconds
    BACKOFF_FACTOR: float = 2.0
    
    # Checkpoint Configuration
    CHECKPOINT_ENABLED: bool = True
    CHECKPOINT_INTERVAL: int = 50  # records
    
    # Schema Drift Detection
    SCHEMA_DRIFT_ENABLED: bool = True
    SCHEMA_DRIFT_THRESHOLD: float = 0.8  # fuzzy match confidence
    
    # Observability
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # or "text"
    
    # CSV Data Source
    CSV_FILE_PATH: str = "/app/data/crypto_data.csv"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()