from pydantic_settings import BaseSettings
from pydantic import ConfigDict, Field
from typing import List, Optional
import os

class Settings(BaseSettings):
    # DynamoDB - with aliases to handle different naming conventions
    AWS_ACCESS_KEY_ID: Optional[str] = Field(default=None, alias='aws_access_key_id')
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(default=None, alias='aws_secret_access_key')
    AWS_REGION: str = Field(default="us-east-1", alias='aws_default_region')
    DYNAMODB_ENDPOINT_URL: Optional[str] = None  # For local DynamoDB
    DYNAMODB_TABLE_PREFIX: str = "edyou_"
    
    # Database
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/edyou_engine"
    CLICKHOUSE_URL: str = "clickhouse://localhost:9000/default"
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # MinIO (S3-compatible storage)
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "edyou-data"
    
    # Vector Database
    QDRANT_URL: str = "http://localhost:6333"
    
    # Message Queue
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    
    # AI/LLM
    OPENAI_API_KEY: Optional[str] = None
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"
    
    # Email
    SENDGRID_API_KEY: Optional[str] = None
    FROM_EMAIL: str = "no-reply@edyou.com"
    FROM_NAME: str = "EdYou Team"
    
    # App Settings
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"
    TIMEZONE: str = "UTC"
    
    # Email Rules
    MAX_EMAILS_PER_DAY: int = 1
    MAX_EMAILS_PER_WEEK: int = 2
    EMAIL_QUIET_HOURS_START: int = 20  # 8 PM
    EMAIL_QUIET_HOURS_END: int = 8     # 8 AM

    model_config = ConfigDict(
        env_file=".env",
        extra='ignore'  # Ignore extra environment variables
    )

settings = Settings()
