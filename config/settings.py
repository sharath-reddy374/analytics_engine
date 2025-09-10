from pydantic_settings import BaseSettings
from pydantic import ConfigDict, Field
from typing import List, Optional
import os
from dotenv import load_dotenv

class Settings(BaseSettings):
    # DynamoDB - with aliases to handle different naming conventions
    AWS_ACCESS_KEY_ID: Optional[str] = Field(default=None)
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(default=None)
    AWS_REGION: str = Field(default="us-east-1")
    AWS_DEFAULT_REGION: Optional[str] = Field(default=None)  # Alternative field name
    DYNAMODB_ENDPOINT_URL: Optional[str] = Field(default=None)
    
    DYNAMODB_INVESTOR_TABLE: str = Field(default="Investor_Prod")
    DYNAMODB_LOGIN_HISTORY_TABLE: str = Field(default="InvestorLoginHistory_Prod")
    DYNAMODB_ITP_TABLE: str = Field(default="User_Infinite_TestSeries_Prod")
    DYNAMODB_TEST_RECORDS_TABLE: str = Field(default="TestSereiesRecord_Prod")
    DYNAMODB_LEARNING_RECORDS_TABLE: str = Field(default="LearningRecord_Prod")
    DYNAMODB_QUESTIONS_TABLE: str = Field(default="Question_Prod")
    DYNAMODB_PRESENTATIONS_TABLE: str = Field(default="presentation_prod")
    DYNAMODB_ICP_TABLE: str = Field(default="ICP_Prod")
    
    DYNAMODB_TABLE_PREFIX: str = Field(default="edyou_")
    
    # Optional services - can be disabled for basic testing
    CLICKHOUSE_URL: str = "http://localhost:8123/default"
    CLICKHOUSE_ENABLED: bool = False
    
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_ENABLED: bool = False
    
    # MinIO (S3-compatible storage) - optional for basic testing
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "edyou-data"
    MINIO_ENABLED: bool = False
    
    # Vector Database - optional for basic testing
    QDRANT_URL: str = "http://localhost:6333"
    QDRANT_ENABLED: bool = False
    
    # Message Queue - optional for basic testing
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    RABBITMQ_ENABLED: bool = False
    
    # AI/LLM
    OPENAI_API_KEY: Optional[str] = None
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"
    
    # Email
    SENDGRID_API_KEY: Optional[str] = None
    FROM_EMAIL: str = "no-reply@edyou.com"
    FROM_NAME: str = "EdYou Team"
    EMAIL_SIMULATION_MODE: bool = True  # Added simulation mode for testing
    
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
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
        env_file_encoding='utf-8',
        case_sensitive=True,
        extra='ignore'  # Ignore extra environment variables
    )

def get_settings() -> Settings:
    """Get settings instance with proper .env loading"""
    load_dotenv(".env", override=True)
    return Settings()

settings = get_settings()
