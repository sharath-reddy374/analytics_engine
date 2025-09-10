#!/usr/bin/env python3
"""
Local development runner for the EdYou AI Engine
This script sets up and runs the engine for local testing with AWS DynamoDB
"""

import sys
import os
import subprocess
import time
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_environment():
    """Setup local environment"""
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            import shutil
            shutil.copy(".env.example", ".env")
            logger.info("Copied .env.example to .env")
            logger.warning("Please update .env with your AWS credentials before running")
            return False
        else:
            logger.error("No .env.example file found. Cannot create .env file")
            return False
    return True

def check_aws_credentials():
    """Check if AWS credentials are configured"""
    try:
        from config.settings import settings
        if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
            logger.error("AWS credentials not configured in .env file")
            logger.info("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env")
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking AWS credentials: {e}")
        return False

def test_dynamodb_connection():
    """Test DynamoDB connection"""
    try:
        logger.info("Testing DynamoDB connection...")
        from database.dynamodb_connection import verify_existing_tables
        verify_existing_tables()
        logger.info("✅ DynamoDB connection successful")
        return True
    except Exception as e:
        logger.error(f"❌ DynamoDB connection failed: {e}")
        logger.info("Please check your AWS credentials and region settings")
        return False

def run_engine():
    """Run the AI engine locally"""
    try:
        logger.info("🚀 Starting EdYou AI Engine (Local Mode)")
        
        # Setup environment
        if not setup_environment():
            return False
        
        # Check AWS credentials
        if not check_aws_credentials():
            return False
        
        # Test DynamoDB connection
        if not test_dynamodb_connection():
            return False
        
        from config.settings import settings
        logger.info(f"Starting FastAPI server on http://{settings.HOST}:{settings.PORT}")
        subprocess.run([
            sys.executable, "-m", "uvicorn", 
            "api.main:app", 
            "--host", settings.HOST, 
            "--port", str(settings.PORT), 
            "--reload" if settings.DEBUG else "--no-reload"
        ])
        
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
    except Exception as e:
        logger.error(f"❌ Error running engine: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = run_engine()
    sys.exit(0 if success else 1)
