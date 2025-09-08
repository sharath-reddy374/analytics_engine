#!/usr/bin/env python3
"""
Local development runner for the EdYou AI Engine
This script sets up and runs the engine for local testing with DynamoDB
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

def check_dynamodb_local():
    """Check if DynamoDB Local is running"""
    try:
        import requests
        response = requests.get("http://localhost:8000", timeout=5)
        return True
    except:
        return False

def start_dynamodb_local():
    """Instructions for starting DynamoDB Local"""
    logger.info("DynamoDB Local is not running!")
    logger.info("To start DynamoDB Local:")
    logger.info("1. Download from: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html")
    logger.info("2. Extract and run: java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb")
    logger.info("3. Or use Docker: docker run -p 8000:8000 amazon/dynamodb-local")
    return False

def setup_environment():
    """Setup local environment"""
    # Copy .env.local to .env if .env doesn't exist
    if not os.path.exists(".env"):
        if os.path.exists(".env.local"):
            import shutil
            shutil.copy(".env.local", ".env")
            logger.info("Copied .env.local to .env")
        else:
            logger.warning("No .env.local file found. Please create one based on .env.example")
            return False
    return True

def run_engine():
    """Run the AI engine"""
    try:
        logger.info("Starting EdYou AI Engine...")
        
        # Setup environment
        if not setup_environment():
            return False
        
        # Check DynamoDB Local
        if not check_dynamodb_local():
            start_dynamodb_local()
            return False
        
        # Initialize DynamoDB tables
        logger.info("Setting up DynamoDB tables...")
        from scripts.setup_dynamodb_local import setup_dynamodb
        setup_dynamodb()
        
        # Start the FastAPI server
        logger.info("Starting FastAPI server on http://localhost:8080")
        subprocess.run([
            sys.executable, "-m", "uvicorn", 
            "api.main:app", 
            "--host", "0.0.0.0", 
            "--port", "8080", 
            "--reload"
        ])
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error running engine: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = run_engine()
    sys.exit(0 if success else 1)
