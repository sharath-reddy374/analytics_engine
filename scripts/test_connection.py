#!/usr/bin/env python3
"""
Test script to verify DynamoDB connection and table access
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.dynamodb_connection import verify_existing_tables, get_dynamodb
from database.dynamodb_models import DataFetcher
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    """Test DynamoDB connection and data access"""
    try:
        # Verify tables exist
        logger.info("Verifying DynamoDB tables...")
        verify_existing_tables()
        
        # Test data fetching
        logger.info("Testing data fetching...")
        fetcher = DataFetcher()
        
        # Get sample users
        users = fetcher.get_all_users()
        logger.info(f"Found {len(users)} users in investor_prod table")
        
        if users:
            # Test fetching data for first user
            sample_user = users[0]
            user_email = sample_user.get('email', 'unknown')
            logger.info(f"Testing data fetch for user: {user_email}")
            
            user_data = fetcher.get_all_user_data(user_email)
            logger.info(f"User data keys: {list(user_data.keys())}")
            
        # Test content metadata
        content = fetcher.get_content_metadata()
        logger.info(f"Content metadata keys: {list(content.keys())}")
        
        logger.info("✅ DynamoDB connection test successful!")
        return True
        
    except Exception as e:
        logger.error(f"❌ DynamoDB connection test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
