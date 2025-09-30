import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from config.settings import settings
import logging
from typing import Dict, List, Optional, Any
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class DynamoDBConnection:
    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        
        # For AWS DynamoDB (user's existing tables)
        self.dynamodb = self.session.resource('dynamodb')
        self.client = self.dynamodb.meta.client
        
    def get_table(self, table_name: str):
        """Get DynamoDB table - use exact table name for existing tables"""
        return self.dynamodb.Table(table_name)
    
    def list_tables(self):
        """List all available tables"""
        try:
            response = self.client.list_tables()
            return response.get('TableNames', [])
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def verify_table_exists(self, table_name: str) -> bool:
        """Verify if a table exists"""
        try:
            self.client.describe_table(TableName=table_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            raise

# Global DynamoDB connection instance
dynamodb_conn = DynamoDBConnection()

def get_dynamodb():
    """Get DynamoDB connection"""
    return dynamodb_conn

def get_dynamodb_client():
    """Get DynamoDB client - alias for get_dynamodb for compatibility"""
    return get_dynamodb()

def verify_existing_tables():
    """Verify all required tables exist"""
    required_tables = [
        'Investor_Prod',
        'InvestorLoginHistory_Prod', 
        'User_Infinite_TestSeries_Prod',
        'TestSereiesRecord_Prod',
        'LearningRecord_Prod',
        'Question_Prod',
        'Presentation_Prod',
        'ICP_Prod'
    ]
    
    missing_tables = []
    for table_name in required_tables:
        if not dynamodb_conn.verify_table_exists(table_name):
            missing_tables.append(table_name)
    
    if missing_tables:
        logger.error(f"Missing required tables: {missing_tables}")
        raise Exception(f"Required DynamoDB tables not found: {missing_tables}")
    
    logger.info("All required DynamoDB tables verified successfully")
    return True
