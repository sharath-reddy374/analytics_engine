#!/usr/bin/env python3
"""
Test DynamoDB connection and verify table access
"""
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.dynamodb_connection import get_dynamodb_client
from database.dynamodb_models import DataFetcher
from config.settings import settings

def test_dynamodb_connection():
    """Test DynamoDB connection and table access"""
    print("🔍 Testing DynamoDB connection...")
    
    try:
        dynamodb_conn = get_dynamodb_client()
        client = dynamodb_conn.client
        print("✅ DynamoDB client created successfully")
        
        # Test table access
        data_fetcher = DataFetcher()
        
        # Test individual table access
        table_names = [
            'investor_prod',
            'InvestorLoginHistory_Prod', 
            'User_Infinite_TestSeries_Prod',
            'TestSereiesRecord_Prod',
            'LearningRecord_Prod',
            'Question_Prod',
            'presentation_prod',
            'ICP_Prod'
        ]
        
        print("\n📊 Testing Table Access:")
        for table_name in table_names:
            try:
                response = client.describe_table(TableName=table_name)
                status = response['Table']['TableStatus']
                print(f"  ✅ {table_name}: {status}")
            except Exception as e:
                print(f"  ❌ {table_name}: {str(e)}")
        
        # Test reading a sample user (if exists)
        print("\n👤 Testing user data access...")
        try:
            user_data = data_fetcher.get_all_user_data("sharath_b2c@yopmail.com")
            if user_data and any(user_data.values()):
                print(f"✅ Found user data with profile: {bool(user_data.get('profile'))}")
            else:
                print("ℹ️  No data found for test user (this is normal if user doesn't exist)")
        except Exception as e:
            print(f"⚠️  Could not fetch user data: {str(e)}")
        
        print("\n🎉 DynamoDB connection test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ DynamoDB connection failed: {str(e)}")
        print("\n🔧 Check your AWS credentials and region settings:")
        print(f"   AWS_REGION: {settings.AWS_REGION}")
        print(f"   AWS_ACCESS_KEY_ID: {'Set' if settings.AWS_ACCESS_KEY_ID else 'Not set'}")
        print(f"   AWS_SECRET_ACCESS_KEY: {'Set' if settings.AWS_SECRET_ACCESS_KEY else 'Not set'}")
        return False

if __name__ == "__main__":
    success = test_dynamodb_connection()
    sys.exit(0 if success else 1)
