#!/usr/bin/env python3
"""
Test DynamoDB connection and verify table access
"""
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_dynamodb_connection():
    """Test DynamoDB connection and table access"""
    print("🔍 Testing DynamoDB connection...")
    
    try:
        print("\n🔧 Debug: Importing settings...")
        from config.settings import settings
        print("✅ Settings imported successfully")
        
        # Debug settings attributes
        print("\n📋 Debug: Checking settings attributes...")
        print(f"   Settings type: {type(settings)}")
        print(f"   Settings dir: {[attr for attr in dir(settings) if 'DYNAMODB' in attr]}")
        
        # Check if table attributes exist
        table_attrs = [
            'DYNAMODB_INVESTOR_TABLE',
            'DYNAMODB_LOGIN_HISTORY_TABLE', 
            'DYNAMODB_ITP_TABLE',
            'DYNAMODB_TEST_RECORDS_TABLE',
            'DYNAMODB_LEARNING_RECORDS_TABLE',
            'DYNAMODB_QUESTIONS_TABLE',
            'DYNAMODB_PRESENTATIONS_TABLE',
            'DYNAMODB_ICP_TABLE'
        ]
        
        print("\n🔍 Debug: Checking table name attributes...")
        for attr in table_attrs:
            if hasattr(settings, attr):
                value = getattr(settings, attr)
                print(f"   ✅ {attr}: {value}")
            else:
                print(f"   ❌ {attr}: NOT FOUND")
        
        # Check .env file
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
        print(f"\n📁 Debug: Checking .env file at: {env_path}")
        print(f"   .env exists: {os.path.exists(env_path)}")
        
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                env_content = f.read()
                dynamodb_lines = [line for line in env_content.split('\n') if 'DYNAMODB' in line and not line.startswith('#')]
                print(f"   DynamoDB env vars found: {len(dynamodb_lines)}")
                for line in dynamodb_lines[:3]:  # Show first 3
                    print(f"     {line}")
                
                aws_lines = [line for line in env_content.split('\n') if line.startswith('AWS_') and not line.startswith('#')]
                print(f"   AWS env vars found: {len(aws_lines)}")
                for line in aws_lines:
                    if 'SECRET' in line:
                        # Hide secret key value for security
                        key, value = line.split('=', 1)
                        print(f"     {key}={'*' * len(value) if value else 'NOT_SET'}")
                    else:
                        print(f"     {line}")
        
        print("\n🌍 Debug: Checking environment variables...")
        aws_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'AWS_DEFAULT_REGION']
        for var in aws_env_vars:
            env_value = os.environ.get(var)
            if env_value:
                if 'SECRET' in var:
                    print(f"   {var}: {'*' * len(env_value)} (from os.environ)")
                else:
                    print(f"   {var}: {env_value} (from os.environ)")
            else:
                print(f"   {var}: NOT SET (in os.environ)")
        
        print("\n🔗 Debug: Testing DynamoDB connection...")
        from database.dynamodb_connection import get_dynamodb_client
        dynamodb_conn = get_dynamodb_client()
        client = dynamodb_conn.client
        print("✅ DynamoDB client created successfully")
        
        print("\n🔐 Debug: AWS Configuration...")
        print(f"   AWS_REGION: {settings.AWS_REGION}")
        print(f"   AWS_ACCESS_KEY_ID: {'Set (' + settings.AWS_ACCESS_KEY_ID[:8] + '...)' if settings.AWS_ACCESS_KEY_ID else 'Not set'}")
        print(f"   AWS_SECRET_ACCESS_KEY: {'Set' if settings.AWS_SECRET_ACCESS_KEY else 'Not set'}")
        print(f"   DYNAMODB_ENDPOINT_URL: {settings.DYNAMODB_ENDPOINT_URL or 'None (using AWS)'}")
        
        print("\n🔑 Debug: Boto3 credential resolution...")
        try:
            import boto3
            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials:
                print(f"   Boto3 found credentials: {credentials.access_key[:8]}...")
                print(f"   Credential source: {type(credentials).__name__}")
            else:
                print("   ❌ Boto3 could not find any credentials")
        except Exception as e:
            print(f"   ❌ Boto3 credential check failed: {e}")
        
        # Test table access
        from database.dynamodb_models import DataFetcher
        data_fetcher = DataFetcher()
        
        # Test individual table access using settings from environment
        table_names = []
        for attr in table_attrs:
            if hasattr(settings, attr):
                table_names.append(getattr(settings, attr))
        
        print(f"\n📊 Testing Table Access for {len(table_names)} tables:")
        for table_name in table_names:
            try:
                response = client.describe_table(TableName=table_name)
                status = response['Table']['TableStatus']
                item_count = response['Table'].get('ItemCount', 'Unknown')
                print(f"  ✅ {table_name}: {status} (Items: {item_count})")
            except Exception as e:
                print(f"  ❌ {table_name}: {str(e)}")
        
        # Test reading a sample user (if exists)
        print("\n👤 Testing user data access...")
        try:
            user_data = data_fetcher.get_all_user_data("sharath_b2c@yopmail.com")
            if user_data and any(user_data.values()):
                print(f"✅ Found user data with profile: {bool(user_data.get('profile'))}")
                print(f"   Data keys: {list(user_data.keys())}")
            else:
                print("ℹ️  No data found for test user (this is normal if user doesn't exist)")
        except Exception as e:
            print(f"⚠️  Could not fetch user data: {str(e)}")
        
        print("\n🎉 DynamoDB connection test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ DynamoDB connection failed: {str(e)}")
        import traceback
        print(f"\n🔍 Full error traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_dynamodb_connection()
    sys.exit(0 if success else 1)
