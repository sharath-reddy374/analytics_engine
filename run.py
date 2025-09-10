#!/usr/bin/env python3
"""
Main entry point for the EdYou AI Engine - Local Development
"""

import uvicorn
import sys
import os
from config.settings import settings

def main():
    """Run the EdYou AI Engine locally"""
    print("🚀 Starting EdYou AI Engine (Local Mode)")
    print(f"📍 Server: http://{settings.HOST}:{settings.PORT}")
    print(f"🔧 Debug Mode: {settings.DEBUG}")
    print(f"🌍 AWS Region: {settings.AWS_REGION}")
    print("=" * 50)
    
    # Check AWS credentials
    if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
        print("⚠️  Warning: AWS credentials not configured")
        print("   Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file")
    
    # Show optional services status
    optional_services = [
        ("Redis", settings.REDIS_ENABLED),
        ("ClickHouse", settings.CLICKHOUSE_ENABLED),
        ("MinIO", settings.MINIO_ENABLED),
        ("Qdrant", settings.QDRANT_ENABLED),
        ("RabbitMQ", settings.RABBITMQ_ENABLED)
    ]
    
    print("Optional Services:")
    for service, enabled in optional_services:
        status = "✅ Enabled" if enabled else "⚪ Disabled"
        print(f"  {service}: {status}")
    
    print("=" * 50)
    
    try:
        uvicorn.run(
            "api.main:app",
            host=settings.HOST,
            port=settings.PORT,
            reload=settings.DEBUG,
            log_level=settings.LOG_LEVEL.lower()
        )
    except KeyboardInterrupt:
        print("\n👋 EdYou AI Engine stopped")
    except Exception as e:
        print(f"❌ Failed to start server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
