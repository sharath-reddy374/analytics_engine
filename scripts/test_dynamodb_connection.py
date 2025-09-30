#!/usr/bin/env python3
"""
Quick DynamoDB connectivity and data sanity check (DRY RUN).

- Verifies the presence of required DynamoDB tables
- Lists a small sample of users from the investor table
- For the first sample user, fetches cross-table data and prints basic counts

This script does NOT send emails or modify data.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from typing import Optional

from config.settings import settings
from database.dynamodb_connection import verify_existing_tables
from database.dynamodb_models import DataFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_dynamodb_connection")


def main(sample_user: Optional[str] = None) -> int:
    try:
        logger.info("AWS Region: %s", settings.AWS_REGION)
        if not settings.AWS_ACCESS_KEY_ID or not settings.AWS_SECRET_ACCESS_KEY:
            logger.warning("AWS credentials not configured in .env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).")

        logger.info("Verifying required DynamoDB tables...")
        verify_existing_tables()
        logger.info("✅ DynamoDB required tables verified")

        fetcher = DataFetcher()
        users = fetcher.get_all_users() or []
        logger.info("Total users in investor table: %s", len(users))

        # print a small sample
        sample = users[:5]
        if sample:
            logger.info("Sample users:")
            for i, u in enumerate(sample, 1):
                logger.info("  %d) %s", i, (u or {}).get("email", "unknown"))
        else:
            logger.info("No users found in investor table.")

        # Pick a user for a cross-table data probe
        target_email = sample_user or ((sample[0] or {}).get("email") if sample else None)
        if target_email:
            logger.info("Fetching cross-table data for sample user: %s", target_email)
            data = fetcher.get_all_user_data(target_email)
            if not data:
                logger.warning("User not found or partial match required: %s", target_email)
            else:
                logger.info("Profile present: %s", bool(data.get("profile")))
                logger.info("Conversations: %d", len(data.get("conversations") or []))
                logger.info("Test Series: %d", len(data.get("test_series") or []))
                logger.info("Test Records: %d", len(data.get("test_records") or []))
                logger.info("Learning Records: %d", len(data.get("learning_records") or []))
                logger.info("ICP Course Plans: %d", len(data.get("course_plans") or []))
        else:
            logger.info("No sample user available to probe cross-table data.")

        logger.info("DynamoDB connection test completed successfully.")
        return 0
    except Exception as e:
        logger.error("DynamoDB connection test failed: %s", e)
        return 1


if __name__ == "__main__":
    # Allow optional email via CLI: python scripts/test_dynamodb_connection.py user@example.com
    email = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(main(email))
