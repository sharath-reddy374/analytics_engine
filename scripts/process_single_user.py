#!/usr/bin/env python3
"""
Process AI engine pipeline for a single user by email
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.dynamodb_models import DataFetcher
from services.data_processor import DataProcessor
from services.feature_engine import FeatureEngine
from services.decision_engine import DecisionEngine
from services.email_service import EmailService
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_single_user(email: str):
    """Process the complete AI pipeline for a single user"""
    try:
        logger.info(f"🚀 Starting AI engine processing for user: {email}")
        
        # Initialize services
        fetcher = DataFetcher()
        processor = DataProcessor()
        feature_engine = FeatureEngine()
        decision_engine = DecisionEngine()
        email_service = EmailService()
        
        # Step 1: Fetch user data from DynamoDB
        logger.info("📊 Fetching user data from DynamoDB...")
        user_data = fetcher.get_all_user_data(email)
        
        if not user_data or not user_data.get('profile'):
            logger.error(f"❌ User {email} not found in database")
            return False
            
        logger.info(f"✅ Found user data with {len(user_data)} data sources")
        
        # Step 2: Process and normalize data into events
        logger.info("🔄 Processing and normalizing data...")
        events = processor.process_user_data(user_data)
        logger.info(f"✅ Generated {len(events)} normalized events")
        
        # Step 3: Compute user features
        logger.info("🧮 Computing user features...")
        features = feature_engine.compute_user_features(email, events)
        logger.info(f"✅ Computed features: {list(features.keys())}")
        
        # Step 4: Run decision engine
        logger.info("🎯 Running decision engine...")
        decisions = decision_engine.evaluate_user(email, features)
        logger.info(f"✅ Generated {len(decisions)} email decisions")
        
        # Step 5: Send emails
        if decisions:
            logger.info("📧 Sending personalized emails...")
            for decision in decisions:
                result = email_service.send_email(
                    to_email=email,
                    template_name=decision['template'],
                    context=decision['context']
                )
                logger.info(f"✅ Email sent: {decision['template']} - {result}")
        else:
            logger.info("ℹ️ No email campaigns triggered for this user")
        
        logger.info(f"🎉 Successfully processed user: {email}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to process user {email}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Process AI engine for a single user')
    parser.add_argument('email', help='User email address to process')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    success = process_single_user(args.email)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
