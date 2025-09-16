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
from services.email_template_service import EmailTemplateService
try:
    from services.email_service import EmailService
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False
    print("⚠️ Email service not available (sendgrid not installed). Emails will be skipped.")

import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_single_user(email: str, skip_email: bool = False):
    """Process the complete AI pipeline for a single user"""
    try:
        logger.info(f"🚀 Starting AI engine processing for user: {email}")
        
        # Initialize services
        fetcher = DataFetcher()
        processor = DataProcessor()
        feature_engine = FeatureEngine()
        decision_engine = DecisionEngine()
        template_service = EmailTemplateService()
        email_service = EmailService() if EMAIL_AVAILABLE and not skip_email else None
        
        # Step 1: Fetch user data from DynamoDB using proper GetItem/Query operations
        logger.info("📊 Fetching user data from DynamoDB...")
        user_data = fetcher.get_all_user_data(email)
        
        if not user_data or not any(user_data.values()):
            logger.error(f"❌ User {email} not found in database")
            return False
            
        data_sources = sum(1 for v in user_data.values() if v)
        logger.info(f"✅ Found user data with {data_sources} data sources")
        
        # Step 2: Process and normalize data into events
        logger.info("🔄 Processing and normalizing data...")
        events = processor.process_all_user_data(email)
        logger.info(f"✅ Generated {len(events)} normalized events")
        
        # Step 3: Compute user features
        logger.info("🧮 Computing user features...")
        features = feature_engine.compute_user_features(email, events)
        logger.info(f"✅ Computed features: {list(features.keys())}")
        
        logger.info("📋 Feature Values:")
        for key, value in features.items():
            if isinstance(value, (list, dict)):
                logger.info(f"  {key}: {value}")
            else:
                logger.info(f"  {key}: {value}")
        
        # Step 4: Run decision engine
        logger.info("🎯 Running decision engine...")
        decisions = decision_engine.evaluate_user(email, features)
        logger.info(f"✅ Generated {len(decisions)} email decisions")
        
        if not decisions:
            logger.info("🔍 Rule Evaluation Details:")
            logger.info("Available rules and their conditions:")
            for rule in decision_engine.rules:
                logger.info(f"  Rule '{rule['id']}':")
                logger.info(f"    Conditions: {rule['when']}")
                logger.info(f"    Template: {rule['action']['template_id']}")
                logger.info(f"    Priority: {rule['action']['priority']}")
            
            logger.info("💡 Checking why rules didn't match:")
            logger.info(f"  User eligibility: unsubscribed={features.get('unsubscribed', 'unknown')}, emails_sent_7d={features.get('emails_sent_7d', 'unknown')}")
            logger.info(f"  Key features: recency_days={features.get('recency_days', 'unknown')}, top_topics={features.get('top_topics', 'unknown')}")
        
        # Step 5: Send emails (optional)
        if decisions and email_service:
            logger.info("📧 Sending personalized emails...")
            for decision in decisions:
                result = email_service.send_email(
                    to_email=email,
                    template_name=decision['template_id'],  # Fixed key name from 'template' to 'template_id'
                    context=decision.get('features', {})  # Use features as context
                )
                logger.info(f"✅ Email sent: {decision['template_id']} - {result}")
        elif decisions and not email_service:
            logger.info("📧 Email campaigns generated (but not sent - email service unavailable):")
            for decision in decisions:
                logger.info(f"  - Rule ID: {decision['rule_id']}")
                logger.info(f"  - Template ID: {decision['template_id']}")
                logger.info(f"  - Priority: {decision['priority']}")
                logger.info(f"  - User Email: {decision['user_email']}")
                logger.info(f"  - Timestamp: {decision['timestamp']}")
                
                # Generate and display actual email content
                ai_triggers = features.get('ai_email_triggers', [])
                email_content = template_service.generate_email_content(
                    decision['template_id'], 
                    features, 
                    ai_triggers
                )
                
                if email_content:
                    logger.info(f"\n📧 Generated Email Content:")
                    logger.info(f"Subject: {email_content['subject']}")
                    logger.info(f"Content:\n{email_content['content']}")
                    logger.info(f"Generated at: {email_content['generated_at']}")
                else:
                    logger.warning(f"Failed to generate email content for template: {decision['template_id']}")
        else:
            logger.info("ℹ️ No email campaigns triggered for this user")
        
        logger.info(f"🎉 Successfully processed user: {email}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to process user {email}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Process AI engine for a single user')
    parser.add_argument('email', help='User email address to process')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--skip-email', action='store_true', help='Skip email sending (for testing)')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    success = process_single_user(args.email, skip_email=args.skip_email)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()



# make process-user EMAIL=sharath_b2c@yopmail.com
