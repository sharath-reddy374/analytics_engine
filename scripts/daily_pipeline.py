#!/usr/bin/env python3
"""
Daily data pipeline script for feature computation and email campaigns
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import SessionLocal
from services.feature_engine import FeatureEngine
from services.decision_engine import DecisionEngine
from services.email_service import EmailService
import logging
from datetime import date
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_daily_pipeline():
    """Run the complete daily pipeline"""
    
    db = SessionLocal()
    
    try:
        logger.info("Starting daily pipeline...")
        
        # 1. Compute daily features
        logger.info("Computing daily features...")
        feature_engine = FeatureEngine()
        users_processed = feature_engine.compute_daily_features(db)
        logger.info(f"Computed features for {users_processed} users")
        
        # 2. Run decision engine
        logger.info("Running decision engine...")
        decision_engine = DecisionEngine()
        email_candidates = decision_engine.evaluate_users_for_emails(db)
        logger.info(f"Found {len(email_candidates)} email candidates")
        
        # 3. Send emails
        if email_candidates:
            logger.info("Sending campaign emails...")
            email_service = EmailService()
            results = await email_service.send_campaign_emails(email_candidates, db)
            logger.info(f"Email results: {results}")
        else:
            logger.info("No email candidates found")
        
        logger.info("Daily pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Daily pipeline failed: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(run_daily_pipeline())
