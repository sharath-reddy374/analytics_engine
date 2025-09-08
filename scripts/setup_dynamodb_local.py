#!/usr/bin/env python3
"""
Setup script for DynamoDB Local testing
This script initializes DynamoDB tables for local development
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.dynamodb_connection import init_dynamodb
from database.dynamodb_models import (
    UserProfileModel, ConversationModel, TestAttemptModel,
    PresentationModel, CourseModel, CourseEnrollmentModel,
    CourseProgressModel, EmailCampaignModel
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_dynamodb():
    """Initialize DynamoDB tables and create sample data"""
    try:
        # Initialize tables
        logger.info("Initializing DynamoDB tables...")
        init_dynamodb()
        
        # Create model instances
        user_model = UserProfileModel()
        conversation_model = ConversationModel()
        test_model = TestAttemptModel()
        presentation_model = PresentationModel()
        course_model = CourseModel()
        enrollment_model = CourseEnrollmentModel()
        progress_model = CourseProgressModel()
        campaign_model = EmailCampaignModel()
        
        # Create sample user
        sample_user = {
            "user_id": "test-user-123",
            "email": "test@example.com",
            "name": "Test User",
            "registration_date": "2024-01-01T00:00:00Z",
            "subscription_type": "premium",
            "learning_preferences": {
                "subjects": ["mathematics", "science"],
                "difficulty_level": "intermediate",
                "learning_style": "visual"
            }
        }
        
        user_model.create_user(sample_user)
        logger.info(f"Created sample user: {sample_user['email']}")
        
        # Create sample conversation
        sample_conversation = {
            "user_id": "test-user-123",
            "conversation_type": "chat",
            "topic": "algebra help",
            "messages": [
                {
                    "role": "user",
                    "content": "I need help with quadratic equations",
                    "timestamp": "2024-01-15T10:00:00Z"
                },
                {
                    "role": "assistant", 
                    "content": "I'd be happy to help you with quadratic equations! Let's start with the basics.",
                    "timestamp": "2024-01-15T10:00:30Z"
                }
            ],
            "sentiment": "positive",
            "engagement_score": 0.8
        }
        
        conversation_model.create_conversation(sample_conversation)
        logger.info("Created sample conversation")
        
        # Create sample test attempt
        sample_test = {
            "user_id": "test-user-123",
            "test_type": "quiz",
            "subject": "mathematics",
            "topic": "algebra",
            "score": 85,
            "total_questions": 10,
            "correct_answers": 8,
            "time_spent": 1200,  # 20 minutes
            "difficulty": "intermediate"
        }
        
        test_model.create_attempt(sample_test)
        logger.info("Created sample test attempt")
        
        logger.info("DynamoDB setup completed successfully!")
        logger.info("You can now run the AI engine with: python -m uvicorn api.main:app --reload")
        
    except Exception as e:
        logger.error(f"Error setting up DynamoDB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_dynamodb()
