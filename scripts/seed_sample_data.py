#!/usr/bin/env python3
"""
Script to seed the database with sample data for testing
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import SessionLocal, init_db
from database.models import AppUser, Event, ContentItem
from datetime import datetime, timedelta
import uuid
import random

def seed_sample_data():
    """Seed database with sample educational data"""
    
    init_db()
    db = SessionLocal()
    
    try:
        # Create sample users
        users = [
            {
                'email': 'sharath@example.com',
                'first_name': 'Sharath',
                'last_name': 'Kumar',
                'plan': 'Premium',
                'status': 'Active',
                'consent_email': True
            },
            {
                'email': 'alice@example.com',
                'first_name': 'Alice',
                'last_name': 'Johnson',
                'plan': 'Basic',
                'status': 'Active',
                'consent_email': True
            },
            {
                'email': 'bob@example.com',
                'first_name': 'Bob',
                'last_name': 'Smith',
                'plan': 'Trial',
                'status': 'Trial',
                'consent_email': True
            }
        ]
        
        created_users = []
        for user_data in users:
            user = AppUser(**user_data, created_at=datetime.utcnow())
            db.add(user)
            created_users.append(user)
        
        db.commit()
        
        # Create sample content items
        content_items = [
            {
                'content_id': 'bio_cells_101',
                'content_type': 'presentation',
                'title': 'Introduction to Cell Biology',
                'subject': 'Biology',
                'grade_subject': 'Grade 9 Biology'
            },
            {
                'content_id': 'algebra_variables',
                'content_type': 'quiz',
                'title': 'Algebra Variables and Expressions',
                'subject': 'Algebra',
                'grade_subject': 'Grade 8 Math'
            },
            {
                'content_id': 'marketing_4ps',
                'content_type': 'quiz',
                'title': 'Marketing Basics: The 4 Ps',
                'subject': 'Marketing',
                'grade_subject': 'Business Studies'
            }
        ]
        
        for content_data in content_items:
            content = ContentItem(**content_data)
            db.add(content)
        
        db.commit()
        
        # Create sample events
        base_time = datetime.utcnow() - timedelta(days=7)
        
        for i, user in enumerate(created_users):
            # Login events
            for day in range(7):
                event_time = base_time + timedelta(days=day, hours=random.randint(8, 20))
                login_event = Event(
                    user_id=user.user_id,
                    ts=event_time,
                    name='login',
                    source='web',
                    props={'device': 'desktop'}
                )
                db.add(login_event)
            
            # Conversation events
            session_id = str(uuid.uuid4())
            for turn in range(6):
                conv_time = base_time + timedelta(days=2, hours=14, minutes=turn*2)
                role = 'user' if turn % 2 == 0 else 'ai'
                
                if role == 'user':
                    texts = [
                        "Can you help me understand cell structure?",
                        "What's the difference between plant and animal cells?",
                        "I'm confused about algebra variables"
                    ]
                else:
                    texts = [
                        "I'd be happy to help you with cell structure! Let's start with the basics...",
                        "Great question! The main differences are...",
                        "Variables in algebra represent unknown values. Let me explain..."
                    ]
                
                conv_event = Event(
                    user_id=user.user_id,
                    ts=conv_time,
                    name='convo_msg',
                    source='web',
                    session_id=session_id,
                    props={
                        'role': role,
                        'text': random.choice(texts),
                        'turn_number': turn
                    }
                )
                db.add(conv_event)
            
            # Test attempt events
            for attempt in range(random.randint(2, 8)):
                test_time = base_time + timedelta(days=random.randint(1, 6), hours=random.randint(10, 18))
                test_event = Event(
                    user_id=user.user_id,
                    ts=test_time,
                    name='test_attempt',
                    source='web',
                    props={
                        'series_id': random.choice(['algebra_variables', 'bio_cells_101', 'marketing_4ps']),
                        'subject': random.choice(['Biology', 'Algebra', 'Marketing']),
                        'question_number': random.randint(1, 10),
                        'is_correct': random.choice([True, False, True]),  # 66% correct rate
                        'response_idx': random.randint(0, 3),
                        'correct_idx': random.randint(0, 3)
                    }
                )
                db.add(test_event)
            
            # Presentation progress events
            for session in range(random.randint(1, 4)):
                start_time = base_time + timedelta(days=random.randint(1, 6), hours=random.randint(15, 19))
                end_time = start_time + timedelta(minutes=random.randint(10, 45))
                
                # Start event
                start_event = Event(
                    user_id=user.user_id,
                    ts=start_time,
                    name='presentation_progress',
                    source='web',
                    props={
                        'presentation_id': 'bio_cells_101',
                        'trigger': 'start',
                        'slide_number': 1,
                        'chapter': 'Chapter-01'
                    }
                )
                db.add(start_event)
                
                # End event
                end_event = Event(
                    user_id=user.user_id,
                    ts=end_time,
                    name='presentation_progress',
                    source='web',
                    props={
                        'presentation_id': 'bio_cells_101',
                        'trigger': 'end',
                        'slide_number': random.randint(5, 15),
                        'chapter': 'Chapter-01',
                        'is_completed': random.choice([True, False])
                    }
                )
                db.add(end_event)
        
        db.commit()
        print(f"Successfully seeded database with {len(created_users)} users and sample events")
        
    except Exception as e:
        print(f"Failed to seed data: {str(e)}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed_sample_data()
