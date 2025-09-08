from sqlalchemy.orm import Session
from database.models import Event, AppUser, ContentItem
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import uuid
import logging
from dateutil import parser
import pytz

logger = logging.getLogger(__name__)

class IngestorService:
    """
    Service for ingesting and normalizing data from 8 source tables
    """
    
    def __init__(self):
        self.source_mappings = {
            'investor_prod': self._process_investor_prod,
            'conversation_history': self._process_conversation_history,
            'InvestorLoginHistory_Prod': self._process_login_history,
            'User_Infinite_TestSeries_Prod': self._process_test_series,
            'TestSereiesRecord_Prod': self._process_test_record,
            'LearningRecord_Prod': self._process_learning_record,
            'Question_Prod': self._process_question_prod,
            'presentation_prod': self._process_presentation_prod,
            'ICP_Prod': self._process_icp_prod
        }
    
    async def process_bulk_data(self, data: List[Dict[str, Any]], db: Session) -> List[Event]:
        """Process bulk data from multiple sources"""
        events = []
        
        for record in data:
            source_table = record.get('source_table')
            if source_table in self.source_mappings:
                try:
                    processed_events = self.source_mappings[source_table](record, db)
                    events.extend(processed_events)
                except Exception as e:
                    logger.error(f"Failed to process record from {source_table}: {str(e)}")
                    continue
        
        # Bulk insert events
        db.add_all(events)
        db.commit()
        
        logger.info(f"Processed {len(events)} events from bulk data")
        return events
    
    async def process_single_event(self, event_data: Dict[str, Any], db: Session) -> Event:
        """Process a single event"""
        event = Event(
            user_id=event_data.get('user_id'),
            ts=self._parse_timestamp(event_data.get('ts')),
            name=event_data.get('name'),
            source=event_data.get('source'),
            session_id=event_data.get('session_id'),
            props=event_data.get('props', {})
        )
        
        db.add(event)
        db.commit()
        db.refresh(event)
        
        return event
    
    def _process_investor_prod(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process investor_prod table records"""
        events = []
        
        # Create or update user
        user_data = record.get('data', {})
        user = self._upsert_user(user_data, db)
        
        # Create user profile upsert event
        events.append(Event(
            user_id=user.user_id,
            ts=self._parse_timestamp(user_data.get('created_at')),
            name='user_profile_upsert',
            source='batch',
            props={
                'plan': user_data.get('Subscription'),
                'grade': user_data.get('grade'),
                'avatar': user_data.get('avatar'),
                'flags': {
                    'PIE_on_off': user_data.get('PIE_on_off'),
                    'Rasa': user_data.get('Rasa'),
                    'gpt3': user_data.get('gpt3')
                }
            }
        ))
        
        # Process conversation messages
        chat_history = user_data.get('conversation_history', {}).get('chat_history', [])
        session_id = str(uuid.uuid4())
        
        for i, message in enumerate(chat_history):
            if isinstance(message, dict):
                events.append(Event(
                    user_id=user.user_id,
                    ts=self._parse_timestamp(user_data.get('created_at')),
                    name='convo_msg',
                    source='web',
                    session_id=session_id,
                    props={
                        'role': 'user' if i % 2 == 0 else 'ai',
                        'text': self._clean_text(message.get('text', '')),
                        'turn_number': i
                    }
                ))
        
        return events
    
    def _process_conversation_history(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process conversation_history table records"""
        events = []
        data = record.get('data', {})
        
        user_id = data.get('user_id')
        if not user_id:
            return events
        
        session_id = str(uuid.uuid4())
        chat_history = data.get('chat_history', [])
        
        for i, message in enumerate(chat_history):
            events.append(Event(
                user_id=user_id,
                ts=self._parse_timestamp(data.get('timestamp')),
                name='convo_msg',
                source='web',
                session_id=session_id,
                props={
                    'role': 'user' if i % 2 == 0 else 'ai',
                    'text': self._clean_text(str(message)),
                    'turn_number': i
                }
            ))
        
        return events
    
    def _process_login_history(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process InvestorLoginHistory_Prod table records"""
        events = []
        data = record.get('data', {})
        
        # Login event
        events.append(Event(
            user_id=data.get('user_id'),
            ts=self._parse_timestamp(data.get('timestamp')),
            name='login',
            source='web',
            props={
                'device': data.get('device'),
                'ip_hash': self._hash_ip(data.get('ip_address')),
                'user_agent': data.get('user_agent')
            }
        ))
        
        return events
    
    def _process_test_series(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process User_Infinite_TestSeries_Prod table records"""
        events = []
        data = record.get('data', {})
        
        series_id = data.get('series_id')
        user_id = data.get('user_id')
        
        # Create content item
        self._upsert_content_item({
            'content_id': series_id,
            'content_type': 'quiz',
            'title': data.get('series_title'),
            'subject': data.get('subject'),
            'metadata': {
                'topic': data.get('topic'),
                'total_questions': len(data.get('Response', {}))
            }
        }, db)
        
        # Process test attempts
        responses = data.get('Response', {})
        for timestamp, response_data in responses.items():
            if isinstance(response_data, list):
                for i, response in enumerate(response_data):
                    events.append(Event(
                        user_id=user_id,
                        ts=self._parse_timestamp(timestamp),
                        name='test_attempt',
                        source='web',
                        props={
                            'series_id': series_id,
                            'series_title': data.get('series_title'),
                            'subject': data.get('subject'),
                            'topic': data.get('topic'),
                            'question_number': i + 1,
                            'response_idx': response.get('response_idx'),
                            'correct_idx': response.get('correct_idx'),
                            'is_correct': response.get('response_idx') == response.get('correct_idx'),
                            'question_text': response.get('question')
                        }
                    ))
        
        return events
    
    def _process_test_record(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process TestSereiesRecord_Prod table records"""
        events = []
        data = record.get('data', {})
        
        user_id = data.get('user_id')
        test_id = data.get('test_id', str(uuid.uuid4()))
        
        # Test session started
        events.append(Event(
            user_id=user_id,
            ts=self._parse_timestamp(data.get('start_time')),
            name='test_session_started',
            source='web',
            props={
                'test_id': test_id,
                'subject': data.get('subject'),
                'test_type': 'ACT Science'
            }
        ))
        
        # Process individual questions
        questions = data.get('questions', [])
        for i, question in enumerate(questions):
            if question.get('Answer_To_The_Question') is not False:  # Skip incomplete
                events.append(Event(
                    user_id=user_id,
                    ts=self._parse_timestamp(data.get('timestamp')),
                    name='test_attempt',
                    source='web',
                    props={
                        'test_id': test_id,
                        'question_number': i + 1,
                        'question_text': question.get('question'),
                        'user_answer': question.get('Answer_To_The_Question'),
                        'correct_answer': question.get('correct_answer'),
                        'is_correct': question.get('Answer_To_The_Question') == question.get('correct_answer')
                    }
                ))
        
        return events
    
    def _process_learning_record(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process LearningRecord_Prod table records"""
        events = []
        data = record.get('data', {})
        
        user_id = data.get('user_id')
        presentation_id = data.get('presentation_id')
        
        # Create content item
        self._upsert_content_item({
            'content_id': presentation_id,
            'content_type': 'presentation',
            'title': data.get('name'),
            'subject': data.get('subject'),
            'metadata': {
                'total_length': data.get('total_length'),
                'chapter': data.get('chapter')
            }
        }, db)
        
        # Process presentation progress events
        responses = data.get('Response', {})
        for timestamp, response_data in responses.items():
            if isinstance(response_data, list):
                for response in response_data:
                    events.append(Event(
                        user_id=user_id,
                        ts=self._parse_timestamp(timestamp),
                        name='presentation_progress',
                        source='web',
                        props={
                            'presentation_id': presentation_id,
                            'trigger': response.get('trigger'),
                            'slide_number': response.get('slide_number'),
                            'chapter': response.get('chapter'),
                            'total_length': response.get('total_length'),
                            'is_completed': response.get('isCompleted')
                        }
                    ))
        
        return events
    
    def _process_question_prod(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process Question_Prod table records"""
        events = []
        data = record.get('data', {})
        
        # Create content items for questions
        questions = data.get('question', [])
        for question in questions:
            content_id = f"question_{uuid.uuid4()}"
            
            self._upsert_content_item({
                'content_id': content_id,
                'content_type': 'question',
                'title': question.get('question_text', '')[:100],
                'subject': data.get('subject'),
                'metadata': {
                    'grade_subject': data.get('grade_subject'),
                    'series_title': data.get('series_title'),
                    'options': question.get('options'),
                    'correct_answer': question.get('correctAnswer')
                }
            }, db)
            
            # Question created event
            events.append(Event(
                user_id=None,  # System generated
                ts=datetime.utcnow(),
                name='question_created',
                source='batch',
                props={
                    'content_id': content_id,
                    'subject': data.get('subject'),
                    'topic': data.get('topic', 'Ancient India'),
                    'grade_subject': data.get('grade_subject')
                }
            ))
        
        return events
    
    def _process_presentation_prod(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process presentation_prod table records"""
        data = record.get('data', {})
        
        # Create content item for presentation
        self._upsert_content_item({
            'content_id': data.get('presentation_id'),
            'content_type': 'presentation',
            'title': data.get('title'),
            'subject': data.get('Subject'),
            'metadata': {
                'grade_subject': data.get('grade_subject'),
                'total_length': data.get('total_length'),
                'chapters': data.get('chapter', [])
            }
        }, db)
        
        return []  # No events, just content metadata
    
    def _process_icp_prod(self, record: Dict[str, Any], db: Session) -> List[Event]:
        """Process ICP_Prod table records"""
        data = record.get('data', {})
        
        # Create content items for lessons and sections
        lessons = data.get('lessons', [])
        for lesson in lessons:
            lesson_id = f"lesson_{lesson.get('lesson_id', uuid.uuid4())}"
            
            self._upsert_content_item({
                'content_id': lesson_id,
                'content_type': 'lesson',
                'title': lesson.get('title', 'Exploring Life'),
                'subject': 'Biology',
                'metadata': {
                    'sections': lesson.get('sections', []),
                    'status_flags': lesson.get('status_flags', {})
                }
            }, db)
        
        return []  # No events, just content metadata
    
    def _upsert_user(self, user_data: Dict[str, Any], db: Session) -> AppUser:
        """Create or update user record"""
        email = user_data.get('email')
        if not email:
            raise ValueError("Email is required for user creation")
        
        user = db.query(AppUser).filter(AppUser.email == email).first()
        
        if not user:
            user = AppUser(
                email=email,
                first_name=user_data.get('first_name'),
                last_name=user_data.get('last_name'),
                plan=user_data.get('Subscription'),
                status='Active' if user_data.get('Subscription') else 'Trial',
                tenant_id=user_data.get('tenant_id'),
                tenant_name=user_data.get('tenant_name'),
                consent_email=user_data.get('expiredPassword') != True,
                created_at=self._parse_timestamp(user_data.get('created_at')),
                raw=self._sanitize_user_data(user_data)
            )
            db.add(user)
        else:
            # Update existing user
            user.first_name = user_data.get('first_name') or user.first_name
            user.last_name = user_data.get('last_name') or user.last_name
            user.plan = user_data.get('Subscription') or user.plan
            user.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(user)
        return user
    
    def _upsert_content_item(self, content_data: Dict[str, Any], db: Session):
        """Create or update content item"""
        content_id = content_data.get('content_id')
        if not content_id:
            return
        
        content = db.query(ContentItem).filter(ContentItem.content_id == content_id).first()
        
        if not content:
            content = ContentItem(**content_data)
            db.add(content)
        else:
            for key, value in content_data.items():
                if hasattr(content, key) and value is not None:
                    setattr(content, key, value)
        
        db.commit()
    
    def _parse_timestamp(self, timestamp_str: Optional[str]) -> datetime:
        """Parse various timestamp formats to UTC datetime"""
        if not timestamp_str:
            return datetime.utcnow()
        
        try:
            # Handle "2025-02-11,22:36:49" format
            if ',' in str(timestamp_str):
                timestamp_str = str(timestamp_str).replace(',', ' ')
            
            dt = parser.parse(str(timestamp_str))
            
            # Convert to UTC if timezone-aware
            if dt.tzinfo is not None:
                dt = dt.astimezone(pytz.UTC).replace(tzinfo=None)
            
            return dt
        except Exception as e:
            logger.warning(f"Failed to parse timestamp {timestamp_str}: {str(e)}")
            return datetime.utcnow()
    
    def _clean_text(self, text: str) -> str:
        """Clean text by removing SSML and other unwanted content"""
        import re
        
        # Remove SSML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _hash_ip(self, ip_address: Optional[str]) -> Optional[str]:
        """Hash IP address for privacy"""
        if not ip_address:
            return None
        
        import hashlib
        return hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    
    def _sanitize_user_data(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive data from user record"""
        sensitive_fields = ['password', 'token', 'link', 'email_content']
        
        sanitized = {}
        for key, value in user_data.items():
            if not any(field in key.lower() for field in sensitive_fields):
                sanitized[key] = value
        
        return sanitized
