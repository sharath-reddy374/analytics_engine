from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import json
import re
from database.dynamodb_models import DataFetcher

class DataProcessor:
    """Processes raw DynamoDB data into normalized events"""
    
    def __init__(self):
        self.data_fetcher = DataFetcher()
    
    def process_user_profile(self, profile_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert user profile to normalized events"""
        events = []
        
        if not profile_data:
            return events
        
        # Create user profile upsert event
        event = {
            "event_id": self._generate_event_id(),
            "user_id": profile_data.get('email', ''),  # Using email as user_id
            "ts": self._parse_timestamp(profile_data.get('created_at', '')),
            "name": "user_profile_upsert",
            "source": "dashboard",
            "props": {
                "email": profile_data.get('email'),
                "first_name": profile_data.get('first_name'),
                "last_name": profile_data.get('last_name'),
                "plan": profile_data.get('Subscription'),
                "grade": profile_data.get('grade'),
                "avatar": profile_data.get('avatar'),
                "consent_email": not profile_data.get('expiredPassword', False)
            }
        }
        events.append(event)
        
        return events
    
    def process_conversation_history(self, conversation_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert conversation history to normalized events"""
        events = []
        
        for record in conversation_data:
            try:
                # Parse the conversation data
                data = json.loads(record.get('data', '{}')) if isinstance(record.get('data'), str) else record.get('data', {})
                chat_history = data.get('conversation_history', {}).get('chat_history', [])
                
                session_id = self._generate_session_id(record.get('timestamp', ''), data.get('email', ''))
                
                # Process each message pair
                for i in range(0, len(chat_history), 2):
                    if i + 1 < len(chat_history):
                        user_msg = chat_history[i]
                        ai_msg = chat_history[i + 1]
                        
                        # User message event
                        user_event = {
                            "event_id": self._generate_event_id(),
                            "user_id": data.get('email', ''),
                            "ts": self._parse_timestamp(record.get('timestamp', '')),
                            "name": "convo_msg",
                            "source": "web",
                            "session_id": session_id,
                            "props": {
                                "role": "user",
                                "text": user_msg.get('user', ''),
                                "avatar": data.get('avatar', '')
                            }
                        }
                        events.append(user_event)
                        
                        # AI message event
                        ai_text = self._clean_ssml(ai_msg.get('bot', ''))
                        ai_event = {
                            "event_id": self._generate_event_id(),
                            "user_id": data.get('email', ''),
                            "ts": self._parse_timestamp(record.get('timestamp', '')),
                            "name": "convo_msg",
                            "source": "web",
                            "session_id": session_id,
                            "props": {
                                "role": "ai",
                                "text": ai_text,
                                "avatar": data.get('avatar', '')
                            }
                        }
                        events.append(ai_event)
                        
            except Exception as e:
                print(f"Error processing conversation record: {e}")
                continue
        
        return events
    
    def process_test_attempts(self, test_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert test series data to normalized events"""
        events = []
        
        for record in test_data:
            try:
                responses = record.get('Response', {})
                
                for timestamp, response_data in responses.items():
                    if isinstance(response_data, list):
                        for question_data in response_data:
                            event = {
                                "event_id": self._generate_event_id(),
                                "user_id": record.get('email', ''),
                                "ts": self._parse_timestamp(timestamp),
                                "name": "test_attempt",
                                "source": "web",
                                "props": {
                                    "series_id": record.get('series_id'),
                                    "series_title": record.get('series_title'),
                                    "subject": record.get('Subject'),
                                    "question": question_data.get('Question'),
                                    "response_idx": question_data.get('Response'),
                                    "correct_idx": question_data.get('Correct_Answer'),
                                    "is_correct": question_data.get('Response') == question_data.get('Correct_Answer'),
                                    "topic": question_data.get('Topic')
                                }
                            }
                            events.append(event)
                            
            except Exception as e:
                print(f"Error processing test record: {e}")
                continue
        
        return events
    
    def process_learning_records(self, learning_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert presentation usage to normalized events"""
        events = []
        
        for record in learning_data:
            try:
                responses = record.get('Response', {})
                
                for timestamp, response_data in responses.items():
                    if isinstance(response_data, list):
                        for progress_data in response_data:
                            event = {
                                "event_id": self._generate_event_id(),
                                "user_id": record.get('email', ''),
                                "ts": self._parse_timestamp(timestamp),
                                "name": "presentation_progress",
                                "source": "web",
                                "props": {
                                    "presentation_id": record.get('presentation_id'),
                                    "presentation_name": record.get('name'),
                                    "trigger": progress_data.get('trigger'),
                                    "slide_number": progress_data.get('slide_number'),
                                    "chapter": progress_data.get('chapter'),
                                    "total_length": progress_data.get('total_length'),
                                    "is_completed": progress_data.get('isCompleted', False)
                                }
                            }
                            events.append(event)
                            
            except Exception as e:
                print(f"Error processing learning record: {e}")
                continue
        
        return events
    
    def process_all_user_data(self, user_email: str) -> List[Dict[str, Any]]:
        """Process all data for a single user"""
        all_events = []
        
        # Get all user data
        user_data = self.data_fetcher.get_all_user_data(user_email)
        
        # Process each data type
        if user_data['profile']:
            all_events.extend(self.process_user_profile(user_data['profile']))
        
        if user_data['conversations']:
            all_events.extend(self.process_conversation_history(user_data['conversations']))
        
        if user_data['test_series']:
            all_events.extend(self.process_test_attempts(user_data['test_series']))
        
        if user_data['test_records']:
            all_events.extend(self.process_test_attempts(user_data['test_records']))
        
        if user_data['learning_records']:
            all_events.extend(self.process_learning_records(user_data['learning_records']))
        
        # Sort events by timestamp
        all_events.sort(key=lambda x: x['ts'])
        
        return all_events
    
    def _generate_event_id(self) -> str:
        """Generate a unique event ID"""
        import uuid
        return str(uuid.uuid4())
    
    def _generate_session_id(self, timestamp: str, user_email: str) -> str:
        """Generate a session ID based on timestamp and user"""
        import hashlib
        session_data = f"{user_email}_{timestamp[:10]}"  # Group by day
        return hashlib.md5(session_data.encode()).hexdigest()[:16]
    
    def _parse_timestamp(self, timestamp_str: str) -> str:
        """Parse various timestamp formats to ISO 8601 UTC"""
        if not timestamp_str:
            return datetime.now(timezone.utc).isoformat()
        
        try:
            # Handle format: "2025-02-11,22:36:49"
            if ',' in timestamp_str:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d,%H:%M:%S")
                return dt.replace(tzinfo=timezone.utc).isoformat()
            
            # Handle ISO format
            if 'T' in timestamp_str:
                return timestamp_str
            
            # Default fallback
            return datetime.now(timezone.utc).isoformat()
            
        except Exception:
            return datetime.now(timezone.utc).isoformat()
    
    def _clean_ssml(self, text: str) -> str:
        """Remove SSML tags from text"""
        if not text:
            return ""
        
        # Remove SSML break tags and other markup
        cleaned = re.sub(r'<break[^>]*/?>', '', text)
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        return cleaned.strip()
