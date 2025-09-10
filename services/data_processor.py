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
        
        print(f"🔍 Processing {len(conversation_data)} conversation records...")
        
        for idx, record in enumerate(conversation_data):
            try:
                print(f"📝 Processing record {idx + 1}: type={type(record)}")
                
                if isinstance(record, list):
                    print(f"⚠️ Record is a list with {len(record)} items, skipping...")
                    continue
                
                if not isinstance(record, dict):
                    print(f"⚠️ Record is not a dict: {type(record)}, skipping...")
                    continue
                
                print(f"📋 Record keys: {list(record.keys())}")
                
                conversation_list = []
                user_email = record.get('email', '')
                timestamp = record.get('time', record.get('timestamp', record.get('created_at', '')))
                
                # The user's data structure has 'data' as a list of conversation objects
                if 'data' in record:
                    data_field = record['data']
                    print(f"📊 Found 'data' field of type: {type(data_field)}")
                    
                    if isinstance(data_field, list):
                        # This is the correct structure for the user's data
                        conversation_list = data_field
                        print(f"✅ Found conversation list with {len(conversation_list)} messages")
                    elif isinstance(data_field, str):
                        try:
                            data = json.loads(data_field)
                            if isinstance(data, list):
                                conversation_list = data
                                print(f"✅ Parsed JSON conversation list with {len(conversation_list)} messages")
                            else:
                                print(f"❌ Parsed JSON is not a list: {type(data)}")
                                continue
                        except json.JSONDecodeError as e:
                            print(f"❌ Failed to parse JSON data: {e}")
                            continue
                    else:
                        print(f"❌ Data field is unexpected type: {type(data_field)}")
                        continue
                
                # Fallback: Direct conversation field
                elif 'conversation' in record:
                    conversation_list = record['conversation']
                    print(f"✅ Found direct 'conversation' field with {len(conversation_list) if isinstance(conversation_list, list) else 'non-list'} items")
                
                if not isinstance(conversation_list, list):
                    print(f"❌ Conversation data is not a list: {type(conversation_list)}")
                    continue
                
                if not conversation_list:
                    print(f"⚠️ Empty conversation list")
                    continue
                
                print(f"🎯 Processing {len(conversation_list)} conversation messages...")
                
                session_id = self._generate_session_id(timestamp, user_email)
                
                for i, message in enumerate(conversation_list):
                    if not isinstance(message, dict):
                        print(f"⚠️ Message {i} is not a dict: {type(message)}")
                        continue
                    
                    print(f"💬 Message {i} keys: {list(message.keys())}")
                    
                    user_text = message.get('user', '')
                    ai_text = message.get('bot', '')
                    msg_time = message.get('time', timestamp)
                    
                    # Create user message event if user text exists
                    if user_text and user_text.strip():
                        user_event = {
                            "event_id": self._generate_event_id(),
                            "user_id": user_email,
                            "ts": self._parse_timestamp(msg_time),
                            "name": "convo_msg",
                            "source": "web",
                            "session_id": session_id,
                            "props": {
                                "role": "user",
                                "content": user_text.strip(),
                                "text": user_text.strip(),
                                "message_index": i,
                                "avatar": record.get('avatar', '')
                            }
                        }
                        events.append(user_event)
                        print(f"✅ Added user message: {user_text[:50]}...")
                    
                    # Create AI message event if AI text exists
                    if ai_text and ai_text.strip():
                        cleaned_ai_text = self._clean_ssml(ai_text)
                        ai_event = {
                            "event_id": self._generate_event_id(),
                            "user_id": user_email,
                            "ts": self._parse_timestamp(msg_time),
                            "name": "convo_msg",
                            "source": "web",
                            "session_id": session_id,
                            "props": {
                                "role": "ai",
                                "content": cleaned_ai_text,
                                "text": cleaned_ai_text,
                                "message_index": i,
                                "avatar": record.get('avatar', '')
                            }
                        }
                        events.append(ai_event)
                        print(f"✅ Added AI message: {cleaned_ai_text[:50]}...")
                        
            except Exception as e:
                print(f"❌ Error processing conversation record {idx}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"🎉 Generated {len(events)} conversation events total")
        return events
    
    def process_test_attempts(self, test_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert test series data to normalized events"""
        events = []
        
        for record in test_data:
            try:
                responses = record.get('Response', {})
                user_email = record.get('email', '')
                subject = record.get('Subject', 'Unknown')
                
                if isinstance(responses, dict):
                    for timestamp, response_list in responses.items():
                        if isinstance(response_list, list):
                            for response_data in response_list:
                                if isinstance(response_data, dict):
                                    # Extract actual response data structure
                                    correct_response = response_data.get('Correct_Response')
                                    user_response = response_data.get('Response')
                                    question_text = response_data.get('Question', '')
                                    
                                    # Calculate if answer was correct
                                    is_correct = (correct_response == user_response) if correct_response is not None else False
                                    
                                    event = {
                                        "event_id": self._generate_event_id(),
                                        "user_id": user_email,
                                        "ts": self._parse_timestamp(timestamp),
                                        "name": "test_attempt",
                                        "source": "web",
                                        "props": {
                                            "Response": responses,  # Include full Response data for FeatureEngine
                                            "Subject": subject,
                                            "series_id": record.get('series_id', record.get('id')),
                                            "series_title": record.get('series_title', record.get('title')),
                                            "question": question_text,
                                            "user_response": user_response,
                                            "correct_response": correct_response,
                                            "is_correct": is_correct,
                                            "topic": response_data.get('Topic', subject)
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
        
        if user_data.get('course_plans'):
            all_events.extend(self.process_icp_data(user_data['course_plans']))
        
        # Sort events by timestamp
        all_events.sort(key=lambda x: x['ts'])
        
        return all_events
    
    def process_icp_data(self, icp_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert ICP (Individualized Course Plans) data to normalized events"""
        events = []
        
        print(f"🎓 Processing {len(icp_data)} ICP course records...")
        
        for record in icp_data:
            try:
                user_email = record.get('email', '')
                course_id = record.get('id', record.get('course_id', 'unknown'))
                
                # Extract status information from real ICP_Prod structure
                status = record.get('status', {})
                if isinstance(status, dict):
                    completed_lessons = status.get('completedLesson', 0)
                    if hasattr(completed_lessons, '__float__'):
                        completed_lessons = float(completed_lessons)
                    elif not isinstance(completed_lessons, (int, float)):
                        completed_lessons = 0
                    
                    total_lessons = 25  # User specified: always divide by 25 for completion percentage
                    current_lesson = status.get('currentLesson', 1)
                    progress = status.get('progress', 0)
                    
                    completion_rate = float(completed_lessons) / 25.0
                    
                    print(f"📚 Course {course_id}: {completed_lessons}/25 lessons = {completion_rate:.1%} complete")
                    
                    # Create ICP progress event
                    event = {
                        "event_id": self._generate_event_id(),
                        "user_id": user_email,
                        "ts": self._parse_timestamp(record.get('created_at', record.get('updated_at', ''))),
                        "name": "icp_progress",
                        "source": "web",
                        "props": {
                            "id": course_id,
                            "title": record.get('title', 'Unknown Course'),
                            "status": status,  # Include full status for FeatureEngine
                            "completed_lessons": float(completed_lessons),  # Convert to float
                            "total_lessons": total_lessons,
                            "current_lesson": current_lesson,
                            "progress": progress,
                            "completion_rate": completion_rate
                        }
                    }
                    events.append(event)
                    
            except Exception as e:
                print(f"❌ Error processing ICP record: {e}")
                continue
        
        print(f"✅ Generated {len(events)} ICP progress events")
        return events
    
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
