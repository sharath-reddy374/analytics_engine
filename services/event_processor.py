from sqlalchemy.orm import Session
from database.connection import SessionLocal
from database.models import Event, ConvoSummary, UserDailyFeatures
from services.llm_service import LLMService
from services.feature_engine import FeatureEngine
import logging
from typing import List
from datetime import datetime, timedelta
import uuid

logger = logging.getLogger(__name__)

class EventProcessor:
    """
    Service for processing events and triggering downstream workflows
    """
    
    def __init__(self):
        self.llm_service = LLMService()
        self.feature_engine = FeatureEngine()
    
    async def process_events_async(self, event_ids: List[str]):
        """Process multiple events asynchronously"""
        db = SessionLocal()
        try:
            for event_id in event_ids:
                await self.process_single_event(event_id, db)
        finally:
            db.close()
    
    async def process_single_event(self, event_id: str, db: Session = None):
        """Process a single event"""
        if db is None:
            db = SessionLocal()
            close_db = True
        else:
            close_db = False
        
        try:
            event = db.query(Event).filter(Event.event_id == event_id).first()
            if not event:
                logger.warning(f"Event {event_id} not found")
                return
            
            # Process based on event type
            if event.name == 'convo_msg':
                await self._process_conversation_event(event, db)
            elif event.name in ['test_attempt', 'presentation_progress']:
                await self._process_learning_event(event, db)
            elif event.name == 'login':
                await self._process_login_event(event, db)
            
            logger.info(f"Processed event {event_id} of type {event.name}")
            
        except Exception as e:
            logger.error(f"Failed to process event {event_id}: {str(e)}")
        finally:
            if close_db:
                db.close()
    
    async def _process_conversation_event(self, event: Event, db: Session):
        """Process conversation message events"""
        if not event.session_id:
            return
        
        # Check if we need to summarize this session
        session_events = db.query(Event).filter(
            Event.session_id == event.session_id,
            Event.name == 'convo_msg'
        ).order_by(Event.ts).all()
        
        # Trigger summarization if session has enough messages or is old enough
        if len(session_events) >= 10 or self._is_session_old(session_events):
            await self._summarize_conversation_session(event.session_id, session_events, db)
    
    async def _process_learning_event(self, event: Event, db: Session):
        """Process learning-related events (tests, presentations)"""
        # Update user's learning progress
        # This could trigger immediate feature updates or recommendations
        pass
    
    async def _process_login_event(self, event: Event, db: Session):
        """Process login events"""
        # Update user's last login time
        from database.models import AppUser
        
        user = db.query(AppUser).filter(AppUser.user_id == event.user_id).first()
        if user:
            user.last_login_at = event.ts
            db.commit()
    
    async def _summarize_conversation_session(self, session_id: str, events: List[Event], db: Session):
        """Summarize a conversation session using LLM"""
        try:
            # Build conversation text
            conversation_text = []
            for event in events:
                role = event.props.get('role', 'user')
                text = event.props.get('text', '')
                conversation_text.append(f"{role}: {text}")
            
            full_conversation = "\n".join(conversation_text)
            
            # Get LLM analysis
            analysis = await self.llm_service.analyze_conversation(full_conversation)
            
            # Create or update conversation summary
            summary = db.query(ConvoSummary).filter(ConvoSummary.session_id == session_id).first()
            
            if not summary:
                summary = ConvoSummary(
                    session_id=session_id,
                    user_id=events[0].user_id,
                    started_at=events[0].ts,
                    ended_at=events[-1].ts,
                    summary=analysis['summary'],
                    topics=analysis['topics'],
                    sentiment=analysis['sentiment'],
                    needs=analysis['needs'],
                    embedding=analysis['embedding']
                )
                db.add(summary)
            else:
                summary.ended_at = events[-1].ts
                summary.summary = analysis['summary']
                summary.topics = analysis['topics']
                summary.sentiment = analysis['sentiment']
                summary.needs = analysis['needs']
                summary.embedding = analysis['embedding']
            
            db.commit()
            logger.info(f"Summarized conversation session {session_id}")
            
        except Exception as e:
            logger.error(f"Failed to summarize session {session_id}: {str(e)}")
    
    def _is_session_old(self, events: List[Event]) -> bool:
        """Check if conversation session is old enough to summarize"""
        if not events:
            return False
        
        last_event_time = max(event.ts for event in events)
        return datetime.utcnow() - last_event_time > timedelta(hours=1)
