from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from database.models import Event, UserDailyFeatures, AppUser, ConvoSummary, EmailSend
from datetime import datetime, date, timedelta
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class FeatureEngine:
    """
    Service for computing user features and analytics
    """
    
    def compute_daily_features(self, db: Session, target_date: date = None) -> int:
        """
        Compute daily features for all users
        Returns number of users processed
        """
        if target_date is None:
            target_date = date.today()
        
        users = db.query(AppUser).all()
        processed_count = 0
        
        for user in users:
            try:
                features = self._compute_user_features(user.user_id, target_date, db)
                self._upsert_user_features(user.user_id, target_date, features, db)
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to compute features for user {user.user_id}: {str(e)}")
        
        db.commit()
        logger.info(f"Computed features for {processed_count} users on {target_date}")
        return processed_count
    
    def _compute_user_features(self, user_id: str, as_of_date: date, db: Session) -> Dict[str, Any]:
        """Compute features for a specific user"""
        
        # Date ranges
        today = datetime.combine(as_of_date, datetime.min.time())
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        # Recency: days since last activity
        last_activity = db.query(func.max(Event.ts)).filter(
            Event.user_id == user_id,
            Event.ts <= today
        ).scalar()
        
        recency_days = (today - last_activity).days if last_activity else 999
        
        # Frequency: activity count in last 7 days
        frequency_7d = db.query(func.count(Event.event_id)).filter(
            Event.user_id == user_id,
            Event.ts >= week_ago,
            Event.ts <= today,
            Event.name.in_(['login', 'convo_msg', 'test_attempt', 'presentation_progress'])
        ).scalar() or 0
        
        # Minutes spent in last 7 days (from presentation events)
        presentation_events = db.query(Event).filter(
            Event.user_id == user_id,
            Event.ts >= week_ago,
            Event.ts <= today,
            Event.name == 'presentation_progress'
        ).all()
        
        minutes_7d = self._calculate_presentation_minutes(presentation_events)
        
        # Test attempts in last 7 days
        tests_7d = db.query(func.count(Event.event_id)).filter(
            Event.user_id == user_id,
            Event.ts >= week_ago,
            Event.ts <= today,
            Event.name == 'test_attempt'
        ).scalar() or 0
        
        # Average score change over 30 days
        avg_score_change_30d = self._calculate_score_trend(user_id, month_ago, today, db)
        
        # Top topics from recent conversations
        top_topics = self._get_top_topics(user_id, week_ago, today, db)
        
        # Subject affinity
        subject_affinity = self._calculate_subject_affinity(user_id, month_ago, today, db)
        
        # Conversation sentiment (7-day average)
        convo_sentiment_7d_avg = self._calculate_sentiment_avg(user_id, week_ago, today, db)
        
        # Churn risk assessment
        churn_risk = self._assess_churn_risk(recency_days, frequency_7d, convo_sentiment_7d_avg)
        
        # Email fatigue metrics
        last_email_ts, emails_sent_7d = self._get_email_metrics(user_id, week_ago, today, db)
        
        # Check unsubscribe status
        unsubscribed = self._check_unsubscribe_status(user_id, db)
        
        return {
            'recency_days': recency_days,
            'frequency_7d': frequency_7d,
            'minutes_7d': minutes_7d,
            'tests_7d': tests_7d,
            'avg_score_change_30d': avg_score_change_30d,
            'top_topics': top_topics,
            'subject_affinity': subject_affinity,
            'convo_sentiment_7d_avg': convo_sentiment_7d_avg,
            'churn_risk': churn_risk,
            'last_email_ts': last_email_ts,
            'emails_sent_7d': emails_sent_7d,
            'unsubscribed': unsubscribed
        }
    
    def _calculate_presentation_minutes(self, events: List[Event]) -> int:
        """Calculate total minutes from presentation events"""
        total_minutes = 0
        
        # Group events by presentation and calculate time spent
        presentation_sessions = {}
        
        for event in events:
            presentation_id = event.props.get('presentation_id')
            trigger = event.props.get('trigger')
            
            if not presentation_id:
                continue
            
            if presentation_id not in presentation_sessions:
                presentation_sessions[presentation_id] = {'start': None, 'end': None}
            
            if trigger == 'start':
                presentation_sessions[presentation_id]['start'] = event.ts
            elif trigger == 'end':
                presentation_sessions[presentation_id]['end'] = event.ts
        
        # Calculate duration for each session
        for session in presentation_sessions.values():
            if session['start'] and session['end']:
                duration = (session['end'] - session['start']).total_seconds() / 60
                total_minutes += max(0, min(duration, 120))  # Cap at 2 hours per session
        
        return int(total_minutes)
    
    def _calculate_score_trend(self, user_id: str, start_date: datetime, end_date: datetime, db: Session) -> float:
        """Calculate average score change trend"""
        test_events = db.query(Event).filter(
            Event.user_id == user_id,
            Event.ts >= start_date,
            Event.ts <= end_date,
            Event.name == 'test_attempt'
        ).order_by(Event.ts).all()
        
        if len(test_events) < 5:
            return 0.0
        
        # Calculate weekly averages
        weekly_scores = {}
        for event in test_events:
            week = event.ts.isocalendar()[1]
            is_correct = event.props.get('is_correct', False)
            
            if week not in weekly_scores:
                weekly_scores[week] = []
            weekly_scores[week].append(1.0 if is_correct else 0.0)
        
        # Calculate trend
        week_averages = []
        for week in sorted(weekly_scores.keys()):
            avg_score = sum(weekly_scores[week]) / len(weekly_scores[week])
            week_averages.append(avg_score)
        
        if len(week_averages) < 2:
            return 0.0
        
        # Simple linear trend
        return week_averages[-1] - week_averages[0]
    
    def _get_top_topics(self, user_id: str, start_date: datetime, end_date: datetime, db: Session) -> List[str]:
        """Get top topics from recent conversations"""
        summaries = db.query(ConvoSummary).filter(
            ConvoSummary.user_id == user_id,
            ConvoSummary.started_at >= start_date,
            ConvoSummary.started_at <= end_date
        ).all()
        
        topic_counts = {}
        for summary in summaries:
            for topic in summary.topics or []:
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
        
        # Return top 5 topics
        sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
        return [topic for topic, count in sorted_topics[:5]]
    
    def _calculate_subject_affinity(self, user_id: str, start_date: datetime, end_date: datetime, db: Session) -> Dict[str, float]:
        """Calculate subject affinity scores"""
        events = db.query(Event).filter(
            Event.user_id == user_id,
            Event.ts >= start_date,
            Event.ts <= end_date,
            Event.name.in_(['test_attempt', 'presentation_progress', 'convo_msg'])
        ).all()
        
        subject_time = {}
        total_time = 0
        
        for event in events:
            subject = event.props.get('subject')
            if subject:
                # Weight recent events more heavily
                days_ago = (datetime.utcnow() - event.ts).days
                weight = max(0.1, 1.0 - (days_ago / 30.0))
                
                subject_time[subject] = subject_time.get(subject, 0) + weight
                total_time += weight
        
        # Normalize to probabilities
        if total_time > 0:
            return {subject: time / total_time for subject, time in subject_time.items()}
        else:
            return {}
    
    def _calculate_sentiment_avg(self, user_id: str, start_date: datetime, end_date: datetime, db: Session) -> float:
        """Calculate average conversation sentiment"""
        summaries = db.query(ConvoSummary).filter(
            ConvoSummary.user_id == user_id,
            ConvoSummary.started_at >= start_date,
            ConvoSummary.started_at <= end_date
        ).all()
        
        if not summaries:
            return 0.0
        
        sentiments = [float(summary.sentiment) for summary in summaries if summary.sentiment is not None]
        return sum(sentiments) / len(sentiments) if sentiments else 0.0
    
    def _assess_churn_risk(self, recency_days: int, frequency_7d: int, sentiment: float) -> str:
        """Assess churn risk based on engagement metrics"""
        risk_score = 0
        
        # Recency factor
        if recency_days > 7:
            risk_score += 2
        elif recency_days > 3:
            risk_score += 1
        
        # Frequency factor
        if frequency_7d < 2:
            risk_score += 2
        elif frequency_7d < 5:
            risk_score += 1
        
        # Sentiment factor
        if sentiment < -0.3:
            risk_score += 2
        elif sentiment < 0:
            risk_score += 1
        
        if risk_score >= 4:
            return 'high'
        elif risk_score >= 2:
            return 'medium'
        else:
            return 'low'
    
    def _get_email_metrics(self, user_id: str, start_date: datetime, end_date: datetime, db: Session) -> tuple:
        """Get email sending metrics"""
        emails = db.query(EmailSend).filter(
            EmailSend.user_id == user_id,
            EmailSend.ts >= start_date,
            EmailSend.ts <= end_date
        ).all()
        
        emails_sent_7d = len(emails)
        last_email_ts = max([email.ts for email in emails]) if emails else None
        
        return last_email_ts, emails_sent_7d
    
    def _check_unsubscribe_status(self, user_id: str, db: Session) -> bool:
        """Check if user has unsubscribed"""
        from database.models import Unsubscribe
        
        unsubscribe = db.query(Unsubscribe).filter(Unsubscribe.user_id == user_id).first()
        return unsubscribe is not None
    
    def _upsert_user_features(self, user_id: str, as_of_date: date, features: Dict[str, Any], db: Session):
        """Insert or update user daily features"""
        existing = db.query(UserDailyFeatures).filter(
            UserDailyFeatures.user_id == user_id,
            UserDailyFeatures.as_of == as_of_date
        ).first()
        
        if existing:
            for key, value in features.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            new_features = UserDailyFeatures(
                user_id=user_id,
                as_of=as_of_date,
                **features
            )
            db.add(new_features)
