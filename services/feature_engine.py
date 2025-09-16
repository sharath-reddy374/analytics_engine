from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from database.models import Event, UserDailyFeatures, AppUser, ConvoSummary, EmailSend, Unsubscribe
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class FeatureEngine:
    """
    Service for computing user features and analytics
    """
    
    def __init__(self):
        from services.llm_service import LLMService
        self.llm_service = LLMService()
    
    def compute_user_features(self, email: str, events: List[Dict]) -> Dict[str, Any]:
        """
        Compute features for a specific user from normalized events
        This is the public interface called by the processing pipeline
        """
        if not events:
            return self._get_default_features()
        
        # Convert events to feature calculations
        features = {}
        
        # Calculate recency (days since last activity)
        latest_event = max(events, key=lambda x: x['ts'])
        latest_ts = datetime.fromisoformat(latest_event['ts'].replace('Z', '+00:00'))
        recency_days = (datetime.now(timezone.utc) - latest_ts).days
        features['recency_days'] = recency_days
        
        # Calculate frequency (activity count in last 7 days)
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        recent_events = [e for e in events if datetime.fromisoformat(e['ts'].replace('Z', '+00:00')) >= week_ago]
        features['frequency_7d'] = len(recent_events)
        
        test_events = [e for e in events if e['name'] == 'test_attempt']
        features.update(self._analyze_itp_performance(test_events, week_ago))
        
        icp_events = [e for e in events if e['name'] == 'icp_progress']
        features.update(self._analyze_icp_completion(icp_events))
        
        login_events = [e for e in events if e['name'] == 'login_session']
        features['minutes_7d'] = self._calculate_login_minutes_from_events(login_events, week_ago)
        
        convo_events = [e for e in events if e['name'] == 'convo_msg']
        features.update(self._analyze_conversations_with_ai(convo_events, week_ago))

        # === Trigger convenience booleans for rules engine ===
        trigs = features.get('ai_email_triggers', []) or []
        def _is_dict(d): return isinstance(d, dict)

        features['has_exam_last_minute_prep'] = any(
            _is_dict(t) and (
                t.get('message_type') == 'last_minute_prep' or
                (t.get('trigger') in ('exam_prep', 'pre_exam'))
            ) for t in trigs
        )
        features['has_exam_post_checkin'] = any(
            _is_dict(t) and (
                t.get('message_type') == 'how_did_it_go' or
                (t.get('trigger') in ('post_exam', 'exam_followup'))
            ) for t in trigs
        )
        features['has_learning_support'] = any(
            _is_dict(t) and (
                t.get('trigger') == 'learning_support' or
                t.get('trigger_type') == 'learning_support'
            ) for t in trigs
        )
        
        # Calculate subject affinity
        features['subject_affinity'] = self._calculate_subject_affinity_from_events(events)
        
        # Calculate churn risk
        features['churn_risk'] = self._assess_churn_risk(
            features['recency_days'], 
            features['frequency_7d'], 
            features.get('convo_sentiment_7d_avg', 0.0)
        )
        
        # Set default values for other features
        features.update({
            'last_email_ts': None,
            'emails_sent_7d': 0,
            'unsubscribed': False
        })
        
        return features
    
    def _get_default_features(self) -> Dict[str, Any]:
        """Return default features when no events are available"""
        return {
            'recency_days': 999,
            'frequency_7d': 0,
            'minutes_7d': 0,
            'tests_7d': 0,
            'test_accuracy': 0.0,
            'avg_itp_score': 0.0,
            'itp_improvement_trend': 0.0,
            'weak_subjects': [],
            'strong_subjects': [],
            'active_courses': 0,
            'completed_courses': 0,
            'completed_course_titles': [],
            'stalled_courses': [],
            'recent_progress': False,
            'subject_affinity': {},
            'convo_sentiment_7d_avg': 0.0,
            'churn_risk': 'high',
            'last_email_ts': None,
            'emails_sent_7d': 0,
            'unsubscribed': False,
            'has_exam_last_minute_prep': False,
            'has_exam_post_checkin': False,
            'has_learning_support': False,
        }
    
    def _calculate_presentation_minutes_from_events(self, events: List[Dict], since_date: datetime) -> int:
        """Calculate presentation minutes from normalized events"""
        recent_events = [e for e in events if datetime.fromisoformat(e['ts'].replace('Z', '+00:00')) >= since_date]
        
        # Group by presentation and session
        sessions = {}
        for event in recent_events:
            presentation_id = event['props'].get('presentation_id', 'unknown')
            trigger = event['props'].get('trigger')
            
            if presentation_id not in sessions:
                sessions[presentation_id] = {'start': None, 'end': None}
            
            if trigger == 'start':
                sessions[presentation_id]['start'] = datetime.fromisoformat(event['ts'].replace('Z', '+00:00'))
            elif trigger == 'end':
                sessions[presentation_id]['end'] = datetime.fromisoformat(event['ts'].replace('Z', '+00:00'))
        
        # Calculate total minutes
        total_minutes = 0
        for session in sessions.values():
            if session['start'] and session['end']:
                duration = (session['end'] - session['start']).total_seconds() / 60
                total_minutes += max(0, min(duration, 120))  # Cap at 2 hours
        
        return int(total_minutes)
    
    def _calculate_subject_affinity_from_events(self, events: List[Dict]) -> Dict[str, float]:
        """Calculate subject affinity from events"""
        subject_counts = {}
        total_events = 0
        
        for event in events:
            subject = event['props'].get('subject')
            if subject:
                subject_counts[subject] = subject_counts.get(subject, 0) + 1
                total_events += 1
        
        if total_events == 0:
            return {}
        
        return {subject: count / total_events for subject, count in subject_counts.items()}
    
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
        
        # Minutes spent in last 7 days (from login session durations)
        login_events = db.query(Event).filter(
            Event.user_id == user_id,
            Event.ts >= week_ago,
            Event.ts <= today,
            Event.name == 'login_session'
        ).all()
        
        minutes_7d = self._calculate_login_minutes(login_events)
        
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
        
        # Email fatigue metrics
        last_email_ts, emails_sent_7d = self._get_email_metrics(user_id, week_ago, today, db)
        
        # Check unsubscribe status
        unsubscribed = self._check_unsubscribe_status(user_id, db)
        
        icp_events = db.query(Event).filter(
            Event.user_id == user_id,
            Event.ts >= week_ago,
            Event.ts <= today,
            Event.name == 'icp_progress'
        ).all()
        
        icp_features = self._analyze_icp_completion(icp_events)
        
        convo_events = db.query(Event).filter(
            Event.user_id == user_id,
            Event.ts >= week_ago,
            Event.ts <= today,
            Event.name == 'convo_msg'
        ).all()
        
        convo_features = self._analyze_conversations_with_ai(convo_events, week_ago)

        # === Trigger convenience booleans for rules engine (DB path) ===
        trigs = (convo_features or {}).get('ai_email_triggers', []) or []
        def _is_dict(d): return isinstance(d, dict)
        has_exam_last_minute_prep = any(
            _is_dict(t) and (
                t.get('message_type') == 'last_minute_prep' or
                (t.get('trigger') in ('exam_prep', 'pre_exam'))
            ) for t in trigs
        )
        has_exam_post_checkin = any(
            _is_dict(t) and (
                t.get('message_type') == 'how_did_it_go' or
                (t.get('trigger') in ('post_exam', 'exam_followup'))
            ) for t in trigs
        )
        has_learning_support = any(
            _is_dict(t) and (
                t.get('trigger') == 'learning_support' or
                t.get('trigger_type') == 'learning_support'
            ) for t in trigs
        )
        
        return {
            'recency_days': recency_days,
            'frequency_7d': frequency_7d,
            'minutes_7d': minutes_7d,
            'tests_7d': tests_7d,
            'avg_score_change_30d': avg_score_change_30d,
            'top_topics': top_topics,
            'subject_affinity': subject_affinity,
            'convo_sentiment_7d_avg': convo_sentiment_7d_avg,
            'churn_risk': self._assess_churn_risk(recency_days, frequency_7d, convo_sentiment_7d_avg),
            'last_email_ts': last_email_ts,
            'emails_sent_7d': emails_sent_7d,
            'unsubscribed': unsubscribed,
            **icp_features,
            **convo_features,
            'has_exam_last_minute_prep': has_exam_last_minute_prep,
            'has_exam_post_checkin': has_exam_post_checkin,
            'has_learning_support': has_learning_support,
        }
    
    def _calculate_login_minutes(self, events: List[Dict]) -> int:
        """Calculate total minutes from login session durations"""
        total_minutes = 0
        
        # Group events by session and calculate time spent
        login_sessions = {}
        
        for event in events:
            session_id = event.props.get('session_id')
            if not session_id:
                continue
            
            if session_id not in login_sessions:
                login_sessions[session_id] = {'start': None, 'end': None}
            
            login_time = event.props.get('login_time')
            logout_time = event.props.get('logout_time')
            session_duration = event.props.get('session_duration_minutes')
            
            if login_time:
                login_sessions[session_id]['start'] = datetime.fromisoformat(login_time.replace('Z', '+00:00'))
            if logout_time:
                login_sessions[session_id]['end'] = datetime.fromisoformat(logout_time.replace('Z', '+00:00'))
        
        # Calculate duration for each session
        for session in login_sessions.values():
            if session['start'] and session['end']:
                duration = (session['end'] - session['start']).total_seconds() / 60
                total_minutes += max(0, min(duration, 180))  # Cap at 3 hours per session
        
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
    
    def _analyze_itp_performance(self, test_events: List[Dict], week_ago: datetime) -> Dict[str, Any]:
        """
        Analyze ITP (Infinite Test Series) performance with detailed scoring from real data structure
        """
        if not test_events:
            return {
                'test_accuracy': 0.0,
                'tests_7d': 0,
                'avg_itp_score': 0.0,
                'itp_improvement_trend': 0.0,
                'weak_subjects': [],
                'strong_subjects': []
            }
        
        all_responses = []
        subject_performance = {}
        recent_tests = 0
        
        for event in test_events:
            props = event['props']
            
            # Extract Response data (array of test attempts)
            responses = props.get('Response', {})
            if isinstance(responses, dict):
                for timestamp, response_list in responses.items():
                    if isinstance(response_list, list):
                        for response in response_list:
                            if isinstance(response, dict):
                                correct_response = response.get('Correct_Response')
                                user_response = response.get('Response')
                                subject = props.get('Subject', 'Unknown')
                                
                                # Calculate if answer was correct
                                is_correct = (correct_response == user_response) if correct_response is not None else False
                                
                                all_responses.append({
                                    'is_correct': is_correct,
                                    'subject': subject,
                                    'timestamp': timestamp,
                                    'question': response.get('Question', ''),
                                    'correct_response': correct_response,
                                    'user_response': user_response
                                })
                                
                                # Track subject performance
                                if subject not in subject_performance:
                                    subject_performance[subject] = []
                                subject_performance[subject].append(1 if is_correct else 0)
                                
                                # Count recent tests (last 7 days)
                                try:
                                    response_date = datetime.strptime(timestamp, '%Y-%m-%d,%H:%M:%S')
                                    if response_date >= week_ago.replace(tzinfo=None):
                                        recent_tests += 1
                                except:
                                    pass
        
        if not all_responses:
            return {
                'test_accuracy': 0.0,
                'tests_7d': 0,
                'avg_itp_score': 0.0,
                'itp_improvement_trend': 0.0,
                'weak_subjects': [],
                'strong_subjects': []
            }
        
        # Calculate accuracy
        correct_count = sum(1 for r in all_responses if r['is_correct'])
        accuracy = correct_count / len(all_responses)
        
        # Calculate average score (percentage correct)
        avg_score = accuracy * 100
        
        # Calculate improvement trend (recent vs older performance)
        if len(all_responses) >= 10:
            recent_half = all_responses[-len(all_responses)//2:]
            older_half = all_responses[:len(all_responses)//2]
            
            recent_accuracy = sum(1 for r in recent_half if r['is_correct']) / len(recent_half)
            older_accuracy = sum(1 for r in older_half if r['is_correct']) / len(older_half)
            improvement_trend = (recent_accuracy - older_accuracy) * 100
        else:
            improvement_trend = 0.0
        
        # Identify weak and strong subjects
        weak_subjects = []
        strong_subjects = []
        
        for subject, performances in subject_performance.items():
            if len(performances) >= 3:  # Only consider subjects with enough data
                avg_performance = sum(performances) / len(performances)
                if avg_performance < 0.6:
                    weak_subjects.append(subject)
                elif avg_performance > 0.8:
                    strong_subjects.append(subject)
        
        return {
            'test_accuracy': accuracy,
            'tests_7d': recent_tests,
            'avg_itp_score': avg_score,
            'itp_improvement_trend': improvement_trend,
            'weak_subjects': weak_subjects,
            'strong_subjects': strong_subjects
        }
    
    def _analyze_icp_completion(self, icp_events: List[Dict]) -> Dict[str, Any]:
        """
        Analyze ICP completion using section-level fields emitted by DataProcessor.process_icp_data():
          - total_sections, completed_sections, is_completed, progress_percent, completion_rate
          - id/title + ts
        Works with either dict events ({'props': ..., 'ts': ...}) or ORM Event objects (.props, .ts).
        """
        def _get(ev, key, default=None):
            return ev.get(key, default) if isinstance(ev, dict) else getattr(ev, key, default)

        def _get_props(ev) -> Dict[str, Any]:
            p = _get(ev, 'props')
            return p or {}

        def _get_event_time(ev) -> datetime:
            ts = _get(ev, 'ts')
            if isinstance(ts, datetime):
                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            if isinstance(ts, str):
                try:
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except Exception:
                    return datetime.now(timezone.utc)
            return datetime.now(timezone.utc)

        if not icp_events:
            return {
                'icp_completion_rate': 0.0,
                'active_courses': 0,
                'completed_courses': 0,
                'completed_course_titles': [],
                'stalled_courses': [],
                'recent_progress': False
            }

        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        course_progress: Dict[str, Dict[str, Any]] = {}

        # Use the latest event per course_id
        for ev in icp_events:
            props = _get_props(ev)
            if not props:
                continue

            course_id = props.get('id') or props.get('course_id') or 'unknown'
            event_time = _get_event_time(ev)

            total_sections = int(props.get('total_sections') or 0)
            completed_sections = int(props.get('completed_sections') or 0)

            # Prefer boolean from DataProcessor; fall back to equality check
            is_completed = props.get('is_completed')
            if is_completed is None:
                is_completed = (total_sections > 0 and completed_sections >= total_sections)

            title = props.get('title') or props.get('course_title') or 'Unknown Course'

            existing = course_progress.get(course_id)
            if (existing is None) or (event_time > existing['last_activity']):
                course_progress[course_id] = {
                    'total_sections': total_sections,
                    'completed_sections': completed_sections,
                    'is_completed': bool(is_completed),
                    'last_activity': event_time,
                    'recent_activity': event_time >= week_ago,
                    'title': title,
                }
            else:
                if event_time >= week_ago:
                    existing['recent_activity'] = True

        if not course_progress:
            return {
                'icp_completion_rate': 0.0,
                'active_courses': 0,
                'completed_courses': 0,
                'completed_course_titles': [],
                'stalled_courses': [],
                'recent_progress': False
            }

        # Aggregate across courses
        total_completion = 0.0
        active_courses = 0
        completed_courses = 0
        completed_titles: List[str] = []
        stalled_courses: List[str] = []
        recent_progress = any(c['recent_activity'] for c in course_progress.values())

        now = datetime.now(timezone.utc)
        for cid, c in course_progress.items():
            total = c['total_sections'] or 0
            comp = c['completed_sections'] or 0
            completion_rate = (comp / total) if total else 0.0
            total_completion += completion_rate

            if c['is_completed']:
                completed_courses += 1
                completed_titles.append(c['title'])
                logger.info(
                    "📚 Course %s marked as COMPLETED: sections=%s/%s (%.2f%%)",
                    cid, comp, total, completion_rate * 100.0
                )
            elif c['recent_activity']:
                active_courses += 1
            elif completion_rate > 0 and (now - c['last_activity']).days > 14:
                stalled_courses.append(c['title'])

        avg_completion_rate = total_completion / len(course_progress) if course_progress else 0.0

        return {
            'icp_completion_rate': min(avg_completion_rate, 1.0),
            'active_courses': active_courses,
            'completed_courses': completed_courses,
            'completed_course_titles': completed_titles,
            'stalled_courses': stalled_courses,
            'recent_progress': recent_progress
        }
    
    def _calculate_login_minutes_from_events(self, login_events: List[Dict], since_date: datetime) -> int:
        """
        Calculate learning minutes from login session durations in InvestorLoginHistory_Prod
        """
        recent_events = [e for e in login_events if datetime.fromisoformat(e['ts'].replace('Z', '+00:00')) >= since_date]
        
        total_minutes = 0
        for event in recent_events:
            props = event['props']
            
            login_time = props.get('login_time')
            logout_time = props.get('logout_time')
            session_duration = props.get('session_duration_minutes')
            
            # Try to get session info from the event timestamp and device info
            session_start = props.get('Session', {})
            if isinstance(session_start, dict):
                start_time = session_start.get('start_time')
                end_time = session_start.get('end_time')
                if start_time and end_time:
                    try:
                        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                        duration_minutes = (end_dt - start_dt).total_seconds() / 60
                        total_minutes += max(0, min(duration_minutes, 180))  # Cap at 3 hours
                        continue
                    except:
                        pass
            
            if session_duration:
                # If duration is already calculated, use it
                total_minutes += min(float(session_duration), 180)  # Cap at 3 hours per session
            elif login_time and logout_time:
                # Calculate duration from timestamps
                try:
                    login_dt = datetime.fromisoformat(login_time.replace('Z', '+00:00'))
                    logout_dt = datetime.fromisoformat(logout_time.replace('Z', '+00:00'))
                    duration_minutes = (logout_dt - login_dt).total_seconds() / 60
                    total_minutes += max(0, min(duration_minutes, 180))  # Cap at 3 hours
                except:
                    pass
            else:
                conversation_count = len(props.get('data', []))  # Count conversation messages
                device_info = props.get('device_info', {})
                
                if conversation_count > 0:
                    # Estimate ~1.5 minutes per conversation message (more realistic)
                    estimated_minutes = min(conversation_count * 1.5, 90)
                    total_minutes += estimated_minutes
                elif device_info:
                    # If we have device info but no conversation, estimate minimal session time
                    total_minutes += 5  # 5 minute minimum session
        
        return int(total_minutes)
    
    def _analyze_conversations_with_ai(self, convo_events: List[Dict], week_ago: datetime) -> Dict[str, Any]:
        """
        Analyze conversations using OpenAI to extract topics, sentiment, and email triggers.
        Improvements:
        - Sort recent messages by timestamp (stable ordering)
        - Use a larger window (last 50 user msgs)
        - Backfill heuristic triggers even when LLM returns an empty list
        - Align fallback message_type names with email_rules.yaml
        """
        def _parse_ts(ts) -> datetime:
            if isinstance(ts, datetime):
                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            if isinstance(ts, str):
                try:
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except Exception:
                    return datetime.now(timezone.utc)
            return datetime.now(timezone.utc)

        if not convo_events:
            return {
                'conversations_7d': 0,
                'top_topics': [],
                'convo_sentiment_7d_avg': 0.0,
                'ai_email_triggers': [],
                'conversation_insights': {}
            }

        # Keep only user messages within 7d, then sort by time
        user_messages = [e for e in convo_events if e['props'].get('role') == 'user']
        recent_messages = [e for e in user_messages if _parse_ts(e['ts']) >= week_ago]
        if not recent_messages:
            return {
                'conversations_7d': 0,
                'top_topics': [],
                'convo_sentiment_7d_avg': 0.0,
                'ai_email_triggers': [],
                'conversation_insights': {}
            }
        recent_messages.sort(key=lambda m: _parse_ts(m['ts']))  # <<< important

        # Prepare last N for LLM
        WINDOW = 50
        msgs_for_llm = recent_messages[-WINDOW:]
        conversations_for_analysis = []
        for msg in msgs_for_llm:
            content = msg['props'].get('content', msg['props'].get('message', ''))
            conversations_for_analysis.append({
                'content': content,
                'timestamp': msg['ts'],
                'role': 'user'
            })

        # Helper: heuristic backfill when LLM gives no triggers
        def _heuristic_triggers(all_msgs_text: str, topics: list) -> list:
            t = (all_msgs_text or '').lower()
            inferred = []
            # crude subject guess from topics if available
            subj_guess = None
            if topics:
                first = topics[0]
                subj_guess = first.split('>')[0] if '>' in first else first

            # exam tomorrow?
            if "exam" in t and any(k in t for k in ("tomorrow", "tmrw", "next day")):
                inferred.append({
                    'trigger': 'exam_prep',
                    'subject': subj_guess or 'General',
                    'days_before': 1,
                    'message_type': 'last_minute_prep',   # matches yaml
                })

            # appointment tomorrow?
            if "appointment" in t and any(k in t for k in ("tomorrow", "tmrw", "next day")):
                inferred.append({
                    'trigger': 'appointment_reminder',
                    'days_before': 1,
                    'message_type': 'reminder',           # if you later add appointment rules
                })

            # learning support ask
            if any(k in t for k in ("difficult", "hard", "struggling", "confused", "help")):
                inferred.append({
                    'trigger': 'learning_support',
                    'subject': subj_guess or 'General',
                    'days_before': 0,
                    'message_type': 'learning_support_offer',  # matches yaml
                })
            return inferred

        all_text = " ".join([m['content'] if isinstance(m.get('content'), str)
                            else m.get('message', '') for m in conversations_for_analysis])

        try:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            analysis = loop.run_until_complete(
                self.llm_service.analyze_conversations_for_triggers(conversations_for_analysis)
            )

            topics = analysis.get('topics') or []
            sentiment = analysis.get('sentiment_avg', 0.0)
            triggers = analysis.get('triggers') or analysis.get('ai_triggers') or []
            insights = analysis.get('insights') or {}

            # If LLM returned no triggers, use heuristics on the same text
            if not triggers:
                triggers = _heuristic_triggers(all_text, topics)

            return {
                'conversations_7d': len(recent_messages),
                'top_topics': topics,
                'convo_sentiment_7d_avg': sentiment,
                'ai_email_triggers': triggers,
                'conversation_insights': insights
            }

        except Exception as e:
            logger.error(f"Failed to analyze conversations with AI: {str(e)}")

            # Fallback topics (simple keywording)
            fallback_topics = []
            text_lower = (all_text or "").lower()
            topic_patterns = {
                'Biology>Cells': ['cell', 'cellular', 'mitochondria', 'nucleus', 'membrane', 'organelle'],
                'Biology>Photosynthesis': ['photosynthesis', 'chloroplast', 'light reaction', 'calvin cycle'],
                'Biology>Cellular Mechanisms': ['cellular', 'mechanism', 'biology', 'molecular'],
                'Pharmacology>Antibiotics': ['antibiotic', 'antimicrobial', 'penicillin', 'vancomycin', 'resistance', 'bacteria'],
                'History>Stone Age': ['stone age', 'neolithic', 'paleolithic'],
            }
            for topic, keywords in topic_patterns.items():
                if any(k in text_lower for k in keywords):
                    fallback_topics.append(topic)

            fallback_triggers = _heuristic_triggers(all_text, fallback_topics)

            return {
                'conversations_7d': len(recent_messages),
                'top_topics': fallback_topics,
                'convo_sentiment_7d_avg': 0.0,
                'ai_email_triggers': fallback_triggers,
                'conversation_insights': {
                    'engagement_level': 'high' if len(recent_messages) > 20 else 'medium',
                    'learning_gaps': [],
                    'upcoming_events': [],
                    'needs': [],
                }
            }
