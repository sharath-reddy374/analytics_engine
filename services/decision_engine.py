from sqlalchemy.orm import Session
from database.models import UserDailyFeatures, EmailSend, AppUser
from typing import List, Dict, Any, Optional
import yaml
import logging
from datetime import datetime, timedelta, time
import pytz

logger = logging.getLogger(__name__)

class DecisionEngine:
    """
    Rule-based decision engine for email campaigns
    """
    
    def __init__(self, rules_file: str = "config/email_rules.yaml"):
        self.rules_file = rules_file
        self.rules = self._load_rules()
    
    def _load_rules(self) -> List[Dict[str, Any]]:
        """Load email rules from YAML file"""
        try:
            with open(self.rules_file, 'r') as f:
                rules_data = yaml.safe_load(f)
                return rules_data.get('rules', [])
        except FileNotFoundError:
            logger.warning(f"Rules file {self.rules_file} not found, using default rules")
            return self._get_default_rules()
        except Exception as e:
            logger.error(f"Failed to load rules: {str(e)}")
            return self._get_default_rules()
    
    def _get_default_rules(self) -> List[Dict[str, Any]]:
        """Default email rules if file is not available"""
        return [
            {
                'id': 'help_biology_general',
                'when': {
                    'all': [
                        {'contains_pattern': {'field': 'top_topics', 'pattern': 'Biology>*'}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 2}},
                        {'eq': {'field': 'unsubscribed', 'value': False}},
                        {'gte': {'field': 'frequency_7d', 'value': 2}}
                    ]
                },
                'action': {
                    'template_id': 'biology_help_v1',
                    'priority': 90,
                    'cooldown_days': 3
                }
            },
            {
                'id': 'help_pharmacology_general',
                'when': {
                    'all': [
                        {'contains_pattern': {'field': 'top_topics', 'pattern': 'Pharmacology>*'}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 2}},
                        {'eq': {'field': 'unsubscribed', 'value': False}},
                        {'gte': {'field': 'frequency_7d', 'value': 2}}
                    ]
                },
                'action': {
                    'template_id': 'pharmacology_help_v1',
                    'priority': 92,
                    'cooldown_days': 3
                }
            },
            {
                'id': 'help_immunology_general',
                'when': {
                    'all': [
                        {'contains_pattern': {'field': 'top_topics', 'pattern': 'Immunology>*'}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 2}},
                        {'eq': {'field': 'unsubscribed', 'value': False}},
                        {'gte': {'field': 'frequency_7d', 'value': 2}}
                    ]
                },
                'action': {
                    'template_id': 'immunology_help_v1',
                    'priority': 88,
                    'cooldown_days': 3
                }
            },
            {
                'id': 'help_history_general',
                'when': {
                    'all': [
                        {'contains_pattern': {'field': 'top_topics', 'pattern': 'History>*'}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 2}},
                        {'eq': {'field': 'unsubscribed', 'value': False}},
                        {'gte': {'field': 'frequency_7d', 'value': 2}}
                    ]
                },
                'action': {
                    'template_id': 'history_help_v1',
                    'priority': 85,
                    'cooldown_days': 3
                }
            },
            {
                'id': 'exam_followup_trigger',
                'when': {
                    'all': [
                        {'contains_trigger': {'field': 'ai_email_triggers', 'trigger_type': 'exam_followup'}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 2}},
                        {'eq': {'field': 'unsubscribed', 'value': False}}
                    ]
                },
                'action': {
                    'template_id': 'exam_followup_v1',
                    'priority': 95,
                    'cooldown_days': 2
                }
            },
            {
                'id': 'learning_support_trigger',
                'when': {
                    'all': [
                        {'contains_trigger': {'field': 'ai_email_triggers', 'trigger_type': 'learning_support'}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 2}},
                        {'eq': {'field': 'unsubscribed', 'value': False}}
                    ]
                },
                'action': {
                    'template_id': 'learning_support_v1',
                    'priority': 93,
                    'cooldown_days': 3
                }
            },
            {
                'id': 'high_engagement_reward',
                'when': {
                    'all': [
                        {'gte': {'field': 'frequency_7d', 'value': 100}},
                        {'gte': {'field': 'conversations_7d', 'value': 50}},
                        {'gt': {'field': 'convo_sentiment_7d_avg', 'value': 0.5}},
                        {'eq': {'field': 'unsubscribed', 'value': False}}
                    ]
                },
                'action': {
                    'template_id': 'engagement_reward_v1',
                    'priority': 96,
                    'cooldown_days': 14
                }
            },
            {
                'id': 'test_performance_help',
                'when': {
                    'all': [
                        {'gte': {'field': 'tests_7d', 'value': 100}},
                        {'lt': {'field': 'test_accuracy', 'value': 0.5}},
                        {'eq': {'field': 'unsubscribed', 'value': False}}
                    ]
                },
                'action': {
                    'template_id': 'test_improvement_v1',
                    'priority': 87,
                    'cooldown_days': 5
                }
            },
            {
                'id': 'course_completion_celebration',
                'when': {
                    'all': [
                        {'gte': {'field': 'completed_courses', 'value': 1}},
                        {'eq': {'field': 'unsubscribed', 'value': False}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 1}}
                    ]
                },
                'action': {
                    'template_id': 'course_completion_v1',
                    'priority': 94,
                    'cooldown_days': 7
                }
            },
            {
                'id': 'winback_idle',
                'when': {
                    'all': [
                        {'gte': {'field': 'recency_days', 'value': 7}},
                        {'lte': {'field': 'recency_days', 'value': 30}},
                        {'eq': {'field': 'unsubscribed', 'value': False}}
                    ]
                },
                'action': {
                    'template_id': 'winback_study_plan_v1',
                    'priority': 60,
                    'cooldown_days': 7
                }
            }
        ]
    
    def evaluate_users_for_emails(self, db: Session) -> List[Dict[str, Any]]:
        """
        Evaluate all users and return email candidates
        """
        from datetime import date
        
        # Get today's features for all users
        features = db.query(UserDailyFeatures).filter(
            UserDailyFeatures.as_of == date.today()
        ).all()
        
        email_candidates = []
        
        for user_features in features:
            # Check email eligibility
            if not self._is_email_eligible(user_features, db):
                continue
            
            # Evaluate rules for this user
            matching_rules = self._evaluate_rules_for_user(user_features)
            
            if matching_rules:
                # Select highest priority rule
                best_rule = max(matching_rules, key=lambda r: r['action']['priority'])
                
                email_candidates.append({
                    'user_id': str(user_features.user_id),
                    'rule_id': best_rule['id'],
                    'template_id': best_rule['action']['template_id'],
                    'priority': best_rule['action']['priority'],
                    'features': self._serialize_features(user_features)
                })
        
        # Sort by priority and apply daily limits
        email_candidates.sort(key=lambda x: x['priority'], reverse=True)
        
        return self._apply_daily_limits(email_candidates, db)
    
    def evaluate_user(self, email: str, features: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Evaluate a single user and return email decisions
        Args:
            email: User email address
            features: Dictionary of computed user features
        Returns:
            List of email decisions for the user
        """
        # Check basic eligibility
        if not self._is_user_eligible_basic(features):
            logger.info(f"User {email} not eligible for emails")
            return []
        
        # Evaluate rules for this user
        matching_rules = self._evaluate_rules_for_features(features)
        
        if not matching_rules:
            logger.info(f"No matching rules for user {email}")
            return []
        
        # Select highest priority rule
        best_rule = max(matching_rules, key=lambda r: r['action']['priority'])
        
        decision = {
            'user_email': email,
            'rule_id': best_rule['id'],
            'template_id': best_rule['action']['template_id'],
            'priority': best_rule['action']['priority'],
            'features': features,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Selected rule '{best_rule['id']}' for user {email} (priority: {best_rule['action']['priority']})")
        return [decision]
    
    def _is_email_eligible(self, user_features: UserDailyFeatures, db: Session) -> bool:
        """Check if user is eligible for emails"""
        
        # Check unsubscribe status
        if user_features.unsubscribed:
            return False
        
        # Check email consent
        user = db.query(AppUser).filter(AppUser.user_id == user_features.user_id).first()
        if not user or not user.consent_email:
            return False
        
        # Check daily email limit
        from config.settings import settings
        if user_features.emails_sent_7d >= settings.MAX_EMAILS_PER_WEEK:
            return False
        
        # Check if already sent email today
        today_start = datetime.combine(user_features.as_of, time.min)
        today_end = datetime.combine(user_features.as_of, time.max)
        
        today_emails = db.query(EmailSend).filter(
            EmailSend.user_id == user_features.user_id,
            EmailSend.ts >= today_start,
            EmailSend.ts <= today_end,
            EmailSend.status.in_(['sent', 'queued'])
        ).count()
        
        if today_emails >= settings.MAX_EMAILS_PER_DAY:
            return False
        
        # Check quiet hours
        if not self._is_within_send_hours(user):
            return False
        
        return True
    
    def _is_user_eligible_basic(self, features: Dict[str, Any]) -> bool:
        """Check basic eligibility without database access"""
        
        # Check unsubscribe status
        if features.get('unsubscribed', False):
            logger.debug("User unsubscribed")
            return False
        
        # Check weekly email limit
        emails_sent_7d = features.get('emails_sent_7d', 0)
        if emails_sent_7d >= 5:  # Max 5 emails per week
            logger.debug(f"User hit weekly email limit: {emails_sent_7d}")
            return False
        
        return True
    
    def _is_within_send_hours(self, user: AppUser) -> bool:
        """Return True if NOW (user's local time) is outside quiet hours."""
        from config.settings import settings
        try:
            user_tz = pytz.timezone(getattr(user, 'tz', None) or 'America/Los_Angeles')
            hour = datetime.now(user_tz).hour

            q_start = int(settings.EMAIL_QUIET_HOURS_START)  # e.g., 20 (8pm)
            q_end   = int(settings.EMAIL_QUIET_HOURS_END)    # e.g., 8  (8am)

            if q_start == q_end:
                # degenerate: no quiet hours
                return True

            if q_start < q_end:
                # Quiet window does NOT cross midnight (e.g., 22 -> 6 is NOT this case)
                in_quiet = (q_start <= hour < q_end)
            else:
                # Quiet window crosses midnight (e.g., 20 -> 8)
                in_quiet = (hour >= q_start) or (hour < q_end)

            return not in_quiet
        except Exception as e:
            logger.warning(f"Failed to check send hours for user {getattr(user, 'user_id', 'unknown')}: {str(e)}")
            return True  # be permissive on failure

    def _evaluate_rules_for_user(self, user_features: UserDailyFeatures) -> List[Dict[str, Any]]:
        """Evaluate all rules for a specific user"""
        matching_rules = []
        
        for rule in self.rules:
            if self._evaluate_rule_conditions(rule['when'], user_features):
                matching_rules.append(rule)
        
        return matching_rules
    
    def _evaluate_rules_for_features(self, features: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluate all rules for user features dictionary"""
        matching_rules = []
        
        for rule in self.rules:
            if self._evaluate_rule_conditions_dict(rule['when'], features):
                matching_rules.append(rule)
                logger.debug(f"Rule '{rule['id']}' matched")
        
        return matching_rules
    
    def _evaluate_rule_conditions(self, conditions: Dict[str, Any], user_features: UserDailyFeatures) -> bool:
        """Evaluate rule conditions against user features"""
        
        if 'all' in conditions:
            return all(self._evaluate_condition(cond, user_features) for cond in conditions['all'])
        elif 'any' in conditions:
            return any(self._evaluate_condition(cond, user_features) for cond in conditions['any'])
        else:
            return self._evaluate_condition(conditions, user_features)
    
    def _evaluate_rule_conditions_dict(self, conditions: Dict[str, Any], features: Dict[str, Any]) -> bool:
        """Evaluate rule conditions against features dictionary"""
        
        if 'all' in conditions:
            return all(self._evaluate_condition_dict(cond, features) for cond in conditions['all'])
        elif 'any' in conditions:
            return any(self._evaluate_condition_dict(cond, features) for cond in conditions['any'])
        else:
            return self._evaluate_condition_dict(conditions, features)
    
    def _evaluate_condition(self, condition: Dict[str, Any], user_features: UserDailyFeatures) -> bool:
        """Evaluate a single condition against ORM model (DB path)."""
        for operator, params in condition.items():
            field = params.get('field')
            value = params.get('value')
            pattern = params.get('pattern')
            trigger_type = params.get('trigger_type')

            # Pull raw field value off the ORM row
            field_value = getattr(user_features, field, None)

            if operator == 'eq':
                return field_value == value
            elif operator == 'ne':
                return field_value != value
            elif operator == 'gt':
                return field_value is not None and field_value > value
            elif operator == 'gte':
                return field_value is not None and field_value >= value
            elif operator == 'lt':
                return field_value is not None and field_value < value
            elif operator == 'lte':
                return field_value is not None and field_value <= value
            elif operator == 'contains':
                if isinstance(field_value, list):
                    return value in field_value
                elif isinstance(field_value, str):
                    return value in field_value
                return False
            elif operator == 'not_contains':
                if isinstance(field_value, list):
                    return value not in field_value
                elif isinstance(field_value, str):
                    return value not in field_value
                return True
            elif operator == 'contains_pattern':
                # supports {"pattern": "Biology>*"}
                if isinstance(field_value, list) and isinstance(pattern, str):
                    if pattern.endswith('*'):
                        prefix = pattern[:-1]
                        return any(isinstance(item, str) and item.startswith(prefix) for item in field_value)
                    else:
                        return pattern in field_value
                return False
            elif operator == 'contains_trigger':
                # supports {"trigger_type": "post_exam"} etc.
                if isinstance(field_value, list) and isinstance(trigger_type, str):
                    return any(
                        isinstance(t, dict) and (t.get('trigger') == trigger_type or t.get('trigger_type') == trigger_type)
                        for t in field_value
                    )
                return False

        return False

    def _evaluate_condition_dict(self, condition: Dict[str, Any], features: Dict[str, Any]) -> bool:
        """Evaluate a single condition against features dictionary"""
        
        for operator, params in condition.items():
            field = params['field']
            value = params.get('value')
            pattern = params.get('pattern')
            trigger_type = params.get('trigger_type')
            
            # Get field value from features
            field_value = features.get(field)
            
            if operator == 'eq':
                return field_value == value
            elif operator == 'ne':
                return field_value != value
            elif operator == 'gt':
                return field_value is not None and field_value > value
            elif operator == 'gte':
                return field_value is not None and field_value >= value
            elif operator == 'lt':
                return field_value is not None and field_value < value
            elif operator == 'lte':
                return field_value is not None and field_value <= value
            elif operator == 'contains':
                if isinstance(field_value, list):
                    return value in field_value
                elif isinstance(field_value, str):
                    return value in field_value
                return False
            elif operator == 'not_contains':
                if isinstance(field_value, list):
                    return value not in field_value
                elif isinstance(field_value, str):
                    return value not in field_value
                return True
            elif operator == 'contains_pattern':
                if isinstance(field_value, list):
                    if pattern.endswith('*'):
                        # Wildcard pattern matching (e.g., 'Biology>*' matches any Biology subtopic)
                        prefix = pattern[:-1]
                        return any(item.startswith(prefix) for item in field_value)
                    else:
                        return pattern in field_value
                return False
            elif operator == 'contains_trigger':
                if isinstance(field_value, list):
                    return any(
                        isinstance(trigger, dict) and trigger.get('trigger') == trigger_type
                        for trigger in field_value
                    )
                return False
        
        return False
    
    def _apply_daily_limits(self, candidates: List[Dict[str, Any]], db: Session) -> List[Dict[str, Any]]:
        """Apply daily sending limits and cooldowns"""
        filtered_candidates = []
        
        for candidate in candidates:
            user_id = candidate['user_id']
            rule_id = candidate['rule_id']
            
            # Check rule cooldown
            if self._is_rule_in_cooldown(user_id, rule_id, db):
                continue
            
            filtered_candidates.append(candidate)
        
        return filtered_candidates
    
    def _is_rule_in_cooldown(self, user_id: str, rule_id: str, db: Session) -> bool:
        """Check if rule is in cooldown period for user."""
        rule = next((r for r in self.rules if r['id'] == rule_id), None)
        if not rule:
            return True
        cooldown_days = rule['action'].get('cooldown_days', 1)
        cutoff = datetime.utcnow() - timedelta(days=cooldown_days)

        try:
            recent = db.query(EmailSend).filter(
                EmailSend.user_id == user_id,
                EmailSend.ts >= cutoff,
                EmailSend.meta['rule_id'].astext == rule_id
            ).first()
            return recent is not None
        except Exception:
            # Fallback: fetch a handful and check in Python
            recent_list = db.query(EmailSend).filter(
                EmailSend.user_id == user_id,
                EmailSend.ts >= cutoff
            ).order_by(EmailSend.ts.desc()).limit(25).all()
            for e in recent_list:
                meta = getattr(e, 'meta', {}) or {}
                if isinstance(meta, dict) and meta.get('rule_id') == rule_id:
                    return True
            return False

    def _serialize_features(self, user_features: UserDailyFeatures) -> Dict[str, Any]:
        """Expose enough context for content generation."""
        return {
            'email': getattr(user_features, 'email', None) or None,  # if you store it; optional
            'recency_days': user_features.recency_days,
            'frequency_7d': user_features.frequency_7d,
            'minutes_7d': user_features.minutes_7d,
            'tests_7d': user_features.tests_7d,
            'test_accuracy': getattr(user_features, 'test_accuracy', None),
            'avg_itp_score': getattr(user_features, 'avg_itp_score', None),
            'itp_improvement_trend': getattr(user_features, 'itp_improvement_trend', None),

            'icp_completion_rate': getattr(user_features, 'icp_completion_rate', None),
            'active_courses': getattr(user_features, 'active_courses', 0),
            'completed_courses': getattr(user_features, 'completed_courses', 0),
            'completed_course_titles': getattr(user_features, 'completed_course_titles', []) or [],
            'stalled_courses': getattr(user_features, 'stalled_courses', []) or [],
            'recent_progress': getattr(user_features, 'recent_progress', False),

            'conversations_7d': getattr(user_features, 'conversations_7d', 0),
            'top_topics': user_features.top_topics or [],
            'convo_sentiment_7d_avg': getattr(user_features, 'convo_sentiment_7d_avg', 0.0),
            'ai_email_triggers': getattr(user_features, 'ai_email_triggers', []) or [],
            'conversation_insights': getattr(user_features, 'conversation_insights', {}) or {},

            'has_exam_last_minute_prep': getattr(user_features, 'has_exam_last_minute_prep', False),
            'has_exam_post_checkin': getattr(user_features, 'has_exam_post_checkin', False),
            'has_learning_support': getattr(user_features, 'has_learning_support', False),

            'subject_affinity': user_features.subject_affinity or {},
            'churn_risk': user_features.churn_risk,
            'last_email_ts': getattr(user_features, 'last_email_ts', None),
            'emails_sent_7d': getattr(user_features, 'emails_sent_7d', 0),
            'unsubscribed': user_features.unsubscribed,
        }
