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
                'id': 'help_cells_bio',
                'when': {
                    'all': [
                        {'contains': {'field': 'top_topics', 'value': 'Biology>Cells'}},
                        {'lt': {'field': 'emails_sent_7d', 'value': 2}},
                        {'eq': {'field': 'unsubscribed', 'value': False}}
                    ]
                },
                'action': {
                    'template_id': 'bio_cells_help_v1',
                    'priority': 90,
                    'cooldown_days': 3
                }
            },
            {
                'id': 'algebra_followup',
                'when': {
                    'all': [
                        {'contains': {'field': 'top_topics', 'value': 'Algebra'}},
                        {'gte': {'field': 'recency_days', 'value': 1}}
                    ]
                },
                'action': {
                    'template_id': 'algebra_next_steps_v1',
                    'priority': 80,
                    'cooldown_days': 3
                }
            },
            {
                'id': 'winback_idle',
                'when': {
                    'all': [
                        {'gte': {'field': 'recency_days', 'value': 7}}
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
    
    def _is_within_send_hours(self, user: AppUser) -> bool:
        """Check if current time is within user's send hours"""
        from config.settings import settings
        
        try:
            user_tz = pytz.timezone(user.tz or 'America/Los_Angeles')
            user_time = datetime.now(user_tz)
            current_hour = user_time.hour
            
            # Check if within allowed hours (8 AM to 8 PM local time)
            return settings.EMAIL_QUIET_HOURS_END <= current_hour < settings.EMAIL_QUIET_HOURS_START
        except Exception as e:
            logger.warning(f"Failed to check send hours for user {user.user_id}: {str(e)}")
            return True  # Default to allowing send
    
    def _evaluate_rules_for_user(self, user_features: UserDailyFeatures) -> List[Dict[str, Any]]:
        """Evaluate all rules for a specific user"""
        matching_rules = []
        
        for rule in self.rules:
            if self._evaluate_rule_conditions(rule['when'], user_features):
                matching_rules.append(rule)
        
        return matching_rules
    
    def _evaluate_rule_conditions(self, conditions: Dict[str, Any], user_features: UserDailyFeatures) -> bool:
        """Evaluate rule conditions against user features"""
        
        if 'all' in conditions:
            return all(self._evaluate_condition(cond, user_features) for cond in conditions['all'])
        elif 'any' in conditions:
            return any(self._evaluate_condition(cond, user_features) for cond in conditions['any'])
        else:
            return self._evaluate_condition(conditions, user_features)
    
    def _evaluate_condition(self, condition: Dict[str, Any], user_features: UserDailyFeatures) -> bool:
        """Evaluate a single condition"""
        
        for operator, params in condition.items():
            field = params['field']
            value = params['value']
            
            # Get field value from user features
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
        """Check if rule is in cooldown period for user"""
        
        # Find the rule to get cooldown period
        rule = next((r for r in self.rules if r['id'] == rule_id), None)
        if not rule:
            return True
        
        cooldown_days = rule['action'].get('cooldown_days', 1)
        cutoff_date = datetime.utcnow() - timedelta(days=cooldown_days)
        
        # Check if this rule was used recently
        recent_email = db.query(EmailSend).filter(
            EmailSend.user_id == user_id,
            EmailSend.ts >= cutoff_date,
            EmailSend.meta['rule_id'].astext == rule_id
        ).first()
        
        return recent_email is not None
    
    def _serialize_features(self, user_features: UserDailyFeatures) -> Dict[str, Any]:
        """Serialize user features for email template"""
        return {
            'recency_days': user_features.recency_days,
            'frequency_7d': user_features.frequency_7d,
            'minutes_7d': user_features.minutes_7d,
            'tests_7d': user_features.tests_7d,
            'top_topics': user_features.top_topics or [],
            'subject_affinity': user_features.subject_affinity or {},
            'churn_risk': user_features.churn_risk
        }
