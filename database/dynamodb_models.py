from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import uuid
import logging
from database.dynamodb_connection import get_dynamodb
from config.settings import get_settings
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger(__name__)
# Track which tables have had their schema logged to avoid duplicate logs
_SCHEMA_LOGGED_TABLES: set = set()

class DynamoDBModel:
    """Base class for DynamoDB models with proper GetItem/Query operations"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.dynamodb_conn = get_dynamodb()
        self.table = self.dynamodb_conn.get_table(table_name)
        self._log_table_schema()
    
    def _log_table_schema(self):
        """Log table schema including primary key and GSIs"""
        # Log schema only once per table to avoid duplicate startup logs
        if self.table_name in _SCHEMA_LOGGED_TABLES:
            return
        try:
            table_description = self.table.meta.client.describe_table(TableName=self.table_name)
            table_info = table_description['Table']
            
            # Log primary key
            key_schema = table_info.get('KeySchema', [])
            primary_key = {item['AttributeName']: item['KeyType'] for item in key_schema}
            logger.info(f"📋 Table {self.table_name} - Primary Key: {primary_key}")
            
            # Log GSIs
            gsis = table_info.get('GlobalSecondaryIndexes', [])
            if gsis:
                for gsi in gsis:
                    gsi_name = gsi['IndexName']
                    gsi_keys = {item['AttributeName']: item['KeyType'] for item in gsi['KeySchema']}
                    logger.info(f"🔍 Table {self.table_name} - GSI '{gsi_name}': {gsi_keys}")
            else:
                logger.info(f"❌ Table {self.table_name} - No GSIs found")
            
            # Mark this table as logged to prevent duplicate schema logs
            _SCHEMA_LOGGED_TABLES.add(self.table_name)
                
        except Exception as e:
            logger.warning(f"⚠️ Could not describe table {self.table_name}: {e}")
    
    def _normalize_email(self, email: str) -> str:
        """Normalize email by trimming and lowercasing"""
        return email.strip().lower() if email else ""
    
    def get_item_by_key(self, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get single item using GetItem operation"""
        try:
            response = self.table.get_item(Key=key)
            return response.get('Item')
        except Exception as e:
            logger.error(f"GetItem failed for {self.table_name}: {e}")
            return None
    
    def query_by_partition_key(self, partition_key: str, partition_value: Any,
                              sort_key: Optional[str] = None, sort_value: Optional[Any] = None,
                              scan_forward: bool = True, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Query items using partition key and optional sort key using boto3 Key() expressions."""
        try:
            key_expr = Key(partition_key).eq(partition_value)
            if sort_key is not None and sort_value is not None:
                key_expr = key_expr & Key(sort_key).eq(sort_value)

            query_kwargs = {
                'KeyConditionExpression': key_expr,
                'ScanIndexForward': scan_forward
            }
            if limit:
                query_kwargs['Limit'] = int(limit)

            response = self.table.query(**query_kwargs)
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"Query failed for {self.table_name}: {e}")
            return []
    
    def scan_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Scan all items in table with proper pagination (only for tables without email key)"""
        all_items = []
        scan_kwargs = {}
        
        while True:
            response = self.table.scan(**scan_kwargs)
            items = response.get('Items', [])
            all_items.extend(items)
            
            # Apply limit after collecting all results
            if limit and len(all_items) >= limit:
                return all_items[:limit]
            
            # Check if there are more pages
            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break
                
            scan_kwargs['ExclusiveStartKey'] = last_key
        
        return all_items

class InvestorProdModel(DynamoDBModel):
    """User profiles from investor_prod table - email is partition key"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_INVESTOR_TABLE)
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email using GetItem (email is partition key)"""
        normalized_email = self._normalize_email(email)
        return self.get_item_by_key({'email': normalized_email})
    
    def get_all_users(self) -> List[Dict[str, Any]]:
        """Get all user profiles"""
        return self.scan_all()
    
    def find_users_by_partial_email(self, partial_email: str) -> List[Dict[str, Any]]:
        """Find users by partial email match - only use scan for this special case"""
        normalized_partial = self._normalize_email(partial_email)
        matching_users = []
        scan_kwargs = {}
        
        while True:
            response = self.table.scan(**scan_kwargs)
            items = response.get('Items', [])
            
            for user in items:
                user_email = self._normalize_email(user.get('email', ''))
                if normalized_partial in user_email:
                    matching_users.append(user)
            
            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break
                
            scan_kwargs['ExclusiveStartKey'] = last_key
        
        return matching_users

    def get_users_by_tenant(
        self,
        tenant_name: str,
        limit: int = 100,
        next_token: Optional[Dict[str, Any]] = None,
        email_prefix: Optional[str] = None,
        projection: Optional[List[str]] = None,
        scan_forward: bool = True,
    ) -> Dict[str, Any]:
        """
        Query users by tenant using the GSI 'tenantName-email-index'.
        - tenant_name: tenant partition (exact match)
        - limit: page size
        - next_token: ExclusiveStartKey from previous query (pagination)
        - email_prefix: optional begins_with() on email sort key for prefix search
        - projection: list of attributes to return (to keep payload small)
        - returns: {'items': [...], 'next_token': {...} or None}
        """
        try:
            query_kwargs: Dict[str, Any] = {
                'IndexName': 'tenantName-email-index',
                'KeyConditionExpression': Key('tenantName').eq(tenant_name),
                'ScanIndexForward': scan_forward,
                'Limit': int(limit)
            }

            if email_prefix:
                # begins_with only applies to the sort key (email)
                query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & Key('email').begins_with(email_prefix)

            if projection:
                # De-duplicate fields and build a safe ProjectionExpression
                uniq: List[str] = []
                for field in projection:
                    if field and field not in uniq:
                        uniq.append(field)
                if uniq:
                    names = {f"#{field.replace('.', '_')}_{i}": field for i, field in enumerate(uniq)}
                    query_kwargs['ExpressionAttributeNames'] = names
                    query_kwargs['ProjectionExpression'] = ", ".join(names.keys())

            if next_token:
                query_kwargs['ExclusiveStartKey'] = next_token

            resp = self.table.query(**query_kwargs)
            return {
                'items': resp.get('Items', []),
                'next_token': resp.get('LastEvaluatedKey')
            }
        except Exception as e:
            logger.error(f"Query get_users_by_tenant failed: {e}")
            return {'items': [], 'next_token': None}

class InvestorLoginHistoryModel(DynamoDBModel):
    """Conversation history from InvestorLoginHistory_Prod table - email (HASH), time (RANGE)"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_LOGIN_HISTORY_TABLE)
    
    def get_conversations_by_user(self, user_email: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Get conversations for user using Query (email is partition key)"""
        normalized_email = self._normalize_email(user_email)
        return self.query_by_partition_key(
            partition_key='email', 
            partition_value=normalized_email,
            scan_forward=False,  # Get latest first
            limit=limit
        )
    
    def get_all_conversations(self) -> List[Dict[str, Any]]:
        """Get all conversation records - only use scan when needed"""
        return self.scan_all()

class UserInfiniteTestSeriesModel(DynamoDBModel):
    """Quiz data from User_Infinite_TestSeries_Prod table - email (HASH), series_id (RANGE)"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_ITP_TABLE)
    
    def get_user_test_series(self, user_email: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Get test series for user using Query (email is partition key)"""
        normalized_email = self._normalize_email(user_email)
        return self.query_by_partition_key(
            partition_key='email', 
            partition_value=normalized_email,
            limit=limit
        )
    
    def get_all_test_series(self) -> List[Dict[str, Any]]:
        """Get all test series records - only use scan when needed"""
        return self.scan_all()

class TestSeriesRecordModel(DynamoDBModel):
    """ACT Science quiz from TestSereiesRecord_Prod table - email (HASH), test_id (RANGE)"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_TEST_RECORDS_TABLE)
    
    def get_user_test_records(self, user_email: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Get test records for user using Query (email is partition key)"""
        normalized_email = self._normalize_email(user_email)
        return self.query_by_partition_key(
            partition_key='email', 
            partition_value=normalized_email,
            limit=limit
        )
    
    def get_all_test_records(self) -> List[Dict[str, Any]]:
        """Get all test records - only use scan when needed"""
        return self.scan_all()

class LearningRecordModel(DynamoDBModel):
    """Presentation usage from LearningRecord_Prod table - email (HASH), record_id (RANGE)"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_LEARNING_RECORDS_TABLE)
    
    def get_user_learning_records(self, user_email: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Get learning records for user using Query (email is partition key)"""
        normalized_email = self._normalize_email(user_email)
        return self.query_by_partition_key(
            partition_key='email', 
            partition_value=normalized_email,
            limit=limit
        )
    
    def get_all_learning_records(self) -> List[Dict[str, Any]]:
        """Get all learning records - only use scan when needed"""
        return self.scan_all()

class ICPProdModel(DynamoDBModel):
    """Course plans from ICP_Prod table - email (HASH), plan_id (RANGE)"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_ICP_TABLE)
    
    def get_user_course_plans(self, user_email: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Get course plans for user using Query (email is partition key)"""
        normalized_email = self._normalize_email(user_email)
        return self.query_by_partition_key(
            partition_key='email', 
            partition_value=normalized_email,
            limit=limit
        )
    
    def get_all_course_plans(self) -> List[Dict[str, Any]]:
        """Get all course plans - only use scan when needed"""
        return self.scan_all()

class QuestionProdModel(DynamoDBModel):
    """Content generation from Question_Prod table - email is NOT a key, use scan"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_QUESTIONS_TABLE)
    
    def get_all_questions(self) -> List[Dict[str, Any]]:
        """Get all generated questions"""
        return self.scan_all()
    
    def get_questions_by_subject(self, subject: str) -> List[Dict[str, Any]]:
        """Get questions for a specific subject with pagination"""
        all_items = []
        scan_kwargs = {
            'FilterExpression': Attr('Subject').contains(subject)
        }

        while True:
            response = self.table.scan(**scan_kwargs)
            items = response.get('Items', [])
            all_items.extend(items)

            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break

            scan_kwargs['ExclusiveStartKey'] = last_key

        return all_items

class PresentationProdModel(DynamoDBModel):
    """Course metadata from presentation_prod table - email is NOT a key, use scan"""
    def __init__(self):
        settings = get_settings()
        super().__init__(settings.DYNAMODB_PRESENTATIONS_TABLE)
    
    def get_all_presentations(self) -> List[Dict[str, Any]]:
        """Get all presentation metadata"""
        return self.scan_all()
    
    def get_presentation_by_id(self, presentation_id: str) -> Optional[Dict[str, Any]]:
        """Get presentation by ID with pagination"""
        scan_kwargs = {
            'FilterExpression': Attr('id').eq(presentation_id)
        }

        while True:
            response = self.table.scan(**scan_kwargs)
            items = response.get('Items', [])
            if items:
                return items[0]

            last_key = response.get('LastEvaluatedKey')
            if not last_key:
                break

            scan_kwargs['ExclusiveStartKey'] = last_key

        return None

class UserAnalytics:
    """Analyzes user behavior and learning patterns from real data"""
    
    @staticmethod
    def analyze_learning_engagement(user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze user's learning engagement patterns"""
        profile = user_data.get('profile', {})
        conversations = user_data.get('conversations', [])
        test_series = user_data.get('test_series', [])
        learning_records = user_data.get('learning_records', [])
        
        # Extract engagement metrics
        total_conversations = len(conversations)
        total_test_attempts = len(test_series)
        total_presentations = len(learning_records)
        
        # Analyze conversation quality
        conversation_topics = []
        for conv in conversations:
            data = conv.get('data', [])
            for interaction in data:
                if 'user' in interaction:
                    conversation_topics.append(interaction['user'])
        
        # Analyze test performance
        test_performance = []
        for test in test_series:
            correct_responses = test.get('Correct_Response_by_User', 0)
            total_questions = test.get('Total_Question', 1)
            accuracy = (correct_responses / total_questions) * 100 if total_questions > 0 else 0
            test_performance.append({
                'subject': test.get('Subject', 'Unknown'),
                'accuracy': accuracy,
                'questions_answered': test.get('current_Answer_Position', 0)
            })
        
        # Analyze learning preferences from profile
        grade_subjects = profile.get('grade_subject', [])
        preferred_subjects = [item.get('item_text', '') for item in grade_subjects if isinstance(item, dict)]
        
        return {
            'engagement_score': min(100, (total_conversations * 10 + total_test_attempts * 15 + total_presentations * 20)),
            'conversation_count': total_conversations,
            'test_attempts': total_test_attempts,
            'presentation_views': total_presentations,
            'average_test_accuracy': sum(t['accuracy'] for t in test_performance) / len(test_performance) if test_performance else 0,
            'preferred_subjects': preferred_subjects[:5],  # Top 5 subjects
            'conversation_topics': conversation_topics[-10:],  # Recent 10 topics
            'learning_streak': profile.get('Streak_Count', 0),
            'last_activity': profile.get('lastlogin', 'Unknown')
        }
    
    @staticmethod
    def assess_churn_risk(user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess user's risk of churning based on engagement patterns"""
        profile = user_data.get('profile', {})
        
        # Parse last login date
        last_login = profile.get('lastlogin', '')
        days_since_login = 0
        if last_login:
            try:
                last_date = datetime.strptime(last_login, '%Y-%m-%d,%H:%M:%S')
                days_since_login = (datetime.now() - last_date).days
            except:
                days_since_login = 999  # Very high if can't parse
        
        # Calculate risk factors
        risk_factors = []
        risk_score = 0
        
        if days_since_login > 30:
            risk_factors.append("No login in 30+ days")
            risk_score += 40
        elif days_since_login > 14:
            risk_factors.append("No login in 14+ days")
            risk_score += 25
        elif days_since_login > 7:
            risk_factors.append("No login in 7+ days")
            risk_score += 15
        
        # Check engagement metrics
        conversations = len(user_data.get('conversations', []))
        if conversations == 0:
            risk_factors.append("No conversations recorded")
            risk_score += 30
        elif conversations < 3:
            risk_factors.append("Low conversation activity")
            risk_score += 15
        
        test_series = len(user_data.get('test_series', []))
        if test_series == 0:
            risk_factors.append("No test attempts")
            risk_score += 25
        elif test_series < 2:
            risk_factors.append("Low test activity")
            risk_score += 10
        
        # Determine risk level
        if risk_score >= 60:
            risk_level = "HIGH"
        elif risk_score >= 30:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        return {
            'risk_level': risk_level,
            'risk_score': min(100, risk_score),
            'risk_factors': risk_factors,
            'days_since_login': days_since_login,
            'recommendation': UserAnalytics._get_retention_recommendation(risk_level, risk_factors)
        }
    
    @staticmethod
    def _get_retention_recommendation(risk_level: str, risk_factors: List[str]) -> str:
        """Get personalized retention recommendation"""
        if risk_level == "HIGH":
            return "Send re-engagement campaign with personalized course recommendations"
        elif risk_level == "MEDIUM":
            return "Send motivational content and learning progress updates"
        else:
            return "Send regular educational content and new feature announcements"
    
    @staticmethod
    def generate_personalized_recommendations(user_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate personalized learning recommendations"""
        profile = user_data.get('profile', {})
        test_series = user_data.get('test_series', [])
        
        recommendations = []
        
        # Analyze weak subjects from test performance
        subject_performance = {}
        for test in test_series:
            subject = test.get('Subject', 'Unknown')
            correct = test.get('Correct_Response_by_User', 0)
            total = test.get('Total_Question', 1)
            accuracy = (correct / total) * 100 if total > 0 else 0
            
            if subject not in subject_performance:
                subject_performance[subject] = []
            subject_performance[subject].append(accuracy)
        
        # Find subjects needing improvement
        for subject, accuracies in subject_performance.items():
            avg_accuracy = sum(accuracies) / len(accuracies)
            if avg_accuracy < 70:  # Below 70% accuracy
                recommendations.append({
                    'type': 'improvement',
                    'subject': subject,
                    'message': f"Focus on {subject} - current accuracy: {avg_accuracy:.1f}%",
                    'priority': 'high' if avg_accuracy < 50 else 'medium'
                })
        
        # Recommend new subjects based on interests
        grade_subjects = profile.get('grade_subject', [])
        if len(grade_subjects) > 0:
            # Pick a random subject they haven't tested much
            untested_subjects = [item.get('item_text', '') for item in grade_subjects 
                              if isinstance(item, dict) and item.get('item_text', '') not in subject_performance]
            if untested_subjects:
                recommendations.append({
                    'type': 'exploration',
                    'subject': untested_subjects[0],
                    'message': f"Try exploring {untested_subjects[0]} - it's in your interest list!",
                    'priority': 'low'
                })
        
        return recommendations[:3]  # Return top 3 recommendations

class DataFetcher:
    """Coordinates data fetching from all DynamoDB tables"""
    
    def __init__(self):
        self.investor_prod = InvestorProdModel()
        self.login_history = InvestorLoginHistoryModel()
        self.test_series = UserInfiniteTestSeriesModel()
        self.test_records = TestSeriesRecordModel()
        self.learning_records = LearningRecordModel()
        self.questions = QuestionProdModel()
        self.presentations = PresentationProdModel()
        self.icp = ICPProdModel()
    
    def get_all_user_data(self, user_email: str) -> Dict[str, Any]:
        """Get all data for a specific user across all tables"""
        profile = self.investor_prod.get_user_by_email(user_email)
        
        # If not found, try partial matching
        if not profile:
            partial_matches = self.investor_prod.find_users_by_partial_email(user_email.split('@')[0])
            if partial_matches:
                print(f"🔍 Found {len(partial_matches)} users with similar email patterns:")
                for i, match in enumerate(partial_matches[:5]):  # Show first 5 matches
                    print(f"   {i+1}. {match.get('email', 'No email')} - {match.get('name', 'No name')}")
                return None
        
        if not profile:
            return None
            
        return {
            'profile': profile,
            'conversations': self.login_history.get_conversations_by_user(user_email),
            'test_series': self.test_series.get_user_test_series(user_email),
            'test_records': self.test_records.get_user_test_records(user_email),
            'learning_records': self.learning_records.get_user_learning_records(user_email),
            'course_plans': self.icp.get_user_course_plans(user_email)
        }

    def get_all_users(self) -> List[Dict[str, Any]]:
        """Get all user profiles"""
        return self.investor_prod.get_all_users()
    
    def get_content_metadata(self) -> Dict[str, Any]:
        """Get all content metadata"""
        return {
            'questions': self.questions.get_all_questions(),
            'presentations': self.presentations.get_all_presentations(),
            'course_plans': self.icp.get_all_course_plans()
        }
