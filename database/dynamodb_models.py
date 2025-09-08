from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import uuid
from database.dynamodb_connection import get_dynamodb

class DynamoDBModel:
    """Base class for DynamoDB models"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.dynamodb_conn = get_dynamodb()
        self.table = self.dynamodb_conn.get_table(table_name)
    
    def get_item(self, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get an item by key"""
        response = self.table.get_item(Key=key)
        return response.get('Item')
    
    def scan_all(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Scan all items in table"""
        kwargs = {}
        if limit:
            kwargs['Limit'] = limit
        
        response = self.table.scan(**kwargs)
        return response.get('Items', [])

class InvestorProdModel(DynamoDBModel):
    """User profiles from investor_prod table"""
    def __init__(self):
        super().__init__('investor_prod')
    
    def get_all_users(self) -> List[Dict[str, Any]]:
        """Get all user profiles"""
        return self.scan_all()
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        # Scan for user by email since we don't know the key structure
        response = self.table.scan(
            FilterExpression='email = :email',
            ExpressionAttributeValues={':email': email}
        )
        items = response.get('Items', [])
        return items[0] if items else None

class InvestorLoginHistoryModel(DynamoDBModel):
    """Conversation history from InvestorLoginHistory_Prod table"""
    def __init__(self):
        super().__init__('InvestorLoginHistory_Prod')
    
    def get_all_conversations(self) -> List[Dict[str, Any]]:
        """Get all conversation records"""
        return self.scan_all()
    
    def get_conversations_by_user(self, user_email: str) -> List[Dict[str, Any]]:
        """Get conversations for a specific user"""
        response = self.table.scan(
            FilterExpression='contains(#data, :email)',
            ExpressionAttributeNames={'#data': 'data'},
            ExpressionAttributeValues={':email': user_email}
        )
        return response.get('Items', [])

class UserInfiniteTestSeriesModel(DynamoDBModel):
    """Quiz data from User_Infinite_TestSeries_Prod table"""
    def __init__(self):
        super().__init__('User_Infinite_TestSeries_Prod')
    
    def get_all_test_series(self) -> List[Dict[str, Any]]:
        """Get all test series records"""
        return self.scan_all()
    
    def get_user_test_series(self, user_email: str) -> List[Dict[str, Any]]:
        """Get test series for a specific user"""
        response = self.table.scan(
            FilterExpression='email = :email',
            ExpressionAttributeValues={':email': user_email}
        )
        return response.get('Items', [])

class TestSeriesRecordModel(DynamoDBModel):
    """ACT Science quiz from TestSereiesRecord_Prod table"""
    def __init__(self):
        super().__init__('TestSereiesRecord_Prod')
    
    def get_all_test_records(self) -> List[Dict[str, Any]]:
        """Get all test records"""
        return self.scan_all()
    
    def get_user_test_records(self, user_email: str) -> List[Dict[str, Any]]:
        """Get test records for a specific user"""
        response = self.table.scan(
            FilterExpression='email = :email',
            ExpressionAttributeValues={':email': user_email}
        )
        return response.get('Items', [])

class LearningRecordModel(DynamoDBModel):
    """Presentation usage from LearningRecord_Prod table"""
    def __init__(self):
        super().__init__('LearningRecord_Prod')
    
    def get_all_learning_records(self) -> List[Dict[str, Any]]:
        """Get all learning records"""
        return self.scan_all()
    
    def get_user_learning_records(self, user_email: str) -> List[Dict[str, Any]]:
        """Get learning records for a specific user"""
        response = self.table.scan(
            FilterExpression='email = :email',
            ExpressionAttributeValues={':email': user_email}
        )
        return response.get('Items', [])

class QuestionProdModel(DynamoDBModel):
    """Content generation from Question_Prod table"""
    def __init__(self):
        super().__init__('Question_Prod')
    
    def get_all_questions(self) -> List[Dict[str, Any]]:
        """Get all generated questions"""
        return self.scan_all()
    
    def get_questions_by_subject(self, subject: str) -> List[Dict[str, Any]]:
        """Get questions for a specific subject"""
        response = self.table.scan(
            FilterExpression='contains(Subject, :subject)',
            ExpressionAttributeValues={':subject': subject}
        )
        return response.get('Items', [])

class PresentationProdModel(DynamoDBModel):
    """Course metadata from presentation_prod table"""
    def __init__(self):
        super().__init__('presentation_prod')
    
    def get_all_presentations(self) -> List[Dict[str, Any]]:
        """Get all presentation metadata"""
        return self.scan_all()
    
    def get_presentation_by_id(self, presentation_id: str) -> Optional[Dict[str, Any]]:
        """Get presentation by ID"""
        response = self.table.scan(
            FilterExpression='presentation_id = :id',
            ExpressionAttributeValues={':id': presentation_id}
        )
        items = response.get('Items', [])
        return items[0] if items else None

class ICPProdModel(DynamoDBModel):
    """Course plans from ICP_Prod table"""
    def __init__(self):
        super().__init__('ICP_Prod')
    
    def get_all_course_plans(self) -> List[Dict[str, Any]]:
        """Get all course plans"""
        return self.scan_all()
    
    def get_course_plan_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Get course plan by title"""
        response = self.table.scan(
            FilterExpression='contains(title, :title)',
            ExpressionAttributeValues={':title': title}
        )
        items = response.get('Items', [])
        return items[0] if items else None

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
        return {
            'profile': self.investor_prod.get_user_by_email(user_email),
            'conversations': self.login_history.get_conversations_by_user(user_email),
            'test_series': self.test_series.get_user_test_series(user_email),
            'test_records': self.test_records.get_user_test_records(user_email),
            'learning_records': self.learning_records.get_user_learning_records(user_email)
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
