from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import logging
from datetime import datetime
import json

from database.dynamodb_connection import get_dynamodb_client, verify_existing_tables
from database.dynamodb_models import DataFetcher
from services.data_processor import DataProcessor
from config.settings import settings

# Configure logging
logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="EdYou AI Engine",
    description="Educational AI Engine for personalized learning experiences",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

data_processor = DataProcessor()
dynamodb_access = DataFetcher()

@app.on_event("startup")
async def startup_event():
    """Initialize DynamoDB connection and services on startup"""
    try:
        # Test DynamoDB connection
        dynamodb = get_dynamodb_client()
        logger.info("DynamoDB connection established successfully")
        
        verify_existing_tables()
        
        logger.info("EdYou AI Engine started successfully")
    except Exception as e:
        logger.error(f"Failed to initialize DynamoDB connection: {str(e)}")
        raise e

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test DynamoDB connection
        dynamodb = get_dynamodb_client()
        return {"status": "healthy", "timestamp": datetime.utcnow(), "dynamodb": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "timestamp": datetime.utcnow(), "error": str(e)}

@app.post("/process-user")
async def process_single_user(
    user_email: str,
    background_tasks: BackgroundTasks
):
    """
    Process a single user through the AI pipeline
    """
    try:
        # Process the user data through the pipeline
        result = await data_processor.process_user_pipeline(user_email)
        
        return {
            "status": "success",
            "user_email": user_email,
            "events_processed": result.get("events_processed", 0),
            "features_computed": result.get("features_computed", False),
            "email_campaigns": result.get("email_campaigns", []),
            "message": "User processed successfully"
        }
    
    except Exception as e:
        logger.error(f"User processing failed for {user_email}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.get("/users/{user_email}/data")
async def get_user_data(user_email: str):
    """
    Get user's raw data from DynamoDB tables
    """
    try:
        user_data = dynamodb_access.get_all_user_data(user_email)
        
        if not user_data or not user_data.get("profile"):
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "user_email": user_email,
            "profile": user_data.get("profile"),
            "conversations": len(user_data.get("conversations", [])),
            "test_attempts": len(user_data.get("test_series", [])) + len(user_data.get("test_records", [])),
            "learning_records": len(user_data.get("learning_records", [])),
            "last_activity": user_data.get("last_activity")
        }
    
    except Exception as e:
        logger.error(f"Failed to get user data for {user_email}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get user data: {str(e)}")

@app.get("/analytics/dashboard")
async def get_analytics_dashboard():
    """
    Get system analytics dashboard data
    """
    try:
        all_users = dynamodb_access.get_all_users()
        content_metadata = dynamodb_access.get_content_metadata()
        
        return {
            "total_users": len(all_users),
            "total_conversations": len(dynamodb_access.login_history.get_all_conversations()),
            "total_test_attempts": len(dynamodb_access.test_series.get_all_test_series()),
            "total_questions": len(content_metadata.get("questions", [])),
            "timestamp": datetime.utcnow()
        }
    
    except Exception as e:
        logger.error(f"Failed to get analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analytics failed: {str(e)}")

@app.get("/tables/status")
async def get_tables_status():
    """
    Check the status of all DynamoDB tables
    """
    try:
        table_status = {}
        tables = [
            ("investor_prod", dynamodb_access.investor_prod),
            ("InvestorLoginHistory_Prod", dynamodb_access.login_history),
            ("User_Infinite_TestSeries_Prod", dynamodb_access.test_series),
            ("TestSereiesRecord_Prod", dynamodb_access.test_records),
            ("LearningRecord_Prod", dynamodb_access.learning_records),
            ("Question_Prod", dynamodb_access.questions),
            ("presentation_prod", dynamodb_access.presentations),
            ("ICP_Prod", dynamodb_access.icp)
        ]
        
        for table_name, model in tables:
            try:
                # Try to scan one item to check if table is accessible
                model.scan_all(limit=1)
                table_status[table_name] = "accessible"
            except Exception as e:
                table_status[table_name] = f"error: {str(e)}"
        
        return {
            "status": "success",
            "tables": table_status,
            "timestamp": datetime.utcnow()
        }
    
    except Exception as e:
        logger.error(f"Failed to check table status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Table check failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
