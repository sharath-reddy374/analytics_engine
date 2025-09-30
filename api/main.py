from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import json

from database.dynamodb_connection import get_dynamodb_client, verify_existing_tables
from database.dynamodb_models import DataFetcher
from services.data_processor import DataProcessor
from config.settings import settings
from database import postgres as pg

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

# =========================
# Admin/ops read-only APIs
# =========================

def _resolve_user(email: Optional[str], user_id: Optional[str]) -> Dict[str, Any]:
    """
    Resolve user by email or user_id. Returns dict with user row and id.
    Raises HTTPException if not found.
    """
    if user_id:
        row = pg.fetch_one(
            "SELECT id, email, first_name, last_name, created_at, updated_at FROM public.users WHERE id = :id",
            {"id": user_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="User not found by id")
        return row
    if email:
        row = pg.fetch_one(
            "SELECT id, email, first_name, last_name, created_at, updated_at FROM public.users WHERE email ILIKE :email",
            {"email": email},
        )
        if not row:
            raise HTTPException(status_code=404, detail="User not found by email")
        return row
    raise HTTPException(status_code=400, detail="Provide email or user_id")


@app.get("/admin/user")
async def admin_get_user(email: str):
    """
    Get user row by email.
    """
    try:
        user = _resolve_user(email, None)
        return {"user": user}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_user failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/runs")
async def admin_get_runs(email: Optional[str] = None, user_id: Optional[str] = None, limit: int = 10):
    """
    Get recent runs for a user (by email or user_id).
    """
    try:
        user = _resolve_user(email, user_id)
        runs = pg.fetch_all(
            """
            SELECT id, user_id, started_at, finished_at, status, context
            FROM public.runs
            WHERE user_id = :uid
            ORDER BY started_at DESC
            LIMIT :limit
            """,
            {"uid": user["id"], "limit": limit},
        )
        return {"user": user, "runs": runs}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_runs failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/run/{run_id}")
async def admin_get_run(run_id: str):
    """
    Get a single run and its associated events, decisions, and email_attempts.
    """
    try:
        run = pg.fetch_one(
            "SELECT id, user_id, started_at, finished_at, status, context FROM public.runs WHERE id = :rid",
            {"rid": run_id},
        )
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        events = pg.fetch_all(
            """
            SELECT id, event_type, payload, occurred_at
            FROM public.events
            WHERE run_id = :rid
            ORDER BY occurred_at ASC
            """,
            {"rid": run_id},
        )
        decisions = pg.fetch_all(
            """
            SELECT id, rule, decision, rationale, decided_at
            FROM public.decisions
            WHERE run_id = :rid
            ORDER BY decided_at ASC
            """,
            {"rid": run_id},
        )
        attempts = pg.fetch_all(
            """
            SELECT id, template_key, stage, status, reason, unique_key, scheduled_at, sent_at, created_at
            FROM public.email_attempts
            WHERE run_id = :rid
            ORDER BY created_at ASC
            """,
            {"rid": run_id},
        )
        features = pg.fetch_all(
            """
            SELECT name, value, computed_at
            FROM public.features
            WHERE run_id = :rid
            ORDER BY computed_at ASC
            """,
            {"rid": run_id},
        )
        return {"run": run, "events": events, "decisions": decisions, "email_attempts": attempts, "features": features}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_run failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/events")
async def admin_get_events(
    run_id: Optional[str] = None, email: Optional[str] = None, user_id: Optional[str] = None, limit: int = 100
):
    """
    Get events by run_id or by user (email/user_id).
    """
    try:
        if run_id:
            rows = pg.fetch_all(
                """
                SELECT id, run_id, user_id, event_type, payload, occurred_at
                FROM public.events
                WHERE run_id = :rid
                ORDER BY occurred_at ASC
                LIMIT :limit
                """,
                {"rid": run_id, "limit": limit},
            )
            return {"events": rows}
        user = _resolve_user(email, user_id)
        rows = pg.fetch_all(
            """
            SELECT id, run_id, user_id, event_type, payload, occurred_at
            FROM public.events
            WHERE user_id = :uid
            ORDER BY occurred_at DESC
            LIMIT :limit
            """,
            {"uid": user["id"], "limit": limit},
        )
        return {"user": user, "events": rows}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_events failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/features")
async def admin_get_features(user_id: Optional[str] = None, email: Optional[str] = None, run_id: Optional[str] = None):
    """
    Get features either for a specific run or latest per feature for the user.
    """
    try:
        user = _resolve_user(email, user_id)
        if run_id:
            rows = pg.fetch_all(
                """
                SELECT name, value, computed_at
                FROM public.features
                WHERE user_id = :uid AND run_id = :rid
                ORDER BY computed_at DESC
                """,
                {"uid": user["id"], "rid": run_id},
            )
            return {"user": user, "run_id": run_id, "features": rows}
        # Latest features view
        rows = pg.fetch_all(
            """
            SELECT name, value, computed_at
            FROM public.latest_features
            WHERE user_id = :uid
            ORDER BY name ASC
            """,
            {"uid": user["id"]},
        )
        return {"user": user, "features": rows}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_features failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/decisions")
async def admin_get_decisions(user_id: Optional[str] = None, email: Optional[str] = None, run_id: Optional[str] = None, limit: int = 100):
    """
    Get decisions for a user (optionally filter by run).
    """
    try:
        user = _resolve_user(email, user_id)
        if run_id:
            rows = pg.fetch_all(
                """
                SELECT rule, decision, rationale, decided_at
                FROM public.decisions
                WHERE user_id = :uid AND run_id = :rid
                ORDER BY decided_at DESC
                LIMIT :limit
                """,
                {"uid": user["id"], "rid": run_id, "limit": limit},
            )
            return {"user": user, "run_id": run_id, "decisions": rows}
        rows = pg.fetch_all(
            """
            SELECT rule, decision, rationale, decided_at, run_id
            FROM public.decisions
            WHERE user_id = :uid
            ORDER BY decided_at DESC
            LIMIT :limit
            """,
            {"uid": user["id"], "limit": limit},
        )
        return {"user": user, "decisions": rows}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_decisions failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/email_attempts")
async def admin_get_email_attempts(
    user_id: Optional[str] = None, email: Optional[str] = None, status: Optional[str] = None, limit: int = 100
):
    """
    Get email attempts for a user (optionally filter by status).
    """
    try:
        user = _resolve_user(email, user_id)
        if status:
            rows = pg.fetch_all(
                """
                SELECT template_key, stage, status, reason, unique_key, scheduled_at, sent_at, created_at, run_id
                FROM public.email_attempts
                WHERE user_id = :uid AND status = :status
                ORDER BY created_at DESC
                LIMIT :limit
                """,
                {"uid": user["id"], "status": status, "limit": limit},
            )
        else:
            rows = pg.fetch_all(
                """
                SELECT template_key, stage, status, reason, unique_key, scheduled_at, sent_at, created_at, run_id
                FROM public.email_attempts
                WHERE user_id = :uid
                ORDER BY created_at DESC
                LIMIT :limit
                """,
                {"uid": user["id"], "limit": limit},
            )
        return {"user": user, "email_attempts": rows}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_email_attempts failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/user_overview")
async def admin_user_overview(email: str, limit: int = 100):
    """
    Composite overview: user, recent runs, latest run details (events, features, decisions, email attempts).
    """
    try:
        user = _resolve_user(email, None)
        runs = pg.fetch_all(
            """
            SELECT id, started_at, finished_at, status, context
            FROM public.runs
            WHERE user_id = :uid
            ORDER BY started_at DESC
            LIMIT 10
            """,
            {"uid": user["id"]},
        )
        latest_run = runs[0] if runs else None
        events = decisions = attempts = features = []
        if latest_run:
            rid = latest_run["id"]
            events = pg.fetch_all(
                """
                SELECT id, event_type, payload, occurred_at
                FROM public.events
                WHERE run_id = :rid
                ORDER BY occurred_at ASC
                """,
                {"rid": rid},
            )
            decisions = pg.fetch_all(
                """
                SELECT rule, decision, rationale, decided_at
                FROM public.decisions
                WHERE run_id = :rid
                ORDER BY decided_at ASC
                """,
                {"rid": rid},
            )
            attempts = pg.fetch_all(
                """
                SELECT template_key, stage, status, reason, unique_key, scheduled_at, sent_at, created_at
                FROM public.email_attempts
                WHERE run_id = :rid
                ORDER BY created_at DESC
                """,
                {"rid": rid},
            )
            features = pg.fetch_all(
                """
                SELECT name, value, computed_at
                FROM public.features
                WHERE run_id = :rid
                ORDER BY computed_at ASC
                """,
                {"rid": rid},
            )
        latest_features = pg.fetch_all(
            """
            SELECT name, value, computed_at
            FROM public.latest_features
            WHERE user_id = :uid
            ORDER BY name ASC
            """,
            {"uid": user["id"]},
        )
        return {
            "user": user,
            "runs": runs,
            "latest_run": latest_run,
            "latest_run_events": events,
            "latest_run_decisions": decisions,
            "latest_run_email_attempts": attempts,
            "latest_run_features": features,
            "latest_features_view": latest_features,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_user_overview failed")
        raise HTTPException(status_code=500, detail=str(e))


# ================
# Tenant endpoints
# ================

@app.get("/admin/tenants")
async def admin_get_tenants():
    """
    List tenants aggregated from investor profiles (tenantName).
    """
    try:
        users = dynamodb_access.investor_prod.get_all_users()
        counts: Dict[str, int] = {}
        for u in users or []:
            tn = (u.get("tenantName") or u.get("tenant") or "unknown") or "unknown"
            counts[tn] = counts.get(tn, 0) + 1
        tenants = [{"tenantName": k, "count": v} for k, v in counts.items()]
        tenants.sort(key=lambda x: x["count"], reverse=True)
        return {"tenants": tenants, "total_users": len(users or [])}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_tenants failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/users")
async def admin_get_users(tenantName: Optional[str] = None, q: Optional[str] = None, limit: int = 100, offset: int = 0):
    """
    List users filtered by tenantName and optional search query (q).
    q matches on email, name, first_name, last_name (case-insensitive).
    """
    try:
        users = dynamodb_access.investor_prod.get_all_users() or []
        filtered = []
        ql = (q or "").strip().lower()
        tnl = (tenantName or "").strip().lower()

        for u in users:
            tn = (u.get("tenantName") or u.get("tenant") or "") or ""
            if tnl and tn.lower() != tnl:
                continue

            if ql:
                em = (u.get("email") or "").lower()
                nm = (u.get("name") or "")
                fn = (u.get("first_name") or "")
                ln = (u.get("last_name") or "")
                hay = " ".join([em, nm, fn, ln]).lower()
                if ql not in hay:
                    continue

            filtered.append({
                "email": u.get("email"),
                "tenantName": tn,
                "first_name": u.get("first_name"),
                "last_name": u.get("last_name"),
                "name": u.get("name"),
            })

        total = len(filtered)
        page = filtered[offset: offset + max(0, int(limit))]
        return {"total": total, "count": len(page), "items": page, "tenantName": tenantName, "q": q}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin_get_users failed")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
