from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from uuid import UUID, uuid4

class AppUser(BaseModel):
    user_id: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    tz: str = 'America/Los_Angeles'
    plan: Optional[str] = None
    status: Optional[str] = None
    tenant_id: Optional[str] = None
    tenant_name: Optional[str] = None
    consent_email: bool = True
    created_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None
    raw: Optional[Dict[str, Any]] = None
    updated_at: Optional[datetime] = None

class ContentItem(BaseModel):
    content_id: str
    content_type: Optional[str] = None  # presentation|quiz|lesson|section|question
    title: Optional[str] = None
    subject: Optional[str] = None
    grade_subject: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    ts: datetime
    name: str  # login|convo_msg|test_attempt|...
    source: Optional[str] = None
    session_id: Optional[str] = None
    props: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None

class ConvoSummary(BaseModel):
    session_id: str
    user_id: str
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    summary: Optional[str] = None
    topics: Optional[List[str]] = None
    sentiment: Optional[float] = None  # -1..+1
    needs: Optional[List[str]] = None
    embedding: Optional[List[float]] = None  # OpenAI embedding

class UserDailyFeatures(BaseModel):
    user_id: str
    as_of: date
    recency_days: Optional[int] = None
    frequency_7d: Optional[int] = None
    minutes_7d: Optional[int] = None
    tests_7d: Optional[int] = None
    avg_score_change_30d: Optional[float] = None
    top_topics: Optional[List[str]] = None
    subject_affinity: Optional[Dict[str, float]] = None  # {"Biology":0.7,"Algebra":0.3}
    convo_sentiment_7d_avg: Optional[float] = None
    churn_risk: Optional[str] = None  # low|med|high
    last_email_ts: Optional[datetime] = None
    emails_sent_7d: int = 0
    unsubscribed: bool = False

class EmailSend(BaseModel):
    send_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    ts: Optional[datetime] = None
    template_id: Optional[str] = None
    subject: Optional[str] = None
    provider_id: Optional[str] = None
    status: Optional[str] = None  # queued|sent|bounced|complaint|opened|clicked
    meta: Optional[Dict[str, Any]] = None

class Unsubscribe(BaseModel):
    user_id: str
    ts: Optional[datetime] = None
    reason: Optional[str] = None
