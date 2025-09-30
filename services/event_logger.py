import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from uuid import UUID

from database import postgres as pg

logger = logging.getLogger(__name__)


class EventLogger:
    """
    Thin wrapper around database.postgres helpers to:
    - Ensure a user row exists
    - Create/finish a run row
    - Emit structured events
    - Persist features and decisions
    - Create/update email attempts with idempotency
    """

    def __init__(self):
        pass

    def ensure_user(self, email: str, first_name: Optional[str] = None, last_name: Optional[str] = None,
                    metadata: Optional[Dict[str, Any]] = None) -> UUID:
        return pg.upsert_user_by_email(email=email, first_name=first_name, last_name=last_name, metadata=metadata)

    def start_run(self, user_id: UUID, context: Optional[Dict[str, Any]] = None) -> UUID:
        return pg.start_run(user_id=user_id, context=context or {})

    def finish_run(self, run_id: UUID, success: bool = True):
        pg.finish_run(run_id, status="success" if success else "failed")

    def event(self,
              event_type: str,
              payload: Optional[Dict[str, Any]] = None,
              user_id: Optional[UUID] = None,
              run_id: Optional[UUID] = None,
              dedupe_key: Optional[str] = None) -> Optional[UUID]:
        return pg.log_event(event_type=event_type, payload=payload or {}, user_id=user_id, run_id=run_id, dedupe_key=dedupe_key)

    def save_features(self, user_id: UUID, run_id: Optional[UUID], features: Dict[str, Any]) -> None:
        # Convert non-JSON-serializable values if needed
        safe_features: Dict[str, Any] = {}
        for k, v in (features or {}).items():
            try:
                _ = (v if isinstance(v, (dict, list, str, int, float, bool)) or v is None else str(v))
                safe_features[k] = v if isinstance(v, (dict, list, str, int, float, bool)) or v is None else str(v)
            except Exception:
                safe_features[k] = str(v)
        pg.insert_features(user_id=user_id, run_id=run_id, features=safe_features)

    def save_decisions(self, user_id: UUID, run_id: Optional[UUID], decisions: List[Dict[str, Any]]) -> None:
        pg.insert_decisions(user_id=user_id, run_id=run_id, decisions=decisions or [])

    def ensure_template(self, key: str, subject: str, body_html: str) -> None:
        pg.ensure_email_template(key=key, subject=subject, body_html=body_html)

    def queue_email(self,
                    user_id: UUID,
                    run_id: Optional[UUID],
                    template_key: str,
                    stage: str = "initial",
                    metadata: Optional[Dict[str, Any]] = None,
                    scheduled_at: Optional[datetime] = None) -> Tuple[Optional[UUID], str]:
        return pg.create_email_attempt(
            user_id=user_id,
            run_id=run_id,
            template_key=template_key,
            stage=stage,
            status="queued",
            reason=None,
            metadata=metadata or {},
            scheduled_at=scheduled_at,
            sent_at=None,
        )

    def mark_email_sent(self, unique_key: Optional[str] = None, attempt_id: Optional[UUID] = None):
        pg.update_email_attempt_status(unique_key=unique_key, attempt_id=attempt_id, status="sent")

    def mark_email_failed(self, reason: str, unique_key: Optional[str] = None, attempt_id: Optional[UUID] = None):
        pg.update_email_attempt_status(unique_key=unique_key, attempt_id=attempt_id, status="failed", reason=reason)

    def mark_email_skipped(self, reason: str, unique_key: Optional[str] = None, attempt_id: Optional[UUID] = None):
        pg.update_email_attempt_status(unique_key=unique_key, attempt_id=attempt_id, status="skipped", reason=reason)
