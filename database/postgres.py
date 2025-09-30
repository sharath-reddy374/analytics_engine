import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from uuid import UUID
import json
from psycopg2.extras import Json
from sqlalchemy import text
from database.connection import engine

logger = logging.getLogger(__name__)


def _ensure_engine():
    if engine is None:
        raise RuntimeError(
            "Postgres engine is not initialized. Set DATABASE_URL in .env and install required deps."
        )


def fetch_one(sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        row = conn.execute(text(sql), params or {})
        r = row.mappings().first()
        return dict(r) if r else None


def fetch_all(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        res = conn.execute(text(sql), params or {})
        return [dict(r) for r in res.mappings().all()]


def execute(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        conn.execute(text(sql), params or {})


# ============ Domain helpers ============

def upsert_user_by_email(email: str, first_name: Optional[str] = None, last_name: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> UUID:
    """
    Returns user id (uuid). Ensures a user row exists keyed by email (citext unique).
    """
    _ensure_engine()
    meta = metadata or {}
    with engine.begin() as conn:  # type: ignore
        # Try existing
        row = conn.execute(
            text("SELECT id FROM public.users WHERE email = :email"),
            {"email": email},
        ).mappings().first()
        if row:
            user_id = row["id"]
            # Optionally update names/metadata
            conn.execute(
                text("""
                    UPDATE public.users
                    SET first_name = COALESCE(:first_name, first_name),
                        last_name = COALESCE(:last_name, last_name),
                        updated_at = now()
                    WHERE id = :id
                """),
                {"id": user_id, "first_name": first_name, "last_name": last_name},
            )
            return user_id

        # Insert new
        row = conn.execute(
            text("""
                INSERT INTO public.users (email, first_name, last_name, metadata)
                VALUES (:email, :first_name, :last_name, :metadata)
                RETURNING id
            """),
            {"email": email, "first_name": first_name, "last_name": last_name, "metadata": Json(meta)},
        ).mappings().first()
        return row["id"]  # type: ignore


def start_run(user_id: UUID, context: Optional[Dict[str, Any]] = None) -> UUID:
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        row = conn.execute(
            text("""
                INSERT INTO public.runs (user_id, status, context)
                VALUES (:user_id, 'started', :context)
                RETURNING id
            """),
            {"user_id": user_id, "context": Json(context or {})},
        ).mappings().first()
        return row["id"]  # type: ignore


def finish_run(run_id: UUID, status: str = "success") -> None:
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        conn.execute(
            text("""
                UPDATE public.runs
                SET status = :status, finished_at = now()
                WHERE id = :run_id
            """),
            {"run_id": run_id, "status": status},
        )


def log_event(
    event_type: str,
    payload: Optional[Dict[str, Any]] = None,
    user_id: Optional[UUID] = None,
    run_id: Optional[UUID] = None,
    dedupe_key: Optional[str] = None,
) -> Optional[UUID]:
    """
    Insert an event row. If dedupe_key provided, uses ON CONFLICT DO NOTHING.
    Returns event id if inserted, else None.
    """
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        if dedupe_key:
            row = conn.execute(
                text("""
                    INSERT INTO public.events (run_id, user_id, event_type, payload, dedupe_key)
                    VALUES (:run_id, :user_id, :event_type, :payload, :dedupe_key)
                    ON CONFLICT (dedupe_key) DO NOTHING
                    RETURNING id
                """),
                {"run_id": run_id, "user_id": user_id, "event_type": event_type, "payload": Json(payload or {}), "dedupe_key": dedupe_key},
            ).mappings().first()
            return row["id"] if row else None
        else:
            row = conn.execute(
                text("""
                    INSERT INTO public.events (run_id, user_id, event_type, payload)
                    VALUES (:run_id, :user_id, :event_type, :payload)
                    RETURNING id
                """),
                {"run_id": run_id, "user_id": user_id, "event_type": event_type, "payload": Json(payload or {})},
            ).mappings().first()
            return row["id"]  # type: ignore


def insert_features(user_id: UUID, run_id: Optional[UUID], features: Dict[str, Any]) -> None:
    """
    Upsert per (user_id, name, run_id). Values stored as jsonb.
    """
    _ensure_engine()
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:  # type: ignore
        for name, value in (features or {}).items():
            conn.execute(
                text("""
                    INSERT INTO public.features (user_id, run_id, name, value, computed_at)
                    VALUES (:user_id, :run_id, :name, :value, now())
                    ON CONFLICT (user_id, name, run_id)
                    DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
                """),
                {"user_id": user_id, "run_id": run_id, "name": str(name), "value": Json(value)},
            )


def insert_decisions(user_id: UUID, run_id: Optional[UUID], decisions: List[Dict[str, Any]]) -> None:
    """
    Each decision: {rule_id|rule, decision, rationale?}
    """
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        for d in decisions or []:
            rule = d.get("rule_id") or d.get("rule") or "unknown_rule"
            decision_text = d.get("decision") or ("send_email" if d.get("template_id") else "skip")
            rationale = {
                "priority": d.get("priority"),
                "features": d.get("features"),
                "raw": d,
            }
            conn.execute(
                text("""
                    INSERT INTO public.decisions (user_id, run_id, rule, decision, rationale, decided_at)
                    VALUES (:user_id, :run_id, :rule, :decision, :rationale, now())
                """),
                {"user_id": user_id, "run_id": run_id, "rule": rule, "decision": decision_text, "rationale": Json(rationale)},
            )


def ensure_email_template(key: str, subject: str, body_html: str) -> None:
    """
    Ensures an email_templates row exists for referential integrity of email_attempts.
    """
    _ensure_engine()
    with engine.begin() as conn:  # type: ignore
        existing = conn.execute(
            text("SELECT key FROM public.email_templates WHERE key = :k"),
            {"k": key},
        ).mappings().first()
        if existing:
            return
        conn.execute(
            text("""
                INSERT INTO public.email_templates (key, subject, body_html)
                VALUES (:key, :subject, :body_html)
            """),
            {"key": key, "subject": subject or key, "body_html": body_html or f"<p>{key}</p>"},
        )


def create_email_attempt(
    user_id: UUID,
    run_id: Optional[UUID],
    template_key: str,
    stage: str = "initial",
    status: str = "queued",
    reason: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    scheduled_at: Optional[datetime] = None,
    sent_at: Optional[datetime] = None,
) -> Tuple[Optional[UUID], str]:
    """
    Insert an email_attempts row with idempotency via unique_key (user_id:template_key:stage).
    Returns (attempt_id or None if duplicate, unique_key).
    """
    _ensure_engine()
    unique_key = f"{user_id}:{template_key}:{stage}"
    with engine.begin() as conn:  # type: ignore
        row = conn.execute(
            text("""
                INSERT INTO public.email_attempts
                    (user_id, run_id, template_key, stage, status, reason, unique_key, scheduled_at, sent_at, metadata)
                VALUES
                    (:user_id, :run_id, :template_key, :stage, :status, :reason, :unique_key, :scheduled_at, :sent_at, :metadata)
                ON CONFLICT (unique_key) DO NOTHING
                RETURNING id
            """),
            {
                "user_id": user_id,
                "run_id": run_id,
                "template_key": template_key,
                "stage": stage,
                "status": status,
                "reason": reason,
                "unique_key": unique_key,
                "scheduled_at": scheduled_at,
                "sent_at": sent_at,
                "metadata": Json(metadata or {}),
            },
        ).mappings().first()
        return (row["id"] if row else None, unique_key)


def update_email_attempt_status(
    unique_key: Optional[str] = None,
    attempt_id: Optional[UUID] = None,
    status: str = "sent",
    reason: Optional[str] = None,
) -> None:
    """
    Update an email_attempt by unique_key or id.
    """
    _ensure_engine()
    if not unique_key and not attempt_id:
        raise ValueError("Provide unique_key or attempt_id")
    with engine.begin() as conn:  # type: ignore
        if unique_key:
            conn.execute(
                text("""
                    UPDATE public.email_attempts
                    SET status = :status,
                        reason = COALESCE(:reason, reason),
                        sent_at = CASE WHEN :status = 'sent' THEN now() ELSE sent_at END
                    WHERE unique_key = :unique_key
                """),
                {"status": status, "reason": reason, "unique_key": unique_key},
            )
        else:
            conn.execute(
                text("""
                    UPDATE public.email_attempts
                    SET status = :status,
                        reason = COALESCE(:reason, reason),
                        sent_at = CASE WHEN :status = 'sent' THEN now() ELSE sent_at END
                    WHERE id = :id
                """),
                {"status": status, "reason": reason, "id": attempt_id},
            )
