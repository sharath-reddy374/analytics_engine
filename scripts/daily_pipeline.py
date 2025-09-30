#!/usr/bin/env python3
"""
Daily data pipeline script for feature computation and email campaigns

Refined to support two modes:
- ORM mode (if SQLAlchemy + DATABASE_URL configured): uses DB-backed daily features and rule evaluation
- Dynamo-only mode (default): iterates users directly from DynamoDB, computes features from normalized events

This script is DRY RUN by default: it does NOT send emails.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import asyncio
from datetime import date

from config.settings import settings
from database.connection import SessionLocal

from services.feature_engine import FeatureEngine
from services.decision_engine import DecisionEngine
from services.data_processor import DataProcessor
from database.dynamodb_models import DataFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DRY_RUN = True  # Do not send emails; just compute and print summaries


def _orm_enabled() -> bool:
    """
    Detect whether ORM is enabled. Returns True if a DB session can be created.
    """
    try:
        db = SessionLocal()  # may raise if disabled
        try:
            db.close()
        except Exception:
            pass
        return True
    except Exception:
        return False


async def _run_pipeline_orm():
    """
    ORM-backed pipeline:
      - Compute daily features for all users (FeatureEngine.compute_daily_features)
      - Evaluate rule matches (DecisionEngine.evaluate_users_for_emails)
      - DRY RUN: print decisions; do not send
    """
    # Late import types only used in ORM path
    from sqlalchemy.orm import Session  # noqa: F401

    db = SessionLocal()
    try:
        logger.info("Computing daily features (ORM mode)...")
        feat = FeatureEngine()
        users_processed = feat.compute_daily_features(db)
        logger.info("Computed features for %s users (as_of=%s)", users_processed, date.today())

        dec = DecisionEngine()
        candidates = dec.evaluate_users_for_emails(db)

        logger.info("Found %s email candidates (DRY RUN)", len(candidates))
        # Print a concise summary
        for c in candidates[:20]:
            logger.info(
                "user_id=%s template=%s priority=%s rule=%s",
                c.get("user_id"),
                c.get("template_id"),
                c.get("priority"),
                c.get("rule_id"),
            )

        if not DRY_RUN:
            logger.info("Email sending disabled by configuration. No emails will be sent.")
        logger.info("Daily pipeline (ORM mode) completed.")
    except Exception as e:
        logger.error("Daily pipeline (ORM mode) failed: %s", e)
        raise
    finally:
        try:
            db.close()
        except Exception:
            pass


async def _run_pipeline_dynamo():
    """
    Dynamo-only pipeline:
      - List users from DynamoDB
      - For each, normalize events, compute features, run rule evaluation
      - DRY RUN: print decisions; do not send
    """
    fetcher = DataFetcher()
    processor = DataProcessor()
    feat = FeatureEngine()
    dec = DecisionEngine()

    try:
        users = fetcher.get_all_users() or []
        logger.info("Found %s users in DynamoDB (investor table).", len(users))

        total_events = 0
        total_decisions = 0
        users_with_decisions = 0

        for idx, u in enumerate(users, 1):
            email = (u or {}).get("email")
            if not email:
                continue

            # Normalize events from all sources
            events = processor.process_all_user_data(email)
            total_events += len(events)

            # Compute features and evaluate rules
            features = feat.compute_user_features(email, events)
            decisions = dec.evaluate_user(email, features)
            total_decisions += len(decisions)
            if decisions:
                users_with_decisions += 1

            # Print a small sample
            if decisions:
                d = decisions[0]
                logger.info(
                    "[%d/%d] %s → rule=%s template=%s priority=%s (DRY RUN)",
                    idx,
                    len(users),
                    email,
                    d.get("rule_id"),
                    d.get("template_id"),
                    d.get("priority"),
                )

        logger.info(
            "Dynamo pipeline summary: users=%s users_with_decisions=%s total_events=%s total_decisions=%s (DRY RUN)",
            len(users),
            users_with_decisions,
            total_events,
            total_decisions,
        )
        logger.info("Daily pipeline (Dynamo mode) completed.")
    except Exception as e:
        logger.error("Daily pipeline (Dynamo mode) failed: %s", e)
        raise


async def run_daily_pipeline():
    """Run the complete daily pipeline without sending emails."""
    logger.info("Starting daily pipeline (DRY RUN). ORM enabled: %s", _orm_enabled())
    if _orm_enabled():
        await _run_pipeline_orm()
    else:
        await _run_pipeline_dynamo()


if __name__ == "__main__":
    asyncio.run(run_daily_pipeline())
