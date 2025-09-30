#!/usr/bin/env python3
"""
Supabase/Postgres smoke test.

- Validates that DATABASE_URL is loaded and shows parsed host/port.
- Attempts a simple SELECT now().
- Optional --write flag performs small inserts via database.postgres helpers:
  * upsert a users row
  * create a runs row
  * insert an events row
  * ensure an email_templates row
  * queue an email_attempts row (idempotent)

Usage:
  python scripts/smoke_supabase_db.py
  python scripts/smoke_supabase_db.py --write
"""

import os
import sys
import argparse
import json
import time
import logging
from urllib.parse import urlparse

# Ensure project root on sys.path when called directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings
from database.connection import engine
from sqlalchemy import text

# Optional domain helpers if --write is used
try:
    from database import postgres as pg
except Exception:
    pg = None  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smoke_supabase_db")


def mask_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        # Mask password if present
        netloc = parsed.netloc
        if "@" in netloc and ":" in netloc.split("@")[0]:
            userinfo, hostport = netloc.split("@", 1)
            user, pwd = userinfo.split(":", 1)
            netloc = f"{user}:****@{hostport}"
        masked = parsed._replace(netloc=netloc).geturl()
        return masked
    except Exception:
        return url


def show_config():
    url = settings.DATABASE_URL
    logger.info("DATABASE_URL loaded: %s", mask_url(url) if url else None)
    if not url:
        logger.error("DATABASE_URL is not set. Set it in .env (see docs/supabase_setup.md).")
        return

    try:
        parsed = urlparse(url)
        logger.info("Parsed DB host: %s", parsed.hostname)
        logger.info("Parsed DB port: %s", parsed.port)
        logger.info("Parsed DB user: %s", parsed.username)
        logger.info("Parsed DB dbname/path: %s", parsed.path)
    except Exception as e:
        logger.warning("Failed to parse DATABASE_URL: %s", e)


def test_select_now():
    if engine is None:
        logger.error("SQLAlchemy engine is not initialized (engine is None).")
        return False

    try:
        with engine.begin() as conn:  # type: ignore
            row = conn.execute(text("SELECT now() AS now, current_user AS user")).mappings().first()
            logger.info("SELECT now(): %s | current_user: %s", row["now"], row["user"])  # type: ignore
        return True
    except Exception as e:
        logger.error("Connectivity SELECT failed: %s", e)
        return False


def perform_writes():
    if pg is None:
        logger.error("database.postgres helpers not available.")
        return False

    try:
        email = f"smoke_test+{int(time.time())}@example.com"
        logger.info("Attempting upsert_user_by_email for %s", email)
        user_id = pg.upsert_user_by_email(email=email, first_name="Smoke", last_name="Test", metadata={"smoke": True})
        logger.info("Upserted user_id: %s", user_id)

        logger.info("Starting run...")
        run_id = pg.start_run(user_id=user_id, context={"smoke": True})
        logger.info("Started run_id: %s", run_id)

        logger.info("Inserting event...")
        ev_id = pg.log_event(event_type="smoke_test", payload={"ok": True}, user_id=user_id, run_id=run_id, dedupe_key=None)
        logger.info("Inserted event id: %s", ev_id)

        logger.info("Ensuring template...")
        pg.ensure_email_template(key="smoke_tpl", subject="Smoke Test", body_html="<p>Smoke OK</p>")
        logger.info("Ensured template 'smoke_tpl'")

        logger.info("Queueing email attempt...")
        attempt_id, unique_key = pg.create_email_attempt(
            user_id=user_id, run_id=run_id, template_key="smoke_tpl", stage="initial", status="queued", metadata={"smoke": True}
        )
        logger.info("Email attempt id: %s | unique_key: %s", attempt_id, unique_key)

        logger.info("Finishing run...")
        pg.finish_run(run_id, status="success")
        logger.info("Finished run successfully.")

        logger.info("Smoke writes completed successfully.")
        return True
    except Exception as e:
        logger.error("Write operations failed: %s", e)
        return False


def main():
    parser = argparse.ArgumentParser(description="Supabase/Postgres smoke test")
    parser.add_argument("--write", action="store_true", help="Perform small insert operations to verify writes")
    args = parser.parse_args()

    show_config()
    ok = test_select_now()
    if not ok:
        logger.error("Connectivity check failed. Writes will likely fail. Fix DATABASE_URL/host resolution first.")
        sys.exit(2)

    if args.write:
        success = perform_writes()
        sys.exit(0 if success else 1)
    else:
        logger.info("Connectivity OK. Skipping writes (run with --write to test inserts).")
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
