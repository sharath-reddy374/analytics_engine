#!/usr/bin/env python3
"""
Apply a .sql schema file to the configured Postgres (e.g., Supabase) database.

Usage:
  python scripts/apply_sql.py database/supabase_schema.sql

Requirements:
  - Set DATABASE_URL in .env (Supabase Postgres connection string)
  - Install requirements (sqlalchemy, psycopg2-binary)
"""

import sys
import os
import logging

# Ensure project root on path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from database.connection import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/apply_sql.py <path-to-sql-file>")
        sys.exit(1)

    sql_path = sys.argv[1]
    if not os.path.exists(sql_path):
        print(f"SQL file not found: {sql_path}")
        sys.exit(1)

    if engine is None:
        print("Database engine is not initialized. Ensure DATABASE_URL is set in .env and dependencies are installed.")
        sys.exit(1)

    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        logger.info("Applying SQL from %s ...", sql_path)
        # Use exec_driver_sql to allow multiple statements
        with engine.begin() as conn:  # type: ignore
            conn.exec_driver_sql(sql_content)

        logger.info("SQL applied successfully.")
    except Exception as e:
        logger.error("Failed to apply SQL: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
