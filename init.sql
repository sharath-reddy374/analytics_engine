-- Initialize PostgreSQL extensions and setup
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS citext;

-- Create Airflow database
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO edyou;

-- Create vector extension if available (for pgvector)
-- CREATE EXTENSION IF NOT EXISTS vector;
