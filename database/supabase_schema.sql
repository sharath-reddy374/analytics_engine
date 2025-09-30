-- Supabase Postgres schema for analytics + email state
-- Safe to run in Supabase SQL editor. No CREATE DATABASE here.

-- Extensions (Supabase allows these)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS citext;

-- ==========================================
-- users
-- ==========================================
CREATE TABLE IF NOT EXISTS public.users (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  external_user_id citext UNIQUE,   -- optional external key (e.g., platform user_id)
  email citext UNIQUE,              -- optional; use if you key by email
  first_name text,
  last_name text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_email ON public.users (email);

-- ==========================================
-- runs: each invocation of process_single_user
-- ==========================================
CREATE TYPE run_status AS ENUM ('started', 'success', 'failed');

CREATE TABLE IF NOT EXISTS public.runs (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  status run_status NOT NULL DEFAULT 'started',
  context jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_runs_user_started ON public.runs (user_id, started_at DESC);

-- ==========================================
-- events: append-only audit of pipeline events
-- ==========================================
CREATE TABLE IF NOT EXISTS public.events (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_id uuid REFERENCES public.runs(id) ON DELETE SET NULL,
  user_id uuid REFERENCES public.users(id) ON DELETE SET NULL,
  event_type text NOT NULL,  -- e.g., ingestion, features_computed, decision, email_queued, email_sent, llm_prompt, llm_response, error
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  dedupe_key text UNIQUE    -- optional: set when you need idempotency
);

CREATE INDEX IF NOT EXISTS idx_events_user_time ON public.events (user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_type ON public.events (event_type);

-- ==========================================
-- features: computed features per run
-- ==========================================
CREATE TABLE IF NOT EXISTS public.features (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_id uuid REFERENCES public.runs(id) ON DELETE SET NULL,
  user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  name text NOT NULL,
  value jsonb NOT NULL,
  computed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, name, run_id)
);

CREATE INDEX IF NOT EXISTS idx_features_user_name ON public.features (user_id, name);

-- ==========================================
-- decisions: rule engine outcomes
-- ==========================================
CREATE TABLE IF NOT EXISTS public.decisions (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_id uuid REFERENCES public.runs(id) ON DELETE SET NULL,
  user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  rule text NOT NULL,        -- e.g., id from config/email_rules.yaml
  decision text NOT NULL,    -- e.g., send_email | skip | wait
  rationale jsonb NOT NULL DEFAULT '{}'::jsonb,
  decided_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_decisions_user_time ON public.decisions (user_id, decided_at DESC);

-- ==========================================
-- email_templates: source of truth for template metadata
-- ==========================================
CREATE TABLE IF NOT EXISTS public.email_templates (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  key text UNIQUE NOT NULL,  -- e.g., algebra_next_steps, bio_cells_help
  subject text NOT NULL,
  body_html text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ==========================================
-- email_attempts: email state machine + idempotency
-- unique_key prevents duplicates per user/template/stage
-- ==========================================
CREATE TYPE email_status AS ENUM ('queued', 'sent', 'failed', 'skipped');

CREATE TABLE IF NOT EXISTS public.email_attempts (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  run_id uuid REFERENCES public.runs(id) ON DELETE SET NULL,
  template_key text NOT NULL REFERENCES public.email_templates(key) ON DELETE RESTRICT,
  stage text NOT NULL,       -- e.g., initial, followup_1, followup_2
  status email_status NOT NULL DEFAULT 'queued',
  reason text,               -- skip reason or failure reason
  unique_key text UNIQUE,    -- e.g., concat(user_id, template_key, stage)
  scheduled_at timestamptz,  -- for delayed follow-ups
  sent_at timestamptz,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_email_attempts_user_status ON public.email_attempts (user_id, status);
CREATE INDEX IF NOT EXISTS idx_email_attempts_template ON public.email_attempts (template_key);

-- ==========================================
-- automation_triggers: scheduled rules to enqueue follow-ups
-- ==========================================
CREATE TABLE IF NOT EXISTS public.automation_triggers (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  trigger_type text NOT NULL,    -- e.g., no_response_after_days, feature_threshold_crossed
  params jsonb NOT NULL DEFAULT '{}'::jsonb,
  next_fire_at timestamptz,
  active boolean NOT NULL DEFAULT true,
  last_fired_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_automation_triggers_due ON public.automation_triggers (active, next_fire_at);

-- ==========================================
-- email_suppression: opt-outs per template or global
-- ==========================================
CREATE TABLE IF NOT EXISTS public.email_suppression (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  template_key text,         -- null means suppress all
  reason text,
  suppressed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, template_key)
);

-- ==========================================
-- Helper views (optional): latest features per user
-- ==========================================
CREATE OR REPLACE VIEW public.latest_features AS
SELECT DISTINCT ON (user_id, name)
  user_id, name, value, computed_at
FROM public.features
ORDER BY user_id, name, computed_at DESC;
