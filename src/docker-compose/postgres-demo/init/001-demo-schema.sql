CREATE DATABASE analytics_pg;

\connect workflow_pg

CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  full_name TEXT NOT NULL,
  lifecycle_state TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS deployment_requests (
  id SERIAL PRIMARY KEY,
  ticket TEXT NOT NULL UNIQUE,
  workflow_kind TEXT NOT NULL,
  requested_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO customers (email, full_name, lifecycle_state)
VALUES
  ('ivy@example.com', 'Ivy Walsh', 'active'),
  ('liam@example.com', 'Liam Price', 'paused')
ON CONFLICT (email) DO UPDATE SET
  full_name = EXCLUDED.full_name,
  lifecycle_state = EXCLUDED.lifecycle_state;

INSERT INTO deployment_requests (ticket, workflow_kind, requested_by)
VALUES
  ('REQ-2001', 'dml', 'demo_requester'),
  ('REQ-2002', 'ddl', 'demo_requester')
ON CONFLICT (ticket) DO UPDATE SET
  workflow_kind = EXCLUDED.workflow_kind,
  requested_by = EXCLUDED.requested_by;

\connect analytics_pg

CREATE TABLE IF NOT EXISTS daily_metrics (
  metric_day DATE PRIMARY KEY,
  signups INTEGER NOT NULL,
  revenue NUMERIC(10, 2) NOT NULL
);

INSERT INTO daily_metrics (metric_day, signups, revenue)
VALUES
  ('2026-01-01', 12, 123.45),
  ('2026-01-02', 8, 88.10)
ON CONFLICT (metric_day) DO UPDATE SET
  signups = EXCLUDED.signups,
  revenue = EXCLUDED.revenue;
