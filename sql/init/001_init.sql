-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;

-- Raw recalls: store both extracted fields + full payload
CREATE TABLE IF NOT EXISTS raw.recalls (
  recall_pk        TEXT PRIMARY KEY,          -- your deterministic key (idempotency)
  source           TEXT NOT NULL DEFAULT 'socrata',
  source_id        TEXT,                       -- if dataset has a stable id
  make             TEXT,
  model            TEXT,
  model_year       INT,
  component        TEXT,
  report_date      DATE,                       -- whatever recall date field maps to
  source_updated_at TIMESTAMPTZ,               -- if available
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw_payload      JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_recalls_report_date ON raw.recalls(report_date);
CREATE INDEX IF NOT EXISTS idx_raw_recalls_make ON raw.recalls(make);

-- Raw complaints
CREATE TABLE IF NOT EXISTS raw.complaints (
  complaint_pk     TEXT PRIMARY KEY,
  source           TEXT NOT NULL DEFAULT 'socrata',
  source_id        TEXT,
  odi_number       TEXT,                       -- if present
  make             TEXT,
  model            TEXT,
  model_year       INT,
  component        TEXT,
  incident_date    DATE,
  received_date    DATE,
  state            TEXT,
  source_updated_at TIMESTAMPTZ,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw_payload      JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_complaints_received_date ON raw.complaints(received_date);
CREATE INDEX IF NOT EXISTS idx_raw_complaints_make ON raw.complaints(make);
