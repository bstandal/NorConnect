BEGIN;

CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS ingest_run (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_name TEXT NOT NULL,
  input_path TEXT,
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS source_document (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_name TEXT,
  url TEXT NOT NULL UNIQUE,
  doc_type TEXT,
  published_at DATE,
  retrieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  sha256 TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS person (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  canonical_name CITEXT NOT NULL UNIQUE,
  country_code CHAR(2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS person_alias (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
  alias CITEXT NOT NULL,
  source_system TEXT,
  source_document_id BIGINT REFERENCES source_document(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (person_id, alias)
);
CREATE INDEX IF NOT EXISTS idx_person_alias_alias ON person_alias (alias);

CREATE TABLE IF NOT EXISTS organization (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  canonical_name CITEXT NOT NULL UNIQUE,
  org_type TEXT,
  hq_country TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS organization_alias (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  organization_id BIGINT NOT NULL REFERENCES organization(id) ON DELETE CASCADE,
  alias CITEXT NOT NULL,
  source_system TEXT,
  source_document_id BIGINT REFERENCES source_document(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (organization_id, alias)
);
CREATE INDEX IF NOT EXISTS idx_organization_alias_alias ON organization_alias (alias);

CREATE TABLE IF NOT EXISTS role_event (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE RESTRICT,
  organization_id BIGINT NOT NULL REFERENCES organization(id) ON DELETE RESTRICT,
  role_title TEXT NOT NULL,
  role_level TEXT,
  norwegian_position_before TEXT,
  announced_on DATE,
  start_on DATE,
  end_on DATE,
  confidence NUMERIC(4,3) NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (start_on IS NULL OR end_on IS NULL OR end_on >= start_on),
  UNIQUE (person_id, organization_id, role_title, start_on)
);
CREATE INDEX IF NOT EXISTS idx_role_event_person ON role_event (person_id);
CREATE INDEX IF NOT EXISTS idx_role_event_org ON role_event (organization_id);
CREATE INDEX IF NOT EXISTS idx_role_event_period ON role_event (start_on, end_on);

CREATE TABLE IF NOT EXISTS funding_flow (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  donor_organization_id BIGINT REFERENCES organization(id) ON DELETE SET NULL,
  donor_country_code CHAR(2),
  recipient_organization_id BIGINT REFERENCES organization(id) ON DELETE SET NULL,
  recipient_name_raw TEXT,
  funding_channel TEXT,
  amount_nok NUMERIC(20,2),
  amount_original NUMERIC(20,2),
  currency_code CHAR(3),
  fiscal_year INT CHECK (fiscal_year BETWEEN 1900 AND 2100),
  period_start DATE,
  period_end DATE,
  confidence NUMERIC(4,3) NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (period_start IS NULL OR period_end IS NULL OR period_end >= period_start),
  CHECK (recipient_organization_id IS NOT NULL OR recipient_name_raw IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_funding_flow_donor ON funding_flow (donor_organization_id);
CREATE INDEX IF NOT EXISTS idx_funding_flow_recipient ON funding_flow (recipient_organization_id);
CREATE INDEX IF NOT EXISTS idx_funding_flow_year ON funding_flow (fiscal_year);

CREATE TABLE IF NOT EXISTS person_source_document (
  person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
  source_document_id BIGINT NOT NULL REFERENCES source_document(id) ON DELETE CASCADE,
  relation_type TEXT NOT NULL DEFAULT 'bio',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (person_id, source_document_id, relation_type)
);

CREATE TABLE IF NOT EXISTS organization_source_document (
  organization_id BIGINT NOT NULL REFERENCES organization(id) ON DELETE CASCADE,
  source_document_id BIGINT NOT NULL REFERENCES source_document(id) ON DELETE CASCADE,
  relation_type TEXT NOT NULL DEFAULT 'reference',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, source_document_id, relation_type)
);

CREATE TABLE IF NOT EXISTS role_event_source_document (
  role_event_id BIGINT NOT NULL REFERENCES role_event(id) ON DELETE CASCADE,
  source_document_id BIGINT NOT NULL REFERENCES source_document(id) ON DELETE CASCADE,
  relation_type TEXT NOT NULL DEFAULT 'appointment',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (role_event_id, source_document_id, relation_type)
);

CREATE TABLE IF NOT EXISTS funding_flow_source_document (
  funding_flow_id BIGINT NOT NULL REFERENCES funding_flow(id) ON DELETE CASCADE,
  source_document_id BIGINT NOT NULL REFERENCES source_document(id) ON DELETE CASCADE,
  relation_type TEXT NOT NULL DEFAULT 'donor_report',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (funding_flow_id, source_document_id, relation_type)
);

CREATE TABLE IF NOT EXISTS stg_excel_organisasjoner (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ingest_run_id BIGINT NOT NULL REFERENCES ingest_run(id) ON DELETE CASCADE,
  excel_source_path TEXT NOT NULL,
  excel_sheet TEXT NOT NULL,
  excel_row INT NOT NULL,
  row_payload JSONB NOT NULL,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_excel_organisasjoner_run ON stg_excel_organisasjoner (ingest_run_id);

CREATE TABLE IF NOT EXISTS stg_excel_datakilder (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ingest_run_id BIGINT NOT NULL REFERENCES ingest_run(id) ON DELETE CASCADE,
  excel_source_path TEXT NOT NULL,
  excel_sheet TEXT NOT NULL,
  excel_row INT NOT NULL,
  row_payload JSONB NOT NULL,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stg_excel_datakilder_run ON stg_excel_datakilder (ingest_run_id);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_person_updated_at ON person;
CREATE TRIGGER trg_person_updated_at
BEFORE UPDATE ON person
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_organization_updated_at ON organization;
CREATE TRIGGER trg_organization_updated_at
BEFORE UPDATE ON organization
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_role_event_updated_at ON role_event;
CREATE TRIGGER trg_role_event_updated_at
BEFORE UPDATE ON role_event
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_funding_flow_updated_at ON funding_flow;
CREATE TRIGGER trg_funding_flow_updated_at
BEFORE UPDATE ON funding_flow
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
