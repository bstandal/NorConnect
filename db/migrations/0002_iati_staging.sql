BEGIN;

CREATE TABLE IF NOT EXISTS funding_flow_ingest_key (
  source_system TEXT NOT NULL,
  event_key TEXT NOT NULL,
  funding_flow_id BIGINT NOT NULL REFERENCES funding_flow(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_system, event_key)
);
CREATE INDEX IF NOT EXISTS idx_funding_flow_ingest_key_flow
  ON funding_flow_ingest_key (funding_flow_id);

CREATE TABLE IF NOT EXISTS stg_iati_transaction (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ingest_run_id BIGINT NOT NULL REFERENCES ingest_run(id) ON DELETE CASCADE,
  registry_query TEXT NOT NULL,
  package_name TEXT NOT NULL,
  package_title TEXT,
  package_url TEXT,
  publisher_iati_id TEXT,
  resource_id TEXT,
  resource_name TEXT,
  resource_format TEXT,
  resource_url TEXT NOT NULL,
  activity_iati_identifier TEXT NOT NULL,
  activity_title TEXT,
  reporting_org_ref TEXT,
  reporting_org_name TEXT,
  recipient_country_code CHAR(2),
  transaction_ref TEXT,
  transaction_type_code TEXT,
  transaction_date DATE,
  value_date DATE,
  value_amount NUMERIC(24, 4),
  value_currency CHAR(3),
  receiver_org_ref TEXT,
  receiver_org_name TEXT,
  provider_org_ref TEXT,
  provider_org_name TEXT,
  event_key TEXT NOT NULL,
  row_payload JSONB NOT NULL,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (ingest_run_id, event_key)
);
CREATE INDEX IF NOT EXISTS idx_stg_iati_transaction_run
  ON stg_iati_transaction (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_stg_iati_transaction_activity
  ON stg_iati_transaction (activity_iati_identifier);
CREATE INDEX IF NOT EXISTS idx_stg_iati_transaction_resource
  ON stg_iati_transaction (resource_url);
CREATE INDEX IF NOT EXISTS idx_stg_iati_transaction_date
  ON stg_iati_transaction (transaction_date);

COMMIT;
