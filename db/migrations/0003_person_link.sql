BEGIN;

CREATE TABLE IF NOT EXISTS person_link (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  person_a_id BIGINT NOT NULL REFERENCES person(id) ON DELETE RESTRICT,
  person_b_id BIGINT NOT NULL REFERENCES person(id) ON DELETE RESTRICT,
  relation_type TEXT NOT NULL,
  relation_label TEXT,
  start_on DATE,
  end_on DATE,
  confidence NUMERIC(4,3) NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (person_a_id <> person_b_id),
  CHECK (person_a_id < person_b_id),
  CHECK (start_on IS NULL OR end_on IS NULL OR end_on >= start_on),
  UNIQUE (person_a_id, person_b_id, relation_type, start_on)
);
CREATE INDEX IF NOT EXISTS idx_person_link_a ON person_link (person_a_id);
CREATE INDEX IF NOT EXISTS idx_person_link_b ON person_link (person_b_id);
CREATE INDEX IF NOT EXISTS idx_person_link_relation ON person_link (relation_type);

CREATE TABLE IF NOT EXISTS person_link_source_document (
  person_link_id BIGINT NOT NULL REFERENCES person_link(id) ON DELETE CASCADE,
  source_document_id BIGINT NOT NULL REFERENCES source_document(id) ON DELETE CASCADE,
  relation_type TEXT NOT NULL DEFAULT 'reference',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (person_link_id, source_document_id, relation_type)
);

DROP TRIGGER IF EXISTS trg_person_link_updated_at ON person_link;
CREATE TRIGGER trg_person_link_updated_at
BEFORE UPDATE ON person_link
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

COMMIT;
