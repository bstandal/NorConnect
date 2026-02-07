CREATE CONSTRAINT person_pg_id IF NOT EXISTS
FOR (p:Person)
REQUIRE p.pg_id IS UNIQUE;

CREATE CONSTRAINT organization_pg_id IF NOT EXISTS
FOR (o:Organization)
REQUIRE o.pg_id IS UNIQUE;

CREATE CONSTRAINT role_event_pg_id IF NOT EXISTS
FOR (r:RoleEvent)
REQUIRE r.pg_id IS UNIQUE;

CREATE CONSTRAINT person_link_pg_id IF NOT EXISTS
FOR (pl:PersonLink)
REQUIRE pl.pg_id IS UNIQUE;

CREATE CONSTRAINT funding_flow_pg_id IF NOT EXISTS
FOR (f:FundingFlow)
REQUIRE f.pg_id IS UNIQUE;

CREATE CONSTRAINT source_document_pg_id IF NOT EXISTS
FOR (s:SourceDocument)
REQUIRE s.pg_id IS UNIQUE;

CREATE CONSTRAINT external_recipient_name_key IF NOT EXISTS
FOR (e:ExternalRecipient)
REQUIRE e.name_key IS UNIQUE;

CREATE CONSTRAINT country_code IF NOT EXISTS
FOR (c:Country)
REQUIRE c.code IS UNIQUE;

CREATE INDEX person_name IF NOT EXISTS
FOR (p:Person)
ON (p.name);

CREATE INDEX organization_name IF NOT EXISTS
FOR (o:Organization)
ON (o.name);
