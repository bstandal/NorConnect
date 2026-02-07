.PHONY: up down migrate ingest normalize seed-curated harvest-iati normalize-iati enrich palestine-orgs open-data sync sync-init web

up:
	docker compose up -d

down:
	docker compose down

migrate:
	python scripts/run_migrations.py

ingest:
	python scripts/ingest_excel.py --file "$${EXCEL_PATH}"

normalize:
	python scripts/normalize_staging.py --truncate-core

seed-curated:
	python scripts/seed_curated_network.py

harvest-iati:
	python scripts/harvest_iati_registry.py

normalize-iati:
	python scripts/normalize_iati_staging.py

enrich:
	python scripts/enrich_norad_oecd.py

palestine-orgs:
	python scripts/load_palestine_organizations.py --start-year 1990 --truncate-history

open-data:
	python scripts/harvest_iati_registry.py
	python scripts/normalize_iati_staging.py

sync-init:
	python scripts/sync_neo4j.py --init-only

sync:
	python scripts/sync_neo4j.py

web:
	uvicorn app.main:app --reload --port 8080
