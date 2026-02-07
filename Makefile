.PHONY: up down migrate ingest normalize enrich sync sync-init web

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

enrich:
	python scripts/enrich_norad_oecd.py

sync-init:
	python scripts/sync_neo4j.py --init-only

sync:
	python scripts/sync_neo4j.py

web:
	uvicorn app.main:app --reload --port 8080
