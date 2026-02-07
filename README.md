# NorConnect

Norske bindinger mellom topproller, internasjonale organisasjoner og finansieringsstrømmer.

## NONGO: nettverk + finansieringsanalyse

Dette repoet setter opp en MVP-arkitektur for å kartlegge bindinger mellom norske toppolitikere/diplomater, internasjonale organisasjoner og finansieringsstrømmer.

## Målarkitektur
- **Postgres (source of truth):** normaliserte fakta + kildesporing.
- **Neo4j (graph read model):** nettverksspørringer og visualisering.
- **Python-scripts:** migrasjoner, ingest fra Excel, sync fra Postgres til Neo4j.

## Struktur
- `docker-compose.yml` - lokal Postgres + Neo4j.
- `db/migrations/0001_init.sql` - første datamodell i Postgres.
- `db/neo4j/0001_constraints.cypher` - constraints/indexer i Neo4j.
- `scripts/run_migrations.py` - enkel migrasjonsrunner.
- `scripts/ingest_excel.py` - ingest til staging-tabeller.
- `scripts/normalize_staging.py` - normalisering fra staging til kjerne-tabeller.
- `scripts/enrich_norad_oecd.py` - beriker `funding_flow` med Norad API + OECD DAC2A.
- `scripts/sync_neo4j.py` - projiserer normaliserte data til graf.
- `app/main.py` - lokal webserver/API for graf, tidslinje, topplister og brokoblinger.
- `app/static/` - frontend (nettverk, tidslinje, topplister, kildepanel).

## Kom i gang
1. Kopier miljøvariabler:
   - `cp .env.example .env`
2. Start databaser:
   - `docker compose up -d`
   - Hvis porter er opptatt lokalt, juster `POSTGRES_PORT`, `NEO4J_PORT` og `NEO4J_HTTP_PORT` i `.env`.
3. Installer avhengigheter:
   - `python3 -m venv .venv && source .venv/bin/activate`
   - `pip install -e .`
4. Kjør migrasjoner:
   - `python scripts/run_migrations.py`
5. Last inn Excel til staging:
   - `python scripts/ingest_excel.py --file "$EXCEL_PATH"`
6. Normaliser staging til kjerne-tabeller:
   - `python scripts/normalize_staging.py --truncate-core`
7. Berik med offentlige data:
   - `python scripts/enrich_norad_oecd.py`
8. Opprett constraints i Neo4j:
   - `python scripts/sync_neo4j.py --init-only`
9. Sync data til graf:
   - `python scripts/sync_neo4j.py`
10. Start webvisning:
   - `uvicorn app.main:app --reload --port 8080`
   - Åpne `http://127.0.0.1:8080`

## Datamodell (første versjon)
Kjerneentiteter i Postgres:
- `person`
- `organization`
- `source_document`
- `role_event`
- `funding_flow`

Kildebevis/junction-tabeller:
- `person_source_document`
- `organization_source_document`
- `role_event_source_document`
- `funding_flow_source_document`

Staging for råimport:
- `stg_excel_organisasjoner`
- `stg_excel_datakilder`

## Neste steg
- Bygg `entity_resolution`-pipeline (alias + deduplisering).
- Gjør normaliseringspipeline mer robust (flerkilder, strengere idempotens, kvalitetsregler).
- Utvid web-UI med tidslinjevisning, eksport og mer avanserte nettverksmål.
- Implementer tidsserieindikatorer for "før/under/etter tiltredelse".
