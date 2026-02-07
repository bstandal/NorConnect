#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase
from psycopg.rows import dict_row

GRAPH_LABELS = [
    "Person",
    "Organization",
    "RoleEvent",
    "FundingFlow",
    "SourceDocument",
    "ExternalRecipient",
    "Country",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Postgres tables into Neo4j.")
    parser.add_argument(
        "--constraints-file",
        default="db/neo4j/0001_constraints.cypher",
        help="Cypher file with constraints and indexes.",
    )
    parser.add_argument(
        "--init-only",
        action="store_true",
        help="Apply constraints/indexes and exit.",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="Delete existing graph nodes for managed labels before sync.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for Neo4j writes.",
    )
    return parser.parse_args()


def split_cypher_statements(text: str) -> list[str]:
    statements: list[str] = []
    for part in text.split(";"):
        statement = part.strip()
        if statement:
            statements.append(statement)
    return statements


def normalize_for_neo4j(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def chunked(rows: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def convert_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    converted: list[dict[str, Any]] = []
    for row in rows:
        converted.append({k: normalize_for_neo4j(v) for k, v in row.items()})
    return converted


def fetch_rows(conn: psycopg.Connection, query: str) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(query).fetchall()]


def apply_constraints(session, file_path: Path) -> None:
    text = file_path.read_text(encoding="utf-8")
    statements = split_cypher_statements(text)
    for statement in statements:
        session.run(statement).consume()


def purge_graph(session) -> None:
    for label in GRAPH_LABELS:
        session.run(f"MATCH (n:{label}) DETACH DELETE n").consume()


def execute_in_batches(session, query: str, rows: list[dict[str, Any]], batch_size: int) -> None:
    for batch in chunked(rows, batch_size):
        session.run(query, rows=batch).consume()


def main() -> int:
    load_dotenv()
    args = parse_args()

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("POSTGRES_DSN is required.", file=sys.stderr)
        return 1

    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    if not all([neo4j_uri, neo4j_user, neo4j_password]):
        print("NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD are required.", file=sys.stderr)
        return 1

    constraints_file = Path(args.constraints_file)
    if not constraints_file.exists():
        print(f"Constraints file not found: {constraints_file}", file=sys.stderr)
        return 1

    with GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password)) as driver:
        with driver.session() as session:
            apply_constraints(session, constraints_file)

            if args.init_only:
                print("Neo4j constraints/indexes applied.")
                return 0

            if args.purge:
                purge_graph(session)

            with psycopg.connect(dsn, row_factory=dict_row) as conn:
                person_rows = convert_rows(
                    fetch_rows(conn, "SELECT id, canonical_name, country_code FROM person")
                )
                org_rows = convert_rows(
                    fetch_rows(conn, "SELECT id, canonical_name, org_type, hq_country FROM organization")
                )
                source_rows = convert_rows(
                    fetch_rows(
                        conn,
                        """
                        SELECT id, source_name, url, doc_type, published_at, retrieved_at
                        FROM source_document
                        """,
                    )
                )
                role_rows = convert_rows(
                    fetch_rows(
                        conn,
                        """
                        SELECT id, person_id, organization_id, role_title, role_level,
                               norwegian_position_before, announced_on, start_on, end_on,
                               confidence
                        FROM role_event
                        """,
                    )
                )
                funding_rows = convert_rows(
                    fetch_rows(
                        conn,
                        """
                        SELECT id, donor_organization_id, donor_country_code,
                               recipient_organization_id, recipient_name_raw,
                               funding_channel, amount_nok, amount_original,
                               currency_code, fiscal_year, period_start, period_end,
                               confidence
                        FROM funding_flow
                        """,
                    )
                )
                role_source_rows = convert_rows(
                    fetch_rows(
                        conn,
                        """
                        SELECT role_event_id, source_document_id, relation_type
                        FROM role_event_source_document
                        """,
                    )
                )
                funding_source_rows = convert_rows(
                    fetch_rows(
                        conn,
                        """
                        SELECT funding_flow_id, source_document_id, relation_type
                        FROM funding_flow_source_document
                        """,
                    )
                )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (p:Person {pg_id: row.id})
                SET p.name = row.canonical_name,
                    p.country_code = row.country_code
                """,
                person_rows,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (o:Organization {pg_id: row.id})
                SET o.name = row.canonical_name,
                    o.org_type = row.org_type,
                    o.hq_country = row.hq_country
                """,
                org_rows,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (s:SourceDocument {pg_id: row.id})
                SET s.source_name = row.source_name,
                    s.url = row.url,
                    s.doc_type = row.doc_type,
                    s.published_at = row.published_at,
                    s.retrieved_at = row.retrieved_at
                """,
                source_rows,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (p:Person {pg_id: row.person_id})
                MATCH (o:Organization {pg_id: row.organization_id})
                MERGE (r:RoleEvent {pg_id: row.id})
                SET r.role_title = row.role_title,
                    r.role_level = row.role_level,
                    r.norwegian_position_before = row.norwegian_position_before,
                    r.announced_on = row.announced_on,
                    r.start_on = row.start_on,
                    r.end_on = row.end_on,
                    r.confidence = row.confidence
                MERGE (p)-[:HELD_ROLE]->(r)
                MERGE (r)-[:AT_ORGANIZATION]->(o)
                """,
                role_rows,
                args.batch_size,
            )

            donor_org_recipient_org = [
                row
                for row in funding_rows
                if row.get("donor_organization_id") is not None
                and row.get("recipient_organization_id") is not None
            ]
            donor_org_recipient_external = []
            for row in funding_rows:
                if row.get("donor_organization_id") is None:
                    continue
                if row.get("recipient_organization_id") is not None:
                    continue
                recipient_raw = row.get("recipient_name_raw")
                if not recipient_raw:
                    continue
                row["recipient_name_key"] = re.sub(r"\s+", " ", str(recipient_raw).strip().lower())
                donor_org_recipient_external.append(row)

            donor_country_recipient_org = [
                row
                for row in funding_rows
                if row.get("donor_organization_id") is None
                and row.get("donor_country_code") is not None
                and row.get("recipient_organization_id") is not None
            ]
            donor_country_recipient_external = []
            for row in funding_rows:
                if row.get("donor_organization_id") is not None:
                    continue
                if row.get("donor_country_code") is None:
                    continue
                if row.get("recipient_organization_id") is not None:
                    continue
                recipient_raw = row.get("recipient_name_raw")
                if not recipient_raw:
                    continue
                row["recipient_name_key"] = re.sub(r"\s+", " ", str(recipient_raw).strip().lower())
                donor_country_recipient_external.append(row)

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (d:Organization {pg_id: row.donor_organization_id})
                MATCH (rorg:Organization {pg_id: row.recipient_organization_id})
                MERGE (f:FundingFlow {pg_id: row.id})
                SET f.funding_channel = row.funding_channel,
                    f.amount_nok = row.amount_nok,
                    f.amount_original = row.amount_original,
                    f.currency_code = row.currency_code,
                    f.fiscal_year = row.fiscal_year,
                    f.period_start = row.period_start,
                    f.period_end = row.period_end,
                    f.confidence = row.confidence
                MERGE (d)-[:FUNDED]->(f)
                MERGE (f)-[:TO_ORGANIZATION]->(rorg)
                """,
                donor_org_recipient_org,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (d:Organization {pg_id: row.donor_organization_id})
                MERGE (e:ExternalRecipient {name_key: row.recipient_name_key})
                SET e.name = row.recipient_name_raw
                MERGE (f:FundingFlow {pg_id: row.id})
                SET f.funding_channel = row.funding_channel,
                    f.amount_nok = row.amount_nok,
                    f.amount_original = row.amount_original,
                    f.currency_code = row.currency_code,
                    f.fiscal_year = row.fiscal_year,
                    f.period_start = row.period_start,
                    f.period_end = row.period_end,
                    f.confidence = row.confidence
                MERGE (d)-[:FUNDED]->(f)
                MERGE (f)-[:TO_EXTERNAL_RECIPIENT]->(e)
                """,
                donor_org_recipient_external,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (c:Country {code: row.donor_country_code})
                WITH row, c
                MATCH (rorg:Organization {pg_id: row.recipient_organization_id})
                MERGE (f:FundingFlow {pg_id: row.id})
                SET f.funding_channel = row.funding_channel,
                    f.amount_nok = row.amount_nok,
                    f.amount_original = row.amount_original,
                    f.currency_code = row.currency_code,
                    f.fiscal_year = row.fiscal_year,
                    f.period_start = row.period_start,
                    f.period_end = row.period_end,
                    f.confidence = row.confidence
                MERGE (c)-[:FUNDED]->(f)
                MERGE (f)-[:TO_ORGANIZATION]->(rorg)
                """,
                donor_country_recipient_org,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MERGE (c:Country {code: row.donor_country_code})
                MERGE (e:ExternalRecipient {name_key: row.recipient_name_key})
                SET e.name = row.recipient_name_raw
                MERGE (f:FundingFlow {pg_id: row.id})
                SET f.funding_channel = row.funding_channel,
                    f.amount_nok = row.amount_nok,
                    f.amount_original = row.amount_original,
                    f.currency_code = row.currency_code,
                    f.fiscal_year = row.fiscal_year,
                    f.period_start = row.period_start,
                    f.period_end = row.period_end,
                    f.confidence = row.confidence
                MERGE (c)-[:FUNDED]->(f)
                MERGE (f)-[:TO_EXTERNAL_RECIPIENT]->(e)
                """,
                donor_country_recipient_external,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (r:RoleEvent {pg_id: row.role_event_id})
                MATCH (s:SourceDocument {pg_id: row.source_document_id})
                MERGE (r)-[rel:SUPPORTED_BY {relation_type: row.relation_type}]->(s)
                """,
                role_source_rows,
                args.batch_size,
            )

            execute_in_batches(
                session,
                """
                UNWIND $rows AS row
                MATCH (f:FundingFlow {pg_id: row.funding_flow_id})
                MATCH (s:SourceDocument {pg_id: row.source_document_id})
                MERGE (f)-[rel:SUPPORTED_BY {relation_type: row.relation_type}]->(s)
                """,
                funding_source_rows,
                args.batch_size,
            )

    print("Neo4j sync complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
