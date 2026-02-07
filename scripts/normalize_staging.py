#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize Excel staging rows into core Postgres tables."
    )
    parser.add_argument(
        "--run-id",
        type=int,
        help="Ingest run id to normalize (default: latest successful excel run).",
    )
    parser.add_argument(
        "--truncate-core",
        action="store_true",
        help="Truncate core tables before normalizing.",
    )
    return parser.parse_args()


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    text = str(value).strip()
    if not text:
        return None

    if len(text) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", text):
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            pass

    for fmt in ("%d.%m.%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


def parse_amount_nok(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))

    text = str(value).strip()
    if not text:
        return None

    # Handle common text formats like "NOK 270 000 000".
    text = text.replace("\u00a0", " ")
    text = text.replace("NOK", "").replace("nok", "")
    text = text.replace(" ", "")
    text = text.replace(".", "")
    text = text.replace(",", ".")
    text = re.sub(r"[^0-9.\-]", "", text)
    if not text:
        return None

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def source_name_for_url(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower()
    except ValueError:
        return None
    if not host:
        return None
    return host


def ensure_person(conn: psycopg.Connection, canonical_name: str) -> int:
    row = conn.execute(
        """
        INSERT INTO person (canonical_name)
        VALUES (%s)
        ON CONFLICT (canonical_name)
        DO UPDATE SET canonical_name = EXCLUDED.canonical_name
        RETURNING id
        """,
        (canonical_name,),
    ).fetchone()
    return int(row["id"])


def ensure_organization(
    conn: psycopg.Connection,
    canonical_name: str,
    org_type: str | None,
    hq_country: str | None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO organization (canonical_name, org_type, hq_country)
        VALUES (%s, %s, %s)
        ON CONFLICT (canonical_name)
        DO UPDATE SET
          org_type = COALESCE(EXCLUDED.org_type, organization.org_type),
          hq_country = COALESCE(EXCLUDED.hq_country, organization.hq_country)
        RETURNING id
        """,
        (canonical_name, org_type, hq_country),
    ).fetchone()
    return int(row["id"])


def ensure_source_document(
    conn: psycopg.Connection,
    url: str,
    doc_type: str,
    source_name: str | None = None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO source_document (source_name, url, doc_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (url)
        DO UPDATE SET
          source_name = COALESCE(EXCLUDED.source_name, source_document.source_name),
          doc_type = COALESCE(EXCLUDED.doc_type, source_document.doc_type)
        RETURNING id
        """,
        (source_name, url, doc_type),
    ).fetchone()
    return int(row["id"])


def ensure_role_event(
    conn: psycopg.Connection,
    person_id: int,
    organization_id: int,
    role_title: str,
    role_level: str | None,
    norwegian_position_before: str | None,
    announced_on: date | None,
    start_on: date | None,
    end_on: date | None,
) -> int:
    existing = conn.execute(
        """
        SELECT id
        FROM role_event
        WHERE person_id = %s
          AND organization_id = %s
          AND role_title = %s
          AND start_on IS NOT DISTINCT FROM %s
        LIMIT 1
        """,
        (person_id, organization_id, role_title, start_on),
    ).fetchone()

    if existing:
        role_event_id = int(existing["id"])
        conn.execute(
            """
            UPDATE role_event
            SET role_level = COALESCE(%s, role_level),
                norwegian_position_before = COALESCE(%s, norwegian_position_before),
                announced_on = COALESCE(%s, announced_on),
                end_on = COALESCE(%s, end_on)
            WHERE id = %s
            """,
            (role_level, norwegian_position_before, announced_on, end_on, role_event_id),
        )
        return role_event_id

    row = conn.execute(
        """
        INSERT INTO role_event (
          person_id, organization_id, role_title, role_level,
          norwegian_position_before, announced_on, start_on, end_on
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            person_id,
            organization_id,
            role_title,
            role_level,
            norwegian_position_before,
            announced_on,
            start_on,
            end_on,
        ),
    ).fetchone()
    return int(row["id"])


def ensure_funding_flow(
    conn: psycopg.Connection,
    recipient_organization_id: int,
    funding_channel: str | None,
    amount_nok: Decimal | None,
    fiscal_year: int | None,
    notes: str | None,
) -> int:
    existing = conn.execute(
        """
        SELECT id
        FROM funding_flow
        WHERE donor_country_code = 'NO'
          AND recipient_organization_id = %s
          AND fiscal_year IS NOT DISTINCT FROM %s
          AND funding_channel IS NOT DISTINCT FROM %s
          AND amount_nok IS NOT DISTINCT FROM %s
        LIMIT 1
        """,
        (recipient_organization_id, fiscal_year, funding_channel, amount_nok),
    ).fetchone()

    if existing:
        funding_flow_id = int(existing["id"])
        conn.execute(
            """
            UPDATE funding_flow
            SET notes = COALESCE(%s, notes)
            WHERE id = %s
            """,
            (notes, funding_flow_id),
        )
        return funding_flow_id

    row = conn.execute(
        """
        INSERT INTO funding_flow (
          donor_country_code,
          recipient_organization_id,
          funding_channel,
          amount_nok,
          fiscal_year,
          notes
        )
        VALUES ('NO', %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (recipient_organization_id, funding_channel, amount_nok, fiscal_year, notes),
    ).fetchone()
    return int(row["id"])


def ensure_junction(conn: psycopg.Connection, table: str, ids: tuple[Any, ...]) -> None:
    if table == "role_event_source_document":
        conn.execute(
            """
            INSERT INTO role_event_source_document (role_event_id, source_document_id, relation_type)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            ids,
        )
        return
    if table == "funding_flow_source_document":
        conn.execute(
            """
            INSERT INTO funding_flow_source_document (funding_flow_id, source_document_id, relation_type)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            ids,
        )
        return
    if table == "person_source_document":
        conn.execute(
            """
            INSERT INTO person_source_document (person_id, source_document_id, relation_type)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            ids,
        )
        return
    if table == "organization_source_document":
        conn.execute(
            """
            INSERT INTO organization_source_document (organization_id, source_document_id, relation_type)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            ids,
        )
        return

    raise ValueError(f"Unknown junction table: {table}")


def main() -> int:
    load_dotenv()
    args = parse_args()

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("POSTGRES_DSN is required.", file=sys.stderr)
        return 1

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.autocommit = False

        run_id = args.run_id
        if run_id is None:
            run_row = conn.execute(
                """
                SELECT id
                FROM ingest_run
                WHERE source_name = 'excel' AND status = 'success'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if not run_row:
                print("No successful excel ingest run found.", file=sys.stderr)
                return 1
            run_id = int(run_row["id"])

        if args.truncate_core:
            conn.execute(
                """
                TRUNCATE TABLE
                  role_event_source_document,
                  funding_flow_source_document,
                  person_source_document,
                  organization_source_document,
                  funding_flow,
                  role_event,
                  person_alias,
                  organization_alias,
                  person,
                  organization,
                  source_document
                RESTART IDENTITY CASCADE
                """
            )

        org_rows = conn.execute(
            """
            SELECT row_payload
            FROM stg_excel_organisasjoner
            WHERE ingest_run_id = %s
            ORDER BY excel_row
            """,
            (run_id,),
        ).fetchall()

        datakilde_rows = conn.execute(
            """
            SELECT row_payload
            FROM stg_excel_datakilder
            WHERE ingest_run_id = %s
            ORDER BY excel_row
            """,
            (run_id,),
        ).fetchall()

        persons_created = 0
        orgs_created = 0
        roles_created = 0
        funding_created = 0
        sources_created = 0

        # Register datakilde URLs as source documents.
        for row in datakilde_rows:
            payload = row["row_payload"]
            url = clean_text(payload.get("URL"))
            source_title = clean_text(payload.get("Datakilde"))
            if not url or not url.startswith("http"):
                continue

            source_id = ensure_source_document(
                conn,
                url=url,
                doc_type="catalog",
                source_name=source_title or source_name_for_url(url),
            )
            if source_id:
                sources_created += 1

        for row in org_rows:
            payload = row["row_payload"]

            org_name = clean_text(payload.get("Organisasjon"))
            person_name = clean_text(payload.get("Norsk toppperson"))
            role_title = clean_text(payload.get("Rolle/tittel"))

            if not org_name or not person_name or not role_title:
                continue

            org_type = clean_text(payload.get("Type"))
            hq_country = clean_text(payload.get("Hovedsete/land"))
            role_level = clean_text(payload.get("Nivå"))
            norwegian_position_before = clean_text(payload.get("Norsk posisjon før (kort)"))
            announced_on = parse_date(payload.get("Dato kunngjort/valgt"))
            start_on = parse_date(payload.get("Tiltredelse"))
            end_on = parse_date(payload.get("Slutt"))

            person_id = ensure_person(conn, person_name)
            org_id = ensure_organization(conn, org_name, org_type, hq_country)

            # Track rough creation activity via existence checks.
            if conn.execute("SELECT 1 FROM person WHERE id = %s", (person_id,)).fetchone():
                persons_created += 1
            if conn.execute("SELECT 1 FROM organization WHERE id = %s", (org_id,)).fetchone():
                orgs_created += 1

            role_event_id = ensure_role_event(
                conn,
                person_id=person_id,
                organization_id=org_id,
                role_title=role_title,
                role_level=role_level,
                norwegian_position_before=norwegian_position_before,
                announced_on=announced_on,
                start_on=start_on,
                end_on=end_on,
            )
            roles_created += 1

            appointment_url = clean_text(payload.get("Primærkilde: utnevnelse/valg (URL)"))
            bio_url = clean_text(payload.get("Primærkilde: bio/rolle (URL)"))
            donor_url = clean_text(payload.get("Primærkilde: bidrag/donoroversikt (URL)"))

            if appointment_url and appointment_url.startswith("http"):
                source_id = ensure_source_document(
                    conn,
                    url=appointment_url,
                    doc_type="appointment",
                    source_name=source_name_for_url(appointment_url),
                )
                ensure_junction(
                    conn,
                    "role_event_source_document",
                    (role_event_id, source_id, "appointment"),
                )
                ensure_junction(conn, "person_source_document", (person_id, source_id, "appointment"))
                ensure_junction(
                    conn,
                    "organization_source_document",
                    (org_id, source_id, "appointment"),
                )
                sources_created += 1

            if bio_url and bio_url.startswith("http"):
                source_id = ensure_source_document(
                    conn,
                    url=bio_url,
                    doc_type="bio",
                    source_name=source_name_for_url(bio_url),
                )
                ensure_junction(conn, "role_event_source_document", (role_event_id, source_id, "bio"))
                ensure_junction(conn, "person_source_document", (person_id, source_id, "bio"))
                sources_created += 1

            amount_nok = parse_amount_nok(payload.get("Dokumentert beløp (NOK)"))
            funding_channel = clean_text(payload.get("Bidragskanal (typisk)"))
            funding_notes = clean_text(payload.get("Beløp – detaljer/forbehold"))
            fiscal_year = start_on.year if start_on else (announced_on.year if announced_on else None)

            if amount_nok is not None or funding_channel or donor_url:
                funding_flow_id = ensure_funding_flow(
                    conn,
                    recipient_organization_id=org_id,
                    funding_channel=funding_channel,
                    amount_nok=amount_nok,
                    fiscal_year=fiscal_year,
                    notes=funding_notes,
                )
                funding_created += 1

                if donor_url and donor_url.startswith("http"):
                    source_id = ensure_source_document(
                        conn,
                        url=donor_url,
                        doc_type="funding",
                        source_name=source_name_for_url(donor_url),
                    )
                    ensure_junction(
                        conn,
                        "funding_flow_source_document",
                        (funding_flow_id, source_id, "donor_report"),
                    )
                    ensure_junction(
                        conn,
                        "organization_source_document",
                        (org_id, source_id, "funding"),
                    )
                    sources_created += 1

        conn.commit()

    print(f"Normalized run_id={run_id}")
    print(
        "Summary: "
        f"person_rows_seen={persons_created}, "
        f"organization_rows_seen={orgs_created}, "
        f"role_rows_written={roles_created}, "
        f"funding_rows_written={funding_created}, "
        f"source_docs_touched={sources_created}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
