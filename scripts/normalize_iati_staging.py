#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


@dataclass(slots=True)
class OrganizationLookup:
    by_name: dict[str, int]
    by_ref: dict[str, int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize staged IATI transactions into funding_flow."
    )
    parser.add_argument(
        "--run-id",
        type=int,
        help="Ingest run id to normalize (default: latest successful iati_registry run).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        help="Optional cap on number of staged rows processed.",
    )
    parser.add_argument(
        "--source-system",
        default="iati_registry",
        help="Source system key used in funding_flow_ingest_key.",
    )
    parser.add_argument(
        "--truncate-derived",
        action="store_true",
        help="Delete existing funding rows created by --source-system before writing.",
    )
    return parser.parse_args()


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_name(value: str) -> str:
    value = value.lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9æøå ]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_ref(value: str) -> str:
    value = value.upper().strip()
    value = re.sub(r"\s+", "", value)
    return value


def ref_to_country_code(ref: str | None) -> str | None:
    if not ref:
        return None
    match = re.match(r"^([A-Za-z]{2})-", ref.strip())
    if not match:
        return None
    return match.group(1).upper()


def choose_fiscal_date(transaction_date: date | None, value_date: date | None) -> date | None:
    return transaction_date or value_date


def load_organization_lookup(conn: psycopg.Connection) -> OrganizationLookup:
    by_name: dict[str, int] = {}
    by_ref: dict[str, int] = {}

    org_rows = conn.execute("SELECT id, canonical_name FROM organization ORDER BY id").fetchall()
    for row in org_rows:
        org_id = int(row["id"])
        name = clean_text(row["canonical_name"])
        if not name:
            continue
        key = normalize_name(name)
        if key and key not in by_name:
            by_name[key] = org_id

    alias_rows = conn.execute(
        """
        SELECT organization_id, alias
        FROM organization_alias
        ORDER BY id
        """
    ).fetchall()
    for row in alias_rows:
        org_id = int(row["organization_id"])
        alias = clean_text(row["alias"])
        if not alias:
            continue
        name_key = normalize_name(alias)
        if name_key and name_key not in by_name:
            by_name[name_key] = org_id

        if "-" in alias:
            ref_key = normalize_ref(alias)
            if ref_key and ref_key not in by_ref:
                by_ref[ref_key] = org_id

    return OrganizationLookup(by_name=by_name, by_ref=by_ref)


def map_organization(
    lookup: OrganizationLookup,
    *,
    org_ref: str | None,
    org_name: str | None,
) -> tuple[int | None, str]:
    ref = clean_text(org_ref)
    if ref:
        ref_key = normalize_ref(ref)
        org_id = lookup.by_ref.get(ref_key)
        if org_id is not None:
            return org_id, "ref"

    name = clean_text(org_name)
    if name:
        name_key = normalize_name(name)
        org_id = lookup.by_name.get(name_key)
        if org_id is not None:
            return org_id, "name"

    return None, "none"


def ensure_source_document(
    conn: psycopg.Connection,
    *,
    resource_url: str,
    package_name: str | None,
    publisher_iati_id: str | None,
) -> int:
    notes = []
    if package_name:
        notes.append(f"package={package_name}")
    if publisher_iati_id:
        notes.append(f"publisher_iati_id={publisher_iati_id}")

    row = conn.execute(
        """
        INSERT INTO source_document (source_name, url, doc_type, notes)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (url)
        DO UPDATE SET
          source_name = COALESCE(EXCLUDED.source_name, source_document.source_name),
          doc_type = COALESCE(EXCLUDED.doc_type, source_document.doc_type),
          notes = COALESCE(EXCLUDED.notes, source_document.notes)
        RETURNING id
        """,
        (
            "iati-registry",
            resource_url,
            "iati_xml",
            "; ".join(notes) if notes else None,
        ),
    ).fetchone()
    return int(row["id"])


def ensure_org_alias(
    conn: psycopg.Connection,
    *,
    organization_id: int,
    alias: str | None,
    source_document_id: int | None,
) -> None:
    alias = clean_text(alias)
    if not alias:
        return
    conn.execute(
        """
        INSERT INTO organization_alias (organization_id, alias, source_system, source_document_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (organization_id, alias) DO NOTHING
        """,
        (organization_id, alias, "iati_ref", source_document_id),
    )


def lookup_flow_by_ingest_key(
    conn: psycopg.Connection,
    *,
    source_system: str,
    event_key: str,
) -> int | None:
    row = conn.execute(
        """
        SELECT funding_flow_id
        FROM funding_flow_ingest_key
        WHERE source_system = %s AND event_key = %s
        LIMIT 1
        """,
        (source_system, event_key),
    ).fetchone()
    return int(row["funding_flow_id"]) if row else None


def clamp_confidence(value: float) -> float:
    return max(0.50, min(0.95, value))


def build_confidence(
    *,
    recipient_mapped: bool,
    donor_mapped: bool,
    has_date: bool,
    has_type: bool,
) -> float:
    score = 0.68
    if recipient_mapped:
        score += 0.16
    if donor_mapped:
        score += 0.08
    if has_date:
        score += 0.04
    if has_type:
        score += 0.03
    return clamp_confidence(score)


def insert_funding_flow(
    conn: psycopg.Connection,
    *,
    donor_organization_id: int | None,
    donor_country_code: str | None,
    recipient_organization_id: int | None,
    recipient_name_raw: str | None,
    funding_channel: str,
    amount_nok: Decimal | None,
    amount_original: Decimal | None,
    currency_code: str | None,
    fiscal_year: int | None,
    period_start: date | None,
    period_end: date | None,
    confidence: float,
    notes: str | None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO funding_flow (
          donor_organization_id,
          donor_country_code,
          recipient_organization_id,
          recipient_name_raw,
          funding_channel,
          amount_nok,
          amount_original,
          currency_code,
          fiscal_year,
          period_start,
          period_end,
          confidence,
          notes
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        RETURNING id
        """,
        (
            donor_organization_id,
            donor_country_code,
            recipient_organization_id,
            recipient_name_raw,
            funding_channel,
            amount_nok,
            amount_original,
            currency_code,
            fiscal_year,
            period_start,
            period_end,
            confidence,
            notes,
        ),
    ).fetchone()
    return int(row["id"])


def ensure_funding_ingest_key(
    conn: psycopg.Connection,
    *,
    source_system: str,
    event_key: str,
    funding_flow_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO funding_flow_ingest_key (source_system, event_key, funding_flow_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, event_key)
        DO UPDATE SET funding_flow_id = EXCLUDED.funding_flow_id
        """,
        (source_system, event_key, funding_flow_id),
    )


def ensure_funding_source_link(
    conn: psycopg.Connection,
    *,
    funding_flow_id: int,
    source_document_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO funding_flow_source_document (funding_flow_id, source_document_id, relation_type)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (funding_flow_id, source_document_id, "iati_xml"),
    )


def maybe_truncate_source_rows(conn: psycopg.Connection, source_system: str) -> None:
    conn.execute(
        """
        DELETE FROM funding_flow_source_document fs
        USING funding_flow_ingest_key k
        WHERE k.source_system = %s
          AND k.funding_flow_id = fs.funding_flow_id
        """,
        (source_system,),
    )
    conn.execute(
        """
        DELETE FROM funding_flow f
        USING funding_flow_ingest_key k
        WHERE k.source_system = %s
          AND k.funding_flow_id = f.id
        """,
        (source_system,),
    )
    conn.execute(
        """
        DELETE FROM funding_flow_ingest_key
        WHERE source_system = %s
        """,
        (source_system,),
    )


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
            row = conn.execute(
                """
                SELECT id
                FROM ingest_run
                WHERE source_name = 'iati_registry' AND status = 'success'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                print("No successful iati_registry ingest run found.", file=sys.stderr)
                return 1
            run_id = int(row["id"])

        if args.truncate_derived:
            maybe_truncate_source_rows(conn, args.source_system)

        lookup = load_organization_lookup(conn)
        source_doc_by_url: dict[str, int] = {}

        staged_rows = conn.execute(
            """
            SELECT
              id,
              package_name,
              publisher_iati_id,
              resource_url,
              activity_iati_identifier,
              transaction_type_code,
              transaction_date,
              value_date,
              value_amount,
              value_currency,
              receiver_org_ref,
              receiver_org_name,
              provider_org_ref,
              provider_org_name,
              reporting_org_ref,
              reporting_org_name,
              event_key
            FROM stg_iati_transaction
            WHERE ingest_run_id = %s
            ORDER BY id
            """,
            (run_id,),
        )

        processed = 0
        inserted = 0
        skipped_no_recipient = 0
        skipped_existing = 0
        donor_mapped_count = 0
        recipient_mapped_count = 0

        for row in staged_rows:
            if args.max_rows is not None and processed >= args.max_rows:
                break
            processed += 1

            existing_flow_id = lookup_flow_by_ingest_key(
                conn,
                source_system=args.source_system,
                event_key=row["event_key"],
            )
            if existing_flow_id is not None:
                skipped_existing += 1
                continue

            resource_url = str(row["resource_url"])
            source_document_id = source_doc_by_url.get(resource_url)
            if source_document_id is None:
                source_document_id = ensure_source_document(
                    conn,
                    resource_url=resource_url,
                    package_name=row["package_name"],
                    publisher_iati_id=row["publisher_iati_id"],
                )
                source_doc_by_url[resource_url] = source_document_id

            recipient_org_id, recipient_match_mode = map_organization(
                lookup,
                org_ref=row["receiver_org_ref"],
                org_name=row["receiver_org_name"],
            )
            recipient_name_raw = None
            if recipient_org_id is None:
                recipient_name_raw = clean_text(row["receiver_org_name"])
                if recipient_name_raw is None:
                    skipped_no_recipient += 1
                    continue
            else:
                recipient_mapped_count += 1
                ensure_org_alias(
                    conn,
                    organization_id=recipient_org_id,
                    alias=row["receiver_org_ref"],
                    source_document_id=source_document_id,
                )

            donor_org_id, donor_match_mode = map_organization(
                lookup,
                org_ref=row["provider_org_ref"] or row["reporting_org_ref"],
                org_name=row["provider_org_name"] or row["reporting_org_name"],
            )
            if donor_org_id is not None:
                donor_mapped_count += 1
                ensure_org_alias(
                    conn,
                    organization_id=donor_org_id,
                    alias=row["provider_org_ref"] or row["reporting_org_ref"],
                    source_document_id=source_document_id,
                )

            donor_country_code = ref_to_country_code(
                row["provider_org_ref"] or row["reporting_org_ref"]
            )

            amount = row["value_amount"]
            if amount is None:
                continue
            if not isinstance(amount, Decimal):
                amount = Decimal(str(amount))

            currency = clean_text(row["value_currency"])
            currency = currency.upper() if currency else None
            amount_nok: Decimal | None
            amount_original: Decimal | None
            currency_code: str | None
            if currency == "NOK" or currency is None:
                amount_nok = amount
                amount_original = None
                currency_code = None
            else:
                amount_nok = None
                amount_original = amount
                currency_code = currency

            fiscal_date = choose_fiscal_date(row["transaction_date"], row["value_date"])
            fiscal_year = fiscal_date.year if fiscal_date else None

            tx_type = clean_text(row["transaction_type_code"])
            funding_channel = "IATI transaction"
            if tx_type:
                funding_channel = f"IATI transaction type {tx_type}"

            notes = (
                f"IATI activity={row['activity_iati_identifier']}; "
                f"match_recipient={recipient_match_mode}; "
                f"match_donor={donor_match_mode}; "
                f"event_key={row['event_key']}"
            )
            confidence = build_confidence(
                recipient_mapped=recipient_org_id is not None,
                donor_mapped=donor_org_id is not None,
                has_date=fiscal_date is not None,
                has_type=tx_type is not None,
            )

            funding_flow_id = insert_funding_flow(
                conn,
                donor_organization_id=donor_org_id,
                donor_country_code=donor_country_code,
                recipient_organization_id=recipient_org_id,
                recipient_name_raw=recipient_name_raw,
                funding_channel=funding_channel,
                amount_nok=amount_nok,
                amount_original=amount_original,
                currency_code=currency_code,
                fiscal_year=fiscal_year,
                period_start=fiscal_date,
                period_end=fiscal_date,
                confidence=confidence,
                notes=notes,
            )
            inserted += 1

            ensure_funding_ingest_key(
                conn,
                source_system=args.source_system,
                event_key=row["event_key"],
                funding_flow_id=funding_flow_id,
            )
            ensure_funding_source_link(
                conn,
                funding_flow_id=funding_flow_id,
                source_document_id=source_document_id,
            )

        conn.commit()

    print(
        f"Normalized iati run_id={run_id} processed={processed} inserted={inserted} "
        f"skipped_existing={skipped_existing} skipped_no_recipient={skipped_no_recipient} "
        f"recipient_mapped={recipient_mapped_count} donor_mapped={donor_mapped_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
