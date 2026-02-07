#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

REGISTRY_BASE = "https://iatiregistry.org/api/3/action"
REGISTRY_DATASET_URL = "https://iatiregistry.org/dataset"


@dataclass(slots=True)
class ResourceMeta:
    registry_query: str
    package_name: str
    package_title: str | None
    package_url: str
    publisher_iati_id: str | None
    resource_id: str | None
    resource_name: str | None
    resource_format: str | None
    resource_url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Harvest IATI XML transaction data from IATI Registry into staging."
    )
    parser.add_argument(
        "--publisher-iati-id",
        action="append",
        default=[],
        help="Publisher IATI id (repeatable). Example: NO-BRC-971277882",
    )
    parser.add_argument(
        "--organization-slug",
        action="append",
        default=[],
        help="IATI registry organization slug (repeatable). Example: norad",
    )
    parser.add_argument(
        "--no-discover-norwegian-publishers",
        action="store_true",
        help="Disable automatic publisher discovery by publisher_country=NO.",
    )
    parser.add_argument(
        "--max-packages",
        type=int,
        help="Optional cap on number of packages processed.",
    )
    parser.add_argument(
        "--max-resources",
        type=int,
        help="Optional cap on number of resources processed.",
    )
    parser.add_argument(
        "--max-activities",
        type=int,
        help="Optional cap on number of activities parsed per resource.",
    )
    parser.add_argument(
        "--max-transactions",
        type=int,
        help="Optional global cap on number of transactions inserted.",
    )
    parser.add_argument(
        "--rows-per-page",
        type=int,
        default=100,
        help="Page size for package_search pagination.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate IATI staging table before loading.",
    )
    return parser.parse_args()


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def registry_get_json(path: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(f"{REGISTRY_BASE}/{path}", params=params, timeout=90)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "detail" in payload:
        raise RuntimeError(f"IATI registry error: {payload['detail']}")
    if not payload.get("success", False):
        raise RuntimeError(f"IATI registry request failed for {path}: {payload}")
    return payload


def discover_norwegian_publishers() -> list[str]:
    payload = registry_get_json("organization_list", {"all_fields": "true"})
    out: set[str] = set()
    for org in payload.get("result", []):
        publisher_country = (org.get("publisher_country") or "").upper()
        package_count = int(org.get("package_count") or 0)
        publisher_id = clean_text(org.get("publisher_iati_id"))
        if publisher_country != "NO":
            continue
        if package_count <= 0:
            continue
        if not publisher_id:
            continue
        out.add(publisher_id)
    return sorted(out)


def paged_package_search(*, fq: str, rows_per_page: int) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = []
    start = 0

    while True:
        payload = registry_get_json(
            "package_search",
            {"fq": fq, "rows": rows_per_page, "start": start},
        )
        result = payload.get("result", {})
        count = int(result.get("count") or 0)
        items = result.get("results") or []
        if not items:
            break

        packages.extend(items)
        start += len(items)
        if start >= count:
            break

    return packages


def collect_packages(
    *,
    publisher_ids: list[str],
    organization_slugs: list[str],
    rows_per_page: int,
) -> dict[str, tuple[str, dict[str, Any]]]:
    # package_name -> (registry_query, package_payload)
    by_name: dict[str, tuple[str, dict[str, Any]]] = {}

    for publisher_id in publisher_ids:
        fq = f"publisher_iati_id:{publisher_id}"
        for package in paged_package_search(fq=fq, rows_per_page=rows_per_page):
            package_name = clean_text(package.get("name"))
            if not package_name:
                continue
            by_name[package_name] = (fq, package)

    for slug in organization_slugs:
        fq = f"organization:{slug}"
        for package in paged_package_search(fq=fq, rows_per_page=rows_per_page):
            package_name = clean_text(package.get("name"))
            if not package_name:
                continue
            by_name[package_name] = (fq, package)

    return by_name


def is_xml_resource(resource: dict[str, Any]) -> bool:
    fmt = (resource.get("format") or "").lower()
    name = (resource.get("name") or "").lower()
    url = (resource.get("url") or "").lower()
    return "xml" in fmt or url.endswith(".xml") or name.endswith(".xml")


def iter_resource_meta(
    package_entries: Iterable[tuple[str, dict[str, Any]]],
    *,
    max_packages: int | None,
    max_resources: int | None,
) -> list[ResourceMeta]:
    resources: list[ResourceMeta] = []

    for idx, (registry_query, package) in enumerate(package_entries, start=1):
        if max_packages is not None and idx > max_packages:
            break

        package_name = clean_text(package.get("name"))
        if not package_name:
            continue
        package_title = clean_text(package.get("title"))
        package_url = f"{REGISTRY_DATASET_URL}/{package_name}"
        publisher_iati_id = clean_text(package.get("publisher_iati_id"))

        for resource in package.get("resources", []):
            if max_resources is not None and len(resources) >= max_resources:
                return resources
            if not is_xml_resource(resource):
                continue

            resource_url = clean_text(resource.get("url"))
            if not resource_url:
                continue

            resources.append(
                ResourceMeta(
                    registry_query=registry_query,
                    package_name=package_name,
                    package_title=package_title,
                    package_url=package_url,
                    publisher_iati_id=publisher_iati_id,
                    resource_id=clean_text(resource.get("id")),
                    resource_name=clean_text(resource.get("name")),
                    resource_format=clean_text(resource.get("format")),
                    resource_url=resource_url,
                )
            )

    return resources


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", maxsplit=1)[1]
    return tag


def child_elements(elem: ET.Element, tag_name: str) -> list[ET.Element]:
    return [child for child in list(elem) if local_name(child.tag) == tag_name]


def first_child(elem: ET.Element, tag_name: str) -> ET.Element | None:
    for child in elem:
        if local_name(child.tag) == tag_name:
            return child
    return None


def flattened_text(elem: ET.Element | None) -> str | None:
    if elem is None:
        return None
    text = " ".join("".join(elem.itertext()).split())
    return text or None


def narrative_text(elem: ET.Element | None) -> str | None:
    if elem is None:
        return None
    for child in elem:
        if local_name(child.tag) != "narrative":
            continue
        text = flattened_text(child)
        if text:
            return text
    return flattened_text(elem)


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    if len(value) >= 10:
        value = value[:10]
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def normalize_currency(code: str | None) -> str | None:
    text = clean_text(code)
    if not text:
        return None
    text = text.upper()
    if len(text) != 3:
        return None
    return text


def pick_participating_org(
    orgs: list[dict[str, str | None]],
    *,
    roles: set[str],
) -> dict[str, str | None] | None:
    for org in orgs:
        role = (org.get("role") or "").strip()
        if role not in roles:
            continue
        if org.get("ref") or org.get("name"):
            return org
    return None


def make_event_key(parts: list[str | None]) -> str:
    basis = "|".join((p or "").strip() for p in parts)
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def insert_staging_row(
    conn: psycopg.Connection,
    *,
    ingest_run_id: int,
    meta: ResourceMeta,
    row: dict[str, Any],
) -> bool:
    event_key = str(row["event_key"])
    cursor = conn.execute(
        """
        INSERT INTO stg_iati_transaction (
          ingest_run_id,
          registry_query,
          package_name,
          package_title,
          package_url,
          publisher_iati_id,
          resource_id,
          resource_name,
          resource_format,
          resource_url,
          activity_iati_identifier,
          activity_title,
          reporting_org_ref,
          reporting_org_name,
          recipient_country_code,
          transaction_ref,
          transaction_type_code,
          transaction_date,
          value_date,
          value_amount,
          value_currency,
          receiver_org_ref,
          receiver_org_name,
          provider_org_ref,
          provider_org_name,
          event_key,
          row_payload
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (ingest_run_id, event_key) DO NOTHING
        """,
        (
            ingest_run_id,
            meta.registry_query,
            meta.package_name,
            meta.package_title,
            meta.package_url,
            meta.publisher_iati_id,
            meta.resource_id,
            meta.resource_name,
            meta.resource_format,
            meta.resource_url,
            row["activity_iati_identifier"],
            row["activity_title"],
            row["reporting_org_ref"],
            row["reporting_org_name"],
            row["recipient_country_code"],
            row["transaction_ref"],
            row["transaction_type_code"],
            row["transaction_date"],
            row["value_date"],
            row["value_amount"],
            row["value_currency"],
            row["receiver_org_ref"],
            row["receiver_org_name"],
            row["provider_org_ref"],
            row["provider_org_name"],
            event_key,
            Jsonb(row["row_payload"]),
        ),
    )
    return cursor.rowcount > 0


def extract_transactions(
    activity: ET.Element,
    *,
    meta: ResourceMeta,
) -> list[dict[str, Any]]:
    activity_identifier = clean_text(flattened_text(first_child(activity, "iati-identifier")))
    if not activity_identifier:
        return []

    activity_title = narrative_text(first_child(activity, "title"))
    reporting_org = first_child(activity, "reporting-org")
    reporting_org_ref = clean_text(reporting_org.attrib.get("ref")) if reporting_org is not None else None
    reporting_org_name = narrative_text(reporting_org)

    recipient_country = first_child(activity, "recipient-country")
    recipient_country_code = None
    if recipient_country is not None:
        recipient_country_code = clean_text(recipient_country.attrib.get("code"))
        if recipient_country_code:
            recipient_country_code = recipient_country_code.upper()
            if len(recipient_country_code) != 2:
                recipient_country_code = None

    participating_orgs: list[dict[str, str | None]] = []
    for org in child_elements(activity, "participating-org"):
        participating_orgs.append(
            {
                "role": clean_text(org.attrib.get("role")),
                "ref": clean_text(org.attrib.get("ref")),
                "name": narrative_text(org),
            }
        )

    fallback_receiver = pick_participating_org(participating_orgs, roles={"4"})
    default_currency = normalize_currency(activity.attrib.get("default-currency"))
    rows: list[dict[str, Any]] = []

    for transaction in child_elements(activity, "transaction"):
        transaction_ref = clean_text(transaction.attrib.get("ref"))

        tx_type = first_child(transaction, "transaction-type")
        tx_type_code = clean_text(tx_type.attrib.get("code")) if tx_type is not None else None

        tx_date_node = first_child(transaction, "transaction-date")
        tx_date = None
        if tx_date_node is not None:
            tx_date = parse_iso_date(tx_date_node.attrib.get("iso-date"))

        value_node = first_child(transaction, "value")
        value_amount = None
        value_currency = default_currency
        value_date = tx_date
        if value_node is not None:
            value_amount = parse_decimal(flattened_text(value_node))
            value_currency = normalize_currency(value_node.attrib.get("currency")) or default_currency
            value_date = parse_iso_date(value_node.attrib.get("value-date")) or value_date

        if value_amount is None:
            continue

        receiver_node = first_child(transaction, "receiver-org")
        receiver_org_ref = clean_text(receiver_node.attrib.get("ref")) if receiver_node is not None else None
        receiver_org_name = narrative_text(receiver_node)

        receiver_from_participating_org = False
        if not receiver_org_ref and not receiver_org_name and fallback_receiver is not None:
            receiver_org_ref = fallback_receiver.get("ref")
            receiver_org_name = fallback_receiver.get("name")
            receiver_from_participating_org = True

        provider_node = first_child(transaction, "provider-org")
        provider_org_ref = clean_text(provider_node.attrib.get("ref")) if provider_node is not None else None
        provider_org_name = narrative_text(provider_node)
        if not provider_org_ref and not provider_org_name:
            provider_org_ref = reporting_org_ref
            provider_org_name = reporting_org_name

        event_key = make_event_key(
            [
                meta.resource_url,
                activity_identifier,
                transaction_ref,
                tx_type_code,
                tx_date.isoformat() if tx_date else None,
                value_date.isoformat() if value_date else None,
                str(value_amount),
                value_currency,
                receiver_org_ref,
                receiver_org_name,
                provider_org_ref,
                provider_org_name,
            ]
        )

        row_payload = {
            "activity": {
                "iati_identifier": activity_identifier,
                "title": activity_title,
                "reporting_org_ref": reporting_org_ref,
                "reporting_org_name": reporting_org_name,
                "recipient_country_code": recipient_country_code,
            },
            "transaction": {
                "ref": transaction_ref,
                "type_code": tx_type_code,
                "transaction_date": tx_date.isoformat() if tx_date else None,
                "value_date": value_date.isoformat() if value_date else None,
                "value_amount": str(value_amount),
                "value_currency": value_currency,
                "receiver_org_ref": receiver_org_ref,
                "receiver_org_name": receiver_org_name,
                "provider_org_ref": provider_org_ref,
                "provider_org_name": provider_org_name,
                "receiver_from_participating_org": receiver_from_participating_org,
            },
            "participating_orgs": participating_orgs,
        }

        rows.append(
            {
                "activity_iati_identifier": activity_identifier,
                "activity_title": activity_title,
                "reporting_org_ref": reporting_org_ref,
                "reporting_org_name": reporting_org_name,
                "recipient_country_code": recipient_country_code,
                "transaction_ref": transaction_ref,
                "transaction_type_code": tx_type_code,
                "transaction_date": tx_date,
                "value_date": value_date,
                "value_amount": value_amount,
                "value_currency": value_currency,
                "receiver_org_ref": receiver_org_ref,
                "receiver_org_name": receiver_org_name,
                "provider_org_ref": provider_org_ref,
                "provider_org_name": provider_org_name,
                "event_key": event_key,
                "row_payload": row_payload,
            }
        )

    return rows


def ingest_resource(
    conn: psycopg.Connection,
    *,
    ingest_run_id: int,
    meta: ResourceMeta,
    max_activities: int | None,
    max_transactions_remaining: int | None,
) -> tuple[int, int]:
    activities_seen = 0
    inserted_rows = 0

    response = requests.get(meta.resource_url, stream=True, timeout=180)
    response.raise_for_status()
    response.raw.decode_content = True

    try:
        for _, elem in ET.iterparse(response.raw, events=("end",)):
            if local_name(elem.tag) != "iati-activity":
                continue

            activities_seen += 1
            rows = extract_transactions(elem, meta=meta)

            for row in rows:
                if max_transactions_remaining is not None and max_transactions_remaining <= 0:
                    return activities_seen, inserted_rows
                inserted = insert_staging_row(
                    conn,
                    ingest_run_id=ingest_run_id,
                    meta=meta,
                    row=row,
                )
                if inserted:
                    inserted_rows += 1
                    if max_transactions_remaining is not None:
                        max_transactions_remaining -= 1

            elem.clear()

            if max_activities is not None and activities_seen >= max_activities:
                break
            if max_transactions_remaining is not None and max_transactions_remaining <= 0:
                break
    finally:
        response.close()

    return activities_seen, inserted_rows


def main() -> int:
    load_dotenv()
    args = parse_args()

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("POSTGRES_DSN is required.", file=sys.stderr)
        return 1

    publisher_ids: set[str] = {p.strip() for p in args.publisher_iati_id if p.strip()}
    if not args.no_discover_norwegian_publishers:
        for discovered in discover_norwegian_publishers():
            publisher_ids.add(discovered)

    organization_slugs = sorted({s.strip() for s in args.organization_slug if s.strip()})
    if not publisher_ids and not organization_slugs:
        print(
            "No registry filters selected. Provide --publisher-iati-id or --organization-slug.",
            file=sys.stderr,
        )
        return 1

    package_map = collect_packages(
        publisher_ids=sorted(publisher_ids),
        organization_slugs=organization_slugs,
        rows_per_page=args.rows_per_page,
    )
    resources = iter_resource_meta(
        package_map.values(),
        max_packages=args.max_packages,
        max_resources=args.max_resources,
    )
    if not resources:
        print("No XML resources found for selected filters.")
        return 0

    with psycopg.connect(dsn) as conn:
        conn.autocommit = False

        run_id = conn.execute(
            """
            INSERT INTO ingest_run (source_name, input_path, status)
            VALUES (%s, %s, 'running')
            RETURNING id
            """,
            ("iati_registry", "iati-registry"),
        ).fetchone()[0]
        conn.commit()

        try:
            if args.truncate:
                conn.execute("TRUNCATE TABLE stg_iati_transaction")
                conn.commit()

            total_activities = 0
            total_inserted = 0
            resources_attempted = 0
            resources_failed = 0
            remaining = args.max_transactions

            for meta in resources:
                if remaining is not None and remaining <= 0:
                    break

                resources_attempted += 1
                try:
                    seen, inserted = ingest_resource(
                        conn,
                        ingest_run_id=run_id,
                        meta=meta,
                        max_activities=args.max_activities,
                        max_transactions_remaining=remaining,
                    )
                    total_activities += seen
                    total_inserted += inserted
                    if remaining is not None:
                        remaining -= inserted
                    conn.commit()
                    print(
                        "resource",
                        meta.resource_url,
                        f"activities={seen}",
                        f"inserted={inserted}",
                    )
                except Exception as exc:  # noqa: BLE001
                    resources_failed += 1
                    conn.rollback()
                    print(f"warn: failed resource {meta.resource_url}: {exc}")

            notes = {
                "publishers": sorted(publisher_ids),
                "organization_slugs": organization_slugs,
                "resources_attempted": resources_attempted,
                "resources_failed": resources_failed,
                "activities_seen": total_activities,
                "rows_inserted": total_inserted,
            }
            conn.execute(
                """
                UPDATE ingest_run
                SET status = 'success', finished_at = now(), notes = %s
                WHERE id = %s
                """,
                (json.dumps(notes), run_id),
            )
            conn.commit()

            print(
                f"IATI harvest complete run_id={run_id} "
                f"resources={resources_attempted} "
                f"activities={total_activities} inserted_rows={total_inserted} "
                f"failed_resources={resources_failed}"
            )
            return 0

        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            conn.execute(
                """
                UPDATE ingest_run
                SET status = 'failed', finished_at = now(), notes = %s
                WHERE id = %s
                """,
                (str(exc), run_id),
            )
            conn.commit()
            raise


if __name__ == "__main__":
    raise SystemExit(main())
