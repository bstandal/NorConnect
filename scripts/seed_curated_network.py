#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.main import PERSON_DRILLDOWN_GROUPS, PERSON_DRILLDOWN_PROFILES


DEFAULT_KEYS = [
    "terje-rod-larsen",
    "mona-juul",
    "borge-brende",
    "ine-eriksen-soreide",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed curated person->organization ties into Postgres core tables."
    )
    parser.add_argument(
        "--person-key",
        action="append",
        default=[],
        help="Profile key from PERSON_DRILLDOWN_PROFILES. Can be repeated.",
    )
    parser.add_argument(
        "--group",
        action="append",
        default=[],
        help="Profile group key from PERSON_DRILLDOWN_GROUPS. Can be repeated.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve and print what would be inserted without writing.",
    )
    return parser.parse_args()


def parse_start_date(start_year: int | None) -> date | None:
    if not start_year:
        return None
    return date(start_year, 1, 1)


def parse_end_date(end_year: int | None) -> date | None:
    if not end_year:
        return None
    return date(end_year, 12, 31)


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


def ensure_person_alias(
    conn: psycopg.Connection,
    *,
    person_id: int,
    alias: str,
) -> None:
    conn.execute(
        """
        INSERT INTO person_alias (person_id, alias, source_system)
        VALUES (%s, %s, 'curated_drilldown')
        ON CONFLICT (person_id, alias) DO NOTHING
        """,
        (person_id, alias),
    )


def ensure_organization_alias(
    conn: psycopg.Connection,
    *,
    organization_id: int,
    alias: str,
) -> None:
    conn.execute(
        """
        INSERT INTO organization_alias (organization_id, alias, source_system)
        VALUES (%s, %s, 'curated_drilldown')
        ON CONFLICT (organization_id, alias) DO NOTHING
        """,
        (organization_id, alias),
    )


def ensure_organization(
    conn: psycopg.Connection,
    *,
    canonical_name: str,
    org_type: str | None,
) -> int:
    existing = conn.execute(
        """
        SELECT id, canonical_name
        FROM organization
        WHERE lower(canonical_name) = lower(%s)
           OR lower(trim(regexp_replace(canonical_name, '\\s*\\([^)]*\\)', '', 'g'))) = lower(trim(%s))
        ORDER BY CASE WHEN lower(canonical_name) = lower(%s) THEN 0 ELSE 1 END, id
        LIMIT 1
        """,
        (canonical_name, canonical_name, canonical_name),
    ).fetchone()

    if existing:
        org_id = int(existing["id"])
        conn.execute(
            """
            UPDATE organization
            SET org_type = COALESCE(org_type, %s)
            WHERE id = %s
            """,
            (org_type, org_id),
        )
        if (existing["canonical_name"] or "").strip().lower() != canonical_name.strip().lower():
            ensure_organization_alias(conn, organization_id=org_id, alias=canonical_name)
        return org_id

    row = conn.execute(
        """
        INSERT INTO organization (canonical_name, org_type)
        VALUES (%s, %s)
        ON CONFLICT (canonical_name)
        DO UPDATE SET org_type = COALESCE(organization.org_type, EXCLUDED.org_type)
        RETURNING id
        """,
        (canonical_name, org_type),
    ).fetchone()
    return int(row["id"])


def ensure_source_document(
    conn: psycopg.Connection,
    *,
    source_name: str | None,
    url: str,
    doc_type: str | None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO source_document (source_name, url, doc_type, notes)
        VALUES (%s, %s, %s, 'curated_drilldown')
        ON CONFLICT (url)
        DO UPDATE SET
          source_name = COALESCE(EXCLUDED.source_name, source_document.source_name),
          doc_type = COALESCE(EXCLUDED.doc_type, source_document.doc_type)
        RETURNING id
        """,
        (source_name, url, doc_type or "profile"),
    ).fetchone()
    return int(row["id"])


def ensure_role_event(
    conn: psycopg.Connection,
    *,
    person_id: int,
    organization_id: int,
    role_title: str,
    relation_type: str | None,
    start_year: int | None,
    end_year: int | None,
    notes: str | None,
    outside_dataset: bool,
) -> int:
    start_on = parse_start_date(start_year)
    end_on = parse_end_date(end_year)
    role_level = relation_type
    curated_note = f"curated_drilldown:outside_dataset={str(outside_dataset).lower()}"
    full_notes = curated_note if not notes else f"{curated_note} | {notes}"

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
        role_id = int(existing["id"])
        conn.execute(
            """
            UPDATE role_event
            SET role_level = COALESCE(role_level, %s),
                end_on = COALESCE(end_on, %s),
                confidence = GREATEST(confidence, 0.8),
                notes = COALESCE(notes, %s)
            WHERE id = %s
            """,
            (role_level, end_on, full_notes, role_id),
        )
        return role_id

    if start_on is not None:
        existing_without_start = conn.execute(
            """
            SELECT id
            FROM role_event
            WHERE person_id = %s
              AND organization_id = %s
              AND role_title = %s
              AND start_on IS NULL
            ORDER BY id
            LIMIT 1
            """,
            (person_id, organization_id, role_title),
        ).fetchone()
        if existing_without_start:
            role_id = int(existing_without_start["id"])
            conn.execute(
                """
                UPDATE role_event
                SET start_on = COALESCE(start_on, %s),
                    end_on = COALESCE(end_on, %s),
                    role_level = COALESCE(role_level, %s),
                    confidence = GREATEST(confidence, 0.8),
                    notes = COALESCE(notes, %s)
                WHERE id = %s
                """,
                (start_on, end_on, role_level, full_notes, role_id),
            )
            return role_id

    row = conn.execute(
        """
        INSERT INTO role_event (
          person_id,
          organization_id,
          role_title,
          role_level,
          start_on,
          end_on,
          confidence,
          notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, 0.8, %s)
        RETURNING id
        """,
        (
            person_id,
            organization_id,
            role_title,
            role_level,
            start_on,
            end_on,
            full_notes,
        ),
    ).fetchone()
    return int(row["id"])


def ensure_person_link(
    conn: psycopg.Connection,
    *,
    person_1_id: int,
    person_2_id: int,
    relation_type: str,
    relation_label: str | None,
    start_year: int | None,
    end_year: int | None,
    notes: str | None,
) -> int | None:
    if person_1_id == person_2_id:
        return None

    person_a_id, person_b_id = sorted((person_1_id, person_2_id))
    start_on = parse_start_date(start_year)
    end_on = parse_end_date(end_year)

    existing = conn.execute(
        """
        SELECT id
        FROM person_link
        WHERE person_a_id = %s
          AND person_b_id = %s
          AND relation_type = %s
          AND start_on IS NOT DISTINCT FROM %s
        LIMIT 1
        """,
        (person_a_id, person_b_id, relation_type, start_on),
    ).fetchone()

    if existing:
        person_link_id = int(existing["id"])
        conn.execute(
            """
            UPDATE person_link
            SET relation_label = COALESCE(relation_label, %s),
                end_on = COALESCE(end_on, %s),
                confidence = GREATEST(confidence, 0.8),
                notes = COALESCE(notes, %s)
            WHERE id = %s
            """,
            (relation_label, end_on, notes, person_link_id),
        )
        return person_link_id

    row = conn.execute(
        """
        INSERT INTO person_link (
          person_a_id,
          person_b_id,
          relation_type,
          relation_label,
          start_on,
          end_on,
          confidence,
          notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, 0.8, %s)
        RETURNING id
        """,
        (
            person_a_id,
            person_b_id,
            relation_type,
            relation_label,
            start_on,
            end_on,
            notes,
        ),
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
    if table == "person_link_source_document":
        conn.execute(
            """
            INSERT INTO person_link_source_document (person_link_id, source_document_id, relation_type)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            ids,
        )
        return
    raise ValueError(f"Unsupported junction table: {table}")


def resolve_profile_keys(args: argparse.Namespace) -> list[str]:
    keys: list[str] = []
    for key in args.person_key:
        if key not in PERSON_DRILLDOWN_PROFILES:
            raise ValueError(f"Unknown person key: {key}")
        if key not in keys:
            keys.append(key)

    for group in args.group:
        members = PERSON_DRILLDOWN_GROUPS.get(group)
        if members is None:
            raise ValueError(f"Unknown group: {group}")
        for key in members:
            if key in PERSON_DRILLDOWN_PROFILES and key not in keys:
                keys.append(key)

    if not keys:
        keys = [key for key in DEFAULT_KEYS if key in PERSON_DRILLDOWN_PROFILES]

    return keys


def main() -> int:
    load_dotenv()
    args = parse_args()

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("POSTGRES_DSN is required.", file=sys.stderr)
        return 1

    try:
        selected_keys = resolve_profile_keys(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.dry_run:
        for key in selected_keys:
            profile = PERSON_DRILLDOWN_PROFILES[key]
            print(
                f"{key}: person={profile.get('display_name')} "
                f"bindings={len(profile.get('curated_bindings', []))}"
            )
        return 0

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.autocommit = False

        people_touched = 0
        orgs_touched = 0
        roles_touched = 0
        person_links_touched = 0
        sources_touched = 0
        person_ids_by_key: dict[str, int] = {}

        for key in selected_keys:
            profile = PERSON_DRILLDOWN_PROFILES[key]
            display_name = profile.get("display_name")
            if not display_name:
                continue

            person_id = ensure_person(conn, display_name)
            person_ids_by_key[key] = person_id
            people_touched += 1

            for alias in profile.get("aliases", []):
                alias_text = (alias or "").strip()
                if alias_text:
                    ensure_person_alias(conn, person_id=person_id, alias=alias_text)

        for key in selected_keys:
            profile = PERSON_DRILLDOWN_PROFILES[key]
            person_id = person_ids_by_key.get(key)
            if not person_id:
                continue

            for binding in profile.get("curated_bindings", []):
                org_name = (binding.get("institution_name") or "").strip()
                role_title = (binding.get("role_title") or "").strip()
                if not org_name or not role_title:
                    continue

                org_id = ensure_organization(
                    conn,
                    canonical_name=org_name,
                    org_type=binding.get("institution_type"),
                )
                orgs_touched += 1

                role_event_id = ensure_role_event(
                    conn,
                    person_id=person_id,
                    organization_id=org_id,
                    role_title=role_title,
                    relation_type=binding.get("relation_type"),
                    start_year=binding.get("start_year"),
                    end_year=binding.get("end_year"),
                    notes=binding.get("notes"),
                    outside_dataset=bool(binding.get("outside_dataset", True)),
                )
                roles_touched += 1

                for source in binding.get("sources", []):
                    url = (source.get("url") or "").strip()
                    if not url:
                        continue
                    source_id = ensure_source_document(
                        conn,
                        source_name=source.get("source_name"),
                        url=url,
                        doc_type=source.get("doc_type"),
                    )
                    ensure_junction(
                        conn,
                        "role_event_source_document",
                        (
                            role_event_id,
                            source_id,
                            source.get("relation_type") or "curated_reference",
                        ),
                    )
                    ensure_junction(
                        conn,
                        "person_source_document",
                        (person_id, source_id, "curated_profile"),
                    )
                    ensure_junction(
                        conn,
                        "organization_source_document",
                        (org_id, source_id, "curated_profile"),
                    )
                    sources_touched += 1

        for key in selected_keys:
            profile = PERSON_DRILLDOWN_PROFILES[key]
            source_person_id = person_ids_by_key.get(key)
            if not source_person_id:
                continue

            for link in profile.get("person_links", []):
                target_key = (link.get("target_key") or "").strip()
                if not target_key:
                    continue

                target_profile = PERSON_DRILLDOWN_PROFILES.get(target_key)
                target_person_id = person_ids_by_key.get(target_key)
                if not target_person_id and target_profile:
                    target_name = target_profile.get("display_name")
                    if target_name:
                        target_person_id = ensure_person(conn, target_name)
                        person_ids_by_key[target_key] = target_person_id
                        for alias in target_profile.get("aliases", []):
                            alias_text = (alias or "").strip()
                            if alias_text:
                                ensure_person_alias(
                                    conn,
                                    person_id=target_person_id,
                                    alias=alias_text,
                                )
                        people_touched += 1

                if not target_person_id:
                    continue

                relation_type = (link.get("relation_type") or "person_link").strip()
                relation_label = (link.get("label") or relation_type).strip()
                link_notes = link.get("notes")
                person_link_id = ensure_person_link(
                    conn,
                    person_1_id=source_person_id,
                    person_2_id=target_person_id,
                    relation_type=relation_type,
                    relation_label=relation_label,
                    start_year=link.get("start_year"),
                    end_year=link.get("end_year"),
                    notes=link_notes,
                )
                if person_link_id is None:
                    continue
                person_links_touched += 1

                for source in link.get("sources", []):
                    url = (source.get("url") or "").strip()
                    if not url:
                        continue
                    source_id = ensure_source_document(
                        conn,
                        source_name=source.get("source_name"),
                        url=url,
                        doc_type=source.get("doc_type"),
                    )
                    ensure_junction(
                        conn,
                        "person_link_source_document",
                        (
                            person_link_id,
                            source_id,
                            source.get("relation_type") or "curated_reference",
                        ),
                    )
                    ensure_junction(
                        conn,
                        "person_source_document",
                        (source_person_id, source_id, "curated_network"),
                    )
                    ensure_junction(
                        conn,
                        "person_source_document",
                        (target_person_id, source_id, "curated_network"),
                    )
                    sources_touched += 1

        conn.commit()

    print(
        "Curated seed complete: "
        f"profiles={len(selected_keys)}, "
        f"people_touched={people_touched}, "
        f"organizations_touched={orgs_touched}, "
        f"roles_touched={roles_touched}, "
        f"person_links_touched={person_links_touched}, "
        f"sources_touched={sources_touched}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
