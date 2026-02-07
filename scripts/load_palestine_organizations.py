#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.rows import dict_row

NORAD_BASE = "https://apim-br-online-prod.azure-api.net/resultatportal-prod-api-dotnet"

PALESTINE_HINTS = (
    "palestin",
    "gaza",
    "west bank",
    "jerusalem",
    "hebron",
    "ramallah",
    "bethlehem",
    "nablus",
    "rafah",
    "khan yunis",
    "opt",
    "occupied palestinian",
)

STOPWORDS = {
    "the",
    "and",
    "for",
    "of",
    "in",
    "to",
    "international",
    "organization",
    "centre",
    "center",
    "fund",
    "group",
    "global",
    "world",
    "united",
    "nations",
    "program",
    "programme",
}


@dataclass(slots=True)
class NoradPartner:
    code: int
    english: str
    norwegian: str


@dataclass(slots=True)
class CandidatePartner:
    partner: NoradPartner
    reason: str
    score: float
    matched_iati_name: str | None


@dataclass(slots=True)
class WhitelistEntry:
    partner_sid: int
    partner_name: str
    matched_iati_name: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load Palestine-related recipient organizations from public data and "
            "optionally backfill Norad yearly disbursements since 1990."
        )
    )
    parser.add_argument("--start-year", type=int, default=1990)
    parser.add_argument("--end-year", type=int)
    parser.add_argument(
        "--partner-match-threshold",
        type=float,
        default=0.84,
        help="Minimum fuzzy score to connect Norad partner names to IATI recipient names.",
    )
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="Only load organizations and map existing IATI flows, skip Norad historical backfill.",
    )
    parser.add_argument(
        "--whitelist-file",
        default="db/whitelists/palestine_norad_partner_whitelist.csv",
        help=(
            "CSV whitelist with columns partner_sid,partner_name,matched_iati_name. "
            "If present, only listed partner_sid values are imported for history."
        ),
    )
    parser.add_argument(
        "--truncate-history",
        action="store_true",
        help="Delete existing 'NORAD historical partner_sid=*' rows before importing history.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_name(value: str) -> str:
    value = value.lower()
    value = value.replace("&", " and ")
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"[^a-z0-9aeo\\s-]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def token_set(text: str) -> set[str]:
    return {
        token
        for token in normalize_name(text).split()
        if len(token) >= 3 and token not in STOPWORDS and not token.isdigit()
    }


def similarity(a: str, b: str) -> float:
    a_norm = normalize_name(a)
    b_norm = normalize_name(b)
    if not a_norm or not b_norm:
        return 0.0

    seq = SequenceMatcher(None, a_norm, b_norm).ratio()
    a_tokens = token_set(a)
    b_tokens = token_set(b)
    if not a_tokens or not b_tokens:
        jaccard = 0.0
    else:
        jaccard = len(a_tokens & b_tokens) / len(a_tokens | b_tokens)

    contains_boost = 0.1 if (a_norm in b_norm or b_norm in a_norm) else 0.0
    return min((seq * 0.65) + (jaccard * 0.35) + contains_boost, 1.0)


def looks_palestine_related(text: str | None) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(hint in lower for hint in PALESTINE_HINTS)


def norad_get_json(path: str, params: dict[str, Any]) -> Any:
    response = requests.get(
        f"{NORAD_BASE}{path}",
        params=params,
        timeout=90,
    )
    response.raise_for_status()
    return response.json()


def fetch_norad_latest_year() -> int:
    payload = norad_get_json("/latestdatayear", {})
    if isinstance(payload, list) and payload:
        latest = payload[0].get("latest_historic_data_year")
        if latest is not None:
            return int(latest)
    return datetime.utcnow().year


def fetch_norad_partners() -> list[NoradPartner]:
    payload = norad_get_json("/partnercode", {"level": 2})
    out: list[NoradPartner] = []
    for item in payload:
        code = item.get("code")
        english = clean_text(item.get("english"))
        norwegian = clean_text(item.get("norwegian"))
        if code is None or not english:
            continue
        out.append(
            NoradPartner(
                code=int(code),
                english=english,
                norwegian=norwegian or english,
            )
        )
    return out


def load_whitelist(path_value: str | None) -> list[WhitelistEntry]:
    if not path_value:
        return []
    path = Path(path_value)
    if not path.exists():
        return []

    entries: list[WhitelistEntry] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid_raw = clean_text(row.get("partner_sid"))
            name = clean_text(row.get("partner_name"))
            matched = clean_text(row.get("matched_iati_name"))
            if not sid_raw or not sid_raw.isdigit() or not name:
                continue
            entries.append(
                WhitelistEntry(
                    partner_sid=int(sid_raw),
                    partner_name=name,
                    matched_iati_name=matched or name,
                )
            )
    return entries


def fetch_palestine_iati_recipients(
    conn: psycopg.Connection,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          st.receiver_org_name,
          st.receiver_org_ref,
          COUNT(*) AS flow_count
        FROM stg_iati_transaction st
        WHERE (
          st.recipient_country_code = 'PS'
          OR lower(coalesce(st.activity_title, '')) LIKE '%palestin%'
          OR lower(coalesce(st.receiver_org_name, '')) LIKE '%palestin%'
          OR lower(coalesce(st.provider_org_name, '')) LIKE '%palestin%'
        )
          AND st.receiver_org_name IS NOT NULL
          AND btrim(st.receiver_org_name) <> ''
          AND lower(st.receiver_org_name) <> 'undefined'
        GROUP BY st.receiver_org_name, st.receiver_org_ref
        ORDER BY COUNT(*) DESC, st.receiver_org_name
        """
    ).fetchall()
    return [dict(row) for row in rows]


def load_org_lookup(conn: psycopg.Connection) -> dict[str, int]:
    lookup: dict[str, int] = {}

    rows = conn.execute(
        """
        SELECT id, canonical_name
        FROM organization
        ORDER BY id
        """
    ).fetchall()
    for row in rows:
        key = normalize_name(str(row["canonical_name"]))
        if key and key not in lookup:
            lookup[key] = int(row["id"])

    alias_rows = conn.execute(
        """
        SELECT organization_id, alias
        FROM organization_alias
        ORDER BY id
        """
    ).fetchall()
    for row in alias_rows:
        alias = clean_text(row["alias"])
        if not alias:
            continue
        key = normalize_name(alias)
        if key and key not in lookup:
            lookup[key] = int(row["organization_id"])

    return lookup


def ensure_organization(
    conn: psycopg.Connection,
    lookup: dict[str, int],
    *,
    canonical_name: str,
    org_type: str | None = None,
) -> tuple[int, bool]:
    name_key = normalize_name(canonical_name)
    existing_id = lookup.get(name_key)
    if existing_id is not None:
        if org_type:
            conn.execute(
                """
                UPDATE organization
                SET org_type = COALESCE(org_type, %s)
                WHERE id = %s
                """,
                (org_type, existing_id),
            )
        return existing_id, False

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
    org_id = int(row["id"])
    lookup[name_key] = org_id
    return org_id, True


def ensure_org_alias(
    conn: psycopg.Connection,
    lookup: dict[str, int],
    *,
    organization_id: int,
    alias: str | None,
    source_system: str,
) -> bool:
    alias_text = clean_text(alias)
    if not alias_text:
        return False
    conn.execute(
        """
        INSERT INTO organization_alias (organization_id, alias, source_system)
        VALUES (%s, %s, %s)
        ON CONFLICT (organization_id, alias) DO NOTHING
        """,
        (organization_id, alias_text, source_system),
    )
    key = normalize_name(alias_text)
    if key:
        lookup[key] = organization_id
    return True


def ensure_source_document(
    conn: psycopg.Connection,
    *,
    source_name: str,
    url: str,
    doc_type: str,
    notes: str | None = None,
) -> int:
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
        (source_name, url, doc_type, notes),
    ).fetchone()
    return int(row["id"])


def ensure_organization_source_link(
    conn: psycopg.Connection,
    *,
    organization_id: int,
    source_document_id: int,
    relation_type: str,
) -> None:
    conn.execute(
        """
        INSERT INTO organization_source_document (organization_id, source_document_id, relation_type)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (organization_id, source_document_id, relation_type),
    )


def find_existing_funding_flow(
    conn: psycopg.Connection,
    *,
    recipient_organization_id: int,
    fiscal_year: int,
    funding_channel: str,
    amount_nok: float,
) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM funding_flow
        WHERE donor_country_code = 'NO'
          AND recipient_organization_id = %s
          AND fiscal_year = %s
          AND funding_channel = %s
          AND amount_nok = %s
          AND amount_original IS NULL
          AND currency_code IS NULL
        LIMIT 1
        """,
        (recipient_organization_id, fiscal_year, funding_channel, amount_nok),
    ).fetchone()
    return int(row["id"]) if row else None


def upsert_historical_funding(
    conn: psycopg.Connection,
    *,
    recipient_organization_id: int,
    fiscal_year: int,
    amount_nok: float,
    funding_channel: str,
    notes: str,
) -> int:
    existing_id = find_existing_funding_flow(
        conn,
        recipient_organization_id=recipient_organization_id,
        fiscal_year=fiscal_year,
        funding_channel=funding_channel,
        amount_nok=amount_nok,
    )
    if existing_id:
        conn.execute(
            """
            UPDATE funding_flow
            SET notes = COALESCE(notes, %s)
            WHERE id = %s
            """,
            (notes, existing_id),
        )
        return existing_id

    row = conn.execute(
        """
        INSERT INTO funding_flow (
          donor_country_code,
          recipient_organization_id,
          funding_channel,
          amount_nok,
          fiscal_year,
          notes,
          confidence
        )
        VALUES ('NO', %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            recipient_organization_id,
            funding_channel,
            amount_nok,
            fiscal_year,
            notes,
            0.78,
        ),
    ).fetchone()
    return int(row["id"])


def ensure_funding_source_link(
    conn: psycopg.Connection,
    *,
    funding_flow_id: int,
    source_document_id: int,
    relation_type: str,
) -> None:
    conn.execute(
        """
        INSERT INTO funding_flow_source_document (funding_flow_id, source_document_id, relation_type)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (funding_flow_id, source_document_id, relation_type),
    )


def truncate_historical_rows(conn: psycopg.Connection) -> int:
    conn.execute(
        """
        DELETE FROM funding_flow_source_document fs
        USING funding_flow f
        WHERE fs.funding_flow_id = f.id
          AND f.funding_channel LIKE 'NORAD historical partner_sid=%'
        """
    )
    row = conn.execute(
        """
        DELETE FROM funding_flow f
        WHERE f.funding_channel LIKE 'NORAD historical partner_sid=%'
        RETURNING id
        """
    ).fetchall()
    return len(row)


def collect_candidate_partners(
    iati_names: list[str],
    partners: list[NoradPartner],
    threshold: float,
) -> list[CandidatePartner]:
    candidates: list[CandidatePartner] = []
    iati_name_set = {name for name in iati_names if name}
    iati_tokens_by_name = {name: token_set(name) for name in iati_name_set}
    iati_names_by_token: dict[str, set[str]] = defaultdict(set)
    all_iati_tokens: set[str] = set()
    for name, tokens in iati_tokens_by_name.items():
        all_iati_tokens.update(tokens)
        for token in tokens:
            iati_names_by_token[token].add(name)

    for partner in partners:
        names_to_check = [partner.english, partner.norwegian]
        if " - " in partner.english:
            names_to_check.append(partner.english.split(" - ", maxsplit=1)[1])
        if " - " in partner.norwegian:
            names_to_check.append(partner.norwegian.split(" - ", maxsplit=1)[1])

        partner_tokens: set[str] = set()
        for candidate_name in names_to_check:
            partner_tokens.update(token_set(candidate_name))

        best_name: str | None = None
        best_score = 0.0
        candidate_iati_names: set[str] = set()
        for token in partner_tokens:
            candidate_iati_names.update(iati_names_by_token.get(token, set()))

        has_palestine_hint = any(looks_palestine_related(candidate) for candidate in names_to_check)
        if not has_palestine_hint and not (partner_tokens & all_iati_tokens):
            continue

        for iati_name in candidate_iati_names or iati_name_set:
            score = max(similarity(iati_name, candidate) for candidate in names_to_check if candidate)
            if score > best_score:
                best_score = score
                best_name = iati_name

        if has_palestine_hint:
            candidates.append(
                CandidatePartner(
                    partner=partner,
                    reason="palestine_keyword",
                    score=max(best_score, 0.99),
                    matched_iati_name=best_name,
                )
            )
            continue

        if best_score >= threshold:
            candidates.append(
                CandidatePartner(
                    partner=partner,
                    reason="iati_name_match",
                    score=best_score,
                    matched_iati_name=best_name,
                )
            )

    deduped: dict[int, CandidatePartner] = {}
    for candidate in candidates:
        existing = deduped.get(candidate.partner.code)
        if existing is None or candidate.score > existing.score:
            deduped[candidate.partner.code] = candidate

    return sorted(deduped.values(), key=lambda c: (-c.score, c.partner.english))


def map_palestine_iati_flows_to_organizations(
    conn: psycopg.Connection,
    lookup: dict[str, int],
) -> int:
    rows = conn.execute(
        """
        SELECT
          ff.id AS funding_flow_id,
          st.receiver_org_name
        FROM funding_flow ff
        JOIN funding_flow_ingest_key fik
          ON fik.funding_flow_id = ff.id
         AND fik.source_system = 'iati_registry'
        JOIN stg_iati_transaction st
          ON st.event_key = fik.event_key
        WHERE (
          st.recipient_country_code = 'PS'
          OR lower(coalesce(st.activity_title, '')) LIKE '%palestin%'
          OR lower(coalesce(st.receiver_org_name, '')) LIKE '%palestin%'
          OR lower(coalesce(st.provider_org_name, '')) LIKE '%palestin%'
        )
          AND ff.recipient_organization_id IS NULL
          AND st.receiver_org_name IS NOT NULL
          AND btrim(st.receiver_org_name) <> ''
          AND lower(st.receiver_org_name) <> 'undefined'
        ORDER BY ff.id
        """
    ).fetchall()

    updated = 0
    for row in rows:
        receiver_name = clean_text(row["receiver_org_name"])
        if not receiver_name:
            continue
        key = normalize_name(receiver_name)
        org_id = lookup.get(key)
        if org_id is None:
            continue
        conn.execute(
            """
            UPDATE funding_flow
            SET recipient_organization_id = %s,
                recipient_name_raw = COALESCE(recipient_name_raw, %s),
                confidence = LEAST(confidence + 0.12, 0.98)
            WHERE id = %s
            """,
            (org_id, receiver_name, int(row["funding_flow_id"])),
        )
        updated += 1
    return updated


def main() -> int:
    load_dotenv()
    args = parse_args()

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("POSTGRES_DSN is required.", file=sys.stderr)
        return 1

    end_year = args.end_year or fetch_norad_latest_year()
    if end_year < args.start_year:
        print("end-year must be >= start-year", file=sys.stderr)
        return 1

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.autocommit = False
        lookup = load_org_lookup(conn)
        iati_recipients = fetch_palestine_iati_recipients(conn)
        iati_names = [str(row["receiver_org_name"]) for row in iati_recipients]

        iati_source_id = ensure_source_document(
            conn,
            source_name="iati-registry",
            url="https://iatiregistry.org/",
            doc_type="dataset",
            notes="Palestine recipient organizations (derived query)",
        )
        partner_source_id = ensure_source_document(
            conn,
            source_name="norad-resultatportal-api",
            url=f"{NORAD_BASE}/partnercode?level=2",
            doc_type="api",
            notes="Partner list for Palestine organization matching",
        )

        stats: dict[str, int] = defaultdict(int)
        org_by_iati_name: dict[str, int] = {}

        for row in iati_recipients:
            receiver_name = str(row["receiver_org_name"])
            org_id, created = ensure_organization(
                conn,
                lookup,
                canonical_name=receiver_name,
                org_type="recipient_open_data",
            )
            stats["iati_org_total"] += 1
            if created:
                stats["iati_org_created"] += 1
            org_by_iati_name[receiver_name] = org_id

            ensure_org_alias(
                conn,
                lookup,
                organization_id=org_id,
                alias=receiver_name,
                source_system="palestine_iati",
            )
            ensure_org_alias(
                conn,
                lookup,
                organization_id=org_id,
                alias=row.get("receiver_org_ref"),
                source_system="palestine_iati_ref",
            )
            ensure_organization_source_link(
                conn,
                organization_id=org_id,
                source_document_id=iati_source_id,
                relation_type="recipient_reference",
            )

        stats["mapped_existing_flows"] = map_palestine_iati_flows_to_organizations(conn, lookup)

        partners = fetch_norad_partners()
        partner_by_sid = {partner.code: partner for partner in partners}
        whitelist = load_whitelist(args.whitelist_file)

        if whitelist:
            candidates: list[CandidatePartner] = []
            missing_sid = 0
            for entry in whitelist:
                partner = partner_by_sid.get(entry.partner_sid)
                if partner is None:
                    missing_sid += 1
                    continue
                candidates.append(
                    CandidatePartner(
                        partner=partner,
                        reason="strict_whitelist",
                        score=1.0,
                        matched_iati_name=entry.matched_iati_name,
                    )
                )
            stats["whitelist_entries"] = len(whitelist)
            stats["whitelist_missing_sid"] = missing_sid
            stats["norad_candidates"] = len(candidates)
        else:
            candidates = collect_candidate_partners(iati_names, partners, args.partner_match_threshold)
            stats["norad_candidates"] = len(candidates)
            stats["whitelist_entries"] = 0
            stats["whitelist_missing_sid"] = 0

        if not args.skip_history:
            if args.truncate_history:
                stats["historical_rows_deleted"] = truncate_historical_rows(conn)
            for candidate in candidates:
                canonical_name = candidate.partner.english
                if candidate.matched_iati_name and candidate.score >= args.partner_match_threshold:
                    canonical_name = candidate.matched_iati_name

                org_id, created = ensure_organization(
                    conn,
                    lookup,
                    canonical_name=canonical_name,
                    org_type="recipient_open_data",
                )
                if created:
                    stats["history_org_created"] += 1

                ensure_org_alias(
                    conn,
                    lookup,
                    organization_id=org_id,
                    alias=candidate.partner.english,
                    source_system="norad_partnercode",
                )
                ensure_org_alias(
                    conn,
                    lookup,
                    organization_id=org_id,
                    alias=candidate.partner.norwegian,
                    source_system="norad_partnercode",
                )
                if " - " in candidate.partner.english:
                    ensure_org_alias(
                        conn,
                        lookup,
                        organization_id=org_id,
                        alias=candidate.partner.english.split(" - ", maxsplit=1)[1],
                        source_system="norad_partnercode",
                    )
                if " - " in candidate.partner.norwegian:
                    ensure_org_alias(
                        conn,
                        lookup,
                        organization_id=org_id,
                        alias=candidate.partner.norwegian.split(" - ", maxsplit=1)[1],
                        source_system="norad_partnercode",
                    )
                ensure_organization_source_link(
                    conn,
                    organization_id=org_id,
                    source_document_id=partner_source_id,
                    relation_type="partner_registry",
                )

                query_params = {
                    "selection": "data_year",
                    "agreement_partner_sid": candidate.partner.code,
                    "from_year": args.start_year,
                    "to_year": end_year,
                }
                data_url = f"{NORAD_BASE}/money?{urlencode(query_params)}"
                payload = norad_get_json("/money", query_params)
                if not isinstance(payload, list):
                    continue

                funding_source_id = ensure_source_document(
                    conn,
                    source_name="norad-resultatportal-api",
                    url=data_url,
                    doc_type="api",
                    notes=(
                        f"agreement_partner_sid={candidate.partner.code}; "
                        f"reason={candidate.reason}; score={candidate.score:.3f}"
                    ),
                )

                funding_channel = f"NORAD historical partner_sid={candidate.partner.code}"
                for amount_row in payload:
                    year = amount_row.get("data_year")
                    amount_nok = amount_row.get("disbursement_earmarked_nok")
                    if year is None or amount_nok is None:
                        continue

                    fiscal_year = int(year)
                    amount_value = float(amount_nok)
                    if amount_value <= 0:
                        continue

                    notes = (
                        f"Palestine candidate partner from public matching. "
                        f"reason={candidate.reason}; score={candidate.score:.3f}; "
                        f"partner='{candidate.partner.english}'"
                    )
                    flow_id = upsert_historical_funding(
                        conn,
                        recipient_organization_id=org_id,
                        fiscal_year=fiscal_year,
                        amount_nok=amount_value,
                        funding_channel=funding_channel,
                        notes=notes,
                    )
                    ensure_funding_source_link(
                        conn,
                        funding_flow_id=flow_id,
                        source_document_id=funding_source_id,
                        relation_type="norad_partner_history",
                    )
                    stats["historical_rows_upserted"] += 1

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(
        "Loaded Palestine organizations:",
        f"iati_total={stats['iati_org_total']}",
        f"iati_created={stats['iati_org_created']}",
        f"mapped_existing_flows={stats['mapped_existing_flows']}",
    )
    print(
        "Norad partner candidates:",
        f"count={stats['norad_candidates']}",
        f"whitelist_entries={stats['whitelist_entries']}",
        f"whitelist_missing_sid={stats['whitelist_missing_sid']}",
        f"historical_rows_deleted={stats['historical_rows_deleted']}",
        f"history_org_created={stats['history_org_created']}",
        f"historical_rows_upserted={stats['historical_rows_upserted']}",
        f"skip_history={'true' if args.skip_history else 'false'}",
    )
    print("dry_run=" + ("true" if args.dry_run else "false"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
