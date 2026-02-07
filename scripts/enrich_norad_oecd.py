#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlencode
import xml.etree.ElementTree as ET

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.rows import dict_row

NORAD_BASE = "https://apim-br-online-prod.azure-api.net/resultatportal-prod-api-dotnet"
NORAD_FUNCTION_KEY_DEFAULT = ""

OECD_AVAILABLE_CONSTRAINT = (
    "https://sdmx.oecd.org/public/rest/availableconstraint/"
    "OECD.DCD.FSD,DSD_DAC2@DF_DAC2A,1.4"
)
OECD_DAC2_AREA_ORG = (
    "https://sdmx.oecd.org/public/rest/datastructure/"
    "OECD.DCD.FSD/DSD_DAC2/1.5?references=all"
)
OECD_DAC2_DATA_TEMPLATE = (
    "https://sdmx.oecd.org/public/rest/data/"
    "OECD.DCD.FSD,DSD_DAC2@DF_DAC2A,1.4/{key}?startPeriod={start_year}&endPeriod={end_year}"
)

XML_NS = {
    "m": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "g": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
    "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "xml": "http://www.w3.org/XML/1998/namespace",
}

STOPWORDS = {
    "the",
    "and",
    "for",
    "of",
    "in",
    "to",
    "international",
    "organization",
    "organisasjon",
    "centre",
    "center",
    "fund",
    "group",
    "global",
    "world",
    "united",
    "nations",
}

COUNTRY_HINT_TO_ISO3 = {
    "kenya": "KEN",
    "nairobi": "KEN",
    "uganda": "UGA",
    "tanzania": "TZA",
    "ethiopia": "ETH",
    "switzerland": "CHE",
    "sveits": "CHE",
    "geneve": "CHE",
    "genève": "CHE",
    "france": "FRA",
    "frankrike": "FRA",
    "paris": "FRA",
    "denmark": "DNK",
    "danmark": "DNK",
    "kobenhavn": "DNK",
    "københavn": "DNK",
    "usa": "USA",
    "united states": "USA",
    "washington": "USA",
    "uk": "GBR",
    "england": "GBR",
    "norge": "NOR",
    "norway": "NOR",
}


@dataclass(slots=True)
class Organization:
    id: int
    name: str
    hq_country: str | None


@dataclass(slots=True)
class NoradPartner:
    code: int
    english: str
    norwegian: str


@dataclass(slots=True)
class MatchResult:
    score: float
    code: str
    name: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich funding_flow with Norad API and OECD DAC2A public data."
    )
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int)
    parser.add_argument(
        "--norad-match-threshold",
        type=float,
        default=0.72,
        help="Minimum fuzzy score (0-1) for Norad partner matching.",
    )
    parser.add_argument(
        "--oecd-match-threshold",
        type=float,
        default=0.78,
        help="Minimum fuzzy score (0-1) for OECD recipient-code matching.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def normalize_name(text: str) -> str:
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^a-z0-9æøå\s-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def token_set(text: str) -> set[str]:
    tokens = {
        t
        for t in normalize_name(text).split()
        if len(t) >= 3 and t not in STOPWORDS and not t.isdigit()
    }
    return tokens


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
    score = (seq * 0.65) + (jaccard * 0.35) + contains_boost
    return min(score, 1.0)


def norad_get_json(path: str, params: dict[str, Any], key: str) -> Any:
    url = f"{NORAD_BASE}{path}"
    response = requests.get(
        url,
        params=params,
        headers={"x-functions-key": key},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def oecd_get_xml(url: str) -> ET.Element:
    response = requests.get(url, timeout=90)
    response.raise_for_status()
    if response.text.startswith("NoRecordsFound") or response.text.startswith("NoResultsFound"):
        return ET.Element("empty")
    return ET.fromstring(response.text)


def fetch_organizations(conn: psycopg.Connection) -> list[Organization]:
    rows = conn.execute(
        """
        SELECT id, canonical_name, hq_country
        FROM organization
        ORDER BY canonical_name
        """
    ).fetchall()
    return [
        Organization(id=int(r["id"]), name=str(r["canonical_name"]), hq_country=r["hq_country"])
        for r in rows
    ]


def fetch_norad_partners(key: str) -> list[NoradPartner]:
    payload = norad_get_json("/partnercode", {"level": 2}, key)
    partners: list[NoradPartner] = []
    for item in payload:
        code = item.get("code")
        english = str(item.get("english") or "").strip()
        norwegian = str(item.get("norwegian") or "").strip()
        if code is None or not english:
            continue
        partners.append(NoradPartner(code=int(code), english=english, norwegian=norwegian or english))
    return partners


def best_norad_match(org_name: str, partners: list[NoradPartner]) -> MatchResult | None:
    best: MatchResult | None = None
    for partner in partners:
        candidate_names = [partner.english, partner.norwegian]
        # Many entries start with acronym prefix: "ABC - Long Name".
        if " - " in partner.english:
            candidate_names.append(partner.english.split(" - ", maxsplit=1)[1])
        if " - " in partner.norwegian:
            candidate_names.append(partner.norwegian.split(" - ", maxsplit=1)[1])

        score = max(similarity(org_name, c) for c in candidate_names if c)
        if best is None or score > best.score:
            best = MatchResult(score=score, code=str(partner.code), name=partner.english)
    return best


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


def find_existing_funding_flow(
    conn: psycopg.Connection,
    *,
    recipient_organization_id: int,
    fiscal_year: int | None,
    funding_channel: str,
    amount_nok: float | None,
    amount_original: float | None,
    currency_code: str | None,
) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM funding_flow
        WHERE donor_country_code = 'NO'
          AND recipient_organization_id = %s
          AND fiscal_year IS NOT DISTINCT FROM %s
          AND funding_channel = %s
          AND amount_nok IS NOT DISTINCT FROM %s
          AND amount_original IS NOT DISTINCT FROM %s
          AND currency_code IS NOT DISTINCT FROM %s
        LIMIT 1
        """,
        (
            recipient_organization_id,
            fiscal_year,
            funding_channel,
            amount_nok,
            amount_original,
            currency_code,
        ),
    ).fetchone()
    return int(row["id"]) if row else None


def upsert_funding_flow(
    conn: psycopg.Connection,
    *,
    recipient_organization_id: int,
    fiscal_year: int | None,
    funding_channel: str,
    amount_nok: float | None,
    amount_original: float | None,
    currency_code: str | None,
    notes: str | None,
) -> int:
    existing_id = find_existing_funding_flow(
        conn,
        recipient_organization_id=recipient_organization_id,
        fiscal_year=fiscal_year,
        funding_channel=funding_channel,
        amount_nok=amount_nok,
        amount_original=amount_original,
        currency_code=currency_code,
    )
    if existing_id:
        conn.execute(
            """
            UPDATE funding_flow
            SET notes = COALESCE(%s, notes)
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
          amount_original,
          currency_code,
          fiscal_year,
          notes,
          confidence
        )
        VALUES ('NO', %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            recipient_organization_id,
            funding_channel,
            amount_nok,
            amount_original,
            currency_code,
            fiscal_year,
            notes,
            0.85,
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


def fetch_norad_latest_year(key: str) -> int:
    payload = norad_get_json("/latestdatayear", {}, key)
    if not payload:
        return datetime.utcnow().year
    return int(payload[0]["latest_historic_data_year"])


def fetch_oecd_recipient_codes() -> set[str]:
    root = oecd_get_xml(OECD_AVAILABLE_CONSTRAINT)
    values: set[str] = set()
    for value in root.findall(".//c:KeyValue[@id='RECIPIENT']/c:Value", XML_NS):
        if value.text:
            values.add(value.text)
    return values


def fetch_oecd_area_org_names() -> dict[str, str]:
    root = oecd_get_xml(OECD_DAC2_AREA_ORG)
    result: dict[str, str] = {}
    for codelist in root.findall(".//s:Codelist", XML_NS):
        if codelist.attrib.get("id") != "CL_AREA_ORG":
            continue
        for code in codelist.findall("./s:Code", XML_NS):
            code_id = code.attrib.get("id")
            if not code_id:
                continue
            name_node = code.find("./c:Name[@xml:lang='en']", XML_NS)
            if name_node is None:
                name_node = code.find("./c:Name", XML_NS)
            if name_node is None or not name_node.text:
                continue
            result[code_id] = name_node.text.strip()
    return result


def best_oecd_match(
    org_name: str,
    recipient_codes: set[str],
    area_org_names: dict[str, str],
) -> MatchResult | None:
    best: MatchResult | None = None
    for code, name in area_org_names.items():
        if code not in recipient_codes:
            continue
        score = similarity(org_name, name)
        if best is None or score > best.score:
            best = MatchResult(score=score, code=code, name=name)
    return best


def hq_country_to_iso3(hq_country: str | None) -> str | None:
    if not hq_country:
        return None
    text = normalize_name(hq_country)
    for hint, iso3 in COUNTRY_HINT_TO_ISO3.items():
        if hint in text:
            return iso3
    return None


def parse_oecd_obs_values(root: ET.Element) -> tuple[int, list[tuple[int, float]]]:
    if root.tag == "empty":
        return 0, []

    series = root.find(".//g:Series", XML_NS)
    if series is None:
        return 0, []

    unit_mult = 0
    unit_mult_node = series.find(".//g:Attributes/g:Value[@id='UNIT_MULT']", XML_NS)
    if unit_mult_node is not None:
        raw_mult = unit_mult_node.attrib.get("value")
        if raw_mult and raw_mult.lstrip("-").isdigit():
            unit_mult = int(raw_mult)

    points: list[tuple[int, float]] = []
    for obs in series.findall("./g:Obs", XML_NS):
        period_node = obs.find("./g:ObsDimension", XML_NS)
        value_node = obs.find("./g:ObsValue", XML_NS)
        if period_node is None or value_node is None:
            continue
        year_raw = period_node.attrib.get("value")
        amount_raw = value_node.attrib.get("value")
        if not year_raw or not amount_raw:
            continue
        if not year_raw.isdigit():
            continue
        year = int(year_raw)
        amount = float(amount_raw) * (10**unit_mult)
        points.append((year, amount))

    return unit_mult, points


def enrich_with_norad(
    conn: psycopg.Connection,
    organizations: list[Organization],
    key: str,
    start_year: int,
    end_year: int,
    threshold: float,
    dry_run: bool,
) -> dict[str, int]:
    partners = fetch_norad_partners(key)

    counts = {"matches": 0, "funding_rows": 0, "source_links": 0}

    for org in organizations:
        match = best_norad_match(org.name, partners)
        if match is None or match.score < threshold:
            continue

        counts["matches"] += 1

        query_params = {
            "selection": "data_year",
            "agreement_partner_sid": match.code,
            "from_year": start_year,
            "to_year": end_year,
        }
        url = f"{NORAD_BASE}/money?{urlencode(query_params)}"
        payload = norad_get_json("/money", query_params, key)
        if not isinstance(payload, list):
            continue

        source_id = None
        if not dry_run:
            source_id = ensure_source_document(
                conn,
                source_name="norad-resultatportal-api",
                url=url,
                doc_type="api",
                notes=f"agreement_partner_sid={match.code}; matched_name={match.name}",
            )

        for row in payload:
            year = row.get("data_year")
            amount = row.get("disbursement_earmarked_nok")
            if year is None or amount is None:
                continue

            fiscal_year = int(year)
            amount_nok = float(amount)
            if amount_nok <= 0:
                continue

            funding_channel = f"NORAD partner_sid={match.code}"
            notes = (
                f"Norad match '{org.name}' -> '{match.name}' "
                f"(score={match.score:.3f})"
            )
            if dry_run:
                counts["funding_rows"] += 1
                continue

            flow_id = upsert_funding_flow(
                conn,
                recipient_organization_id=org.id,
                fiscal_year=fiscal_year,
                funding_channel=funding_channel,
                amount_nok=amount_nok,
                amount_original=None,
                currency_code=None,
                notes=notes,
            )
            counts["funding_rows"] += 1

            if source_id is not None:
                ensure_funding_source_link(
                    conn,
                    funding_flow_id=flow_id,
                    source_document_id=source_id,
                    relation_type="norad_api",
                )
                counts["source_links"] += 1

    return counts


def enrich_with_oecd(
    conn: psycopg.Connection,
    organizations: list[Organization],
    start_year: int,
    end_year: int,
    threshold: float,
    dry_run: bool,
) -> dict[str, int]:
    recipient_codes = fetch_oecd_recipient_codes()
    area_org_names = fetch_oecd_area_org_names()

    counts = {"matches": 0, "funding_rows": 0, "source_links": 0}

    for org in organizations:
        match = best_oecd_match(org.name, recipient_codes, area_org_names)
        used_code = None
        used_name = None
        used_score = 0.0

        if match is not None and match.score >= threshold:
            used_code = match.code
            used_name = match.name
            used_score = match.score
        else:
            iso3 = hq_country_to_iso3(org.hq_country)
            if iso3 and iso3 in recipient_codes:
                used_code = iso3
                used_name = area_org_names.get(iso3, iso3)
                used_score = 0.0

        if not used_code:
            continue

        counts["matches"] += 1

        key = f"NOR.{used_code}.206.USD.V"
        url = OECD_DAC2_DATA_TEMPLATE.format(key=key, start_year=start_year, end_year=end_year)
        root = oecd_get_xml(url)
        unit_mult, points = parse_oecd_obs_values(root)
        if not points:
            continue

        source_id = None
        if not dry_run:
            source_id = ensure_source_document(
                conn,
                source_name="oecd-dac2a-api",
                url=url,
                doc_type="api",
                notes=f"recipient={used_code}; matched={used_name}",
            )

        for fiscal_year, amount_usd in points:
            notes = (
                f"OECD DAC2A proxy recipient={used_code} ({used_name}); "
                f"unit_mult={unit_mult}; match_score={used_score:.3f}"
            )
            if dry_run:
                counts["funding_rows"] += 1
                continue

            flow_id = upsert_funding_flow(
                conn,
                recipient_organization_id=org.id,
                fiscal_year=fiscal_year,
                funding_channel="OECD DAC2A recipient proxy",
                amount_nok=None,
                amount_original=amount_usd,
                currency_code="USD",
                notes=notes,
            )
            counts["funding_rows"] += 1

            if source_id is not None:
                ensure_funding_source_link(
                    conn,
                    funding_flow_id=flow_id,
                    source_document_id=source_id,
                    relation_type="oecd_dac2a_api",
                )
                counts["source_links"] += 1

    return counts


def main() -> int:
    load_dotenv()
    args = parse_args()

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("POSTGRES_DSN is required.", file=sys.stderr)
        return 1

    norad_key = os.getenv("NORAD_X_FUNCTIONS_KEY", NORAD_FUNCTION_KEY_DEFAULT).strip()
    if not norad_key:
        print("NORAD_X_FUNCTIONS_KEY is required for Norad enrichment.", file=sys.stderr)
        return 1

    end_year = args.end_year
    if end_year is None:
        try:
            end_year = fetch_norad_latest_year(norad_key)
        except Exception:
            end_year = datetime.utcnow().year

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.autocommit = False
        organizations = fetch_organizations(conn)

        norad_counts = enrich_with_norad(
            conn,
            organizations,
            key=norad_key,
            start_year=args.start_year,
            end_year=end_year,
            threshold=args.norad_match_threshold,
            dry_run=args.dry_run,
        )

        oecd_counts = enrich_with_oecd(
            conn,
            organizations,
            start_year=args.start_year,
            end_year=end_year,
            threshold=args.oecd_match_threshold,
            dry_run=args.dry_run,
        )

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(
        "Norad:",
        f"matches={norad_counts['matches']}",
        f"funding_rows={norad_counts['funding_rows']}",
        f"source_links={norad_counts['source_links']}",
    )
    print(
        "OECD:",
        f"matches={oecd_counts['matches']}",
        f"funding_rows={oecd_counts['funding_rows']}",
        f"source_links={oecd_counts['source_links']}",
    )
    print("dry_run=" + ("true" if args.dry_run else "false"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
