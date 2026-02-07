from __future__ import annotations

import os
import re
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="NONGO Graph View", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def get_dsn() -> str:
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    return dsn


def short_label(text: str, limit: int = 28) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def external_recipient_key(name: str) -> str:
    key = re.sub(r"\s+", " ", name.strip().lower())
    key = re.sub(r"[^a-z0-9æøå ]", "", key)
    return key or "unknown"


def slug_key(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


PERSON_DRILLDOWN_PROFILES: dict[str, dict[str, Any]] = {
    "torbjorn-jagland": {
        "display_name": "Torbjørn Jagland",
        "aliases": [
            "Torbjørn Jagland",
            "Thorbjørn Jagland",
            "Torbjorn Jagland",
            "Thorbjorn Jagland",
        ],
        "curated_bindings": [
            {
                "institution_name": "Den norske Nobelkomité",
                "institution_type": "committee",
                "role_title": "Leder",
                "relation_type": "appointment",
                "start_year": 2009,
                "end_year": 2015,
                "outside_dataset": True,
                "notes": "Kuratert binding lagt inn for person-drilldown.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Torbjørn Jagland",
                        "url": "https://no.wikipedia.org/wiki/Torbj%C3%B8rn_Jagland",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Stortinget",
                "institution_type": "parliament",
                "role_title": "Stortingsrepresentant / Stortingspresident",
                "relation_type": "office",
                "start_year": 1993,
                "end_year": 2009,
                "outside_dataset": True,
                "notes": "Kuratert binding lagt inn for person-drilldown.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Torbjørn Jagland",
                        "url": "https://no.wikipedia.org/wiki/Torbj%C3%B8rn_Jagland",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Det norske Arbeiderparti",
                "institution_type": "political_party",
                "role_title": "Partileder",
                "relation_type": "office",
                "start_year": 1992,
                "end_year": 2002,
                "outside_dataset": True,
                "notes": "Kuratert binding lagt inn for person-drilldown.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Torbjørn Jagland",
                        "url": "https://no.wikipedia.org/wiki/Torbj%C3%B8rn_Jagland",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Statsministerens kontor",
                "institution_type": "government",
                "role_title": "Statsminister",
                "relation_type": "office",
                "start_year": 1996,
                "end_year": 1997,
                "outside_dataset": True,
                "notes": "Kuratert binding lagt inn for person-drilldown.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Torbjørn Jagland",
                        "url": "https://no.wikipedia.org/wiki/Torbj%C3%B8rn_Jagland",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Utenriksdepartementet",
                "institution_type": "government",
                "role_title": "Utenriksminister",
                "relation_type": "office",
                "start_year": 2000,
                "end_year": 2001,
                "outside_dataset": True,
                "notes": "Kuratert binding lagt inn for person-drilldown.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Torbjørn Jagland",
                        "url": "https://no.wikipedia.org/wiki/Torbj%C3%B8rn_Jagland",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
        ],
        "person_links": [],
    },
    "terje-rod-larsen": {
        "display_name": "Terje Rød-Larsen",
        "aliases": [
            "Terje Rød-Larsen",
            "Terje Rod-Larsen",
            "Terje Rød Larsen",
            "Terje Rod Larsen",
        ],
        "group": "diplomacy-core",
        "curated_bindings": [
            {
                "institution_name": "International Peace Institute",
                "institution_type": "think_tank",
                "role_title": "President",
                "relation_type": "office",
                "start_year": 2005,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Freds- og diplomatibinding.",
                "sources": [
                    {
                        "source_name": "International Peace Institute",
                        "url": "https://www.ipinst.org/",
                        "doc_type": "organization_profile",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "FAFO",
                "institution_type": "research_foundation",
                "role_title": "Tidligere leder / grunnlegger",
                "relation_type": "office",
                "start_year": None,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Institusjonell kobling gjennom norsk forskningsmiljø.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Terje Rød-Larsen",
                        "url": "https://no.wikipedia.org/wiki/Terje_R%C3%B8d-Larsen",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "FN",
                "institution_type": "international_organization",
                "role_title": "Tidligere spesialkoordinator",
                "relation_type": "appointment",
                "start_year": None,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Kobling til FN-systemet gjennom diplomatisk oppdrag.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Terje Rød-Larsen",
                        "url": "https://no.wikipedia.org/wiki/Terje_R%C3%B8d-Larsen",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
        ],
        "person_links": [
            {
                "target_key": "mona-juul",
                "relation_type": "family",
                "label": "Ektefeller og diplomatisk nettverk",
                "start_year": None,
                "end_year": None,
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Terje Rød-Larsen",
                        "url": "https://no.wikipedia.org/wiki/Terje_R%C3%B8d-Larsen",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            }
        ],
    },
    "mona-juul": {
        "display_name": "Mona Juul",
        "aliases": [
            "Mona Juul",
        ],
        "group": "diplomacy-core",
        "curated_bindings": [
            {
                "institution_name": "FN",
                "institution_type": "international_organization",
                "role_title": "Tidligere FN-ambassadør for Norge",
                "relation_type": "appointment",
                "start_year": 2019,
                "end_year": 2023,
                "outside_dataset": True,
                "notes": "Norsk representasjon mot FN-systemet.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Mona Juul",
                        "url": "https://no.wikipedia.org/wiki/Mona_Juul",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Utenriksdepartementet",
                "institution_type": "government",
                "role_title": "Diplomat",
                "relation_type": "office",
                "start_year": None,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Bred tilknytning til norsk utenrikstjeneste.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Mona Juul",
                        "url": "https://no.wikipedia.org/wiki/Mona_Juul",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Norsk utenrikstjeneste i Storbritannia",
                "institution_type": "foreign_service",
                "role_title": "Tidligere ambassadør",
                "relation_type": "office",
                "start_year": None,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Diplomatisk nøkkelpost i London.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Mona Juul",
                        "url": "https://no.wikipedia.org/wiki/Mona_Juul",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
        ],
        "person_links": [
            {
                "target_key": "ine-eriksen-soreide",
                "relation_type": "diplomatic_network",
                "label": "Tilknytning via UD/FN-spor",
                "start_year": None,
                "end_year": None,
                "sources": [],
            },
            {
                "target_key": "borge-brende",
                "relation_type": "diplomatic_network",
                "label": "Tilknytning via UD og internasjonale arenaer",
                "start_year": None,
                "end_year": None,
                "sources": [],
            },
        ],
    },
    "borge-brende": {
        "display_name": "Børge Brende",
        "aliases": [
            "Børge Brende",
            "Borge Brende",
        ],
        "group": "diplomacy-core",
        "curated_bindings": [
            {
                "institution_name": "World Economic Forum",
                "institution_type": "international_foundation",
                "role_title": "President",
                "relation_type": "office",
                "start_year": 2017,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Sentral kobling mot global policy- og næringslivsarena.",
                "sources": [
                    {
                        "source_name": "World Economic Forum",
                        "url": "https://www.weforum.org/",
                        "doc_type": "organization_profile",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Utenriksdepartementet",
                "institution_type": "government",
                "role_title": "Utenriksminister",
                "relation_type": "office",
                "start_year": 2013,
                "end_year": 2017,
                "outside_dataset": True,
                "notes": "Direkte kobling mot norsk utenrikspolitisk apparat.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Børge Brende",
                        "url": "https://no.wikipedia.org/wiki/B%C3%B8rge_Brende",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Høyre",
                "institution_type": "political_party",
                "role_title": "Politisk verv",
                "relation_type": "office",
                "start_year": None,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Partibinding relevant for norsk policy-nettverk.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Børge Brende",
                        "url": "https://no.wikipedia.org/wiki/B%C3%B8rge_Brende",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
        ],
        "person_links": [
            {
                "target_key": "ine-eriksen-soreide",
                "relation_type": "government_network",
                "label": "Regjerings- og UD-kobling",
                "start_year": None,
                "end_year": None,
                "sources": [],
            }
        ],
    },
    "ine-eriksen-soreide": {
        "display_name": "Ine Eriksen Søreide",
        "aliases": [
            "Ine Eriksen Søreide",
            "Ine Eriksen Soreide",
            "Ine Søreide",
            "Ine Soreide",
        ],
        "group": "diplomacy-core",
        "curated_bindings": [
            {
                "institution_name": "Utenriksdepartementet",
                "institution_type": "government",
                "role_title": "Utenriksminister",
                "relation_type": "office",
                "start_year": 2017,
                "end_year": 2021,
                "outside_dataset": True,
                "notes": "Nøkkelrolle i norsk utenrikspolitikk.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Ine Eriksen Søreide",
                        "url": "https://no.wikipedia.org/wiki/Ine_Eriksen_S%C3%B8reide",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Forsvarsdepartementet",
                "institution_type": "government",
                "role_title": "Forsvarsminister",
                "relation_type": "office",
                "start_year": 2013,
                "end_year": 2017,
                "outside_dataset": True,
                "notes": "Kobling mot norsk sikkerhetspolitisk nettverk.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Ine Eriksen Søreide",
                        "url": "https://no.wikipedia.org/wiki/Ine_Eriksen_S%C3%B8reide",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Stortinget",
                "institution_type": "parliament",
                "role_title": "Stortingsrepresentant",
                "relation_type": "office",
                "start_year": None,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Parlamentarisk forankring i nettverket.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Ine Eriksen Søreide",
                        "url": "https://no.wikipedia.org/wiki/Ine_Eriksen_S%C3%B8reide",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
            {
                "institution_name": "Høyre",
                "institution_type": "political_party",
                "role_title": "Politisk verv",
                "relation_type": "office",
                "start_year": None,
                "end_year": None,
                "outside_dataset": True,
                "notes": "Partibinding relevant for policy-sfæren.",
                "sources": [
                    {
                        "source_name": "Wikipedia (no): Ine Eriksen Søreide",
                        "url": "https://no.wikipedia.org/wiki/Ine_Eriksen_S%C3%B8reide",
                        "doc_type": "biography",
                        "relation_type": "curated_reference",
                    }
                ],
            },
        ],
        "person_links": [],
    },
}

PERSON_DRILLDOWN_GROUPS: dict[str, list[str]] = {
    "diplomacy-core": [
        "terje-rod-larsen",
        "mona-juul",
        "borge-brende",
        "ine-eriksen-soreide",
    ],
}
DEFAULT_PERSON_DRILLDOWN_KEY = "torbjorn-jagland"


def resolve_person_profile(person_key: str | None) -> tuple[str, dict[str, Any]]:
    candidate = slug_key(person_key or DEFAULT_PERSON_DRILLDOWN_KEY) or DEFAULT_PERSON_DRILLDOWN_KEY
    if candidate in PERSON_DRILLDOWN_PROFILES:
        return candidate, PERSON_DRILLDOWN_PROFILES[candidate]

    for key, profile in PERSON_DRILLDOWN_PROFILES.items():
        aliases = [
            key,
            profile.get("display_name", ""),
            *profile.get("aliases", []),
        ]
        normalized = {slug_key(a) for a in aliases if a}
        if candidate in normalized:
            return key, profile

    available = ", ".join(sorted(PERSON_DRILLDOWN_PROFILES))
    raise HTTPException(status_code=404, detail=f"Unknown person key. Available: {available}")


def format_amount(amount: float | None, currency: str | None) -> str:
    if amount is None:
        return "?"
    cur = (currency or "").upper()
    if cur == "USD":
        if abs(amount) >= 1_000_000:
            return f"${amount / 1_000_000:.1f}M"
        return f"${amount:,.0f}"

    if abs(amount) >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.2f} mrd"
    if abs(amount) >= 1_000_000:
        return f"{amount / 1_000_000:.1f} mill"
    return f"{amount:,.0f}"


def in_year_window(
    *,
    year: int | None,
    year_from: int | None,
    year_to: int | None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> bool:
    if year is not None:
        if year_from is not None and year < year_from:
            return False
        if year_to is not None and year > year_to:
            return False
        return True

    effective_start = start_year if start_year is not None else 0
    effective_end = end_year if end_year is not None else 9999
    if year_from is not None and effective_end < year_from:
        return False
    if year_to is not None and effective_start > year_to:
        return False
    return True


def matches_query(texts: list[str | None], q: str | None) -> bool:
    if not q:
        return True
    qn = q.lower().strip()
    if not qn:
        return True
    for text in texts:
        if text and qn in text.lower():
            return True
    return False


def role_year_bounds(row: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    start_year = row["start_on"].year if row["start_on"] else None
    end_year = row["end_on"].year if row["end_on"] else None
    announced_year = row["announced_on"].year if row["announced_on"] else None
    anchor_year = start_year or announced_year
    return start_year, end_year, anchor_year


def funding_year_bounds(row: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    period_start_year = row["period_start"].year if row["period_start"] else None
    period_end_year = row["period_end"].year if row["period_end"] else None
    fiscal_year = row["fiscal_year"]
    return period_start_year, period_end_year, fiscal_year


def funding_amount_fields(row: dict[str, Any]) -> tuple[float | None, str, float | None, float | None]:
    amount_nok = float(row["amount_nok"]) if row["amount_nok"] is not None else None
    amount_original = float(row["amount_original"]) if row["amount_original"] is not None else None
    currency = (row["currency_code"] or "").upper()

    amount_for_label = amount_nok
    label_currency = "NOK"
    if amount_for_label is None and amount_original is not None:
        amount_for_label = amount_original
        label_currency = currency or "USD"

    return amount_for_label, label_currency, amount_nok, amount_original


def fetch_role_rows(conn: psycopg.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          r.id,
          r.role_title,
          r.role_level,
          r.norwegian_position_before,
          r.announced_on,
          r.start_on,
          r.end_on,
          p.id AS person_id,
          p.canonical_name AS person_name,
          o.id AS org_id,
          o.canonical_name AS org_name
        FROM role_event r
        JOIN person p ON p.id = r.person_id
        JOIN organization o ON o.id = r.organization_id
        ORDER BY r.id
        """
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_person_role_rows(conn: psycopg.Connection, aliases: list[str]) -> list[dict[str, Any]]:
    normalized_aliases = sorted({a.strip().lower() for a in aliases if a and a.strip()})
    if not normalized_aliases:
        return []

    rows = conn.execute(
        """
        SELECT
          r.id,
          r.role_title,
          r.role_level,
          r.norwegian_position_before,
          r.announced_on,
          r.start_on,
          r.end_on,
          p.id AS person_id,
          p.canonical_name AS person_name,
          o.id AS org_id,
          o.canonical_name AS org_name
        FROM role_event r
        JOIN person p ON p.id = r.person_id
        JOIN organization o ON o.id = r.organization_id
        WHERE lower(p.canonical_name) = ANY(%s)
        ORDER BY COALESCE(r.start_on, r.announced_on, r.end_on) DESC NULLS LAST, r.id DESC
        """,
        (normalized_aliases,),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_person_row_by_aliases(
    conn: psycopg.Connection,
    aliases: list[str],
) -> dict[str, Any] | None:
    normalized_aliases = sorted({a.strip().lower() for a in aliases if a and a.strip()})
    if not normalized_aliases:
        return None

    row = conn.execute(
        """
        SELECT id, canonical_name
        FROM person
        WHERE lower(canonical_name) = ANY(%s)
        ORDER BY id
        LIMIT 1
        """,
        (normalized_aliases,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


def fetch_role_sources(
    conn: psycopg.Connection,
    role_event_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    ids = sorted({int(i) for i in role_event_ids})
    if not ids:
        return {}

    rows = conn.execute(
        """
        SELECT
          rsd.role_event_id,
          s.source_name,
          s.url,
          s.doc_type,
          rsd.relation_type
        FROM role_event_source_document rsd
        JOIN source_document s ON s.id = rsd.source_document_id
        WHERE rsd.role_event_id = ANY(%s)
        ORDER BY rsd.role_event_id, s.id
        """,
        (ids,),
    ).fetchall()

    out: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        role_id = int(row["role_event_id"])
        out[role_id].append(
            {
                "source_name": row["source_name"],
                "url": row["url"],
                "doc_type": row["doc_type"],
                "relation_type": row["relation_type"],
            }
        )
    return out


def fetch_person_link_rows(
    conn: psycopg.Connection,
    person_ids: list[int],
) -> list[dict[str, Any]]:
    ids = sorted({int(i) for i in person_ids})
    if not ids:
        return []
    rows = conn.execute(
        """
        SELECT
          id,
          person_a_id,
          person_b_id,
          relation_type,
          relation_label,
          start_on,
          end_on,
          confidence,
          notes
        FROM person_link
        WHERE person_a_id = ANY(%s) OR person_b_id = ANY(%s)
        ORDER BY id
        """,
        (ids, ids),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_person_link_sources(
    conn: psycopg.Connection,
    person_link_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    ids = sorted({int(i) for i in person_link_ids})
    if not ids:
        return {}
    rows = conn.execute(
        """
        SELECT
          pls.person_link_id,
          s.source_name,
          s.url,
          s.doc_type,
          pls.relation_type
        FROM person_link_source_document pls
        JOIN source_document s ON s.id = pls.source_document_id
        WHERE pls.person_link_id = ANY(%s)
        ORDER BY pls.person_link_id, s.id
        """,
        (ids,),
    ).fetchall()

    out: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        link_id = int(row["person_link_id"])
        out[link_id].append(
            {
                "source_name": row["source_name"],
                "url": row["url"],
                "doc_type": row["doc_type"],
                "relation_type": row["relation_type"],
            }
        )
    return out


def fetch_funding_rows(conn: psycopg.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          f.id,
          f.funding_channel,
          f.amount_nok,
          f.amount_original,
          f.currency_code,
          f.fiscal_year,
          f.period_start,
          f.period_end,
          f.notes,
          f.recipient_name_raw,
          o.id AS org_id,
          o.canonical_name AS org_name
        FROM funding_flow f
        LEFT JOIN organization o ON o.id = f.recipient_organization_id
        ORDER BY f.id
        """
    ).fetchall()
    return [dict(r) for r in rows]


def filter_role_rows(
    rows: list[dict[str, Any]],
    *,
    q: str | None,
    year_from: int | None,
    year_to: int | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        start_year, end_year, anchor_year = role_year_bounds(row)
        if not in_year_window(
            year=None,
            year_from=year_from,
            year_to=year_to,
            start_year=anchor_year,
            end_year=end_year,
        ):
            continue

        if not matches_query(
            [row["person_name"], row["org_name"], row["role_title"], row["role_level"]],
            q,
        ):
            continue

        row_copy = dict(row)
        row_copy["anchor_year"] = anchor_year
        out.append(row_copy)
    return out


def filter_funding_rows(
    rows: list[dict[str, Any]],
    *,
    q: str | None,
    year_from: int | None,
    year_to: int | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        period_start_year, period_end_year, fiscal_year = funding_year_bounds(row)
        if not in_year_window(
            year=fiscal_year,
            year_from=year_from,
            year_to=year_to,
            start_year=period_start_year,
            end_year=period_end_year,
        ):
            continue

        if not matches_query(
            [row["org_name"], row["recipient_name_raw"], row["funding_channel"]],
            q,
        ):
            continue

        out.append(dict(row))
    return out


def fetch_ud_palestina_flow_rows(conn: psycopg.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          f.id,
          f.funding_channel,
          f.amount_nok,
          f.amount_original,
          f.currency_code,
          f.fiscal_year,
          f.period_start,
          f.period_end,
          f.notes,
          f.donor_organization_id,
          donor.canonical_name AS donor_org_name,
          f.recipient_organization_id,
          recipient.canonical_name AS recipient_org_name,
          f.recipient_name_raw,
          st.transaction_date,
          st.activity_title,
          st.provider_org_name,
          st.receiver_org_name
        FROM funding_flow f
        JOIN funding_flow_ingest_key fik
          ON fik.funding_flow_id = f.id
         AND fik.source_system = 'iati_registry'
        JOIN stg_iati_transaction st
          ON st.event_key = fik.event_key
        LEFT JOIN organization donor ON donor.id = f.donor_organization_id
        LEFT JOIN organization recipient ON recipient.id = f.recipient_organization_id
        WHERE (
          st.recipient_country_code = 'PS'
          OR lower(coalesce(st.activity_title, '')) LIKE '%palestin%'
          OR lower(coalesce(st.receiver_org_name, '')) LIKE '%palestin%'
          OR lower(coalesce(st.provider_org_name, '')) LIKE '%palestin%'
        )
          AND (
            donor.canonical_name = 'Utenriksdepartementet'
            OR st.provider_org_name = 'Norwegian Ministry of Foreign Affairs'
            OR st.provider_org_name = 'Norwegian Ministry of Foreign Affairs - Embassies'
          )
        ORDER BY COALESCE(st.transaction_date, f.period_start, f.period_end) DESC NULLS LAST, f.id DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/graph")
def graph(
    q: str | None = Query(default=None),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
    include_roles: bool = Query(default=True),
    include_funding: bool = Query(default=True),
    max_funding_edges: int = Query(default=5000, ge=1, le=100000),
) -> JSONResponse:
    dsn = get_dsn()
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []

    def add_node(node_id: str, label: str, node_type: str, subtitle: str | None = None) -> None:
        if node_id in nodes:
            return
        nodes[node_id] = {
            "id": node_id,
            "label": label,
            "type": node_type,
            "subtitle": subtitle,
        }

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        role_rows = filter_role_rows(
            fetch_role_rows(conn),
            q=q,
            year_from=year_from,
            year_to=year_to,
        )
        funding_rows = filter_funding_rows(
            fetch_funding_rows(conn),
            q=q,
            year_from=year_from,
            year_to=year_to,
        )

    if include_roles:
        for row in role_rows:
            person_id = f"person:{row['person_id']}"
            org_id = f"org:{row['org_id']}"
            add_node(person_id, row["person_name"], "person")
            add_node(org_id, row["org_name"], "organization")

            edges.append(
                {
                    "id": f"role:{row['id']}",
                    "from": person_id,
                    "to": org_id,
                    "type": "role",
                    "label": short_label(row["role_title"]),
                    "title": row["role_title"],
                    "year": row["anchor_year"],
                }
            )

    if include_funding:
        donor_id = "country:NO"
        add_node(donor_id, "Norge", "country", "Donor")

        funding_edges_added = 0
        funding_edges_total = 0
        for row in funding_rows:
            funding_edges_total += 1
            if funding_edges_added >= max_funding_edges:
                continue

            recipient_name = row["org_name"] or row["recipient_name_raw"] or "Ukjent mottaker"
            if row["org_id"] is not None:
                recipient_id = f"org:{row['org_id']}"
                recipient_type = "organization"
            else:
                recipient_id = f"external:{external_recipient_key(recipient_name)}"
                recipient_type = "external_recipient"
            add_node(recipient_id, recipient_name, recipient_type)

            amount_for_label, label_currency, _, _ = funding_amount_fields(row)
            edges.append(
                {
                    "id": f"funding:{row['id']}",
                    "from": donor_id,
                    "to": recipient_id,
                    "type": "funding",
                    "label": format_amount(amount_for_label, label_currency),
                    "title": row["funding_channel"] or "Funding",
                    "year": row["fiscal_year"],
                }
            )
            funding_edges_added += 1

    connected = {e["from"] for e in edges} | {e["to"] for e in edges}
    filtered_nodes = [n for nid, n in nodes.items() if nid in connected]

    stats = {
        "nodes": len(filtered_nodes),
        "edges": len(edges),
        "role_edges": sum(1 for e in edges if e["type"] == "role"),
        "funding_edges": sum(1 for e in edges if e["type"] == "funding"),
        "funding_edges_total_matched": funding_edges_total if include_funding else 0,
        "funding_edges_truncated": (
            include_funding and funding_edges_total > max_funding_edges
        ),
    }

    return JSONResponse({"nodes": filtered_nodes, "edges": edges, "stats": stats})


@app.get("/api/timeline")
def timeline(
    q: str | None = Query(default=None),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
) -> JSONResponse:
    dsn = get_dsn()
    roles_by_year = defaultdict(int)
    funding_flows_by_year = defaultdict(int)
    funding_nok_by_year = defaultdict(float)
    funding_usd_by_year = defaultdict(float)

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        role_rows = filter_role_rows(
            fetch_role_rows(conn),
            q=q,
            year_from=year_from,
            year_to=year_to,
        )
        funding_rows = filter_funding_rows(
            fetch_funding_rows(conn),
            q=q,
            year_from=year_from,
            year_to=year_to,
        )

    for row in role_rows:
        year = row["anchor_year"]
        if year is None:
            continue
        roles_by_year[year] += 1

    for row in funding_rows:
        year = row["fiscal_year"]
        if year is None:
            continue
        funding_flows_by_year[year] += 1

        _, _, amount_nok, amount_original = funding_amount_fields(row)
        if amount_nok is not None:
            funding_nok_by_year[year] += amount_nok
        elif amount_original is not None and (row["currency_code"] or "").upper() == "USD":
            funding_usd_by_year[year] += amount_original

    all_years = sorted(
        set(roles_by_year)
        | set(funding_flows_by_year)
        | set(funding_nok_by_year)
        | set(funding_usd_by_year)
    )

    if not all_years and year_from is not None and year_to is not None and year_to >= year_from:
        all_years = list(range(year_from, year_to + 1))

    payload = {
        "years": all_years,
        "role_starts": [roles_by_year[y] for y in all_years],
        "funding_flows": [funding_flows_by_year[y] for y in all_years],
        "funding_nok": [round(funding_nok_by_year[y], 2) for y in all_years],
        "funding_usd": [round(funding_usd_by_year[y], 2) for y in all_years],
    }
    return JSONResponse(payload)


@app.get("/api/toplists")
def toplists(
    q: str | None = Query(default=None),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
) -> JSONResponse:
    dsn = get_dsn()

    org_funding: dict[str, dict[str, Any]] = {}
    org_roles: dict[int, dict[str, Any]] = {}
    person_roles: dict[int, dict[str, Any]] = {}

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        role_rows = filter_role_rows(
            fetch_role_rows(conn),
            q=q,
            year_from=year_from,
            year_to=year_to,
        )
        funding_rows = filter_funding_rows(
            fetch_funding_rows(conn),
            q=q,
            year_from=year_from,
            year_to=year_to,
        )

    for row in funding_rows:
        recipient_name = row["org_name"] or row["recipient_name_raw"] or "Ukjent mottaker"
        if row["org_id"] is not None:
            org_id = f"org:{row['org_id']}"
        else:
            org_id = f"external:{external_recipient_key(recipient_name)}"
        bucket = org_funding.setdefault(
            org_id,
            {
                "org_name": recipient_name,
                "nok_total": 0.0,
                "usd_total": 0.0,
                "flow_count": 0,
            },
        )
        bucket["flow_count"] += 1
        _, _, amount_nok, amount_original = funding_amount_fields(row)
        if amount_nok is not None:
            bucket["nok_total"] += amount_nok
        elif amount_original is not None and (row["currency_code"] or "").upper() == "USD":
            bucket["usd_total"] += amount_original

    for row in role_rows:
        org_id = int(row["org_id"])
        org_bucket = org_roles.setdefault(
            org_id,
            {
                "org_name": row["org_name"],
                "role_count": 0,
                "people": set(),
            },
        )
        org_bucket["role_count"] += 1
        org_bucket["people"].add(int(row["person_id"]))

        person_id = int(row["person_id"])
        person_bucket = person_roles.setdefault(
            person_id,
            {
                "person_name": row["person_name"],
                "role_count": 0,
                "orgs": set(),
            },
        )
        person_bucket["role_count"] += 1
        person_bucket["orgs"].add(org_id)

    org_funding_top = sorted(
        org_funding.values(),
        key=lambda x: (x["nok_total"], x["flow_count"], x["usd_total"]),
        reverse=True,
    )[:12]

    org_role_top = sorted(
        (
            {
                "org_name": v["org_name"],
                "role_count": v["role_count"],
                "person_count": len(v["people"]),
            }
            for v in org_roles.values()
        ),
        key=lambda x: (x["role_count"], x["person_count"]),
        reverse=True,
    )[:12]

    person_role_top = sorted(
        (
            {
                "person_name": v["person_name"],
                "role_count": v["role_count"],
                "org_count": len(v["orgs"]),
            }
            for v in person_roles.values()
        ),
        key=lambda x: (x["role_count"], x["org_count"]),
        reverse=True,
    )[:12]

    return JSONResponse(
        {
            "org_funding_top": org_funding_top,
            "org_role_top": org_role_top,
            "person_role_top": person_role_top,
        }
    )


@app.get("/api/coboard")
def coboard(
    q: str | None = Query(default=None),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
) -> JSONResponse:
    dsn = get_dsn()

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        role_rows = filter_role_rows(
            fetch_role_rows(conn),
            q=q,
            year_from=year_from,
            year_to=year_to,
        )

    person_to_orgs: dict[int, dict[str, Any]] = {}
    for row in role_rows:
        person_id = int(row["person_id"])
        bucket = person_to_orgs.setdefault(
            person_id,
            {
                "person_name": row["person_name"],
                "orgs": {},
            },
        )
        bucket["orgs"][int(row["org_id"])] = row["org_name"]

    pair_to_meta: dict[tuple[int, int], dict[str, Any]] = {}
    org_degree = defaultdict(int)
    org_names: dict[int, str] = {}

    for person_bucket in person_to_orgs.values():
        org_items = sorted(person_bucket["orgs"].items())
        for org_id, org_name in org_items:
            org_names[org_id] = org_name

        for (o1, n1), (o2, n2) in combinations(org_items, 2):
            key = (o1, o2)
            meta = pair_to_meta.setdefault(
                key,
                {"count": 0, "person_names": []},
            )
            meta["count"] += 1
            if len(meta["person_names"]) < 8:
                meta["person_names"].append(person_bucket["person_name"])

            org_degree[o1] += 1
            org_degree[o2] += 1
            org_names[o1] = n1
            org_names[o2] = n2

    nodes = [
        {
            "id": f"org:{pid}",
            "label": name,
            "type": "organization",
            "degree": org_degree[pid],
        }
        for pid, name in org_names.items()
        if org_degree[pid] > 0
    ]

    edges = []
    for (o1, o2), meta in pair_to_meta.items():
        edges.append(
            {
                "id": f"coboard:{o1}:{o2}",
                "from": f"org:{o1}",
                "to": f"org:{o2}",
                "type": "coboard",
                "shared_count": meta["count"],
                "person_names": sorted(set(meta["person_names"])),
                "label": str(meta["count"]),
            }
        )

    edges.sort(key=lambda e: e["shared_count"], reverse=True)

    return JSONResponse(
        {
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "nodes": len(nodes),
                "edges": len(edges),
                "max_shared": max((e["shared_count"] for e in edges), default=0),
            },
        }
    )


@app.get("/api/ud-palestina")
def ud_palestina(
    q: str | None = Query(default=None),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
    max_funding_edges: int = Query(default=1200, ge=50, le=10000),
) -> JSONResponse:
    dsn = get_dsn()

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        funding_rows = fetch_ud_palestina_flow_rows(conn)
        role_rows = fetch_role_rows(conn)
        ud_row = conn.execute(
            """
            SELECT id, canonical_name
            FROM organization
            WHERE canonical_name = 'Utenriksdepartementet'
            LIMIT 1
            """
        ).fetchone()

    ud_org_id = int(ud_row["id"]) if ud_row else None
    ud_name = ud_row["canonical_name"] if ud_row else "Utenriksdepartementet"
    ud_node_id = f"org:{ud_org_id}" if ud_org_id is not None else "ud:source"

    filtered_funding: list[dict[str, Any]] = []
    for row in funding_rows:
        tx_year = row["transaction_date"].year if row.get("transaction_date") else None
        fiscal_year = row.get("fiscal_year") or tx_year
        period_start_year = row["period_start"].year if row.get("period_start") else None
        period_end_year = row["period_end"].year if row.get("period_end") else None

        if not in_year_window(
            year=fiscal_year,
            year_from=year_from,
            year_to=year_to,
            start_year=period_start_year,
            end_year=period_end_year,
        ):
            continue

        recipient_name = (
            row.get("recipient_org_name")
            or row.get("recipient_name_raw")
            or row.get("receiver_org_name")
            or "Ukjent mottaker"
        )
        if not matches_query(
            [
                ud_name,
                recipient_name,
                row.get("funding_channel"),
                row.get("activity_title"),
                row.get("provider_org_name"),
                row.get("receiver_org_name"),
            ],
            q,
        ):
            continue

        row_copy = dict(row)
        row_copy["recipient_name"] = recipient_name
        row_copy["event_year"] = fiscal_year
        filtered_funding.append(row_copy)

    nodes: dict[str, dict[str, Any]] = {
        ud_node_id: {
            "id": ud_node_id,
            "label": ud_name,
            "type": "ud_source",
            "subtitle": "Donor",
        }
    }
    edges: list[dict[str, Any]] = []

    funding_edges_total = len(filtered_funding)
    funding_edges_added = 0
    recipient_stats: dict[str, dict[str, Any]] = {}
    recipient_org_ids: set[int] = set()
    total_nok = 0.0

    for row in filtered_funding:
        recipient_org_id = row.get("recipient_organization_id")
        recipient_name = row["recipient_name"]
        if recipient_org_id is not None:
            recipient_node_id = f"org:{recipient_org_id}"
            recipient_type = "organization"
            recipient_org_ids.add(int(recipient_org_id))
        else:
            recipient_node_id = f"udpal-recipient:{external_recipient_key(recipient_name)}"
            recipient_type = "external_recipient"

        if recipient_node_id not in nodes:
            nodes[recipient_node_id] = {
                "id": recipient_node_id,
                "label": recipient_name,
                "type": recipient_type,
                "subtitle": "Mottaker",
            }

        amount_for_label, label_currency, amount_nok, amount_original = funding_amount_fields(row)
        if amount_nok is not None:
            total_nok += amount_nok

        recipient_bucket = recipient_stats.setdefault(
            recipient_node_id,
            {
                "recipient_name": recipient_name,
                "flow_count": 0,
                "nok_total": 0.0,
                "usd_total": 0.0,
            },
        )
        recipient_bucket["flow_count"] += 1
        if amount_nok is not None:
            recipient_bucket["nok_total"] += amount_nok
        elif amount_original is not None and (row.get("currency_code") or "").upper() == "USD":
            recipient_bucket["usd_total"] += amount_original

        if funding_edges_added >= max_funding_edges:
            continue

        edges.append(
            {
                "id": f"funding:{row['id']}",
                "from": ud_node_id,
                "to": recipient_node_id,
                "type": "funding",
                "label": format_amount(amount_for_label, label_currency),
                "title": row.get("activity_title") or row.get("funding_channel") or "Funding",
                "year": row.get("event_year"),
                "metadata": {
                    "funding_channel": row.get("funding_channel"),
                    "transaction_date": (
                        str(row["transaction_date"]) if row.get("transaction_date") else None
                    ),
                    "provider_org_name": row.get("provider_org_name"),
                    "receiver_org_name": row.get("receiver_org_name"),
                },
            }
        )
        funding_edges_added += 1

    role_org_ids = set(recipient_org_ids)
    if ud_org_id is not None:
        role_org_ids.add(ud_org_id)

    people_in_scope: set[int] = set()
    for row in role_rows:
        org_id = int(row["org_id"])
        if org_id not in role_org_ids:
            continue

        start_year, end_year, anchor_year = role_year_bounds(row)
        if not in_year_window(
            year=None,
            year_from=year_from,
            year_to=year_to,
            start_year=anchor_year,
            end_year=end_year,
        ):
            continue

        person_node_id = f"person:{row['person_id']}"
        org_node_id = f"org:{row['org_id']}"
        people_in_scope.add(int(row["person_id"]))

        if person_node_id not in nodes:
            nodes[person_node_id] = {
                "id": person_node_id,
                "label": row["person_name"],
                "type": "person",
                "subtitle": "Rollekobling",
            }

        if org_node_id not in nodes:
            nodes[org_node_id] = {
                "id": org_node_id,
                "label": row["org_name"],
                "type": "organization",
                "subtitle": "Mottaker/aktør",
            }

        edges.append(
            {
                "id": f"role:{row['id']}",
                "from": person_node_id,
                "to": org_node_id,
                "type": "role",
                "label": short_label(row["role_title"]),
                "title": row["role_title"],
                "year": anchor_year,
            }
        )

    top_recipients = sorted(
        recipient_stats.values(),
        key=lambda x: (x["nok_total"], x["flow_count"], x["usd_total"]),
        reverse=True,
    )
    for bucket in top_recipients:
        if bucket["nok_total"] > 0:
            bucket["amount_label"] = format_amount(bucket["nok_total"], "NOK")
        elif bucket["usd_total"] > 0:
            bucket["amount_label"] = format_amount(bucket["usd_total"], "USD")
        else:
            bucket["amount_label"] = "?"

    latest_transactions = []
    for row in filtered_funding[:20]:
        amount_for_label, label_currency, _, _ = funding_amount_fields(row)
        latest_transactions.append(
            {
                "funding_id": int(row["id"]),
                "transaction_date": str(row["transaction_date"]) if row.get("transaction_date") else None,
                "fiscal_year": row.get("event_year"),
                "recipient_name": row.get("recipient_name"),
                "amount_label": format_amount(amount_for_label, label_currency),
                "activity_title": row.get("activity_title"),
            }
        )

    connected_node_ids = {edge["from"] for edge in edges} | {edge["to"] for edge in edges}
    filtered_nodes = [node for node_id, node in nodes.items() if node_id in connected_node_ids]

    first_tx = (
        min((row["transaction_date"] for row in filtered_funding if row.get("transaction_date")), default=None)
    )
    last_tx = (
        max((row["transaction_date"] for row in filtered_funding if row.get("transaction_date")), default=None)
    )

    stats = {
        "nodes": len(filtered_nodes),
        "edges": len(edges),
        "funding_edges": sum(1 for edge in edges if edge["type"] == "funding"),
        "role_edges": sum(1 for edge in edges if edge["type"] == "role"),
        "funding_edges_total_matched": funding_edges_total,
        "funding_edges_truncated": funding_edges_total > max_funding_edges,
        "recipients": len(recipient_stats),
        "people": len(people_in_scope),
        "first_tx": str(first_tx) if first_tx else None,
        "last_tx": str(last_tx) if last_tx else None,
        "amount_nok_total": round(total_nok, 2),
        "amount_nok_label": format_amount(total_nok, "NOK"),
    }

    return JSONResponse(
        {
            "nodes": filtered_nodes,
            "edges": edges,
            "top_recipients": top_recipients[:15],
            "latest_transactions": latest_transactions,
            "stats": stats,
        }
    )


@app.get("/api/person-drilldown")
def person_drilldown(
    person_key: str | None = Query(default=DEFAULT_PERSON_DRILLDOWN_KEY),
    q: str | None = Query(default=None),
    year_from: int | None = Query(default=None),
    year_to: int | None = Query(default=None),
) -> JSONResponse:
    selected_key, selected_profile = resolve_person_profile(person_key)
    group_key = selected_profile.get("group")
    group_members = PERSON_DRILLDOWN_GROUPS.get(group_key or "", [])

    profile_keys = [selected_key]
    for key in group_members:
        if key in PERSON_DRILLDOWN_PROFILES and key not in profile_keys:
            profile_keys.append(key)

    profiles = {key: PERSON_DRILLDOWN_PROFILES[key] for key in profile_keys}

    dsn = get_dsn()
    profile_role_rows: dict[str, list[dict[str, Any]]] = {}
    profile_person_rows: dict[str, dict[str, Any] | None] = {}
    role_sources: dict[int, list[dict[str, Any]]] = {}
    person_link_rows: list[dict[str, Any]] = []
    person_link_sources: dict[int, list[dict[str, Any]]] = {}

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        role_ids: list[int] = []
        for key in profile_keys:
            profile = profiles[key]
            aliases = [
                profile.get("display_name", ""),
                *profile.get("aliases", []),
            ]
            rows = filter_role_rows(
                fetch_person_role_rows(conn, aliases),
                q=q,
                year_from=year_from,
                year_to=year_to,
            )
            profile_role_rows[key] = rows
            role_ids.extend(int(row["id"]) for row in rows)
            if rows:
                profile_person_rows[key] = {
                    "id": int(rows[0]["person_id"]),
                    "canonical_name": rows[0]["person_name"],
                }
            else:
                profile_person_rows[key] = fetch_person_row_by_aliases(conn, aliases)
        role_sources = fetch_role_sources(conn, role_ids)
        person_ids_for_links = [
            int(row["id"]) for row in profile_person_rows.values() if row and row.get("id") is not None
        ]
        person_link_rows = fetch_person_link_rows(conn, person_ids_for_links)
        person_link_sources = fetch_person_link_sources(
            conn, [int(row["id"]) for row in person_link_rows]
        )

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    bindings: list[dict[str, Any]] = []
    org_name_to_node_id: dict[str, str] = {}
    org_node_outside: dict[str, bool] = {}
    org_node_names: dict[str, str] = {}

    person_node_ids: dict[str, str] = {}
    person_names: dict[str, str] = {}
    person_db_ids: dict[str, int] = {}
    person_to_orgs: dict[str, set[str]] = {key: set() for key in profile_keys}
    dataset_binding_signatures: set[tuple[str, str, str, int | None, int | None]] = set()

    def add_node(node_id: str, label: str, node_type: str, subtitle: str | None = None) -> None:
        if node_id in nodes:
            existing = nodes[node_id]
            if existing["type"] != "person_focus" and node_type == "person_focus":
                existing["type"] = node_type
                existing["subtitle"] = subtitle
            return
        nodes[node_id] = {
            "id": node_id,
            "label": label,
            "type": node_type,
            "subtitle": subtitle,
        }

    for key in profile_keys:
        profile = profiles[key]
        rows = profile_role_rows.get(key, [])
        person_row = profile_person_rows.get(key)
        person_name = profile.get("display_name", key)
        person_node_id = f"profile-person:{key}"
        if person_row:
            person_name = person_row.get("canonical_name") or person_name
            person_node_id = f"person:{person_row['id']}"
            person_db_ids[key] = int(person_row["id"])
        elif rows:
            person_name = rows[0]["person_name"] or person_name
            person_node_id = f"person:{rows[0]['person_id']}"
            person_db_ids[key] = int(rows[0]["person_id"])

        node_type = "person_focus" if key == selected_key else "person_peer"
        subtitle = "Fokusperson" if key == selected_key else "I nøkkelnettverket"
        add_node(person_node_id, person_name, node_type, subtitle)
        person_node_ids[key] = person_node_id
        person_names[key] = person_name

    for key in profile_keys:
        rows = profile_role_rows.get(key, [])
        person_node_id = person_node_ids[key]
        person_name = person_names[key]

        for row in rows:
            org_id = f"org:{row['org_id']}"
            org_name = row["org_name"]
            org_key = external_recipient_key(org_name)

            add_node(org_id, org_name, "organization", "Fra datagrunnlag")
            org_name_to_node_id[org_key] = org_id
            org_node_names[org_id] = org_name
            org_node_outside[org_id] = False

            start_year = row["start_on"].year if row["start_on"] else None
            end_year = row["end_on"].year if row["end_on"] else None
            role_title = row["role_title"] or "Rolle"
            edge_id = f"person-role:{key}:{row['id']}"
            edge_sources = role_sources.get(int(row["id"]), [])

            edges.append(
                {
                    "id": edge_id,
                    "from": person_node_id,
                    "to": org_id,
                    "type": "person_role",
                    "source_kind": "dataset",
                    "outside_dataset": False,
                    "label": short_label(role_title, limit=32),
                    "title": role_title,
                    "year": row.get("anchor_year"),
                    "metadata": {
                        "person_name": person_name,
                        "role_title": role_title,
                        "role_level": row.get("role_level"),
                        "start_year": start_year,
                        "end_year": end_year,
                        "source_kind": "dataset",
                        "outside_dataset": False,
                    },
                    "sources": edge_sources,
                }
            )
            bindings.append(
                {
                    "id": edge_id,
                    "person_key": key,
                    "person_name": person_name,
                    "institution_node_id": org_id,
                    "institution_name": org_name,
                    "role_title": role_title,
                    "relation_type": "role_event",
                    "start_year": start_year,
                    "end_year": end_year,
                    "source_kind": "dataset",
                    "outside_dataset": False,
                    "notes": row.get("norwegian_position_before"),
                    "sources": edge_sources,
                }
            )
            person_to_orgs[key].add(org_id)
            dataset_binding_signatures.add(
                (
                    key,
                    org_key,
                    role_title.strip().lower(),
                    start_year,
                    end_year,
                )
            )

    for key in profile_keys:
        profile = profiles[key]
        person_node_id = person_node_ids[key]
        person_name = person_names[key]

        for idx, item in enumerate(profile.get("curated_bindings", [])):
            start_year = item.get("start_year")
            end_year = item.get("end_year")
            if not in_year_window(
                year=None,
                year_from=year_from,
                year_to=year_to,
                start_year=start_year,
                end_year=end_year,
            ):
                continue

            if not matches_query(
                [
                    person_name,
                    item.get("institution_name"),
                    item.get("role_title"),
                    item.get("relation_type"),
                    item.get("notes"),
                ],
                q,
            ):
                continue

            institution_name = item.get("institution_name") or "Ukjent institusjon"
            institution_key = external_recipient_key(institution_name)
            outside_dataset = bool(item.get("outside_dataset", True))
            institution_node_id = org_name_to_node_id.get(institution_key)

            if not institution_node_id:
                if outside_dataset:
                    institution_node_id = f"external-institution:{institution_key}"
                    add_node(
                        institution_node_id,
                        institution_name,
                        "external_institution",
                        "Utenfor datagrunnlag",
                    )
                else:
                    institution_node_id = f"curated-organization:{institution_key}"
                    add_node(
                        institution_node_id,
                        institution_name,
                        "organization",
                        "Kuratert binding",
                    )
                org_name_to_node_id[institution_key] = institution_node_id

            org_node_names[institution_node_id] = institution_name
            org_node_outside[institution_node_id] = (
                org_node_outside.get(institution_node_id, True) and outside_dataset
            )

            role_title = item.get("role_title") or "Binding"
            signature = (
                key,
                institution_key,
                role_title.strip().lower(),
                start_year,
                end_year,
            )
            if signature in dataset_binding_signatures:
                continue
            edge_id = f"curated-binding:{key}:{institution_key}:{idx}"
            sources = [
                {
                    "source_name": s.get("source_name"),
                    "url": s.get("url"),
                    "doc_type": s.get("doc_type"),
                    "relation_type": s.get("relation_type"),
                }
                for s in item.get("sources", [])
            ]

            edges.append(
                {
                    "id": edge_id,
                    "from": person_node_id,
                    "to": institution_node_id,
                    "type": "curated_binding",
                    "source_kind": "curated",
                    "outside_dataset": outside_dataset,
                    "label": short_label(role_title, limit=32),
                    "title": role_title,
                    "year": start_year,
                    "metadata": {
                        "person_name": person_name,
                        "role_title": role_title,
                        "relation_type": item.get("relation_type"),
                        "institution_type": item.get("institution_type"),
                        "start_year": start_year,
                        "end_year": end_year,
                        "source_kind": "curated",
                        "outside_dataset": outside_dataset,
                        "notes": item.get("notes"),
                    },
                    "sources": sources,
                }
            )
            bindings.append(
                {
                    "id": edge_id,
                    "person_key": key,
                    "person_name": person_name,
                    "institution_node_id": institution_node_id,
                    "institution_name": institution_name,
                    "role_title": role_title,
                    "relation_type": item.get("relation_type"),
                    "start_year": start_year,
                    "end_year": end_year,
                    "source_kind": "curated",
                    "outside_dataset": outside_dataset,
                    "notes": item.get("notes"),
                    "sources": sources,
                }
            )
            person_to_orgs[key].add(institution_node_id)

    seen_person_links: set[tuple[str, str, str, int | None, int | None]] = set()
    person_id_to_key = {pid: key for key, pid in person_db_ids.items()}

    for link_row in person_link_rows:
        a_id = int(link_row["person_a_id"])
        b_id = int(link_row["person_b_id"])
        if a_id not in person_id_to_key or b_id not in person_id_to_key:
            continue

        k1 = person_id_to_key[a_id]
        k2 = person_id_to_key[b_id]
        pair = tuple(sorted([k1, k2]))
        relation_type = link_row.get("relation_type") or "person_link"
        label = link_row.get("relation_label") or relation_type
        start_year = link_row["start_on"].year if link_row.get("start_on") else None
        end_year = link_row["end_on"].year if link_row.get("end_on") else None

        if not in_year_window(
            year=None,
            year_from=year_from,
            year_to=year_to,
            start_year=start_year,
            end_year=end_year,
        ):
            continue

        if not matches_query(
            [
                person_names.get(k1),
                person_names.get(k2),
                relation_type,
                label,
                link_row.get("notes"),
            ],
            q,
        ):
            continue

        dedupe_key = (pair[0], pair[1], relation_type, start_year, end_year)
        if dedupe_key in seen_person_links:
            continue
        seen_person_links.add(dedupe_key)

        row_sources = person_link_sources.get(int(link_row["id"]), [])
        edges.append(
            {
                "id": f"person-link-db:{link_row['id']}",
                "from": person_node_ids[pair[0]],
                "to": person_node_ids[pair[1]],
                "type": "person_link",
                "source_kind": "dataset",
                "outside_dataset": False,
                "label": short_label(label, limit=28),
                "title": label,
                "year": start_year,
                "metadata": {
                    "relation_type": relation_type,
                    "start_year": start_year,
                    "end_year": end_year,
                    "confidence": (
                        float(link_row["confidence"])
                        if link_row.get("confidence") is not None
                        else None
                    ),
                    "notes": link_row.get("notes"),
                    "source_kind": "dataset",
                    "outside_dataset": False,
                },
                "sources": row_sources,
            }
        )

    for key in profile_keys:
        profile = profiles[key]
        for link in profile.get("person_links", []):
            target_key = link.get("target_key")
            if not target_key or target_key not in person_node_ids:
                continue

            start_year = link.get("start_year")
            end_year = link.get("end_year")
            if not in_year_window(
                year=None,
                year_from=year_from,
                year_to=year_to,
                start_year=start_year,
                end_year=end_year,
            ):
                continue

            relation_type = link.get("relation_type") or "person_link"
            label = link.get("label") or relation_type
            if not matches_query(
                [person_names[key], person_names[target_key], relation_type, label],
                q,
            ):
                continue

            pair = tuple(sorted([key, target_key]))
            dedupe_key = (pair[0], pair[1], relation_type, start_year, end_year)
            if dedupe_key in seen_person_links:
                continue
            seen_person_links.add(dedupe_key)

            edge_id = f"person-link:{pair[0]}:{pair[1]}:{slug_key(relation_type)}"
            sources = [
                {
                    "source_name": s.get("source_name"),
                    "url": s.get("url"),
                    "doc_type": s.get("doc_type"),
                    "relation_type": s.get("relation_type"),
                }
                for s in link.get("sources", [])
            ]

            edges.append(
                {
                    "id": edge_id,
                    "from": person_node_ids[pair[0]],
                    "to": person_node_ids[pair[1]],
                    "type": "person_link",
                    "source_kind": "curated",
                    "outside_dataset": False,
                    "label": short_label(label, limit=28),
                    "title": label,
                    "year": start_year,
                    "metadata": {
                        "relation_type": relation_type,
                        "start_year": start_year,
                        "end_year": end_year,
                        "source_kind": "curated",
                        "outside_dataset": False,
                    },
                    "sources": sources,
                }
            )

    for k1, k2 in combinations(sorted(profile_keys), 2):
        shared_orgs = sorted(person_to_orgs[k1] & person_to_orgs[k2])
        if not shared_orgs:
            continue

        institution_names = [org_node_names.get(node_id, node_id) for node_id in shared_orgs]
        if not matches_query([person_names[k1], person_names[k2], *institution_names], q):
            continue

        edges.append(
            {
                "id": f"shared-org:{k1}:{k2}",
                "from": person_node_ids[k1],
                "to": person_node_ids[k2],
                "type": "shared_institution",
                "source_kind": "derived",
                "outside_dataset": False,
                "label": str(len(shared_orgs)),
                "title": "Delte institusjoner",
                "year": None,
                "metadata": {
                    "shared_count": len(shared_orgs),
                    "shared_institutions": ", ".join(institution_names),
                    "source_kind": "derived",
                    "outside_dataset": False,
                },
                "sources": [],
            }
        )

    connected_node_ids = set(person_node_ids.values())
    connected_node_ids.update(e["from"] for e in edges)
    connected_node_ids.update(e["to"] for e in edges)
    filtered_nodes = [node for node_id, node in nodes.items() if node_id in connected_node_ids]

    bindings.sort(
        key=lambda item: (
            item["person_key"] != selected_key,
            item["start_year"] is None,
            -(item["start_year"] or 0),
            item["person_name"],
            item["institution_name"],
        )
    )

    outside_dataset_nodes = {
        node_id
        for node_id in connected_node_ids
        if node_id in org_node_outside and org_node_outside[node_id]
    }

    return JSONResponse(
        {
            "person": {
                "key": selected_key,
                "display_name": person_names.get(selected_key, selected_profile.get("display_name", selected_key)),
            },
            "network_scope": {
                "group": group_key,
                "people": [
                    {
                        "key": key,
                        "display_name": person_names.get(key, profiles[key].get("display_name", key)),
                    }
                    for key in profile_keys
                ],
            },
            "available_profiles": [
                {
                    "key": key,
                    "display_name": data.get("display_name", key),
                }
                for key, data in sorted(
                    PERSON_DRILLDOWN_PROFILES.items(),
                    key=lambda pair: pair[1].get("display_name", pair[0]),
                )
            ],
            "nodes": filtered_nodes,
            "edges": edges,
            "bindings": bindings,
            "stats": {
                "nodes": len(filtered_nodes),
                "edges": len(edges),
                "people": len(person_node_ids),
                "dataset_edges": sum(1 for e in edges if e["source_kind"] == "dataset"),
                "curated_edges": sum(1 for e in edges if e["source_kind"] == "curated"),
                "shared_edges": sum(1 for e in edges if e["type"] == "shared_institution"),
                "outside_dataset_institutions": len(outside_dataset_nodes),
            },
        }
    )


@app.get("/api/edge/{edge_id}")
def edge_details(edge_id: str) -> JSONResponse:
    if ":" not in edge_id:
        raise HTTPException(status_code=400, detail="Invalid edge id")

    kind, _, raw_id = edge_id.partition(":")
    if not raw_id.isdigit():
        raise HTTPException(status_code=400, detail="Invalid edge id")

    row_id = int(raw_id)
    dsn = get_dsn()

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        if kind == "role":
            row = conn.execute(
                """
                SELECT
                  r.id,
                  r.role_title,
                  r.role_level,
                  r.norwegian_position_before,
                  r.announced_on,
                  r.start_on,
                  r.end_on,
                  p.canonical_name AS person_name,
                  o.canonical_name AS org_name
                FROM role_event r
                JOIN person p ON p.id = r.person_id
                JOIN organization o ON o.id = r.organization_id
                WHERE r.id = %s
                """,
                (row_id,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Role edge not found")

            sources = conn.execute(
                """
                SELECT
                  s.source_name,
                  s.url,
                  s.doc_type,
                  rsd.relation_type
                FROM role_event_source_document rsd
                JOIN source_document s ON s.id = rsd.source_document_id
                WHERE rsd.role_event_id = %s
                ORDER BY s.id
                """,
                (row_id,),
            ).fetchall()

            payload = {
                "edge_id": edge_id,
                "kind": "role",
                "title": row["role_title"],
                "summary": f"{row['person_name']} -> {row['org_name']}",
                "metadata": {
                    "role_level": row["role_level"],
                    "announced_on": str(row["announced_on"]) if row["announced_on"] else None,
                    "start_on": str(row["start_on"]) if row["start_on"] else None,
                    "end_on": str(row["end_on"]) if row["end_on"] else None,
                    "norwegian_position_before": row["norwegian_position_before"],
                },
                "sources": [
                    {
                        "source_name": s["source_name"],
                        "url": s["url"],
                        "doc_type": s["doc_type"],
                        "relation_type": s["relation_type"],
                    }
                    for s in sources
                ],
            }
            return JSONResponse(payload)

        if kind == "funding":
            row = conn.execute(
                """
                SELECT
                  f.id,
                  f.funding_channel,
                  f.amount_nok,
                  f.amount_original,
                  f.currency_code,
                  f.fiscal_year,
                  f.period_start,
                  f.period_end,
                  f.notes,
                  f.recipient_name_raw,
                  o.canonical_name AS org_name
                FROM funding_flow f
                LEFT JOIN organization o ON o.id = f.recipient_organization_id
                WHERE f.id = %s
                """,
                (row_id,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Funding edge not found")

            sources = conn.execute(
                """
                SELECT
                  s.source_name,
                  s.url,
                  s.doc_type,
                  fsd.relation_type
                FROM funding_flow_source_document fsd
                JOIN source_document s ON s.id = fsd.source_document_id
                WHERE fsd.funding_flow_id = %s
                ORDER BY s.id
                """,
                (row_id,),
            ).fetchall()

            amount = row["amount_nok"]
            currency = "NOK"
            if amount is None and row["amount_original"] is not None:
                amount = row["amount_original"]
                currency = row["currency_code"] or "USD"

            payload = {
                "edge_id": edge_id,
                "kind": "funding",
                "title": row["funding_channel"] or "Funding",
                "summary": f"Norge -> {row['org_name'] or row['recipient_name_raw']}",
                "metadata": {
                    "amount": format_amount(amount, currency),
                    "currency": currency,
                    "fiscal_year": row["fiscal_year"],
                    "period_start": str(row["period_start"]) if row["period_start"] else None,
                    "period_end": str(row["period_end"]) if row["period_end"] else None,
                    "notes": row["notes"],
                },
                "sources": [
                    {
                        "source_name": s["source_name"],
                        "url": s["url"],
                        "doc_type": s["doc_type"],
                        "relation_type": s["relation_type"],
                    }
                    for s in sources
                ],
            }
            return JSONResponse(payload)

    raise HTTPException(status_code=404, detail="Edge type not supported")
