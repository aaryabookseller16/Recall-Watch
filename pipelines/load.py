

"""load.py

Responsible for loading raw extracted records into Postgres (raw layer).

This module:
- computes deterministic primary keys for idempotency
- upserts into raw.recalls and raw.complaints
- stores the full raw payload as JSONB for auditability

It does NOT:
- clean/normalize fields beyond minimal parsing
- build analytics models (dbt does that)
"""

from __future__ import annotations

import os
import json
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# -----------------------------
# Config
# -----------------------------

DEFAULT_DATABASE_URL = "postgresql://recallwatch:recallwatch@localhost:5432/recallwatch"


def _db_url() -> str:
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _parse_date(value: Any) -> Optional[date]:
    """Best-effort parse for API date fields.

    Socrata 'Floating Timestamp' often arrives like '2024-01-02T00:00:00.000'.
    NHTSA often uses 'YYYY-MM-DD'.
    """
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    s = str(value).strip()
    if not s:
        return None

    # Try ISO date
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


# -----------------------------
# Primary key generation
# -----------------------------

def recall_pk_from_record(r: Dict[str, Any]) -> str:
    """Deterministic pk for recalls.

    Prefer NHTSA campaign id when present.
    Fallback to hash of stable-ish fields.
    """
    nhtsa_id = (r.get("nhtsa_id") or r.get("NHTSA_ID") or "").strip()
    if nhtsa_id:
        return f"nhtsa:{nhtsa_id}"

    # Fallback
    manufacturer = (r.get("manufacturer") or r.get("make") or "").strip().upper()
    component = (r.get("component") or "").strip().upper()
    report_date = str(r.get("report_received_date") or r.get("report_date") or "").strip()
    subject = (r.get("subject") or "").strip()

    base = f"{manufacturer}|{component}|{report_date}|{subject}"
    return f"hash:{_sha256_hex(base)}"


def complaint_pk_from_record(r: Dict[str, Any]) -> str:
    """Deterministic pk for complaints.

    Prefer ODI number if present (NHTSA commonly returns an ODI number field).
    Fallback to hash of stable fields.
    """
    # Common variants in NHTSA responses
    odi = (
        r.get("ODINumber")
        or r.get("odi_number")
        or r.get("odiNumber")
        or r.get("complaint_number")
        or ""
    )
    odi = str(odi).strip()
    if odi:
        return f"odi:{odi}"

    make = str(r.get("Make") or r.get("make") or "").strip().upper()
    model = str(r.get("Model") or r.get("model") or "").strip().upper()
    year = str(r.get("ModelYear") or r.get("model_year") or r.get("Year") or "").strip()
    comp = str(r.get("Component") or r.get("component") or "").strip().upper()
    received = str(r.get("DateReceived") or r.get("date_received") or "").strip()
    summary = str(r.get("Summary") or r.get("summary") or r.get("Description") or "").strip()

    base = f"{make}|{model}|{year}|{comp}|{received}|{summary}"
    return f"hash:{_sha256_hex(base)}"


# -----------------------------
# Row mappers (raw layer)
# -----------------------------

def map_recall_row(r: Dict[str, Any]) -> Tuple:
    """Map a Socrata recall record to raw.recalls columns."""
    pk = recall_pk_from_record(r)

    manufacturer = r.get("manufacturer") or r.get("make")
    component = r.get("component")
    nhtsa_id = r.get("nhtsa_id")
    mfr_campaign_number = r.get("mfr_campaign_number")

    report_date = _parse_date(r.get("report_received_date") or r.get("report_date"))

    # raw.recalls schema columns
    return (
        pk,
        "socrata",
        str(nhtsa_id) if nhtsa_id is not None else None,
        str(manufacturer) if manufacturer is not None else None,
        None,  # model
        None,  # model_year
        str(component) if component is not None else None,
        report_date,
        None,  # source_updated_at
        datetime.utcnow(),
        json.dumps(r),
    )


def map_complaint_row(r: Dict[str, Any]) -> Tuple:
    """Map an NHTSA complaint record to raw.complaints columns."""
    pk = complaint_pk_from_record(r)

    odi = r.get("ODINumber") or r.get("odi_number")

    make = r.get("Make") or r.get("make")
    model = r.get("Model") or r.get("model")
    year = r.get("ModelYear") or r.get("model_year") or r.get("Year")

    component = r.get("Component") or r.get("component")

    incident_date = _parse_date(r.get("IncidentDate") or r.get("incident_date"))
    received_date = _parse_date(r.get("DateReceived") or r.get("date_received"))
    state = r.get("State") or r.get("state")

    return (
        pk,
        "nhtsa",
        str(odi) if odi is not None else None,
        str(odi) if odi is not None else None,
        str(make) if make is not None else None,
        str(model) if model is not None else None,
        int(year) if str(year).strip().isdigit() else None,
        str(component) if component is not None else None,
        incident_date,
        received_date,
        str(state) if state is not None else None,
        None,  # source_updated_at
        datetime.utcnow(),
        json.dumps(r),
    )


# -----------------------------
# Upsert helpers
# -----------------------------

RAW_RECALLS_COLS = (
    "recall_pk",
    "source",
    "source_id",
    "make",
    "model",
    "model_year",
    "component",
    "report_date",
    "source_updated_at",
    "ingested_at",
    "raw_payload",
)

RAW_COMPLAINTS_COLS = (
    "complaint_pk",
    "source",
    "source_id",
    "odi_number",
    "make",
    "model",
    "model_year",
    "component",
    "incident_date",
    "received_date",
    "state",
    "source_updated_at",
    "ingested_at",
    "raw_payload",
)


def _upsert(
    conn,
    table: str,
    cols: Tuple[str, ...],
    rows: List[Tuple],
    conflict_col: str,
) -> int:
    if not rows:
        logger.info("No rows to upsert into %s", table)
        return 0

    col_list = ",".join(cols)

    # Update all non-PK cols on conflict
    update_cols = [c for c in cols if c != conflict_col]
    set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ({conflict_col}) DO UPDATE
        SET {set_clause};
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)

    conn.commit()
    logger.info("Upserted %s rows into %s", len(rows), table)
    return len(rows)


# -----------------------------
# Public API
# -----------------------------

def load_recalls(records: Iterable[Dict[str, Any]]) -> int:
    """Upsert recall records into raw.recalls."""
    rows = [map_recall_row(r) for r in records]
    with psycopg2.connect(_db_url()) as conn:
        return _upsert(conn, "raw.recalls", RAW_RECALLS_COLS, rows, "recall_pk")


def load_complaints(records: Iterable[Dict[str, Any]]) -> int:
    """Upsert complaint records into raw.complaints."""
    rows = [map_complaint_row(r) for r in records]
    with psycopg2.connect(_db_url()) as conn:
        return _upsert(conn, "raw.complaints", RAW_COMPLAINTS_COLS, rows, "complaint_pk")


if __name__ == "__main__":
    # Lightweight smoke test: load a tiny slice if you provide JSON on stdin.
    # Example:
    #   python pipelines/load.py < some_records.json
    import sys

    payload = sys.stdin.read().strip()
    if not payload:
        print("Provide JSON array on stdin to smoke-test load.py")
        raise SystemExit(0)

    data = json.loads(payload)
    if not isinstance(data, list) or not data:
        raise ValueError("Expected a non-empty JSON array")

    # Heuristic: choose based on presence of 'nhtsa_id' (recalls) vs 'ODINumber' (complaints)
    if "nhtsa_id" in data[0] or "manufacturer" in data[0]:
        n = load_recalls(data)
        print(f"Loaded {n} recall rows")
    else:
        n = load_complaints(data)
        print(f"Loaded {n} complaint rows")