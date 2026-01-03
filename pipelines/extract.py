"""
extract.py

Responsible for extracting raw vehicle quality data from public APIs
(Socrata / NHTSA ODI datasets).

This module:
- fetches raw data
- handles pagination and retries
- returns raw records (no transformation)

It does NOT:
- clean data
- deduplicate
- write to the database
"""

from __future__ import annotations
import os

import time
import logging
from typing import List, Dict, Optional

import requests

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

SOCRATA_BASE_URL = "https://data.transportation.gov/resource"

# Socrata resource ids for tabular datasets
# Recalls Data: https://data.transportation.gov/Automobiles/Recalls-Data/6axg-epim
RECALLS_DATASET_ID = "6axg-epim"
NHTSA_API_BASE_URL = "https://api.nhtsa.gov"

# Candidate date fields (datasets can evolve)
RECALLS_DATE_FIELDS = [
    "report_received_date",
    "report_receive_date",
    "recall_date",
    "date",
]

PAGE_SIZE = 1000
REQUEST_TIMEOUT = 30
RATE_LIMIT_SLEEP = 0.2  # seconds between requests

# Optional: Socrata app token. Not always required, but some networks/WAFs
# return 403 without a token or a non-empty User-Agent.
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
USER_AGENT = os.getenv("RECALLWATCH_USER_AGENT", "recallwatch/0.1 (+https://github.com/aaryabookseller16/Recall-Watch)")

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------

def _fetch_socrata(
    dataset_id: str,
    where_clause: Optional[str] = None,
    max_pages: Optional[int] = None,
) -> List[Dict]:
    """
    Generic Socrata fetcher with pagination.

    Parameters
    ----------
    dataset_id : str
        Socrata dataset identifier.
    where_clause : str, optional
        SoQL WHERE clause (without 'WHERE').

    Returns
    -------
    List[Dict]
        Raw records returned by the API.
    """
    records: List[Dict] = []
    offset = 0

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }

        if where_clause:
            params["$where"] = where_clause

        url = f"{SOCRATA_BASE_URL}/{dataset_id}.json"

        logger.info("Fetching %s | offset=%s", dataset_id, offset)
        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        if SOCRATA_APP_TOKEN:
            headers["X-App-Token"] = SOCRATA_APP_TOKEN

        response = requests.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT,
            headers=headers,
        )
        if response.status_code >= 400:
            # Socrata sometimes returns 403 from certain networks unless a token
            # and a non-empty User-Agent are provided.
            msg = (
                f"HTTP {response.status_code} fetching {url} with params={params}. "
                f"Response: {response.text[:500]}"
            )
            raise requests.HTTPError(msg, response=response)

        page = response.json()

        if not page:
            logger.info("No more records. Stopping pagination.")
            break

        records.extend(page)
        offset += PAGE_SIZE

        if max_pages is not None and (offset // PAGE_SIZE) >= max_pages:
            logger.info("Reached max_pages=%s; stopping early.", max_pages)
            break

        time.sleep(RATE_LIMIT_SLEEP)

    logger.info("Fetched %s total records from %s", len(records), dataset_id)
    return records

# ---------------------------------------------------------------------
# Helper functions for field detection and where clause building
# ---------------------------------------------------------------------

def _first_present_field(record: Dict, candidates: list[str]) -> Optional[str]:
    for f in candidates:
        if f in record:
            return f
    return None


def _build_where_clause(
    make_field: str,
    make: str,
    start_date: Optional[str],
    end_date: Optional[str],
    date_field: Optional[str],
) -> str:
    # Many manufacturers include suffixes like "TESLA, INC."; use prefix match.
    filters = [f"upper({make_field}) LIKE '{make.upper()}%'"]
    if date_field and start_date:
        filters.append(f"{date_field} >= '{start_date}'")
    if date_field and end_date:
        filters.append(f"{date_field} <= '{end_date}'")
    return " AND ".join(filters)

# ---------------------------------------------------------------------
# Public extractors
# ---------------------------------------------------------------------

def extract_recalls(
    make: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict]:
    # Probe one row to discover actual field names without downloading everything.
    probe = _fetch_socrata(
        dataset_id=RECALLS_DATASET_ID,
        where_clause=None,
        max_pages=1,
    )

    make_field = "make"
    date_field = None
    if probe:
        # Recalls Data uses `manufacturer` (not `make`) on this dataset.
        if "manufacturer" in probe[0] and "make" not in probe[0]:
            make_field = "manufacturer"
        date_field = _first_present_field(probe[0], RECALLS_DATE_FIELDS)
        logger.info("Recalls fields detected: make_field=%s date_field=%s", make_field, date_field)

    where_clause = _build_where_clause(make_field, make, start_date, end_date, date_field)

    return _fetch_socrata(dataset_id=RECALLS_DATASET_ID, where_clause=where_clause)



# --- NHTSA complaints API extractor ---

def extract_complaints_by_vehicle(
    make: str,
    model: str,
    model_year: int,
) -> List[Dict]:
    """Extract complaints from NHTSA's official API for a specific make/model/year.

    NHTSA endpoint (documented):
      /complaints/complaintsByVehicle?make={MAKE}&model={MODEL}&modelYear={YEAR}

    Returns a list of complaint records (dicts). This endpoint does not expose
    a general date-range filter; filtering by date is handled downstream.
    """
    url = f"{NHTSA_API_BASE_URL}/complaints/complaintsByVehicle"
    params = {"make": make, "model": model, "modelYear": str(model_year)}
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}

    logger.info("Fetching NHTSA complaints | make=%s model=%s year=%s", make, model, model_year)
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT, headers=headers)
    if resp.status_code >= 400:
        msg = f"HTTP {resp.status_code} fetching {url} params={params}. Response: {resp.text[:500]}"
        raise requests.HTTPError(msg, response=resp)

    data = resp.json()
    # NHTSA responses commonly include a "results" array.
    results = data.get("results") or data.get("Results") or []
    if not isinstance(results, list):
        results = []
    logger.info("Fetched %s NHTSA complaint records", len(results))
    return results


def extract_complaints(
    make: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict]:
    """Convenience wrapper to keep Day-1 ergonomics.

    Because the DOT Socrata "Complaints Flat File" view is non-tabular (403 for
    row/column access), we use NHTSA's complaintsByVehicle endpoint.

    Provide the vehicle via env vars:
      RECALLWATCH_MODEL=...
      RECALLWATCH_MODEL_YEAR=...

    Date filtering (start_date/end_date) is accepted for API symmetry but is
    applied downstream (dbt staging) because the endpoint doesn't support it.
    """
    model = os.getenv("RECALLWATCH_MODEL")
    year = os.getenv("RECALLWATCH_MODEL_YEAR")
    if not model or not year:
        raise ValueError(
            "extract_complaints requires RECALLWATCH_MODEL and RECALLWATCH_MODEL_YEAR env vars. "
            "Example: RECALLWATCH_MODEL=MODEL3 RECALLWATCH_MODEL_YEAR=2024"
        )

    return extract_complaints_by_vehicle(make=make, model=model, model_year=int(year))


if __name__ == "__main__":
    make = os.getenv("RECALLWATCH_MAKE", "TESLA")
    start = os.getenv("RECALLWATCH_START", "2024-01-01")
    end = os.getenv("RECALLWATCH_END", "2024-12-31")

    recalls = extract_recalls(make=make, start_date=start, end_date=end)
    print(f"Fetched {len(recalls)} recall records for {make}")
    if recalls:
        print("recalls[0] keys:", sorted(recalls[0].keys()))

    # Complaints: requires RECALLWATCH_MODEL and RECALLWATCH_MODEL_YEAR env vars.
    try:
        complaints = extract_complaints(make=make, start_date=start, end_date=end)
        print(f"Fetched {len(complaints)} complaint records for {make}")
        if complaints:
            print("complaints[0] keys:", sorted(complaints[0].keys()))
    except Exception as e:
        print("Complaints extraction skipped:", e)